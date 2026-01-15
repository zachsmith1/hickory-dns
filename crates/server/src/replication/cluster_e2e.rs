//! Multi-node “full stack” replication + serving e2e test.
//!
//! This is intentionally feature-gated to keep default builds lightweight.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::proto::op::Message;
use crate::proto::op::Query;
use crate::proto::rr::{LowerName, Name, RData, RecordType};
use crate::replication::authority::RedbReplicatedZoneHandler;
use crate::replication::delta::{RrsetData, ZoneDeltaOp, ZoneDeltaSegment, CURRENT_DELTA_SCHEMA_VERSION};
use crate::replication::publisher::ArtifactPublisher;
use crate::replication::redb_store::{encode_rrset_value, rrset_key, ZoneLayout};
use crate::replication::replicator::ZoneReplicator;
use crate::replication::snapshot::{ZoneSnapshot, CURRENT_SNAPSHOT_FORMAT_VERSION};
use crate::server::Server;
use crate::zone_handler::{AxfrPolicy, Catalog, ZoneType};

struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let mut path = std::env::temp_dir();
        let nonce = format!(
            "{prefix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        path.push(nonce);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct Node {
    name: &'static str,
    layout: ZoneLayout,
    server: Server<Catalog>,
    addr: SocketAddr,
    repl: ZoneReplicator,
}

impl Node {
    async fn new(
        name: &'static str,
        store: Arc<dyn ObjectStore>,
        zone_manifest_path: ObjPath,
        base_dir: &std::path::Path,
        origin: Name,
    ) -> Self {
        let layout = ZoneLayout::new(base_dir.join(name));

        let handler = Arc::new(RedbReplicatedZoneHandler::new(
            origin.clone(),
            ZoneType::Primary,
            AxfrPolicy::Deny,
            layout.clone(),
        ));

        let mut catalog = Catalog::new();
        catalog.upsert(LowerName::from(&origin), vec![handler]);

        let mut server = Server::new(catalog);
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = socket.local_addr().unwrap();
        server.register_socket(socket);

        let repl = ZoneReplicator::new(store, zone_manifest_path, layout.clone());

        Self {
            name,
            layout,
            server,
            addr,
            repl,
        }
    }

    async fn sync_until_generation(&mut self, target: u64, deadline: Duration) {
        timeout(deadline, async {
            loop {
                let _ = self.repl.sync_once().await.unwrap();
                let gen = crate::replication::redb_store::RedbActiveView::open(self.layout.clone())
                    .map(|v| v.generation)
                    .unwrap_or(0);
                if gen >= target {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("node {} did not reach generation {target} in time", self.name));
    }

    async fn shutdown(mut self) {
        let _ = self.server.shutdown_gracefully().await;
    }
}

async fn query_a(addr: SocketAddr, qname: &str) -> [u8; 4] {
    let sock = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();

    let mut msg = Message::query();
    msg.add_query(Query::query(Name::from_ascii(qname).unwrap(), RecordType::A));
    let bytes = msg.to_vec().unwrap();

    sock.send_to(&bytes, addr).await.unwrap();

    let mut buf = vec![0u8; 2048];
    let (n, _) = sock.recv_from(&mut buf).await.unwrap();
    let resp = Message::from_vec(&buf[..n]).unwrap();

    for rec in resp.answers() {
        if rec.record_type() == RecordType::A {
            if let RData::A(a) = rec.data() {
                return a.0.octets();
            }
        }
    }

    panic!("no A record in response: {resp:?}");
}

#[cfg(feature = "replication-cluster-minio-e2e")]
async fn new_minio_store() -> Arc<dyn ObjectStore> {
    use object_store::aws::AmazonS3Builder;

    let endpoint = std::env::var("HICKORY_REPL_S3_ENDPOINT").unwrap();
    let bucket = std::env::var("HICKORY_REPL_S3_BUCKET").unwrap();
    let access_key = std::env::var("HICKORY_REPL_S3_ACCESS_KEY").unwrap();
    let secret_key = std::env::var("HICKORY_REPL_S3_SECRET_KEY").unwrap();

    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap(),
    )
}

fn snapshot_with_a(zone_id: &str, origin: &str, base_generation: u64, owner: &str, ip: [u8; 4]) -> ZoneSnapshot {
    let mut entries = std::collections::BTreeMap::new();
    let key = rrset_key(owner, u16::from(RecordType::A));
    let val = encode_rrset_value(&RrsetData {
        ttl: 60,
        rdata_wire: vec![ip.to_vec()],
    });
    entries.insert(key, val);
    ZoneSnapshot {
        format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
        zone_id: zone_id.into(),
        origin: origin.into(),
        base_generation,
        entries,
    }
}

fn delta_upsert_a(gen: u64, owner: &str, ip: [u8; 4]) -> ZoneDeltaSegment {
    ZoneDeltaSegment {
        schema_version: CURRENT_DELTA_SCHEMA_VERSION,
        from: gen,
        to: gen,
        ops: vec![ZoneDeltaOp::UpsertRrset {
            owner_fqdn: owner.into(),
            rrtype: u16::from(RecordType::A),
            ttl: 30,
            rdata_wire: vec![ip.to_vec()],
        }],
    }
}

#[tokio::test]
async fn multi_node_cluster_converges_across_updates_and_restarts() {
    // Shared object store root (writer fan-out).
    let dir = TempDir::new("hickory-repl-cluster");
    let store_dir = dir.path.join("store");
    std::fs::create_dir_all(&store_dir).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&store_dir).unwrap());

    let publisher = ArtifactPublisher::new(store.clone(), ObjPath::from(""));
    let origin = Name::from_ascii("example.com.").unwrap();
    let owner = "api.example.com.";

    // Bootstrap generation 1.
    let snap = snapshot_with_a("z1", "example.com.", 1, owner, [10, 0, 0, 1]);
    let zone_manifest_path = publisher.publish_bootstrap(&snap).await.unwrap();

    // Two serving nodes (separate redb layouts).
    let nodes_dir = dir.path.join("nodes");
    std::fs::create_dir_all(&nodes_dir).unwrap();
    let mut n1 = Node::new("n1", store.clone(), zone_manifest_path.clone(), &nodes_dir, origin.clone()).await;
    let mut n2 = Node::new("n2", store.clone(), zone_manifest_path.clone(), &nodes_dir, origin.clone()).await;

    n1.sync_until_generation(1, Duration::from_secs(10)).await;
    n2.sync_until_generation(1, Duration::from_secs(10)).await;
    assert_eq!(query_a(n1.addr, owner).await, [10, 0, 0, 1]);
    assert_eq!(query_a(n2.addr, owner).await, [10, 0, 0, 1]);

    // Apply many incremental changes, with periodic “restarts”.
    let total_gens: u64 = 15;
    for gen in 2..=total_gens {
        let ip = [10, 0, 0, gen as u8];
        let seg = delta_upsert_a(gen, owner, ip);
        publisher
            .publish_delta_and_update_manifests("z1", &seg)
            .await
            .unwrap();

        n1.sync_until_generation(gen, Duration::from_secs(10)).await;
        n2.sync_until_generation(gen, Duration::from_secs(10)).await;

        assert_eq!(query_a(n1.addr, owner).await, ip);
        assert_eq!(query_a(n2.addr, owner).await, ip);

        // Restart simulation at a couple of points: drop the server+replicator objects,
        // keep the on-disk redb, and continue.
        if gen == 5 || gen == 10 {
            let addr1 = n1.addr;
            let addr2 = n2.addr;
            n1.shutdown().await;
            n2.shutdown().await;

            // Recreate nodes pointing at the same ZoneLayout directories.
            n1 = Node::new("n1", store.clone(), zone_manifest_path.clone(), &nodes_dir, origin.clone()).await;
            n2 = Node::new("n2", store.clone(), zone_manifest_path.clone(), &nodes_dir, origin.clone()).await;

            // Ensure the restarted servers still answer, then catch up (no-op).
            assert_eq!(query_a(n1.addr, owner).await, ip);
            assert_eq!(query_a(n2.addr, owner).await, ip);

            // Quiet down: the old addrs are unused now.
            let _ = (addr1, addr2);
        }
    }

    n1.shutdown().await;
    n2.shutdown().await;
}

#[cfg(feature = "replication-cluster-minio-e2e")]
#[tokio::test]
#[ignore]
async fn multi_node_cluster_minio_converges_across_updates_and_restarts() {
    // Shared object store root (writer fan-out): MinIO/S3 via object_store::aws.
    use bytes::Bytes;

    let dir = TempDir::new("hickory-repl-cluster-minio");

    let prefix = format!(
        "hickory-repl-cluster-e2e/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let store = new_minio_store().await;

    let publisher = ArtifactPublisher::new(store.clone(), ObjPath::from(prefix.clone()));
    let origin = Name::from_ascii("example.com.").unwrap();
    let owner = "api.example.com.";

    // Bootstrap generation 1.
    let snap = snapshot_with_a("z1", "example.com.", 1, owner, [10, 1, 0, 1]);
    let zone_manifest_path = publisher.publish_bootstrap(&snap).await.unwrap();

    // Two serving nodes (separate redb layouts).
    let nodes_dir = dir.path.join("nodes");
    std::fs::create_dir_all(&nodes_dir).unwrap();
    let mut n1 =
        Node::new("n1", store.clone(), zone_manifest_path.clone(), &nodes_dir, origin.clone()).await;
    let mut n2 =
        Node::new("n2", store.clone(), zone_manifest_path.clone(), &nodes_dir, origin.clone()).await;

    // MinIO can be a bit slower; allow more time for each catch-up.
    let deadline = Duration::from_secs(30);
    n1.sync_until_generation(1, deadline).await;
    n2.sync_until_generation(1, deadline).await;
    assert_eq!(query_a(n1.addr, owner).await, [10, 1, 0, 1]);
    assert_eq!(query_a(n2.addr, owner).await, [10, 1, 0, 1]);

    // Apply changes; restart once mid-stream.
    let total_gens: u64 = 8;
    for gen in 2..=total_gens {
        let ip = [10, 1, 0, gen as u8];
        let seg = delta_upsert_a(gen, owner, ip);
        publisher
            .publish_delta_and_update_manifests("z1", &seg)
            .await
            .unwrap();

        n1.sync_until_generation(gen, deadline).await;
        n2.sync_until_generation(gen, deadline).await;
        assert_eq!(query_a(n1.addr, owner).await, ip);
        assert_eq!(query_a(n2.addr, owner).await, ip);

        if gen == 4 {
            n1.shutdown().await;
            n2.shutdown().await;
            n1 =
                Node::new("n1", store.clone(), zone_manifest_path.clone(), &nodes_dir, origin.clone()).await;
            n2 =
                Node::new("n2", store.clone(), zone_manifest_path.clone(), &nodes_dir, origin.clone()).await;
            assert_eq!(query_a(n1.addr, owner).await, ip);
            assert_eq!(query_a(n2.addr, owner).await, ip);
        }
    }

    // Optional integrity smoke: publish one more delta (advancing to generation 9), then corrupt
    // that delta object. Since the manifest now points at generation 9, the next sync must fetch
    // the corrupted object and fail checksum verification.
    let gen = 9u64;
    let ip = [10, 1, 0, gen as u8];
    let seg = delta_upsert_a(gen, owner, ip);
    publisher
        .publish_delta_and_update_manifests("z1", &seg)
        .await
        .unwrap();

    let delta9_path = ObjPath::from(prefix)
        .child("zones")
        .child("z1")
        .child("deltas")
        .child("delta-9-9.bin");
    store
        .put(&delta9_path, Bytes::from_static(b"corrupt").into())
        .await
        .unwrap();

    let err = n1.repl.sync_once().await.expect_err("expected sha256 mismatch");
    assert!(err.to_string().contains("sha256 mismatch"));

    n1.shutdown().await;
    n2.shutdown().await;
}

