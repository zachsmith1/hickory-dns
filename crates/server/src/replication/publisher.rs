//! Single-writer artifact publisher (prototype).
//!
//! This module produces replication artifacts (snapshots, delta segments, manifests) and uploads
//! them into an [`object_store::ObjectStore`]. It is designed to pair with the pull-based PoP
//! replicator (`replicator.rs`).

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::artifacts::{
    ArtifactRef, Sha256Digest, SnapshotRef, ZoneManifest, CURRENT_MANIFEST_FORMAT_VERSION, DeltaSegmentRef,
};
use super::delta::ZoneDeltaSegment;
use super::snapshot::{SnapshotError, ZoneSnapshot};
use super::update_stream::{UpdateEntry, UpdateSegment, UpdatesLatest, CURRENT_UPDATE_STREAM_FORMAT_VERSION};

/// Publisher errors.
#[derive(Debug, Error)]
pub enum PublisherError {
    /// Object store error.
    #[error(transparent)]
    Store(#[from] object_store::Error),
    /// Snapshot encode error.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// JSON encode/decode error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Manifest validation failed.
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),
}

/// Publishes artifacts into an object store under a root prefix.
pub struct ArtifactPublisher {
    store: Arc<dyn ObjectStore>,
    root: ObjPath,
    updates_segment_size: u64,
}

impl ArtifactPublisher {
    /// Create a new publisher that writes under `root` (relative to the store prefix).
    pub fn new(store: Arc<dyn ObjectStore>, root: ObjPath) -> Self {
        Self {
            store,
            root,
            updates_segment_size: UpdatesLatest::default().segment_size,
        }
    }

    /// Configure update-segment sizing (entries per segment).
    pub fn with_updates_segment_size(mut self, segment_size: u64) -> Self {
        self.updates_segment_size = segment_size.max(1);
        self
    }

    /// Returns the object-store path for a zone manifest.
    pub fn zone_manifest_path(&self, zone_id: &str) -> ObjPath {
        self.root
            .child("zones")
            .child(zone_id)
            .child("zone-manifest.json")
    }

    fn snapshot_path(&self, zone_id: &str, base_generation: u64) -> ObjPath {
        self.root
            .child("zones")
            .child(zone_id)
            .child("snapshots")
            .child(format!("snap-{base_generation}.bin"))
    }

    fn delta_path(&self, zone_id: &str, from: u64, to: u64) -> ObjPath {
        self.root
            .child("zones")
            .child(zone_id)
            .child("deltas")
            .child(format!("delta-{from}-{to}.bin"))
    }

    fn updates_latest_path(&self) -> ObjPath {
        self.root.child("updates").child("LATEST.json")
    }

    fn updates_segment_path(&self, segment_id: u64) -> ObjPath {
        self.root
            .child("updates")
            .child("segments")
            .child(format!("{segment_id:020}.json"))
    }

    async fn load_updates_latest(&self) -> Result<UpdatesLatest, PublisherError> {
        let path = self.updates_latest_path();
        match self.store.get(&path).await {
            Ok(get) => {
                let bytes = get.bytes().await?;
                Ok(serde_json::from_slice::<UpdatesLatest>(&bytes)?)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(UpdatesLatest {
                segment_size: self.updates_segment_size,
                ..UpdatesLatest::default()
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn put_updates_latest(&self, latest: &UpdatesLatest) -> Result<(), PublisherError> {
        let bytes = serde_json::to_vec(latest)?;
        let path = self.updates_latest_path();
        let _ = self.put_bytes(path, &bytes).await?;
        Ok(())
    }

    async fn load_or_init_segment(
        &self,
        segment_id: u64,
        segment_size: u64,
    ) -> Result<UpdateSegment, PublisherError> {
        let base_seq = segment_id.saturating_mul(segment_size).saturating_add(1);
        let path = self.updates_segment_path(segment_id);
        match self.store.get(&path).await {
            Ok(get) => {
                let bytes = get.bytes().await?;
                Ok(serde_json::from_slice::<UpdateSegment>(&bytes)?)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(UpdateSegment {
                format_version: CURRENT_UPDATE_STREAM_FORMAT_VERSION,
                segment_id,
                segment_size,
                base_seq,
                last_seq: base_seq.saturating_sub(1),
                entries: vec![],
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn put_segment(&self, seg: &UpdateSegment) -> Result<(), PublisherError> {
        let bytes = serde_json::to_vec(seg)?;
        let path = self.updates_segment_path(seg.segment_id);
        let _ = self.put_bytes(path, &bytes).await?;
        Ok(())
    }

    /// Append a batch of update entries to the update stream.
    ///
    /// This is the scalable replacement for rewriting a monolithic global manifest.
    pub async fn append_updates(&self, mut entries: Vec<UpdateEntry>) -> Result<(), PublisherError> {
        if entries.is_empty() {
            return Ok(());
        }

        // Load latest pointer to determine current seq and segment sizing.
        let mut latest = self.load_updates_latest().await?;
        latest.segment_size = self.updates_segment_size.max(1);

        // We assign monotonically increasing seq numbers. Sequence 0 is allowed, but we start at 1
        // so that "missing" can be represented as 0 in PoP state if desired.
        let mut next_seq = if latest.last_seq == 0 { 1 } else { latest.last_seq.saturating_add(1) };

        // Deterministic ordering inside a batch: sort by zone_id to keep segments stable.
        entries.sort_by(|a, b| a.zone_id.cmp(&b.zone_id));

        for entry in entries {
            let segment_id = (next_seq.saturating_sub(1)) / latest.segment_size;
            let mut seg = self
                .load_or_init_segment(segment_id, latest.segment_size)
                .await?;

            // Append at the correct position. We allow overwriting within a segment by rewriting the object.
            let expected_base = seg.base_seq;
            if next_seq < expected_base {
                return Err(PublisherError::InvalidManifest(
                    "next_seq behind segment base".into(),
                ));
            }
            let offset = (next_seq - expected_base) as usize;
            if offset != seg.entries.len() {
                // Out-of-order append indicates concurrent writers; disallow in this prototype.
                return Err(PublisherError::InvalidManifest(
                    "non-contiguous update stream append".into(),
                ));
            }
            seg.entries.push(entry);
            seg.last_seq = next_seq;
            seg.validate().map_err(PublisherError::InvalidManifest)?;
            self.put_segment(&seg).await?;

            latest.last_seq = next_seq;
            next_seq = next_seq.saturating_add(1);
        }

        self.put_updates_latest(&latest).await?;
        Ok(())
    }

    /// Publish a snapshot artifact and return a reference to it.
    pub async fn publish_snapshot(&self, snapshot: &ZoneSnapshot) -> Result<SnapshotRef, PublisherError> {
        let bytes = snapshot.encode_to_vec()?;
        let path = self.snapshot_path(&snapshot.zone_id, snapshot.base_generation);
        let artifact = self.put_bytes(path, &bytes).await?;
        Ok(SnapshotRef {
            base_generation: snapshot.base_generation,
            artifact,
        })
    }

    /// Publish a delta segment and return a reference to it.
    pub async fn publish_delta_segment(
        &self,
        zone_id: &str,
        seg: &ZoneDeltaSegment,
    ) -> Result<DeltaSegmentRef, PublisherError> {
        let bytes = seg.encode_protobuf_to_vec();
        let path = self.delta_path(zone_id, seg.from, seg.to);
        let artifact = self.put_bytes(path, &bytes).await?;
        Ok(DeltaSegmentRef {
            from: seg.from,
            to: seg.to,
            artifact,
        })
    }

    /// Publish a zone manifest at the canonical per-zone path.
    pub async fn publish_zone_manifest(&self, manifest: &ZoneManifest) -> Result<ArtifactRef, PublisherError> {
        manifest
            .validate()
            .map_err(|e| PublisherError::InvalidManifest(e.to_string()))?;
        let bytes = serde_json::to_vec(manifest)?;
        let path = self.zone_manifest_path(&manifest.zone_id);
        self.put_bytes(path, &bytes).await
    }

    /// Publish a bootstrap snapshot and its zone manifest, and append an update-stream entry.
    ///
    /// Returns the per-zone manifest path (useful for wiring a PoP replicator).
    pub async fn publish_bootstrap(&self, snapshot: &ZoneSnapshot) -> Result<ObjPath, PublisherError> {
        let snap_ref = self.publish_snapshot(snapshot).await?;

        let zone_manifest = ZoneManifest {
            format_version: CURRENT_MANIFEST_FORMAT_VERSION,
            zone_id: snapshot.zone_id.clone(),
            origin: snapshot.origin.clone(),
            latest_generation: snapshot.base_generation,
            snapshot: snap_ref,
            deltas: vec![],
        };
        let zone_manifest_artifact = self.publish_zone_manifest(&zone_manifest).await?;

        self.append_updates(vec![UpdateEntry {
            zone_id: snapshot.zone_id.clone(),
            origin: snapshot.origin.clone(),
            latest_generation: snapshot.base_generation,
            zone_manifest: zone_manifest_artifact,
        }])
        .await?;

        Ok(self.zone_manifest_path(&snapshot.zone_id))
    }

    /// Publish a single delta segment and update the per-zone manifest + append an update entry.
    pub async fn publish_delta_and_update_manifests(
        &self,
        zone_id: &str,
        seg: &ZoneDeltaSegment,
    ) -> Result<(), PublisherError> {
        let zone_manifest_path = self.zone_manifest_path(zone_id);
        let current = self.store.get(&zone_manifest_path).await?;
        let current_bytes = current.bytes().await?;
        let mut manifest: ZoneManifest = serde_json::from_slice(&current_bytes)?;

        // Upload the new segment first, then atomically advance the manifest to reference it.
        let seg_ref = self.publish_delta_segment(zone_id, seg).await?;
        manifest.latest_generation = seg.to;
        manifest.deltas.push(seg_ref);
        let zone_manifest_artifact = self.publish_zone_manifest(&manifest).await?;

        self.append_updates(vec![UpdateEntry {
            zone_id: zone_id.to_string(),
            origin: manifest.origin.clone(),
            latest_generation: manifest.latest_generation,
            zone_manifest: zone_manifest_artifact,
        }])
        .await?;

        Ok(())
    }

    async fn put_bytes(&self, path: ObjPath, bytes: &[u8]) -> Result<ArtifactRef, PublisherError> {
        let sha = sha256_hex(bytes);
        self.store
            .put(&path, Bytes::copy_from_slice(bytes).into())
            .await?;
        Ok(ArtifactRef {
            uri: path.to_string(),
            sha256: Sha256Digest(sha),
        })
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    data_encoding::HEXLOWER.encode(out.as_slice())
}

#[cfg(all(test, feature = "replication-replicator"))]
mod tests {
    use super::ArtifactPublisher;
    use crate::replication::delta::{RrsetData, ZoneDeltaOp, ZoneDeltaSegment, CURRENT_DELTA_SCHEMA_VERSION};
    use crate::replication::redb_store::{encode_rrset_value, rrset_key, RedbActiveView, ZoneLayout};
    use crate::replication::replicator::ZoneReplicator;
    use crate::replication::snapshot::{ZoneSnapshot, CURRENT_SNAPSHOT_FORMAT_VERSION};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;
    use std::collections::BTreeMap;
    use std::sync::Arc;

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

    #[tokio::test]
    async fn publish_then_replicate_then_serve() {
        let dir = TempDir::new("hickory-repl-publish");
        let store_dir = dir.path.join("store");
        std::fs::create_dir_all(&store_dir).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&store_dir).unwrap());

        let publisher = ArtifactPublisher::new(store.clone(), ObjPath::from(""));

        // Snapshot at generation 1 with one A RRset.
        let mut entries = BTreeMap::new();
        let key = rrset_key("www.example.com.", u16::from(crate::proto::rr::RecordType::A));
        let val = encode_rrset_value(&RrsetData {
            ttl: 60,
            rdata_wire: vec![vec![1, 2, 3, 4]],
        });
        entries.insert(key, val);
        let snapshot = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries,
        };

        let zone_manifest_path = publisher.publish_bootstrap(&snapshot).await.unwrap();

        // PoP replicator catches up.
        let layout = ZoneLayout::new(dir.path.join("zone-local"));
        let mut repl = ZoneReplicator::new(store.clone(), zone_manifest_path.clone(), layout.clone());
        assert!(repl.sync_once().await.unwrap());
        let active = RedbActiveView::open(layout.clone()).unwrap();
        assert_eq!(active.generation, 1);

        // Publish a delta to generation 2.
        let seg = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 2,
            to: 2,
            ops: vec![ZoneDeltaOp::UpsertRrset {
                owner_fqdn: "api.example.com.".into(),
                rrtype: u16::from(crate::proto::rr::RecordType::A),
                ttl: 30,
                rdata_wire: vec![vec![5, 6, 7, 8]],
            }],
        };
        publisher
            .publish_delta_and_update_manifests("z1", &seg)
            .await
            .unwrap();

        assert!(repl.sync_once().await.unwrap());
        let active = RedbActiveView::open(layout.clone()).unwrap();
        assert_eq!(active.generation, 2);
        assert!(active.get_rrset("api.example.com.", u16::from(crate::proto::rr::RecordType::A)).unwrap().is_some());
    }

    // This is intentionally ignored by default. Run with:
    //   cargo test -p hickory-server --features replication-publisher-minio-e2e -- --ignored
    //
    // Env vars:
    // - HICKORY_REPL_S3_ENDPOINT (e.g. http://127.0.0.1:9000)
    // - HICKORY_REPL_S3_BUCKET
    // - HICKORY_REPL_S3_ACCESS_KEY
    // - HICKORY_REPL_S3_SECRET_KEY
    #[cfg(feature = "replication-publisher-minio-e2e")]
    #[tokio::test]
    #[ignore]
    async fn minio_e2e_publish_then_replicate() {
        use object_store::aws::AmazonS3Builder;
        use bytes::Bytes;

        let endpoint = std::env::var("HICKORY_REPL_S3_ENDPOINT").unwrap();
        let bucket = std::env::var("HICKORY_REPL_S3_BUCKET").unwrap();
        let access_key = std::env::var("HICKORY_REPL_S3_ACCESS_KEY").unwrap();
        let secret_key = std::env::var("HICKORY_REPL_S3_SECRET_KEY").unwrap();

        let prefix = format!(
            "hickory-repl-e2e/{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let store: Arc<dyn ObjectStore> = Arc::new(
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
        );

        let publisher = ArtifactPublisher::new(store.clone(), ObjPath::from(prefix.clone()));

        let snapshot = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries: {
                let mut m = BTreeMap::new();
                let key = rrset_key("www.example.com.", u16::from(crate::proto::rr::RecordType::A));
                let val = encode_rrset_value(&RrsetData {
                    ttl: 60,
                    rdata_wire: vec![vec![1, 2, 3, 4]],
                });
                m.insert(key, val);
                m
            },
        };

        let zone_manifest_path = publisher.publish_bootstrap(&snapshot).await.unwrap();

        let dir = TempDir::new("hickory-repl-minio-pop");
        let layout = ZoneLayout::new(dir.path.join("zone-local"));
        let mut repl = ZoneReplicator::new(store.clone(), zone_manifest_path, layout.clone());
        assert!(repl.sync_once().await.unwrap());

        let active = RedbActiveView::open(layout.clone()).unwrap();
        assert_eq!(active.generation, 1);

        // Publish a delta and ensure the PoP catches up to generation 2.
        let seg = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 2,
            to: 2,
            ops: vec![ZoneDeltaOp::UpsertRrset {
                owner_fqdn: "api.example.com.".into(),
                rrtype: u16::from(crate::proto::rr::RecordType::A),
                ttl: 30,
                rdata_wire: vec![vec![5, 6, 7, 8]],
            }],
        };
        publisher
            .publish_delta_and_update_manifests("z1", &seg)
            .await
            .unwrap();

        // The manifest changed => sync applies the new delta.
        assert!(repl.sync_once().await.unwrap());
        let active = RedbActiveView::open(layout.clone()).unwrap();
        assert_eq!(active.generation, 2);
        assert!(
            active
                .get_rrset("api.example.com.", u16::from(crate::proto::rr::RecordType::A))
                .unwrap()
                .is_some()
        );

        // No manifest change => no-op.
        assert!(!repl.sync_once().await.unwrap());

        // Tamper check: publish a new delta (advancing to generation 3), then corrupt the delta
        // object in-place. The manifest now references generation 3 with the original checksum,
        // so the next sync must fail verification.
        let seg3 = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 3,
            to: 3,
            ops: vec![ZoneDeltaOp::UpsertRrset {
                owner_fqdn: "v3.example.com.".into(),
                rrtype: u16::from(crate::proto::rr::RecordType::A),
                ttl: 30,
                rdata_wire: vec![vec![9, 9, 9, 9]],
            }],
        };
        publisher
            .publish_delta_and_update_manifests("z1", &seg3)
            .await
            .unwrap();

        let delta3_path = ObjPath::from(prefix)
            .child("zones")
            .child("z1")
            .child("deltas")
            .child("delta-3-3.bin");
        store
            .put(&delta3_path, Bytes::from_static(b"corrupt").into())
            .await
            .unwrap();

        let err = repl.sync_once().await.expect_err("expected sha256 mismatch");
        let msg = err.to_string();
        assert!(
            msg.contains("sha256 mismatch"),
            "expected sha256 mismatch, got: {msg}"
        );
    }
}

