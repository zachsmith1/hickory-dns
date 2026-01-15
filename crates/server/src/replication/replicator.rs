//! PoP replicator (pull-based) for artifact-based replication.
//!
//! This module ties together:
//! - per-zone manifests and artifacts (snapshots + deltas)
//! - the object store poller/fetcher
//! - a local store implementation (currently `redb_store`)
//!
//! It is intentionally "one-zone at a time" for now. Multi-zone orchestration can be layered
//! above this API.

use std::sync::Arc;

use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::artifacts::{ArtifactRef, Sha256Digest, ZoneManifest};
use super::delta::ZoneDeltaSegment;
use super::object_store::{JsonManifestPoller, ObjectStoreError};
use super::redb_store::{RedbActiveView, RedbStaging, ZoneLayout};
use super::snapshot::ZoneSnapshot;

/// Replication errors.
#[derive(Debug, Error)]
pub enum ReplicatorError {
    /// Manifest or artifact fetch failed.
    #[error(transparent)]
    Store(#[from] ObjectStoreError),
    /// The zone manifest was decoded but failed validation.
    #[error("invalid zone manifest: {0}")]
    InvalidManifest(String),
    /// Unsupported artifact URI.
    #[error("unsupported artifact uri: {0}")]
    UnsupportedUri(String),
    /// Artifact checksum mismatch.
    #[error("sha256 mismatch for {uri}: expected {expected} got {actual}")]
    Sha256Mismatch {
        /// Artifact URI.
        uri: String,
        /// Expected hex digest (lowercase).
        expected: String,
        /// Actual hex digest (lowercase).
        actual: String,
    },
    /// Snapshot decode error.
    #[error(transparent)]
    Snapshot(#[from] super::snapshot::SnapshotError),
    /// Delta decode/apply error.
    #[error(transparent)]
    Delta(#[from] super::delta::DeltaError),
    /// redb error.
    #[error(transparent)]
    Redb(#[from] redb::Error),
}

/// Pull-based replicator for a single zone.
pub struct ZoneReplicator {
    store: Arc<dyn ObjectStore>,
    poller: JsonManifestPoller<ZoneManifest>,
    layout: ZoneLayout,
}

impl ZoneReplicator {
    /// Create a new replicator that polls a per-zone manifest at `zone_manifest_path`.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        zone_manifest_path: object_store::path::Path,
        layout: ZoneLayout,
    ) -> Self {
        Self {
            poller: JsonManifestPoller::new(store.clone(), zone_manifest_path),
            store,
            layout,
        }
    }

    /// Poll the manifest and, if behind, catch up by loading the snapshot and applying deltas.
    ///
    /// Returns `Ok(true)` if a new generation was activated.
    pub async fn sync_once(&mut self) -> Result<bool, ReplicatorError> {
        let Some(manifest) = self.poller.fetch_if_changed().await? else {
            return Ok(false);
        };
        manifest
            .validate()
            .map_err(|e| ReplicatorError::InvalidManifest(e.to_string()))?;

        let local_gen = RedbActiveView::open(self.layout.clone())
            .map(|v| v.generation)
            .unwrap_or(0);
        if local_gen >= manifest.latest_generation {
            return Ok(false);
        }

        // v1: always materialize from the manifest snapshot + deltas after it.
        let snapshot_bytes = fetch_and_verify(&*self.store, &manifest.snapshot.artifact).await?;
        let snapshot = ZoneSnapshot::decode(&snapshot_bytes)?;

        let mut staging = RedbStaging::open_inactive(self.layout.clone())?;
        // Until the writer/replication protocol carries SOA serials explicitly, preserve the
        // currently-served serial (or 0 for first bootstrap).
        let mut soa_serial = RedbActiveView::open(self.layout.clone())
            .map(|v| v.soa_serial)
            .unwrap_or(0);
        staging.load_snapshot(&snapshot, soa_serial)?;

        let mut next_gen = snapshot.base_generation.saturating_add(1);
        for seg_ref in &manifest.deltas {
            if seg_ref.from != next_gen {
                // manifest.validate() should have caught this; treat as checksum-like failure.
                return Err(ReplicatorError::Delta(super::delta::DeltaError::UnexpectedFromGeneration {
                    expected: next_gen,
                    actual: seg_ref.from,
                }));
            }

            let seg_bytes = fetch_and_verify(&*self.store, &seg_ref.artifact).await?;
            let seg = ZoneDeltaSegment::decode_protobuf(&seg_bytes)?;
            seg.validate()?;
            if seg.from != seg_ref.from || seg.to != seg_ref.to {
                return Err(ReplicatorError::Delta(super::delta::DeltaError::InvalidRange {
                    from: seg.from,
                    to: seg.to,
                }));
            }

            // Apply ops.
            let mut applier = super::delta::ZoneDeltaApplier::new(seg.from);
            applier.apply_segment(&mut staging, &seg)?;

            // Update metadata to match the segment end.
            soa_serial = soa_serial.saturating_add(1);
            staging.set_version(seg.to, soa_serial)?;
            next_gen = seg.to.saturating_add(1);
        }

        staging.activate()?;
        Ok(true)
    }
}

async fn fetch_and_verify(
    store: &dyn ObjectStore,
    artifact: &ArtifactRef,
) -> Result<Vec<u8>, ReplicatorError> {
    let path = path_from_uri(&artifact.uri)?;
    let get = store
        .get(&path)
        .await
        .map_err(ObjectStoreError::Store)?;
    let bytes = get.bytes().await.map_err(ObjectStoreError::Store)?;

    verify_sha256(&artifact.uri, &artifact.sha256, &bytes)?;
    Ok(bytes.to_vec())
}

fn verify_sha256(uri: &str, expected: &Sha256Digest, bytes: &[u8]) -> Result<(), ReplicatorError> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let actual = data_encoding::HEXLOWER.encode(digest.as_slice());

    let expected_norm = expected.0.to_ascii_lowercase();
    if actual != expected_norm {
        return Err(ReplicatorError::Sha256Mismatch {
            uri: uri.to_string(),
            expected: expected_norm,
            actual,
        });
    }
    Ok(())
}

fn path_from_uri(uri: &str) -> Result<object_store::path::Path, ReplicatorError> {
    if let Some(rest) = uri.strip_prefix("file://") {
        // object_store paths are relative to the store prefix; normalize to a relative path.
        let rel = rest.trim_start_matches('/');
        return Ok(object_store::path::Path::from(rel));
    }
    if uri.contains("://") {
        return Err(ReplicatorError::UnsupportedUri(uri.to_string()));
    }
    Ok(object_store::path::Path::from(uri))
}

#[cfg(test)]
mod tests {
    use super::ZoneReplicator;
    use crate::replication::artifacts::{
        ArtifactRef, CURRENT_MANIFEST_FORMAT_VERSION, DeltaSegmentRef, Sha256Digest, SnapshotRef,
        ZoneManifest,
    };
    use crate::replication::delta::{ZoneDeltaOp, ZoneDeltaSegment, CURRENT_DELTA_SCHEMA_VERSION};
    use crate::replication::redb_store::{encode_rrset_value, rrset_key, RedbActiveView, ZoneLayout};
    use crate::replication::snapshot::{ZoneSnapshot, CURRENT_SNAPSHOT_FORMAT_VERSION};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;
    use sha2::{Digest, Sha256};
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

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(bytes);
        let out = h.finalize();
        data_encoding::HEXLOWER.encode(out.as_slice())
    }

    #[tokio::test]
    async fn sync_once_bootstraps_and_activates() {
        let dir = TempDir::new("hickory-repl-pop");
        let store_dir = dir.path.join("store");
        std::fs::create_dir_all(&store_dir).unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&store_dir).unwrap());

        // Build snapshot at generation 1 with one rrset.
        let mut entries = BTreeMap::new();
        let key = rrset_key("www.example.com.", 1);
        let val = encode_rrset_value(&crate::replication::delta::RrsetData {
            ttl: 60,
            rdata_wire: vec![b"1.2.3.4".to_vec()],
        });
        entries.insert(key, val);

        let snapshot = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries,
        };
        let snapshot_bytes = snapshot.encode_to_vec().unwrap();
        std::fs::write(store_dir.join("snap.bin"), &snapshot_bytes).unwrap();

        // Delta segment to generation 2.
        let seg = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 2,
            to: 2,
            ops: vec![ZoneDeltaOp::UpsertRrset {
                owner_fqdn: "api.example.com.".into(),
                rrtype: 1,
                ttl: 30,
                rdata_wire: vec![b"5.6.7.8".to_vec()],
            }],
        };
        let seg_bytes = seg.encode_protobuf_to_vec();
        std::fs::write(store_dir.join("delta-2-2.bin"), &seg_bytes).unwrap();

        // Manifest referencing the artifacts.
        let manifest = ZoneManifest {
            format_version: CURRENT_MANIFEST_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            latest_generation: 2,
            snapshot: SnapshotRef {
                base_generation: 1,
                artifact: ArtifactRef {
                    uri: "file://snap.bin".into(),
                    sha256: Sha256Digest(sha256_hex(&snapshot_bytes)),
                },
            },
            deltas: vec![DeltaSegmentRef {
                from: 2,
                to: 2,
                artifact: ArtifactRef {
                    uri: "file://delta-2-2.bin".into(),
                    sha256: Sha256Digest(sha256_hex(&seg_bytes)),
                },
            }],
        };
        std::fs::write(
            store_dir.join("zone-manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();

        let layout = ZoneLayout::new(dir.path.join("zone-local"));
        let mut repl =
            ZoneReplicator::new(store.clone(), ObjPath::from("zone-manifest.json"), layout.clone());

        assert!(repl.sync_once().await.unwrap());

        let active = RedbActiveView::open(layout).unwrap();
        assert_eq!(active.generation, 2);
        assert!(active.get_rrset("www.example.com.", 1).unwrap().is_some());
        assert!(active.get_rrset("api.example.com.", 1).unwrap().is_some());

        // No manifest change => no-op.
        assert!(!repl.sync_once().await.unwrap());
    }
}

