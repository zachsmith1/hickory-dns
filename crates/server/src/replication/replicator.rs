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
        self.sync_with_manifest(manifest).await
    }

    /// Catch up to the given manifest (already fetched/decoded by the caller).
    ///
    /// This is useful for multi-zone orchestration where the global manifest is polled once and
    /// per-zone manifests are only fetched when a zone's `latest_generation` advances.
    pub async fn sync_with_manifest(&mut self, manifest: ZoneManifest) -> Result<bool, ReplicatorError> {
        manifest
            .validate()
            .map_err(|e| ReplicatorError::InvalidManifest(e.to_string()))?;

        let (local_gen, mut soa_serial) = RedbActiveView::open(self.layout.clone())
            .map(|v| (v.generation, v.soa_serial))
            .unwrap_or((0, 0));
        if local_gen >= manifest.latest_generation {
            return Ok(false);
        }

        // If we are behind the manifest's base snapshot, we must rebuild from the snapshot.
        let can_incremental = local_gen > 0 && local_gen >= manifest.snapshot.base_generation;

        if !can_incremental {
            // Full materialize from snapshot + all deltas.
            let snapshot_bytes = fetch_and_verify(&*self.store, &manifest.snapshot.artifact).await?;
            let snapshot = ZoneSnapshot::decode(&snapshot_bytes)?;

            let mut staging = RedbStaging::open_inactive(self.layout.clone())?;
            staging.load_snapshot(&snapshot, soa_serial)?;

            let mut next_gen = snapshot.base_generation.saturating_add(1);
            for seg_ref in &manifest.deltas {
                if seg_ref.from != next_gen {
                    return Err(ReplicatorError::Delta(
                        super::delta::DeltaError::UnexpectedFromGeneration {
                            expected: next_gen,
                            actual: seg_ref.from,
                        },
                    ));
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

                let mut applier = super::delta::ZoneDeltaApplier::new(seg.from);
                applier.apply_segment(&mut staging, &seg)?;

                soa_serial = soa_serial.saturating_add(1);
                staging.set_version(seg.to, soa_serial)?;
                next_gen = seg.to.saturating_add(1);
            }

            staging.activate()?;
            return Ok(true);
        }

        // Incremental catch-up: copy active DB into inactive slot and apply only new deltas.
        let mut staging = RedbStaging::open_inactive_cloned_from_active(self.layout.clone())?;

        let mut next_gen = local_gen.saturating_add(1);
        let mut any = false;
        for seg_ref in &manifest.deltas {
            if seg_ref.to < next_gen {
                continue;
            }
            if seg_ref.from != next_gen {
                return Err(ReplicatorError::Delta(
                    super::delta::DeltaError::UnexpectedFromGeneration {
                        expected: next_gen,
                        actual: seg_ref.from,
                    },
                ));
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

            let mut applier = super::delta::ZoneDeltaApplier::new(seg.from);
            applier.apply_segment(&mut staging, &seg)?;

            soa_serial = soa_serial.saturating_add(1);
            staging.set_version(seg.to, soa_serial)?;
            next_gen = seg.to.saturating_add(1);
            any = true;
        }

        if !any {
            return Ok(false);
        }

        staging.activate()?;
        Ok(true)
    }
}

pub(crate) async fn fetch_and_verify(
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

pub(crate) fn verify_sha256(
    uri: &str,
    expected: &Sha256Digest,
    bytes: &[u8],
) -> Result<(), ReplicatorError> {
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

pub(crate) fn path_from_uri(uri: &str) -> Result<object_store::path::Path, ReplicatorError> {
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
    use object_store::{GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutPayload, PutResult};
    use futures_util::stream::BoxStream;
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

    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        snap_gets: std::sync::atomic::AtomicU64,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                snap_gets: std::sync::atomic::AtomicU64::new(0),
            }
        }

        fn snap_gets(&self) -> u64 {
            self.snap_gets.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if !options.head && location.as_ref() == "snap.bin" {
                self.snap_gets
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&object_store::path::Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
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

    #[tokio::test]
    async fn incremental_catchup_does_not_refetch_snapshot() {
        let dir = TempDir::new("hickory-repl-pop-incr");
        let store_dir = dir.path.join("store");
        std::fs::create_dir_all(&store_dir).unwrap();
        let raw: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&store_dir).unwrap());
        let counting = Arc::new(CountingStore::new(raw));
        let store: Arc<dyn ObjectStore> = counting.clone();

        // Snapshot at generation 1.
        let snapshot = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries: {
                let mut m = BTreeMap::new();
                let key = rrset_key("www.example.com.", 1);
                let val = encode_rrset_value(&crate::replication::delta::RrsetData {
                    ttl: 60,
                    rdata_wire: vec![b"1.2.3.4".to_vec()],
                });
                m.insert(key, val);
                m
            },
        };
        let snapshot_bytes = snapshot.encode_to_vec().unwrap();
        std::fs::write(store_dir.join("snap.bin"), &snapshot_bytes).unwrap();

        // Delta 2 and 3.
        let seg2 = ZoneDeltaSegment {
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
        let seg2_bytes = seg2.encode_protobuf_to_vec();
        std::fs::write(store_dir.join("delta-2-2.bin"), &seg2_bytes).unwrap();

        let seg3 = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 3,
            to: 3,
            ops: vec![ZoneDeltaOp::UpsertRrset {
                owner_fqdn: "api.example.com.".into(),
                rrtype: 1,
                ttl: 30,
                rdata_wire: vec![b"9.9.9.9".to_vec()],
            }],
        };
        let seg3_bytes = seg3.encode_protobuf_to_vec();
        std::fs::write(store_dir.join("delta-3-3.bin"), &seg3_bytes).unwrap();

        // Start with manifest at latest=2.
        let manifest2 = ZoneManifest {
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
                    sha256: Sha256Digest(sha256_hex(&seg2_bytes)),
                },
            }],
        };
        std::fs::write(
            store_dir.join("zone-manifest.json"),
            serde_json::to_vec(&manifest2).unwrap(),
        )
        .unwrap();

        let layout = ZoneLayout::new(dir.path.join("zone-local"));
        let mut repl = ZoneReplicator::new(store.clone(), ObjPath::from("zone-manifest.json"), layout.clone());

        assert!(repl.sync_once().await.unwrap());
        let snap_gets_after_first = counting.snap_gets();
        assert!(snap_gets_after_first >= 1);

        // Update manifest to latest=3.
        let manifest3 = ZoneManifest {
            latest_generation: 3,
            deltas: vec![
                manifest2.deltas[0].clone(),
                DeltaSegmentRef {
                    from: 3,
                    to: 3,
                    artifact: ArtifactRef {
                        uri: "file://delta-3-3.bin".into(),
                        sha256: Sha256Digest(sha256_hex(&seg3_bytes)),
                    },
                },
            ],
            ..manifest2
        };
        std::fs::write(
            store_dir.join("zone-manifest.json"),
            serde_json::to_vec(&manifest3).unwrap(),
        )
        .unwrap();

        assert!(repl.sync_once().await.unwrap());

        // Snapshot should not be fetched again on incremental catch-up.
        assert_eq!(counting.snap_gets(), snap_gets_after_first);

        let active = RedbActiveView::open(layout).unwrap();
        assert_eq!(active.generation, 3);
    }
}

