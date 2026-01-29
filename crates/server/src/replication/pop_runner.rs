//! PoP background runner / orchestration for replicated zones.
//!
//! This is a thin scheduling layer around the per-zone [`ZoneReplicator`]. It is responsible for:
//! - polling an update stream to discover zone changes
//! - scheduling per-zone `sync_once()` calls on an interval
//! - bounding concurrency and applying backoff on errors

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use futures_util::StreamExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::artifacts::{ArtifactRef, ZoneManifest};
use super::object_store::{JsonManifestPoller, ObjectStoreError};
use super::redb_store::ZoneLayout;
use super::replicator::{fetch_and_verify, ReplicatorError, ZoneReplicator};

use super::redb_store::RedbActiveView;
use super::update_stream::{UpdateEntry, UpdateSegment, UpdatesLatest};

/// Errors from the PoP runner.
#[derive(Debug, thiserror::Error)]
pub enum PopRunnerError {
    /// Update stream fetch failed.
    #[error(transparent)]
    Store(#[from] ObjectStoreError),
    /// Per-zone replication failed.
    #[error("zone {zone_id} replication error: {source}")]
    Zone {
        /// Zone identifier.
        zone_id: String,
        /// Underlying replicator error.
        #[source]
        source: ReplicatorError,
    },
}

/// Backoff configuration for retrying a failing zone.
#[derive(Clone, Debug)]
pub struct BackoffConfig {
    /// Initial backoff after the first failure.
    pub initial: Duration,
    /// Maximum backoff between attempts.
    pub max: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
        }
    }
}

/// Scheduling configuration.
#[derive(Clone, Debug)]
pub struct PopRunnerConfig {
    /// How often to poll the update stream for changes.
    pub global_poll_interval: Duration,
    /// Base interval between per-zone sync attempts.
    pub zone_poll_interval: Duration,
    /// Max jitter added to each zone poll (to avoid thundering herd).
    pub zone_jitter: Duration,
    /// Maximum number of zones syncing concurrently.
    pub max_concurrent_syncs: usize,
    /// Backoff behavior on zone sync failures.
    pub backoff: BackoffConfig,
}

impl Default for PopRunnerConfig {
    fn default() -> Self {
        Self {
            global_poll_interval: Duration::from_secs(5),
            zone_poll_interval: Duration::from_secs(1),
            zone_jitter: Duration::from_millis(250),
            max_concurrent_syncs: 16,
            backoff: BackoffConfig::default(),
        }
    }
}

struct ZoneState {
    zone_id: String,
    layout: ZoneLayout,
    replicator: ZoneReplicator,
    desired_latest_generation: u64,
    desired_zone_manifest: ArtifactRef,
    last_applied_generation: u64,
    next_due: std::time::Instant,
    failures: u32,
}

/// Multi-zone background runner.
pub struct PopRunner {
    store: Arc<dyn ObjectStore>,
    updates_latest_poller: JsonManifestPoller<UpdatesLatest>,
    updates_prefix: ObjPath,
    last_seen_seq: u64,
    segment_size: u64,
    base_dir: PathBuf,
    cfg: PopRunnerConfig,
    zones: HashMap<String, ZoneState>,
    limiter: Arc<Semaphore>,
}

impl PopRunner {
    /// Create a new runner.
    ///
    /// - `updates_latest_path`: object_store path to `updates/LATEST.json`
    /// - `base_dir`: local directory under which each zone gets its own `ZoneLayout` (A/B slots + ACTIVE)
    pub fn new(
        store: Arc<dyn ObjectStore>,
        updates_latest_path: ObjPath,
        base_dir: impl AsRef<Path>,
        cfg: PopRunnerConfig,
    ) -> Self {
        let limiter = Arc::new(Semaphore::new(cfg.max_concurrent_syncs.max(1)));
        let updates_prefix = updates_prefix_from_latest(&updates_latest_path);
        Self {
            updates_latest_poller: JsonManifestPoller::new(store.clone(), updates_latest_path.clone()),
            updates_prefix,
            last_seen_seq: 0,
            segment_size: UpdatesLatest::default().segment_size,
            store,
            base_dir: base_dir.as_ref().to_path_buf(),
            cfg,
            zones: HashMap::new(),
            limiter,
        }
    }

    /// Runs until cancellation is requested.
    pub async fn run(&mut self, cancel: CancellationToken) -> Result<(), PopRunnerError> {
        // Keep track of when we last polled global; we poll immediately once at startup.
        let mut next_global = std::time::Instant::now();

        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }

            let now = std::time::Instant::now();
            if now >= next_global {
                self.refresh_from_updates().await?;
                next_global = now + self.cfg.global_poll_interval;
            }

            // Collect due zones that are not yet at their desired generation.
            let mut due: Vec<String> = self
                .zones
                .iter()
                .filter(|(_, z)| z.next_due <= now && z.last_applied_generation < z.desired_latest_generation)
                .map(|(k, _)| k.clone())
                .collect();
            due.sort(); // stable order for determinism in tests

            // If nothing is due, sleep until the next event (zone due or global poll).
            if due.is_empty() {
                let next_zone = self
                    .zones
                    .values()
                    .map(|z| z.next_due)
                    .min()
                    .unwrap_or(now + self.cfg.zone_poll_interval);
                let next_wakeup = next_zone.min(next_global);
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_wakeup)) => {}
                }
                continue;
            }

            // Sync due zones with a concurrency limit.
            let mut tasks = futures_util::stream::FuturesUnordered::new();
            for zone_id in due {
                // Temporarily take the replicator out of the map to avoid holding a borrow across await.
                let mut state = self
                    .zones
                    .remove(&zone_id)
                    .expect("zone must exist for due list");
                let cfg = self.cfg.clone();
                let store = self.store.clone();
                let limiter = self.limiter.clone();

                tasks.push(async move {
                    // Bound concurrency across zones.
                    let _permit = limiter.acquire_owned().await.unwrap();
                    // Fetch + verify the zone manifest only when this zone is behind.
                    let manifest_bytes = fetch_and_verify(&*store, &state.desired_zone_manifest).await;
                    let res = match manifest_bytes {
                        Ok(bytes) => {
                            match serde_json::from_slice::<ZoneManifest>(&bytes) {
                                Ok(manifest) => state.replicator.sync_with_manifest(manifest).await,
                                Err(e) => Err(ReplicatorError::Store(ObjectStoreError::Json(e))),
                            }
                        }
                        Err(e) => Err(e),
                    };
                    (state, res, cfg)
                });
            }

            while let Some((mut state, res, cfg)) = tasks.next().await {
                let now = std::time::Instant::now();
                match res {
                    Ok(_) => {
                        state.failures = 0;
                        state.last_applied_generation = RedbActiveView::open(state.layout.clone())
                            .map(|v| v.generation)
                            .unwrap_or(state.last_applied_generation);
                        state.next_due = now + cfg.zone_poll_interval + jitter(cfg.zone_jitter, &state.zone_id);
                    }
                    Err(e) => {
                        state.failures = state.failures.saturating_add(1);
                        let backoff = backoff_delay(&cfg.backoff, state.failures);
                        state.next_due = now + backoff + jitter(cfg.zone_jitter, &state.zone_id);
                        // We keep running; callers can also observe errors via logs/metrics later.
                        // For now, surface the last error only if desired by the caller; we choose
                        // to continue without failing the whole runner.
                        let _ = e;
                    }
                }
                self.zones.insert(state.zone_id.clone(), state);
            }
        }
    }

    async fn refresh_from_updates(&mut self) -> Result<(), ObjectStoreError> {
        let Some(latest) = self.updates_latest_poller.fetch_if_changed().await? else {
            return Ok(());
        };
        self.segment_size = latest.segment_size.max(1);
        let target_last = latest.last_seq;

        if target_last <= self.last_seen_seq {
            return Ok(());
        }

        // Fetch and apply update entries since the last seen sequence.
        let mut seq = self.last_seen_seq.saturating_add(1);
        while seq <= target_last {
            let segment_id = (seq.saturating_sub(1)) / self.segment_size;
            let segment_path = updates_segment_path(&self.updates_prefix, segment_id);
            let get = self.store.get(&segment_path).await?;
            let bytes = get.bytes().await?;
            let seg: UpdateSegment = serde_json::from_slice(&bytes)?;

            // Basic sanity; treat validation failure as JSON decode error for now.
            seg.validate().map_err(|e| {
                ObjectStoreError::Json(serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e,
                )))
            })?;

            // Consume entries from this segment starting at `seq`.
            let mut idx = (seq - seg.base_seq) as usize;
            while idx < seg.entries.len() && seq <= target_last {
                let entry: &UpdateEntry = &seg.entries[idx];
                self.apply_update_entry(entry);
                self.last_seen_seq = seq;
                seq = seq.saturating_add(1);
                idx += 1;
            }
        }

        Ok(())
    }
}

fn updates_prefix_from_latest(latest: &ObjPath) -> ObjPath {
    // e.g. "updates/LATEST.json" -> "updates"
    let s = latest.as_ref();
    if let Some((prefix, _)) = s.rsplit_once('/') {
        ObjPath::from(prefix)
    } else {
        ObjPath::from("")
    }
}

fn updates_segment_path(prefix: &ObjPath, segment_id: u64) -> ObjPath {
    prefix.child("segments").child(format!("{segment_id:020}.json"))
}

impl PopRunner {
    fn apply_update_entry(&mut self, entry: &UpdateEntry) {
        let zone_id = entry.zone_id.clone();
        let layout = ZoneLayout::new(self.base_dir.join(&zone_id));
        let last_applied = RedbActiveView::open(layout.clone())
            .map(|v| v.generation)
            .unwrap_or(0);

        match self.zones.get_mut(&zone_id) {
            Some(state) => {
                state.desired_latest_generation = entry.latest_generation;
                state.desired_zone_manifest = entry.zone_manifest.clone();
                if state.last_applied_generation < state.desired_latest_generation {
                    state.next_due = std::time::Instant::now();
                }
            }
            None => {
                // The per-zone manifest path is carried in the ArtifactRef URI; the replicator poller path is unused
                // in this runner, but we still initialize it for completeness.
                let manifest_path = ObjPath::from(entry.zone_manifest.uri.as_str());
                let replicator = ZoneReplicator::new(self.store.clone(), manifest_path, layout.clone());
                self.zones.insert(
                    zone_id.clone(),
                    ZoneState {
                        zone_id,
                        layout,
                        replicator,
                        desired_latest_generation: entry.latest_generation,
                        desired_zone_manifest: entry.zone_manifest.clone(),
                        last_applied_generation: last_applied,
                        next_due: std::time::Instant::now(),
                        failures: 0,
                    },
                );
            }
        }
    }
}

fn jitter(max: Duration, zone_id: &str) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    zone_id.hash(&mut h);
    nanos.hash(&mut h);
    let v = h.finish() % max.as_nanos().max(1) as u64;
    Duration::from_nanos(v)
}

fn backoff_delay(cfg: &BackoffConfig, failures: u32) -> Duration {
    if failures <= 1 {
        return cfg.initial;
    }
    let pow = failures.saturating_sub(1).min(30);
    let mult = 1u64 << pow;
    let base = cfg.initial.as_millis().max(1) as u64;
    let ms = base.saturating_mul(mult).min(cfg.max.as_millis().max(1) as u64);
    Duration::from_millis(ms)
}

#[cfg(test)]
#[cfg(feature = "replication-publisher")]
mod tests {
    use super::{PopRunner, PopRunnerConfig};
    use crate::replication::delta::{RrsetData, ZoneDeltaOp, ZoneDeltaSegment, CURRENT_DELTA_SCHEMA_VERSION};
    use crate::replication::publisher::ArtifactPublisher;
    use crate::replication::redb_store::{encode_rrset_value, rrset_key, RedbActiveView, ZoneLayout};
    use crate::replication::snapshot::{ZoneSnapshot, CURRENT_SNAPSHOT_FORMAT_VERSION};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;
    use futures_util::stream::BoxStream;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions,
        PutPayload, PutResult,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

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

    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        heads: std::sync::atomic::AtomicU64,
        gets: std::sync::atomic::AtomicU64,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                heads: std::sync::atomic::AtomicU64::new(0),
                gets: std::sync::atomic::AtomicU64::new(0),
            }
        }

        fn reset(&self) {
            self.heads.store(0, std::sync::atomic::Ordering::Relaxed);
            self.gets.store(0, std::sync::atomic::Ordering::Relaxed);
        }

        fn heads(&self) -> u64 {
            self.heads.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn gets(&self) -> u64 {
            self.gets.load(std::sync::atomic::Ordering::Relaxed)
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
            if options.head {
                self.heads
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                self.gets
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

    fn make_snapshot(zone_id: &str, origin: &str, gen: u64, owner: &str, ip: [u8; 4]) -> ZoneSnapshot {
        let mut entries = BTreeMap::new();
        let key = rrset_key(owner, u16::from(crate::proto::rr::RecordType::A));
        let val = encode_rrset_value(&RrsetData {
            ttl: 60,
            rdata_wire: vec![ip.to_vec()],
        });
        entries.insert(key, val);
        ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: zone_id.into(),
            origin: origin.into(),
            base_generation: gen,
            entries,
        }
    }

    fn make_delta(gen: u64, owner: &str, ip: [u8; 4]) -> ZoneDeltaSegment {
        ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: gen,
            to: gen,
            ops: vec![ZoneDeltaOp::UpsertRrset {
                owner_fqdn: owner.into(),
                rrtype: u16::from(crate::proto::rr::RecordType::A),
                ttl: 30,
                rdata_wire: vec![ip.to_vec()],
            }],
        }
    }

    #[tokio::test]
    async fn runner_discovers_zones_and_converges_across_updates() {
        let dir = TempDir::new("hickory-repl-runner");
        let store_dir = dir.path.join("store");
        std::fs::create_dir_all(&store_dir).unwrap();
        let raw: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&store_dir).unwrap());
        let counting = Arc::new(CountingStore::new(raw));
        let store: Arc<dyn ObjectStore> = counting.clone();

        let publisher = ArtifactPublisher::new(store.clone(), ObjPath::from(""));
        let base_dir = dir.path.join("zones");
        std::fs::create_dir_all(&base_dir).unwrap();

        // Publish two zones (each bootstrap appends an update-stream entry).
        let z1 = make_snapshot("z1", "example.com.", 1, "api.example.com.", [10, 0, 0, 1]);
        let z2 = make_snapshot("z2", "example.org.", 1, "api.example.org.", [10, 0, 0, 2]);
        publisher.publish_bootstrap(&z1).await.unwrap();
        publisher.publish_bootstrap(&z2).await.unwrap();

        let mut cfg = PopRunnerConfig::default();
        cfg.global_poll_interval = Duration::from_millis(50);
        cfg.zone_poll_interval = Duration::from_millis(25);
        cfg.zone_jitter = Duration::ZERO;
        cfg.max_concurrent_syncs = 4;

        let mut runner = PopRunner::new(
            store.clone(),
            ObjPath::from("updates/LATEST.json"),
            &base_dir,
            cfg,
        );
        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        let h = tokio::spawn(async move { runner.run(cancel2).await });

        // Wait for both zones to reach generation 1.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let g1 = RedbActiveView::open(ZoneLayout::new(base_dir.join("z1")))
                    .map(|v| v.generation)
                    .unwrap_or(0);
                let g2 = RedbActiveView::open(ZoneLayout::new(base_dir.join("z2")))
                    .map(|v| v.generation)
                    .unwrap_or(0);
                if g1 >= 1 && g2 >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        // Publish one delta for each zone.
        let d1 = make_delta(2, "api.example.com.", [10, 0, 0, 3]);
        let d2 = make_delta(2, "api.example.org.", [10, 0, 0, 4]);
        publisher.publish_delta_and_update_manifests("z1", &d1).await.unwrap();
        publisher.publish_delta_and_update_manifests("z2", &d2).await.unwrap();

        // Wait for both zones to reach generation 2.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let g1 = RedbActiveView::open(ZoneLayout::new(base_dir.join("z1")))
                    .map(|v| v.generation)
                    .unwrap_or(0);
                let g2 = RedbActiveView::open(ZoneLayout::new(base_dir.join("z2")))
                    .map(|v| v.generation)
                    .unwrap_or(0);
                if g1 >= 2 && g2 >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        cancel.cancel();
        h.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn runner_is_o1_on_cold_zones_via_update_stream_polling() {
        let dir = TempDir::new("hickory-repl-runner-o1");
        let store_dir = dir.path.join("store");
        std::fs::create_dir_all(&store_dir).unwrap();
        let raw: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&store_dir).unwrap());
        let counting = Arc::new(CountingStore::new(raw));
        let store: Arc<dyn ObjectStore> = counting.clone();

        let publisher = ArtifactPublisher::new(store.clone(), ObjPath::from(""));
        let base_dir = dir.path.join("zones");
        std::fs::create_dir_all(&base_dir).unwrap();

        // Publish many zones.
        let zones = 10usize;
        for i in 0..zones {
            let zone_id = format!("z{i}");
            let origin = format!("example{i}.com.");
            let owner = format!("api.example{i}.com.");
            let snap = make_snapshot(&zone_id, &origin, 1, &owner, [10, 0, 0, (i as u8).saturating_add(1)]);
            publisher.publish_bootstrap(&snap).await.unwrap();
        }

        let mut cfg = PopRunnerConfig::default();
        cfg.global_poll_interval = Duration::from_millis(50);
        cfg.zone_poll_interval = Duration::from_millis(50);
        cfg.zone_jitter = Duration::ZERO;
        cfg.max_concurrent_syncs = 4;

        let mut runner = PopRunner::new(
            store.clone(),
            ObjPath::from("updates/LATEST.json"),
            &base_dir,
            cfg,
        );

        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        let h = tokio::spawn(async move { runner.run(cancel2).await });

        // Wait for all zones to reach generation 1.
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let mut ok = true;
                for i in 0..zones {
                    let zone_id = format!("z{i}");
                    let gen = RedbActiveView::open(ZoneLayout::new(base_dir.join(&zone_id)))
                        .map(|v| v.generation)
                        .unwrap_or(0);
                    if gen < 1 {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();

        // Reset counters and let it run “cold” for a bit.
        counting.reset();
        tokio::time::sleep(Duration::from_millis(150)).await;

        cancel.cancel();
        h.await.unwrap().unwrap();

        // Cold steady-state should be dominated by polling updates/LATEST (HEAD only),
        // not per-zone manifest fetches. We allow a small number of calls due to timing variance.
        assert!(
            counting.heads() <= 25,
            "expected low HEAD count in cold state, got heads={} gets={}",
            counting.heads(),
            counting.gets()
        );
        assert!(
            counting.gets() == 0,
            "expected no GETs in cold state, got heads={} gets={}",
            counting.heads(),
            counting.gets()
        );
    }
}

