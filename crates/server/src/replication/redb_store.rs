//! `redb`-backed local store for replicated zone data.
//!
//! This module is intended for PoP-local storage: replication applies snapshots/deltas into an
//! inactive database, then atomically activates it for serving.

use std::path::{Path, PathBuf};

use redb::{Database, TableDefinition};

use super::delta::{DeltaError, RrsetData, ZoneWritable};
use super::snapshot::ZoneSnapshot;

/// String metadata table keyed by well-known keys.
static META_STR: TableDefinition<'static, &'static str, &'static str> =
    TableDefinition::new("meta_str");
/// Integer metadata table keyed by well-known keys.
static META_U64: TableDefinition<'static, &'static str, u64> = TableDefinition::new("meta_u64");
/// RRset table keyed by `rrset_key(owner_fqdn, rrtype)`.
static RRSETS: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("rrsets");

const ACTIVE_FILE: &str = "ACTIVE";

/// Which on-disk database slot is active.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Slot {
    /// Slot A.
    A,
    /// Slot B.
    B,
}

impl Slot {
    fn other(self) -> Self {
        match self {
            Slot::A => Slot::B,
            Slot::B => Slot::A,
        }
    }

    fn as_char(self) -> char {
        match self {
            Slot::A => 'A',
            Slot::B => 'B',
        }
    }

    fn from_char(c: char) -> Option<Self> {
        match c {
            'A' => Some(Slot::A),
            'B' => Some(Slot::B),
            _ => None,
        }
    }
}

/// Filesystem layout for one replicated zone.
#[derive(Clone, Debug)]
pub struct ZoneLayout {
    db_a: PathBuf,
    db_b: PathBuf,
    active: PathBuf,
}

impl ZoneLayout {
    /// Create a layout rooted at `dir`.
    pub fn new(dir: PathBuf) -> Self {
        Self {
            db_a: dir.join("db-a.redb"),
            db_b: dir.join("db-b.redb"),
            active: dir.join(ACTIVE_FILE),
        }
    }

    fn db_path(&self, slot: Slot) -> &Path {
        match slot {
            Slot::A => &self.db_a,
            Slot::B => &self.db_b,
        }
    }

    /// Returns the currently active slot, defaulting to `Slot::A` if the pointer is missing.
    pub fn active_slot(&self) -> Slot {
        read_active_slot(&self.active).unwrap_or(Slot::A)
    }

    /// Returns the currently inactive slot (the opposite of the active slot).
    pub fn inactive_slot(&self) -> Slot {
        self.active_slot().other()
    }
}

/// Read the active slot from the pointer file.
fn read_active_slot(path: &Path) -> Option<Slot> {
    let data = std::fs::read_to_string(path).ok()?;
    // Format: "A\n" or "B\n"
    let c = data.trim().chars().next()?;
    Slot::from_char(c)
}

/// Atomically write the active slot pointer.
fn write_active_slot_atomic(path: &Path, slot: Slot) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, format!("{}\n", slot.as_char()))?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// Create a new redb database at `path`, creating parent directories as needed.
pub fn create_db(path: &Path) -> Result<Database, redb::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(redb::Error::Io)?;
    }
    Ok(Database::create(path)?)
}

/// Open an existing redb database at `path`.
pub fn open_db(path: &Path) -> Result<Database, redb::Error> {
    Ok(Database::open(path)?)
}

fn write_zone_meta(
    db: &Database,
    zone_id: &str,
    origin: &str,
    generation: u64,
    soa_serial: u32,
) -> Result<(), redb::Error> {
    let tx = db.begin_write()?;
    {
        let mut s = tx.open_table(META_STR)?;
        s.insert("zone_id", zone_id)?;
        s.insert("origin", origin)?;
    }
    {
        let mut n = tx.open_table(META_U64)?;
        n.insert("generation", &generation)?;
        n.insert("soa_serial", &(soa_serial as u64))?;
    }
    tx.commit()?;
    Ok(())
}

fn read_zone_meta(db: &Database) -> Result<(String, String, u64, u32), redb::Error> {
    let tx = db.begin_read()?;
    let s = tx.open_table(META_STR)?;
    let n = tx.open_table(META_U64)?;

    let zone_id = s
        .get("zone_id")?
        .ok_or_else(|| redb::Error::Corrupted("missing zone_id".into()))?
        .value()
        .to_string();
    let origin = s
        .get("origin")?
        .ok_or_else(|| redb::Error::Corrupted("missing origin".into()))?
        .value()
        .to_string();
    let generation = n
        .get("generation")?
        .ok_or_else(|| redb::Error::Corrupted("missing generation".into()))?
        .value();
    let soa_serial_u64 = n
        .get("soa_serial")?
        .ok_or_else(|| redb::Error::Corrupted("missing soa_serial".into()))?
        .value();
    let soa_serial: u32 = soa_serial_u64
        .try_into()
        .map_err(|_| redb::Error::Corrupted("invalid soa_serial".into()))?;
    Ok((zone_id, origin, generation, soa_serial))
}

/// Encode the rrset key for the rrset table.
pub fn rrset_key(owner_fqdn: &str, rrtype: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(owner_fqdn.len() + 1 + 2);
    out.extend_from_slice(owner_fqdn.as_bytes());
    out.push(0);
    out.extend_from_slice(&rrtype.to_le_bytes());
    out
}

/// Encode the rrset value (ttl + list of wire-format RDATA bytes).
pub fn encode_rrset_value(rrset: &RrsetData) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&rrset.ttl.to_le_bytes());
    let count: u32 = rrset.rdata_wire.len().try_into().unwrap_or(u32::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    for r in &rrset.rdata_wire {
        let len: u32 = r.len().try_into().unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(r);
    }
    out
}

#[derive(Debug)]
enum RrsetDecodeError {
    TooShort,
    Truncated,
}

fn decode_rrset_value(bytes: &[u8]) -> Result<RrsetData, RrsetDecodeError> {
    if bytes.len() < 8 {
        return Err(RrsetDecodeError::TooShort);
    }
    let ttl = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let count = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
    let mut pos = 8;
    let mut rdata_wire = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return Err(RrsetDecodeError::Truncated);
        }
        let len = u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
            as usize;
        pos += 4;
        if pos + len > bytes.len() {
            return Err(RrsetDecodeError::Truncated);
        }
        rdata_wire.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    Ok(RrsetData { ttl, rdata_wire })
}

/// A staging writer for the inactive slot.
pub struct RedbStaging {
    layout: ZoneLayout,
    slot: Slot,
    db: Database,
    zone_id: Option<String>,
    origin: Option<String>,
}

impl RedbStaging {
    /// Create (truncate) and open the inactive database for writes.
    pub fn open_inactive(layout: ZoneLayout) -> Result<Self, redb::Error> {
        let slot = layout.inactive_slot();
        let path = layout.db_path(slot).to_path_buf();
        let _ = std::fs::remove_file(&path);
        let db = create_db(&path)?;
        Ok(Self {
            layout,
            slot,
            db,
            zone_id: None,
            origin: None,
        })
    }

    /// Load a snapshot into the inactive database.
    pub fn load_snapshot(
        &mut self,
        snapshot: &ZoneSnapshot,
        soa_serial: u32,
    ) -> Result<(), redb::Error> {
        // Ensure tables exist, then clear and repopulate rrsets.
        let tx = self.db.begin_write()?;
        {
            let mut rrsets = tx.open_table(RRSETS)?;
            rrsets.retain(|_, _| false)?;
            for (k, v) in &snapshot.entries {
                rrsets.insert(k.as_slice(), v.as_slice())?;
            }
        }
        tx.commit()?;
        write_zone_meta(
            &self.db,
            &snapshot.zone_id,
            &snapshot.origin,
            snapshot.base_generation,
            soa_serial,
        )?;
        self.zone_id = Some(snapshot.zone_id.clone());
        self.origin = Some(snapshot.origin.clone());
        Ok(())
    }

    /// Update the zone version metadata after applying deltas.
    pub fn set_version(&mut self, generation: u64, soa_serial: u32) -> Result<(), redb::Error> {
        let zone_id = self
            .zone_id
            .as_deref()
            .ok_or_else(|| redb::Error::Corrupted("missing zone_id".into()))?;
        let origin = self
            .origin
            .as_deref()
            .ok_or_else(|| redb::Error::Corrupted("missing origin".into()))?;
        write_zone_meta(&self.db, zone_id, origin, generation, soa_serial)
    }

    /// Persist this inactive DB as active by atomically flipping the ACTIVE pointer.
    pub fn activate(self) -> Result<(), redb::Error> {
        drop(self.db);
        write_active_slot_atomic(&self.layout.active, self.slot).map_err(redb::Error::Io)?;
        Ok(())
    }
}

fn delta_decode_err(msg: &str) -> DeltaError {
    // prost::DecodeError::new() is deprecated but still the only easy constructor.
    // This path is for mapping local storage errors into `ZoneWritable`'s error type.
    #[allow(deprecated)]
    {
        DeltaError::Decode(prost::DecodeError::new(msg.to_string()))
    }
}

impl ZoneWritable for RedbStaging {
    fn upsert_rrset(
        &mut self,
        owner_fqdn: &str,
        rrtype: u16,
        rrset: RrsetData,
    ) -> Result<(), DeltaError> {
        let key = rrset_key(owner_fqdn, rrtype);
        let val = encode_rrset_value(&rrset);
        let tx = self.db.begin_write().map_err(|_| delta_decode_err("redb begin_write"))?;
        {
            let mut rrsets = tx
                .open_table(RRSETS)
                .map_err(|_| delta_decode_err("redb open_table(rrsets)"))?;
            rrsets
                .insert(key.as_slice(), val.as_slice())
                .map_err(|_| delta_decode_err("redb insert(rrsets)"))?;
        }
        tx.commit().map_err(|_| delta_decode_err("redb commit"))?;
        Ok(())
    }

    fn delete_rrset(&mut self, owner_fqdn: &str, rrtype: u16) -> Result<(), DeltaError> {
        let key = rrset_key(owner_fqdn, rrtype);
        let tx = self.db.begin_write().map_err(|_| delta_decode_err("redb begin_write"))?;
        {
            let mut rrsets = tx
                .open_table(RRSETS)
                .map_err(|_| delta_decode_err("redb open_table(rrsets)"))?;
            let _ = rrsets
                .remove(key.as_slice())
                .map_err(|_| delta_decode_err("redb remove(rrsets)"))?;
        }
        tx.commit().map_err(|_| delta_decode_err("redb commit"))?;
        Ok(())
    }
}

/// Read-only view of the currently active DB for a zone.
pub struct RedbActiveView {
    /// Open database handle for the active slot.
    pub db: Database,
    /// Zone identifier stored in meta.
    pub zone_id: String,
    /// Zone origin stored in meta.
    pub origin: String,
    /// Zone generation stored in meta.
    pub generation: u64,
    /// SOA serial stored in meta.
    pub soa_serial: u32,
}

impl RedbActiveView {
    /// Open the active slot and read metadata.
    pub fn open(layout: ZoneLayout) -> Result<Self, redb::Error> {
        let slot = layout.active_slot();
        let db = open_db(layout.db_path(slot))?;
        let (zone_id, origin, generation, soa_serial) = read_zone_meta(&db)?;
        Ok(Self {
            db,
            zone_id,
            origin,
            generation,
            soa_serial,
        })
    }

    /// Lookup an RRset by (owner, rrtype).
    pub fn get_rrset(
        &self,
        owner_fqdn: &str,
        rrtype: u16,
    ) -> Result<Option<RrsetData>, redb::Error> {
        let tx = self.db.begin_read()?;
        let rrsets = tx.open_table(RRSETS)?;
        let key = rrset_key(owner_fqdn, rrtype);
        let Some(v) = rrsets.get(key.as_slice())? else {
            return Ok(None);
        };
        let decoded = decode_rrset_value(v.value())
            .map_err(|_| redb::Error::Corrupted("invalid rrset encoding".into()))?;
        Ok(Some(decoded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::snapshot::CURRENT_SNAPSHOT_FORMAT_VERSION;
    use super::super::delta::{ZoneDeltaApplier, ZoneDeltaOp, ZoneDeltaSegment, CURRENT_DELTA_SCHEMA_VERSION};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let mut path = std::env::temp_dir();
            let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let nonce = format!(
                "hickory-repl-redb-{}-{}-{}",
                std::process::id(),
                n,
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

    #[test]
    fn atomic_activation_switches_active_view() {
        let dir = TempDir::new();
        let layout = ZoneLayout::new(dir.path.join("zone"));

        // Seed A as active with generation 1.
        {
            let db = create_db(layout.db_path(Slot::A)).unwrap();
            write_zone_meta(&db, "z1", "example.com.", 1, 2026011401).unwrap();
        }
        write_active_slot_atomic(&layout.active, Slot::A).unwrap();

        let active = RedbActiveView::open(layout.clone()).unwrap();
        assert_eq!(active.generation, 1);
        drop(active);

        // Stage a snapshot into B but do not activate: active should remain A.
        let snap = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 2,
            entries: {
                let mut m = BTreeMap::new();
                let key = rrset_key("www.example.com.", 1);
                let val = encode_rrset_value(&RrsetData {
                    ttl: 60,
                    rdata_wire: vec![b"1.2.3.4".to_vec()],
                });
                m.insert(key, val);
                m
            },
        };

        let mut staging = RedbStaging::open_inactive(layout.clone()).unwrap();
        staging.load_snapshot(&snap, 2026011402).unwrap();

        let still_active = RedbActiveView::open(layout.clone()).unwrap();
        assert_eq!(still_active.generation, 1);
        drop(still_active);

        // Activate B.
        staging.activate().unwrap();
        let new_active = RedbActiveView::open(layout.clone()).unwrap();
        assert_eq!(new_active.generation, 2);
        let rr = new_active.get_rrset("www.example.com.", 1).unwrap().unwrap();
        assert_eq!(rr.ttl, 60);
        assert_eq!(rr.rdata_wire, vec![b"1.2.3.4".to_vec()]);
    }

    #[test]
    fn active_pointer_is_atomic_via_rename() {
        let dir = TempDir::new();
        let layout = ZoneLayout::new(dir.path.join("zone"));

        // Seed A as active.
        {
            let db = create_db(layout.db_path(Slot::A)).unwrap();
            write_zone_meta(&db, "z1", "example.com.", 1, 2026011401).unwrap();
        }
        write_active_slot_atomic(&layout.active, Slot::A).unwrap();

        // Write a tmp pointer but don't rename it into place.
        let tmp = layout.active.with_extension("tmp");
        std::fs::write(&tmp, "B\n").unwrap();

        // Active remains A.
        assert_eq!(layout.active_slot(), Slot::A);
    }

    #[test]
    fn deltas_apply_into_staging_and_activation_updates_view() {
        let dir = TempDir::new();
        let layout = ZoneLayout::new(dir.path.join("zone"));

        // Seed A as active.
        {
            let db = create_db(layout.db_path(Slot::A)).unwrap();
            write_zone_meta(&db, "z1", "example.com.", 1, 2026011401).unwrap();
        }
        write_active_slot_atomic(&layout.active, Slot::A).unwrap();

        // Snapshot at generation 1 with empty rrsets.
        let snap = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries: BTreeMap::new(),
        };

        let mut staging = RedbStaging::open_inactive(layout.clone()).unwrap();
        staging.load_snapshot(&snap, 2026011401).unwrap();

        // Apply a delta segment advancing to generation 2.
        let seg = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 2,
            to: 2,
            ops: vec![ZoneDeltaOp::UpsertRrset {
                owner_fqdn: "www.example.com.".into(),
                rrtype: 1,
                ttl: 30,
                rdata_wire: vec![b"5.6.7.8".to_vec()],
            }],
        };
        let mut applier = ZoneDeltaApplier::new(2);
        applier.apply_segment(&mut staging, &seg).unwrap();
        staging.set_version(2, 2026011402).unwrap();
        staging.activate().unwrap();

        let active = RedbActiveView::open(layout.clone()).unwrap();
        assert_eq!(active.generation, 2);
        let rr = active.get_rrset("www.example.com.", 1).unwrap().unwrap();
        assert_eq!(rr.ttl, 30);
        assert_eq!(rr.rdata_wire, vec![b"5.6.7.8".to_vec()]);
    }
}

