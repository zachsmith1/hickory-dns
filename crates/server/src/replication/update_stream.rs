//! Update stream (batched metadata WAL) for scalable zone discovery.
//!
//! This is a control-plane mechanism: it tells PoPs *which zones changed*, without requiring
//! polling a full zone index.

use serde::{Deserialize, Serialize};

use super::artifacts::{ArtifactRef, CURRENT_MANIFEST_FORMAT_VERSION};

/// Current supported update stream format version.
pub const CURRENT_UPDATE_STREAM_FORMAT_VERSION: u16 = 1;

/// A single zone update entry in the stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateEntry {
    /// Stable zone identifier.
    pub zone_id: String,
    /// Canonical DNS name for the zone origin (e.g. `example.com.`).
    pub origin: String,
    /// Latest committed generation for the zone.
    pub latest_generation: u64,
    /// Reference to the per-zone manifest (checksum is validated by PoPs).
    pub zone_manifest: ArtifactRef,
}

/// A batched update segment.
///
/// Segments are addressed deterministically by `segment_id` so PoPs can fetch them without listing:
/// - `segment_id = (seq-1) / segment_size`
/// - `base_seq = segment_id * segment_size + 1`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateSegment {
    /// Format version.
    pub format_version: u16,
    /// Segment identifier.
    pub segment_id: u64,
    /// Number of entries per segment (fixed for a stream).
    pub segment_size: u64,
    /// First sequence number in this segment.
    pub base_seq: u64,
    /// Last sequence number written in this segment (inclusive).
    pub last_seq: u64,
    /// Entries, in order, starting at `base_seq`.
    pub entries: Vec<UpdateEntry>,
}

impl UpdateSegment {
    /// Validate basic invariants.
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version == 0 || self.format_version > CURRENT_UPDATE_STREAM_FORMAT_VERSION {
            return Err(format!("unsupported format_version {}", self.format_version));
        }
        if self.segment_size == 0 {
            return Err("segment_size must be non-zero".into());
        }
        let expected_base = self.segment_id.saturating_mul(self.segment_size).saturating_add(1);
        if self.base_seq != expected_base {
            return Err("base_seq must equal segment_id*segment_size+1".into());
        }
        if self.entries.len() as u64 > self.segment_size {
            return Err("entries exceed segment_size".into());
        }
        if self.entries.is_empty() {
            if self.last_seq != self.base_seq.saturating_sub(1) {
                return Err("empty segment must have last_seq == base_seq-1".into());
            }
            return Ok(());
        }
        let expected_last = self.base_seq.saturating_add((self.entries.len() - 1) as u64);
        if self.last_seq != expected_last {
            return Err("last_seq does not match entries length".into());
        }
        Ok(())
    }
}

/// Pointer to the latest update sequence in the stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdatesLatest {
    /// Format version.
    pub format_version: u16,
    /// Segment sizing for this stream.
    pub segment_size: u64,
    /// Latest sequence number published (inclusive).
    pub last_seq: u64,
    /// Schema/format version of referenced per-zone manifests.
    pub zone_manifest_format_version: u16,
}

impl Default for UpdatesLatest {
    fn default() -> Self {
        Self {
            format_version: CURRENT_UPDATE_STREAM_FORMAT_VERSION,
            segment_size: 1024,
            last_seq: 0,
            zone_manifest_format_version: CURRENT_MANIFEST_FORMAT_VERSION,
        }
    }
}

