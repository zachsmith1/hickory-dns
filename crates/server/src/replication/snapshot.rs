//! Snapshot encoding/decoding for artifact-based replication.
//!
//! A snapshot is an immutable, point-in-time view of a single zone at a given base generation.
//! The snapshot format is designed to be:
//! - deterministic (stable encoding for the same inputs)
//! - easy to stream (framed key/value pairs)
//! - explicitly versioned
//!
//! Compression is intentionally out of scope for this module; snapshots can be stored/transferred
//! compressed at the artifact layer if desired.

use std::collections::BTreeMap;

use thiserror::Error;

/// Current supported snapshot format version.
pub const CURRENT_SNAPSHOT_FORMAT_VERSION: u16 = 1;

const MAGIC: &[u8; 8] = b"HDNSSNP1";

/// A zone snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZoneSnapshot {
    /// Snapshot format version.
    pub format_version: u16,
    /// Stable zone identifier.
    pub zone_id: String,
    /// Canonical DNS name for the zone origin (e.g. `example.com.`).
    pub origin: String,
    /// Base generation represented by this snapshot.
    pub base_generation: u64,
    /// Snapshot contents as an ordered key/value map.
    ///
    /// Keys and values are format-defined by the store layer. They are treated as opaque bytes by
    /// the replication layer.
    pub entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl ZoneSnapshot {
    /// Encode the snapshot as a deterministic byte vector.
    pub fn encode_to_vec(&self) -> Result<Vec<u8>, SnapshotError> {
        if self.format_version == 0 || self.format_version > CURRENT_SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotError::UnsupportedFormatVersion(self.format_version));
        }
        if self.zone_id.is_empty() {
            return Err(SnapshotError::EmptyZoneId);
        }
        if self.origin.is_empty() {
            return Err(SnapshotError::EmptyOrigin);
        }

        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        put_u16(&mut out, self.format_version);
        put_u64(&mut out, self.base_generation);
        put_bytes(&mut out, self.zone_id.as_bytes())?;
        put_bytes(&mut out, self.origin.as_bytes())?;

        put_u32(&mut out, self.entries.len().try_into().map_err(|_| {
            SnapshotError::TooManyEntries {
                count: self.entries.len(),
            }
        })?);

        for (k, v) in &self.entries {
            put_bytes(&mut out, k)?;
            put_bytes(&mut out, v)?;
        }

        Ok(out)
    }

    /// Decode a snapshot from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let mut c = Cursor::new(bytes);

        let magic = c.take(8)?;
        if magic != MAGIC {
            return Err(SnapshotError::InvalidMagic);
        }

        let format_version = c.u16()?;
        if format_version == 0 || format_version > CURRENT_SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotError::UnsupportedFormatVersion(format_version));
        }

        let base_generation = c.u64()?;
        let zone_id = String::from_utf8(c.bytes()?.to_vec()).map_err(SnapshotError::InvalidUtf8)?;
        let origin = String::from_utf8(c.bytes()?.to_vec()).map_err(SnapshotError::InvalidUtf8)?;

        if zone_id.is_empty() {
            return Err(SnapshotError::EmptyZoneId);
        }
        if origin.is_empty() {
            return Err(SnapshotError::EmptyOrigin);
        }

        let entry_count = c.u32()? as usize;
        let mut entries = BTreeMap::new();
        for _ in 0..entry_count {
            let k = c.bytes()?.to_vec();
            let v = c.bytes()?.to_vec();
            entries.insert(k, v);
        }

        if !c.is_eof() {
            return Err(SnapshotError::TrailingBytes);
        }

        Ok(Self {
            format_version,
            zone_id,
            origin,
            base_generation,
            entries,
        })
    }
}

/// Snapshot decode/validation errors.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// Magic bytes did not match the expected snapshot format.
    #[error("invalid snapshot magic")]
    InvalidMagic,
    /// Snapshot format version is unknown to this binary.
    #[error("unsupported snapshot format version: {0}")]
    UnsupportedFormatVersion(u16),
    /// Snapshot was truncated.
    #[error("unexpected end of snapshot")]
    UnexpectedEof,
    /// Snapshot contains trailing bytes after decoding the expected content.
    #[error("trailing bytes after snapshot decode")]
    TrailingBytes,
    /// `zone_id` must not be empty.
    #[error("zone_id must not be empty")]
    EmptyZoneId,
    /// `origin` must not be empty.
    #[error("origin must not be empty")]
    EmptyOrigin,
    /// A length exceeded an internal bound.
    #[error("entry count too large: {count}")]
    TooManyEntries {
        /// Entry count.
        count: usize,
    },
    /// A UTF-8 string field failed to decode.
    #[error("invalid utf-8 string in snapshot")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    /// A field length exceeded the allowed maximum.
    #[error("field length too large: {len}")]
    FieldTooLarge {
        /// Field length.
        len: usize,
    },
}

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SnapshotError> {
    // Keep a simple bound so corrupt inputs can't cause massive allocations on decode.
    const MAX: usize = 16 * 1024 * 1024;
    if bytes.len() > MAX {
        return Err(SnapshotError::FieldTooLarge { len: bytes.len() });
    }
    let len: u32 = bytes
        .len()
        .try_into()
        .map_err(|_| SnapshotError::FieldTooLarge { len: bytes.len() })?;
    put_u32(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos == self.bytes.len()
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], SnapshotError> {
        let end = self.pos.saturating_add(n);
        if end > self.bytes.len() {
            return Err(SnapshotError::UnexpectedEof);
        }
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u16(&mut self) -> Result<u16, SnapshotError> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, SnapshotError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64, SnapshotError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn bytes(&mut self) -> Result<&'a [u8], SnapshotError> {
        // Keep decode bound consistent with encode.
        const MAX: usize = 16 * 1024 * 1024;
        let len = self.u32()? as usize;
        if len > MAX {
            return Err(SnapshotError::FieldTooLarge { len });
        }
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_is_lossless_and_deterministic() {
        let mut entries = BTreeMap::new();
        entries.insert(b"k1".to_vec(), b"v1".to_vec());
        entries.insert(b"k2".to_vec(), b"v2".to_vec());

        let snap = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 42,
            entries,
        };

        let a = snap.encode_to_vec().unwrap();
        let b = snap.encode_to_vec().unwrap();
        assert_eq!(a, b);

        let decoded = ZoneSnapshot::decode(&a).unwrap();
        assert_eq!(decoded, snap);
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let bytes = b"notasnap".to_vec();
        assert!(matches!(
            ZoneSnapshot::decode(&bytes),
            Err(SnapshotError::InvalidMagic)
        ));
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let snap = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries: BTreeMap::new(),
        };
        let bytes = snap.encode_to_vec().unwrap();

        for cut in 0..bytes.len() {
            let truncated = &bytes[..cut];
            let res = ZoneSnapshot::decode(truncated);
            if cut == bytes.len() {
                assert!(res.is_ok());
            } else {
                assert!(matches!(res, Err(SnapshotError::UnexpectedEof)));
            }
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let snap = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries: BTreeMap::new(),
        };
        let mut bytes = snap.encode_to_vec().unwrap();
        bytes.extend_from_slice(b"garbage");
        assert!(matches!(
            ZoneSnapshot::decode(&bytes),
            Err(SnapshotError::TrailingBytes)
        ));
    }
}

