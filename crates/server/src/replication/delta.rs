//! Delta segment encoding and application.
//!
//! A delta segment is an ordered list of mutations for a single zone that advances the zone from
//! generation `from` to generation `to` (inclusive). Segments are designed to be:
//! - **deterministic**: apply in order to reach the same materialized state everywhere
//! - **streamable**: decode without requiring a full snapshot of all state
//! - **versioned**: schema evolution is explicit

use thiserror::Error;

#[cfg(test)]
use std::collections::BTreeMap;

/// Current supported delta schema version.
pub const CURRENT_DELTA_SCHEMA_VERSION: u32 = 1;

/// A decoded delta segment, independent of any on-wire encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZoneDeltaSegment {
    /// Schema version for the encoded segment.
    pub schema_version: u32,
    /// The first generation covered by this segment (inclusive).
    pub from: u64,
    /// The last generation covered by this segment (inclusive).
    pub to: u64,
    /// Ordered mutation operations.
    pub ops: Vec<ZoneDeltaOp>,
}

impl ZoneDeltaSegment {
    /// Validate basic invariants for this segment.
    pub fn validate(&self) -> Result<(), DeltaError> {
        if self.schema_version == 0 || self.schema_version > CURRENT_DELTA_SCHEMA_VERSION {
            return Err(DeltaError::UnsupportedSchemaVersion(self.schema_version));
        }
        if self.to < self.from {
            return Err(DeltaError::InvalidRange {
                from: self.from,
                to: self.to,
            });
        }
        Ok(())
    }

    /// Encode this segment as a protobuf message.
    #[cfg(feature = "replication")]
    pub fn encode_protobuf_to_vec(&self) -> Vec<u8> {
        pb::DeltaSegment::from_domain(self).encode_to_vec()
    }

    /// Decode a protobuf-encoded segment.
    #[cfg(feature = "replication")]
    pub fn decode_protobuf(bytes: &[u8]) -> Result<Self, DeltaError> {
        pb::DeltaSegment::decode_from_slice(bytes)?.into_domain()
    }
}

/// A single mutation in a zone delta segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZoneDeltaOp {
    /// Upsert an RRset at (owner, rrtype).
    UpsertRrset {
        /// Owner name for the RRset (FQDN, with trailing dot).
        owner_fqdn: String,
        /// RR type code (e.g. A=1, AAAA=28).
        rrtype: u16,
        /// TTL for the RRset.
        ttl: u32,
        /// RDATA payloads for the RRset, encoded as DNS wire-format RDATA bytes.
        ///
        /// This is *not* the full RR wire encoding (it excludes owner/type/class/ttl and the
        /// per-record RDLENGTH).
        rdata_wire: Vec<Vec<u8>>,
    },
    /// Delete an RRset at (owner, rrtype).
    DeleteRrset {
        /// Owner name for the RRset (FQDN, with trailing dot).
        owner_fqdn: String,
        /// RR type code (e.g. A=1, AAAA=28).
        rrtype: u16,
    },
}

/// A write surface for applying zone deltas.
pub trait ZoneWritable {
    /// Upsert an RRset at (owner, rrtype).
    fn upsert_rrset(&mut self, owner_fqdn: &str, rrtype: u16, rrset: RrsetData)
        -> Result<(), DeltaError>;

    /// Delete an RRset at (owner, rrtype).
    fn delete_rrset(&mut self, owner_fqdn: &str, rrtype: u16) -> Result<(), DeltaError>;
}

/// Minimal RRset payload for replication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RrsetData {
    /// TTL for the RRset.
    pub ttl: u32,
    /// RDATA payloads for the RRset, encoded as DNS wire-format RDATA bytes.
    pub rdata_wire: Vec<Vec<u8>>,
}

/// Applies delta segments to a zone while enforcing generation continuity.
#[derive(Debug)]
pub struct ZoneDeltaApplier {
    next_expected_generation: u64,
}

impl ZoneDeltaApplier {
    /// Create a new applier, expecting the first segment to start at `next_expected_generation`.
    pub fn new(next_expected_generation: u64) -> Self {
        Self {
            next_expected_generation,
        }
    }

    /// Returns the generation that the next segment must start at.
    pub fn next_expected_generation(&self) -> u64 {
        self.next_expected_generation
    }

    /// Apply a segment and advance `next_expected_generation`.
    pub fn apply_segment<S: ZoneWritable>(
        &mut self,
        store: &mut S,
        segment: &ZoneDeltaSegment,
    ) -> Result<(), DeltaError> {
        segment.validate()?;
        if segment.from != self.next_expected_generation {
            return Err(DeltaError::UnexpectedFromGeneration {
                expected: self.next_expected_generation,
                actual: segment.from,
            });
        }

        for op in &segment.ops {
            match op {
                ZoneDeltaOp::UpsertRrset {
                    owner_fqdn,
                    rrtype,
                    ttl,
                    rdata_wire,
                } => store.upsert_rrset(
                    owner_fqdn,
                    *rrtype,
                    RrsetData {
                        ttl: *ttl,
                        rdata_wire: rdata_wire.clone(),
                    },
                )?,
                ZoneDeltaOp::DeleteRrset { owner_fqdn, rrtype } => {
                    store.delete_rrset(owner_fqdn, *rrtype)?
                }
            }
        }

        self.next_expected_generation = segment.to.saturating_add(1);
        Ok(())
    }
}

/// Protobuf encoding for delta segments.
///
/// We use `prost` with Rust-defined message types (no build script/codegen). This keeps the delta
/// format explicit and versioned while remaining easy to evolve.
#[cfg(feature = "replication")]
mod pb {
    use prost::Message;

    use super::{ZoneDeltaOp, ZoneDeltaSegment};

    #[derive(Clone, PartialEq, Message)]
    pub(super) struct DeltaSegment {
        #[prost(uint32, tag = "1")]
        schema_version: u32,
        #[prost(uint64, tag = "2")]
        from: u64,
        #[prost(uint64, tag = "3")]
        to: u64,
        #[prost(message, repeated, tag = "4")]
        ops: Vec<DeltaOp>,
    }

    #[derive(Clone, PartialEq, Message)]
    struct DeltaOp {
        #[prost(oneof = "delta_op::Op", tags = "1, 2")]
        op: Option<delta_op::Op>,
    }

    pub(super) mod delta_op {
        use prost::Oneof;

        use super::{DeleteRrset, UpsertRrset};

        #[derive(Clone, PartialEq, Oneof)]
        pub(super) enum Op {
            #[prost(message, tag = "1")]
            Upsert(UpsertRrset),
            #[prost(message, tag = "2")]
            Delete(DeleteRrset),
        }
    }

    #[derive(Clone, PartialEq, Message)]
    struct UpsertRrset {
        #[prost(string, tag = "1")]
        owner_fqdn: String,
        #[prost(uint32, tag = "2")]
        rrtype: u32,
        #[prost(uint32, tag = "3")]
        ttl: u32,
        #[prost(bytes, repeated, tag = "4")]
        rdata_wire: Vec<Vec<u8>>,
    }

    #[derive(Clone, PartialEq, Message)]
    struct DeleteRrset {
        #[prost(string, tag = "1")]
        owner_fqdn: String,
        #[prost(uint32, tag = "2")]
        rrtype: u32,
    }

    impl DeltaSegment {
        pub(super) fn from_domain(seg: &ZoneDeltaSegment) -> Self {
            Self {
                schema_version: seg.schema_version,
                from: seg.from,
                to: seg.to,
                ops: seg
                    .ops
                    .iter()
                    .map(|op| match op {
                        ZoneDeltaOp::UpsertRrset {
                            owner_fqdn,
                            rrtype,
                            ttl,
                            rdata_wire,
                        } => DeltaOp {
                            op: Some(delta_op::Op::Upsert(UpsertRrset {
                                owner_fqdn: owner_fqdn.clone(),
                                rrtype: *rrtype as u32,
                                ttl: *ttl,
                                rdata_wire: rdata_wire.clone(),
                            })),
                        },
                        ZoneDeltaOp::DeleteRrset { owner_fqdn, rrtype } => DeltaOp {
                            op: Some(delta_op::Op::Delete(DeleteRrset {
                                owner_fqdn: owner_fqdn.clone(),
                                rrtype: *rrtype as u32,
                            })),
                        },
                    })
                    .collect(),
            }
        }

        pub(super) fn into_domain(self) -> Result<ZoneDeltaSegment, super::DeltaError> {
            let mut ops = Vec::with_capacity(self.ops.len());
            for op in self.ops {
                let Some(op) = op.op else {
                    return Err(super::DeltaError::MissingOp);
                };
                match op {
                    delta_op::Op::Upsert(u) => {
                        let rrtype: u16 = u.rrtype.try_into().map_err(|_| {
                            super::DeltaError::RrtypeOutOfRange { rrtype: u.rrtype }
                        })?;
                        ops.push(ZoneDeltaOp::UpsertRrset {
                            owner_fqdn: u.owner_fqdn,
                            rrtype,
                            ttl: u.ttl,
                            rdata_wire: u.rdata_wire,
                        });
                    }
                    delta_op::Op::Delete(d) => {
                        let rrtype: u16 = d.rrtype.try_into().map_err(|_| {
                            super::DeltaError::RrtypeOutOfRange { rrtype: d.rrtype }
                        })?;
                        ops.push(ZoneDeltaOp::DeleteRrset {
                            owner_fqdn: d.owner_fqdn,
                            rrtype,
                        });
                    }
                }
            }

            Ok(ZoneDeltaSegment {
                schema_version: self.schema_version,
                from: self.from,
                to: self.to,
                ops,
            })
        }

        pub(super) fn encode_to_vec(&self) -> Vec<u8> {
            let mut buf = Vec::with_capacity(self.encoded_len());
            self.encode(&mut buf).expect("Vec<u8> encode is infallible");
            buf
        }

        pub(super) fn decode_from_slice(bytes: &[u8]) -> Result<Self, super::DeltaError> {
            Self::decode(bytes).map_err(super::DeltaError::Decode)
        }
    }
}

/// Delta decode/validation/apply errors.
#[derive(Debug, Error)]
pub enum DeltaError {
    #[error("unsupported delta schema version: {0}")]
    /// Delta schema version is unknown to this binary.
    UnsupportedSchemaVersion(u32),
    #[error("invalid delta generation range: from={from} to={to}")]
    /// Delta range must satisfy `from <= to`.
    InvalidRange {
        /// First generation (inclusive).
        from: u64,
        /// Last generation (inclusive).
        to: u64,
    },
    #[error("unexpected delta segment start generation: expected={expected} actual={actual}")]
    /// The segment did not start at the expected generation.
    UnexpectedFromGeneration {
        /// Expected start generation.
        expected: u64,
        /// Actual start generation.
        actual: u64,
    },
    #[error("missing delta operation")]
    /// Encountered a protobuf DeltaOp without a `oneof` variant set.
    MissingOp,
    #[error("rrtype out of range for u16: {rrtype}")]
    /// RR type did not fit in a `u16`.
    RrtypeOutOfRange {
        /// Original rrtype value.
        rrtype: u32,
    },
    #[cfg(feature = "replication")]
    #[error("delta protobuf decode error: {0}")]
    /// Protobuf decoding failed.
    Decode(#[from] prost::DecodeError),

    #[error("empty owner_fqdn")]
    /// Owner names must be non-empty.
    EmptyOwner,
}

/// A simple in-memory zone store used for delta application tests.
#[cfg(test)]
#[derive(Default)]
struct TestZoneStore {
    rrsets: BTreeMap<(String, u16), RrsetData>,
}

#[cfg(test)]
impl ZoneWritable for TestZoneStore {
    fn upsert_rrset(
        &mut self,
        owner_fqdn: &str,
        rrtype: u16,
        rrset: RrsetData,
    ) -> Result<(), DeltaError> {
        if owner_fqdn.is_empty() {
            return Err(DeltaError::EmptyOwner);
        }
        self.rrsets
            .insert((owner_fqdn.to_string(), rrtype), rrset);
        Ok(())
    }

    fn delete_rrset(&mut self, owner_fqdn: &str, rrtype: u16) -> Result<(), DeltaError> {
        if owner_fqdn.is_empty() {
            return Err(DeltaError::EmptyOwner);
        }
        self.rrsets.remove(&(owner_fqdn.to_string(), rrtype));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_segment_enforces_contiguity() {
        let mut store = TestZoneStore::default();
        let mut applier = ZoneDeltaApplier::new(2);

        let seg = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 3,
            to: 3,
            ops: vec![],
        };

        assert!(matches!(
            applier.apply_segment(&mut store, &seg),
            Err(DeltaError::UnexpectedFromGeneration { expected: 2, actual: 3 })
        ));
    }

    #[test]
    fn apply_segment_mutates_store_deterministically() {
        let mut store = TestZoneStore::default();
        let mut applier = ZoneDeltaApplier::new(1);

        let seg = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 1,
            to: 2,
            ops: vec![
                ZoneDeltaOp::UpsertRrset {
                    owner_fqdn: "www.example.com.".into(),
                    rrtype: 1,
                    ttl: 60,
                    rdata_wire: vec![b"1.2.3.4".to_vec()],
                },
                ZoneDeltaOp::DeleteRrset {
                    owner_fqdn: "old.example.com.".into(),
                    rrtype: 1,
                },
            ],
        };

        applier.apply_segment(&mut store, &seg).unwrap();
        assert_eq!(applier.next_expected_generation(), 3);

        let key = ("www.example.com.".to_string(), 1u16);
        let rrset = store.rrsets.get(&key).unwrap();
        assert_eq!(rrset.ttl, 60);
        assert_eq!(rrset.rdata_wire, vec![b"1.2.3.4".to_vec()]);
    }

    #[test]
    #[cfg(feature = "replication")]
    fn protobuf_roundtrip_preserves_ops_order() {
        let seg = ZoneDeltaSegment {
            schema_version: CURRENT_DELTA_SCHEMA_VERSION,
            from: 10,
            to: 11,
            ops: vec![
                ZoneDeltaOp::DeleteRrset {
                    owner_fqdn: "a.example.com.".into(),
                    rrtype: 1,
                },
                ZoneDeltaOp::UpsertRrset {
                    owner_fqdn: "b.example.com.".into(),
                    rrtype: 28,
                    ttl: 30,
                    rdata_wire: vec![b"::1".to_vec()],
                },
            ],
        };

        let bytes = seg.encode_protobuf_to_vec();
        let decoded = ZoneDeltaSegment::decode_protobuf(&bytes).unwrap();

        assert_eq!(decoded, seg);
    }
}

