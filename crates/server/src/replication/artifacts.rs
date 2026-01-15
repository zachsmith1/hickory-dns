//! Types for describing replication artifacts (manifests, snapshots, deltas).
//!
//! These types are designed for "artifact-based replication" where a writer publishes immutable
//! objects (snapshots and delta segments) and updates a small manifest to point to the latest
//! committed generation.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Current supported manifest format version.
pub const CURRENT_MANIFEST_FORMAT_VERSION: u16 = 1;

/// Hex-encoded SHA-256 digest (64 lowercase/uppercase hex chars).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sha256Digest(pub String);

impl Sha256Digest {
    /// Returns true if this digest is a 64-character hex string.
    pub fn is_valid(&self) -> bool {
        let s = self.0.as_bytes();
        if s.len() != 64 {
            return false;
        }
        s.iter().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F'))
    }
}

/// A reference to an immutable artifact in object storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// A storage URI (e.g. `s3://bucket/key`, `gs://...`, `file://...`).
    pub uri: String,
    /// Content checksum for integrity verification.
    pub sha256: Sha256Digest,
}

impl ArtifactRef {
    /// Validate basic invariants for referencing an artifact.
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        if self.uri.is_empty() {
            return Err(ManifestValidationError::EmptyUri);
        }
        if !self.sha256.is_valid() {
            return Err(ManifestValidationError::InvalidSha256);
        }
        Ok(())
    }
}

/// Global index manifest that points to per-zone manifests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalManifest {
    /// Manifest format version.
    pub format_version: u16,
    /// Zones keyed by a stable zone identifier.
    pub zones: BTreeMap<String, ZoneIndexEntry>,
}

impl GlobalManifest {
    /// Validate this manifest against basic invariants and the current supported format version.
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        if self.format_version == 0 || self.format_version > CURRENT_MANIFEST_FORMAT_VERSION {
            return Err(ManifestValidationError::UnsupportedFormatVersion(
                self.format_version,
            ));
        }
        for (zone_id, entry) in &self.zones {
            if zone_id.is_empty() {
                return Err(ManifestValidationError::EmptyZoneId);
            }
            entry.validate()?;
        }
        Ok(())
    }
}

/// A single zone entry in the global manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZoneIndexEntry {
    /// Canonical DNS name for the zone origin (e.g. `example.com.`).
    pub origin: String,
    /// Latest committed generation for the zone.
    pub latest_generation: u64,
    /// Reference to a per-zone manifest.
    pub zone_manifest: ArtifactRef,
}

impl ZoneIndexEntry {
    /// Validate this entry.
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        if self.origin.is_empty() {
            return Err(ManifestValidationError::EmptyOrigin);
        }
        self.zone_manifest.validate()?;
        Ok(())
    }
}

/// Per-zone manifest describing how to materialize the latest committed generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZoneManifest {
    /// Manifest format version.
    pub format_version: u16,
    /// Stable zone identifier.
    pub zone_id: String,
    /// Canonical DNS name for the zone origin (e.g. `example.com.`).
    pub origin: String,
    /// Latest committed generation for the zone.
    pub latest_generation: u64,
    /// Base snapshot to load before applying deltas.
    pub snapshot: SnapshotRef,
    /// Delta segments to apply in order after loading the snapshot.
    #[serde(default)]
    pub deltas: Vec<DeltaSegmentRef>,
}

impl ZoneManifest {
    /// Validates basic invariants:
    /// - supported format version
    /// - snapshot generation is not ahead of latest_generation
    /// - delta segments, if present, cover a contiguous range from snapshot+1..=latest
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        if self.format_version == 0 || self.format_version > CURRENT_MANIFEST_FORMAT_VERSION {
            return Err(ManifestValidationError::UnsupportedFormatVersion(
                self.format_version,
            ));
        }
        if self.zone_id.is_empty() {
            return Err(ManifestValidationError::EmptyZoneId);
        }
        if self.origin.is_empty() {
            return Err(ManifestValidationError::EmptyOrigin);
        }
        self.snapshot.artifact.validate()?;

        if self.snapshot.base_generation > self.latest_generation {
            return Err(ManifestValidationError::SnapshotAheadOfLatest {
                snapshot: self.snapshot.base_generation,
                latest: self.latest_generation,
            });
        }

        if self.deltas.is_empty() {
            return Ok(());
        }

        // Verify ordering and contiguity.
        let mut expected_from = self.snapshot.base_generation.saturating_add(1);
        for seg in &self.deltas {
            seg.artifact.validate()?;
            if seg.from != expected_from {
                return Err(ManifestValidationError::DeltaGapOrOverlap {
                    expected_from,
                    actual_from: seg.from,
                });
            }
            if seg.to < seg.from {
                return Err(ManifestValidationError::InvalidDeltaRange {
                    from: seg.from,
                    to: seg.to,
                });
            }
            expected_from = seg.to.saturating_add(1);
        }

        let last_to = self.deltas.last().map(|d| d.to).unwrap_or(self.snapshot.base_generation);
        if last_to != self.latest_generation {
            return Err(ManifestValidationError::LatestNotCoveredByDeltas {
                last_to,
                latest: self.latest_generation,
            });
        }

        Ok(())
    }
}

/// Snapshot artifact for a zone at a base generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    /// Generation represented by this snapshot.
    pub base_generation: u64,
    /// Artifact location and checksum.
    #[serde(flatten)]
    pub artifact: ArtifactRef,
}

/// Delta segment artifact covering a contiguous range of generations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaSegmentRef {
    /// First generation included in this segment (inclusive).
    pub from: u64,
    /// Last generation included in this segment (inclusive).
    pub to: u64,
    /// Artifact location and checksum.
    #[serde(flatten)]
    pub artifact: ArtifactRef,
}

/// Manifest validation failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ManifestValidationError {
    #[error("unsupported manifest format version: {0}")]
    /// The manifest format is unknown to this binary.
    UnsupportedFormatVersion(u16),
    #[error("zone_id must not be empty")]
    /// Zone identifiers must be non-empty.
    EmptyZoneId,
    #[error("origin must not be empty")]
    /// Zone origins must be non-empty.
    EmptyOrigin,
    #[error("uri must not be empty")]
    /// Artifact URIs must be non-empty.
    EmptyUri,
    #[error("invalid sha256 digest")]
    /// Artifact digest is not a valid SHA-256 hex string.
    InvalidSha256,
    #[error("snapshot generation {snapshot} is ahead of latest {latest}")]
    /// Snapshot base generation must not be ahead of the latest generation.
    SnapshotAheadOfLatest {
        /// Snapshot base generation.
        snapshot: u64,
        /// Latest committed generation.
        latest: u64,
    },
    #[error("invalid delta range: from={from} to={to}")]
    /// Delta segment range must satisfy `from <= to`.
    InvalidDeltaRange {
        /// First generation (inclusive).
        from: u64,
        /// Last generation (inclusive).
        to: u64,
    },
    #[error("delta gap/overlap: expected from={expected_from} got from={actual_from}")]
    /// Delta segments must be contiguous and ordered.
    DeltaGapOrOverlap {
        /// Expected next `from` value.
        expected_from: u64,
        /// Actual `from` value in the segment.
        actual_from: u64,
    },
    #[error("latest generation {latest} not covered by deltas (last_to={last_to})")]
    /// The final delta segment must cover the latest generation.
    LatestNotCoveredByDeltas {
        /// `to` value of the final delta segment.
        last_to: u64,
        /// Latest committed generation.
        latest: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha() -> Sha256Digest {
        Sha256Digest("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into())
    }

    #[test]
    fn sha256_validation_accepts_hex_64() {
        assert!(sha().is_valid());
        assert!(!Sha256Digest("nope".into()).is_valid());
    }

    #[test]
    fn zone_manifest_validate_allows_snapshot_only() {
        let m = ZoneManifest {
            format_version: CURRENT_MANIFEST_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            latest_generation: 10,
            snapshot: SnapshotRef {
                base_generation: 10,
                artifact: ArtifactRef {
                    uri: "s3://bucket/z1/snap-10.bin".into(),
                    sha256: sha(),
                },
            },
            deltas: vec![],
        };
        assert_eq!(m.validate(), Ok(()));
    }

    #[test]
    fn zone_manifest_validate_rejects_gapped_deltas() {
        let m = ZoneManifest {
            format_version: CURRENT_MANIFEST_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            latest_generation: 5,
            snapshot: SnapshotRef {
                base_generation: 1,
                artifact: ArtifactRef {
                    uri: "s3://bucket/z1/snap-1.bin".into(),
                    sha256: sha(),
                },
            },
            deltas: vec![DeltaSegmentRef {
                from: 3,
                to: 5,
                artifact: ArtifactRef {
                    uri: "s3://bucket/z1/delta-3-5.bin".into(),
                    sha256: sha(),
                },
            }],
        };
        assert_eq!(
            m.validate(),
            Err(ManifestValidationError::DeltaGapOrOverlap {
                expected_from: 2,
                actual_from: 3
            })
        );
    }

    #[test]
    fn zone_manifest_validate_accepts_contiguous_deltas() {
        let m = ZoneManifest {
            format_version: CURRENT_MANIFEST_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            latest_generation: 5,
            snapshot: SnapshotRef {
                base_generation: 1,
                artifact: ArtifactRef {
                    uri: "s3://bucket/z1/snap-1.bin".into(),
                    sha256: sha(),
                },
            },
            deltas: vec![
                DeltaSegmentRef {
                    from: 2,
                    to: 3,
                    artifact: ArtifactRef {
                        uri: "s3://bucket/z1/delta-2-3.bin".into(),
                        sha256: sha(),
                    },
                },
                DeltaSegmentRef {
                    from: 4,
                    to: 5,
                    artifact: ArtifactRef {
                        uri: "s3://bucket/z1/delta-4-5.bin".into(),
                        sha256: sha(),
                    },
                },
            ],
        };
        assert_eq!(m.validate(), Ok(()));
    }

    #[test]
    fn json_roundtrip_zone_manifest() {
        let m = ZoneManifest {
            format_version: CURRENT_MANIFEST_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            latest_generation: 5,
            snapshot: SnapshotRef {
                base_generation: 1,
                artifact: ArtifactRef {
                    uri: "s3://bucket/z1/snap-1.bin".into(),
                    sha256: sha(),
                },
            },
            deltas: vec![DeltaSegmentRef {
                from: 2,
                to: 5,
                artifact: ArtifactRef {
                    uri: "s3://bucket/z1/delta-2-5.bin".into(),
                    sha256: sha(),
                },
            }],
        };

        let json = serde_json::to_string(&m).unwrap();
        let decoded: ZoneManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, m);
        assert_eq!(decoded.validate(), Ok(()));
    }
}

