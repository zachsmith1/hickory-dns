//! Replication building blocks (feature-gated).
//!
//! This module is behind the `replication` crate feature. It is not used by the server by default.
//! The initial scope is small, testable primitives that let a replicated zone present Hickory with
//! an immutable serving view that can be atomically swapped.

pub mod soa_serial;
pub mod swap;
pub mod artifacts;
pub mod delta;
pub mod snapshot;
pub mod object_store;
pub mod zone_view;
pub mod update_stream;

#[cfg(feature = "replication-redb")]
pub mod redb_store;

#[cfg(feature = "replication-replicator")]
pub mod replicator;

#[cfg(feature = "replication-authority")]
pub mod authority;

#[cfg(feature = "replication-publisher")]
pub mod publisher;

