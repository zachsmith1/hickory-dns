//! Replication building blocks (feature-gated).
//!
//! This module is behind the `replication` crate feature. It is not used by the server by default.
//! The initial scope is small, testable primitives that let a replicated zone present Hickory with
//! an immutable serving view that can be atomically swapped.

pub mod soa_serial;
pub mod swap;
pub mod artifacts;
pub mod zone_view;

