//! Hickory-facing immutable zone view primitives.
//!
//! The goal is to give Hickory an immutable, read-only view of a zone that can be swapped
//! atomically when replication applies a new generation.

use std::sync::Arc;

use crate::proto::rr::LowerName;

use super::swap::ArcSwap;

/// A read-only snapshot of a zone at a specific generation.
///
/// This trait is intentionally minimal in v1: it captures the metadata we need to reason about
/// correctness (origin + generation + SOA serial). Query APIs will be layered on later without
/// forcing Hickory internals to participate in replication.
pub trait ZoneView: Send + Sync {
    /// The zone origin (apex).
    fn origin(&self) -> &LowerName;

    /// Monotonic internal generation number for this zone.
    fn generation(&self) -> u64;

    /// SOA serial to serve on the wire for this zone view.
    fn soa_serial(&self) -> u32;
}

/// An atomically swappable provider for a single zone view.
///
/// This is the simplest building block for "atomic activation": replication stages a new view and
/// then swaps a single pointer so all future readers observe the new view at once.
pub struct ZoneViewProvider {
    active: ArcSwap<dyn ZoneView>,
}

impl ZoneViewProvider {
    /// Create a provider with an initial active view.
    pub fn new(initial: Arc<dyn ZoneView>) -> Self {
        Self {
            active: ArcSwap::new(initial),
        }
    }

    /// Load the currently active view.
    pub fn current(&self) -> Arc<dyn ZoneView> {
        self.active.load()
    }

    /// Atomically swap the active view.
    pub fn swap(&self, new_view: Arc<dyn ZoneView>) {
        self.active.store(new_view);
    }
}

#[cfg(test)]
mod tests {
    use super::{ZoneView, ZoneViewProvider};
    use std::sync::Arc;

    use crate::proto::rr::Name;

    #[derive(Debug)]
    struct TestView {
        origin: crate::proto::rr::LowerName,
        generation: u64,
        soa_serial: u32,
    }

    impl ZoneView for TestView {
        fn origin(&self) -> &crate::proto::rr::LowerName {
            &self.origin
        }

        fn generation(&self) -> u64 {
            self.generation
        }

        fn soa_serial(&self) -> u32 {
            self.soa_serial
        }
    }

    #[test]
    fn current_is_stable_across_swaps_for_existing_readers() {
        let origin = Name::from_ascii("example.com.").unwrap();

        let v1: Arc<dyn ZoneView> = Arc::new(TestView {
            origin: crate::proto::rr::LowerName::new(&origin),
            generation: 1,
            soa_serial: 2026011401,
        });
        let provider = ZoneViewProvider::new(v1.clone());

        let reader_view = provider.current();
        assert_eq!(reader_view.generation(), 1);

        let v2: Arc<dyn ZoneView> = Arc::new(TestView {
            origin: crate::proto::rr::LowerName::new(&origin),
            generation: 2,
            soa_serial: 2026011402,
        });
        provider.swap(v2);

        // Existing reader holds a stable snapshot (Arc) even after swap.
        assert_eq!(reader_view.generation(), 1);
        assert_eq!(reader_view.soa_serial(), 2026011401);

        // New reads observe the updated view.
        let now = provider.current();
        assert_eq!(now.generation(), 2);
        assert_eq!(now.soa_serial(), 2026011402);
    }
}

