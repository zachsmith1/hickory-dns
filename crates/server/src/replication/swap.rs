//! A minimal hot-swap primitive for `Arc<T>` without additional dependencies.
//!
//! This is intended for "atomic activation" patterns: a writer prepares a new immutable view
//! (e.g. a zone snapshot) and then swaps a single pointer so readers switch over at once.

use std::sync::{Arc, RwLock};

/// A cheaply clonable handle that allows atomically swapping the active `Arc<T>`.
///
/// Semantics:
/// - `load()` returns an `Arc<T>` that remains valid even if a concurrent `store()` occurs.
/// - `store()` swaps the active value as a single pointer update.
///
/// Implementation note: this uses a `RwLock` rather than lock-free atomics. For our expected
/// pattern (many reads, rare swaps) this is typically sufficient and keeps dependencies minimal.
#[derive(Debug)]
pub struct ArcSwap<T: ?Sized> {
    inner: RwLock<Arc<T>>,
}

impl<T: ?Sized> ArcSwap<T> {
    /// Create a new [`ArcSwap`] holding `value`.
    pub fn new(value: Arc<T>) -> Self {
        Self {
            inner: RwLock::new(value),
        }
    }

    /// Load the currently active value.
    pub fn load(&self) -> Arc<T> {
        self.inner
            .read()
            .expect("ArcSwap lock poisoned")
            .clone()
    }

    /// Store a new active value.
    pub fn store(&self, value: Arc<T>) {
        *self.inner.write().expect("ArcSwap lock poisoned") = value;
    }
}

impl<T> Default for ArcSwap<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::new(Arc::new(T::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::ArcSwap;
    use std::sync::Arc;

    #[test]
    fn load_returns_a_stable_arc_even_after_store() {
        let swap = ArcSwap::new(Arc::new(String::from("v1")));

        let v1 = swap.load();
        assert_eq!(v1.as_str(), "v1");

        swap.store(Arc::new(String::from("v2")));

        // previously loaded Arc remains valid and unchanged
        assert_eq!(v1.as_str(), "v1");
        // new loads see the updated value
        let v2 = swap.load();
        assert_eq!(v2.as_str(), "v2");
    }
}

