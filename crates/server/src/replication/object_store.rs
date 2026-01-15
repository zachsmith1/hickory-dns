//! Object storage helpers for replication artifacts.
//!
//! This module provides a small polling/fetch layer on top of the [`object_store`] crate.
//! The intent is:
//! - keep the replication logic cloud-agnostic (S3/GCS/Azure/local)
//! - make PoP replication easy to implement as “poll manifest → fetch artifacts”

use std::marker::PhantomData;
use std::sync::Arc;

use object_store::ObjectStore;
use serde::de::DeserializeOwned;
use thiserror::Error;

/// Errors from interacting with object storage or decoding manifests.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// Error from the underlying object_store implementation.
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    /// JSON decoding error for a manifest.
    #[error("manifest json decode error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChangeToken {
    etag: Option<String>,
    last_modified: String,
    size: u64,
}

impl ChangeToken {
    fn from_meta(meta: &object_store::ObjectMeta) -> Self {
        Self {
            etag: meta.e_tag.clone(),
            last_modified: format!("{:?}", meta.last_modified),
            size: meta.size,
        }
    }
}

/// Polls a JSON manifest stored in an object store.
///
/// The poller caches a "change token" and returns `None` if the object hasn't changed since the
/// last successful fetch.
pub struct JsonManifestPoller<M> {
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    last_seen: Option<ChangeToken>,
    _phantom: PhantomData<M>,
}

impl<M> JsonManifestPoller<M>
where
    M: DeserializeOwned,
{
    /// Create a new poller for a manifest at `location`.
    pub fn new(store: Arc<dyn ObjectStore>, location: object_store::path::Path) -> Self {
        Self {
            store,
            location,
            last_seen: None,
            _phantom: PhantomData,
        }
    }

    /// Returns the last seen change token (if any).
    pub fn last_seen(&self) -> Option<(Option<&str>, &str, usize)> {
        self.last_seen.as_ref().map(|t| {
            (
                t.etag.as_deref(),
                t.last_modified.as_str(),
                t.size as usize,
            )
        })
    }

    /// Fetch the manifest if it has changed since the last successful fetch.
    ///
    /// Returns:
    /// - `Ok(Some(manifest))` if the object changed and was fetched+decoded
    /// - `Ok(None)` if the object appears unchanged
    pub async fn fetch_if_changed(&mut self) -> Result<Option<M>, ObjectStoreError> {
        let meta = self.store.head(&self.location).await?;
        let token = ChangeToken::from_meta(&meta);

        if self.last_seen.as_ref() == Some(&token) {
            return Ok(None);
        }

        let get = self.store.get(&self.location).await?;
        let bytes = get.bytes().await?;
        let manifest: M = serde_json::from_slice(&bytes)?;

        self.last_seen = Some(token);
        Ok(Some(manifest))
    }
}

#[cfg(test)]
mod tests {
    use super::JsonManifestPoller;
    use crate::replication::artifacts::{CURRENT_MANIFEST_FORMAT_VERSION, GlobalManifest};
    use bytes::Bytes;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;
    use std::sync::Arc;

    struct TempDir {
        path: std::path::PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let mut path = std::env::temp_dir();
            let nonce = format!(
                "hickory-repl-{}-{}",
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

    #[tokio::test]
    async fn poller_returns_none_when_unchanged() {
        let dir = TempDir::new();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir.path).unwrap());

        let loc = ObjPath::from("manifest.json");
        let m = GlobalManifest {
            format_version: CURRENT_MANIFEST_FORMAT_VERSION,
            zones: Default::default(),
        };
        let json = serde_json::to_vec(&m).unwrap();
        store.put(&loc, Bytes::from(json).into()).await.unwrap();

        let mut poller = JsonManifestPoller::<GlobalManifest>::new(store.clone(), loc.clone());

        let first = poller.fetch_if_changed().await.unwrap();
        assert!(first.is_some());

        let second = poller.fetch_if_changed().await.unwrap();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn poller_returns_some_when_updated() {
        let dir = TempDir::new();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir.path).unwrap());

        let loc = ObjPath::from("manifest.json");
        let m1 = GlobalManifest {
            format_version: CURRENT_MANIFEST_FORMAT_VERSION,
            zones: Default::default(),
        };
        store
            .put(&loc, Bytes::from(serde_json::to_vec(&m1).unwrap()).into())
            .await
            .unwrap();

        let mut poller = JsonManifestPoller::<GlobalManifest>::new(store.clone(), loc.clone());
        poller.fetch_if_changed().await.unwrap();

        // Update file contents.
        let mut m2 = m1.clone();
        m2.zones.insert(
            "z1".into(),
            crate::replication::artifacts::ZoneIndexEntry {
                origin: "example.com.".into(),
                latest_generation: 1,
                zone_manifest: crate::replication::artifacts::ArtifactRef {
                    uri: "file://zone/z1/manifest.json".into(),
                    sha256: crate::replication::artifacts::Sha256Digest(
                        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
                    ),
                },
            },
        );
        store
            .put(&loc, Bytes::from(serde_json::to_vec(&m2).unwrap()).into())
            .await
            .unwrap();

        let changed = poller.fetch_if_changed().await.unwrap();
        assert!(changed.is_some());
    }
}

