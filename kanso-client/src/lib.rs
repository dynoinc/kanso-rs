use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/// Error type for object store operations
#[derive(Debug, Error)]
pub enum Error {
    // Empty for now - will be populated as implementations are added
}

/// Represents a version/etag for an object in the store
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Version(String);

impl Version {
    /// Create a new version from a string
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Get the version as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for Version {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Version {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Condition for conditional writes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Condition {
    /// Only write if the key does not exist
    IfAbsent,
    /// Only write if the current version matches the specified version
    IfVersionMatches(Version),
}

/// Trait representing an object store client
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Get the value associated with a key
    ///
    /// Returns `Ok(None)` if the key does not exist
    async fn get(&self, key: String) -> Result<Option<Bytes>, Error>;

    /// Put a value associated with a key, optionally with a condition
    ///
    /// Returns the new version of the stored object
    async fn put(
        &self,
        key: String,
        value: Bytes,
        condition: Option<Condition>,
    ) -> Result<Version, Error>;
}

/// Type alias for the object store client
///
/// Users should use this type to interact with the object store
pub type Client = Arc<dyn ObjectStore>;
