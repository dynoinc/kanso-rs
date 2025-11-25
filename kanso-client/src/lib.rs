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

/// Request builder for get operations
pub struct GetRequest {
    store: Client,
    key: String,
}

impl GetRequest {
    /// Execute the get request
    pub async fn execute(self) -> Result<Option<Bytes>, Error> {
        self.store.execute_get(self.key).await
    }
}

/// Request builder for put operations
pub struct PutRequest {
    store: Client,
    key: String,
    value: Bytes,
    condition: Option<Condition>,
}

impl PutRequest {
    /// Set the condition to only write if the key does not exist
    pub fn if_absent(mut self) -> Self {
        self.condition = Some(Condition::IfAbsent);
        self
    }

    /// Set the condition to only write if the current version matches
    pub fn if_version_matches(mut self, version: Version) -> Self {
        self.condition = Some(Condition::IfVersionMatches(version));
        self
    }

    /// Execute the put request
    pub async fn execute(self) -> Result<Version, Error> {
        self.store
            .execute_put(self.key, self.value, self.condition)
            .await
    }
}

/// Trait representing an object store client
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Execute a get operation
    async fn execute_get(&self, key: String) -> Result<Option<Bytes>, Error>;

    /// Execute a put operation
    async fn execute_put(
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

/// Extension trait providing builder methods for the object store client
pub trait ObjectStoreExt {
    /// Create a get request for the specified key
    fn get(&self, key: impl Into<String>) -> GetRequest;

    /// Create a put request for the specified key and value
    fn put(&self, key: impl Into<String>, value: Bytes) -> PutRequest;
}

impl ObjectStoreExt for Client {
    fn get(&self, key: impl Into<String>) -> GetRequest {
        GetRequest {
            store: self.clone(),
            key: key.into(),
        }
    }

    fn put(&self, key: impl Into<String>, value: Bytes) -> PutRequest {
        PutRequest {
            store: self.clone(),
            key: key.into(),
            value,
            condition: None,
        }
    }
}
