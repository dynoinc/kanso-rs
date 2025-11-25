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

/// Request for a get operation
#[derive(Debug, Clone)]
pub struct GetRequest {
    pub key: String,
}

impl GetRequest {
    /// Create a new get request
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Execute the get request against a client
    pub async fn execute(self, client: &Client) -> Result<Option<GetResponse>, Error> {
        client.get(self).await
    }
}

/// Response from a get operation
#[derive(Debug, Clone)]
pub struct GetResponse {
    /// The value associated with the key
    pub value: Bytes,
    /// The version of the object
    pub version: Version,
}

/// Request for a put operation
#[derive(Debug, Clone)]
pub struct PutRequest {
    pub key: String,
    pub value: Bytes,
    pub condition: Option<Condition>,
}

impl PutRequest {
    /// Create a new put request
    pub fn new(key: impl Into<String>, value: Bytes) -> Self {
        Self {
            key: key.into(),
            value,
            condition: None,
        }
    }

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

    /// Execute the put request against a client
    pub async fn execute(self, client: &Client) -> Result<PutResponse, Error> {
        client.put(self).await
    }
}

/// Response from a put operation
#[derive(Debug, Clone)]
pub struct PutResponse {
    /// The new version of the stored object
    pub version: Version,
}

/// Trait representing an object store client
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Execute a get operation
    ///
    /// Returns `None` if the key does not exist
    async fn get(&self, request: GetRequest) -> Result<Option<GetResponse>, Error>;

    /// Execute a put operation
    async fn put(&self, request: PutRequest) -> Result<PutResponse, Error>;
}

/// Type alias for the object store client
///
/// Users should use this type to interact with the object store
pub type Client = Arc<dyn ObjectStore>;
