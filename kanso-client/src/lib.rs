use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/// Error type for object store operations
#[derive(Debug, Error)]
pub enum Error {
    #[error("condition failed: {condition:?}")]
    ConditionFailed { condition: Condition },

    #[error("not found")]
    NotFound,

    #[error("{0}")]
    Other(String),
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

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metadata associated with an object (e.g., user-defined headers)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Metadata {
    pub headers: HashMap<String, String>,
}

impl Metadata {
    /// Create empty metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Create metadata with a single key-value pair
    pub fn with(key: impl Into<String>, value: impl Into<String>) -> Self {
        let mut headers = HashMap::new();
        headers.insert(key.into(), value.into());
        Self { headers }
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.headers.insert(key.into(), value.into());
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Option<&String> {
        self.headers.get(key)
    }

    /// Check if a key exists
    pub fn contains_key(&self, key: &str) -> bool {
        self.headers.contains_key(key)
    }

    /// Remove a key-value pair
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.headers.remove(key)
    }

    /// Check if metadata is empty
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.headers.len()
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
    /// Metadata associated with the object
    pub metadata: Metadata,
}

/// Request for a put operation
#[derive(Debug, Clone)]
pub struct PutRequest {
    pub key: String,
    pub value: Bytes,
    pub condition: Option<Condition>,
    pub metadata: Option<Metadata>,
}

impl PutRequest {
    /// Create a new put request
    pub fn new(key: impl Into<String>, value: Bytes) -> Self {
        Self {
            key: key.into(),
            value,
            condition: None,
            metadata: None,
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

    /// Set metadata for the object
    pub fn metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
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

/// Request for a copy operation (copy object to itself with updated metadata)
#[derive(Debug, Clone)]
pub struct CopyRequest {
    pub key: String,
    pub metadata: Metadata,
    pub condition: Option<Condition>,
}

impl CopyRequest {
    /// Create a new copy request
    pub fn new(key: impl Into<String>, metadata: Metadata) -> Self {
        Self {
            key: key.into(),
            metadata,
            condition: None,
        }
    }

    /// Set the condition to only copy if the current version matches
    pub fn if_version_matches(mut self, version: Version) -> Self {
        self.condition = Some(Condition::IfVersionMatches(version));
        self
    }

    /// Execute the copy request against a client
    pub async fn execute(self, client: &Client) -> Result<CopyResponse, Error> {
        client.copy(self).await
    }
}

/// Response from a copy operation
#[derive(Debug, Clone)]
pub struct CopyResponse {
    /// The new version of the copied object
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

    /// Execute a copy operation (copy object to itself with updated metadata)
    async fn copy(&self, request: CopyRequest) -> Result<CopyResponse, Error>;
}

/// Type alias for the object store client
///
/// Users should use this type to interact with the object store
pub type Client = Arc<dyn ObjectStore>;
