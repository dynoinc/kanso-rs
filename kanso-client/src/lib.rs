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

/// Error type for path validation
#[derive(Debug, Error)]
pub enum PathError {
    #[error("path cannot have leading or trailing slashes")]
    LeadingTrailingSlash,

    #[error("path cannot contain empty segments")]
    EmptySegment,

    #[error("path cannot contain relative segments (. or ..)")]
    RelativeSegment,

    #[error("path cannot contain ASCII control characters")]
    ControlCharacter,

    #[error("path cannot be empty")]
    Empty,
}

/// Represents a validated path in the object store
///
/// A Path maintains the following invariants:
/// - Paths are delimited by `/`
/// - Paths do not contain leading or trailing `/`
/// - Paths do not contain relative path segments (`.` or `..`)
/// - Paths do not contain empty path segments
/// - Paths do not contain any ASCII control characters
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path(String);

impl Path {
    /// Create a new path with validation
    pub fn new(s: impl AsRef<str>) -> Result<Self, PathError> {
        let s = s.as_ref();

        // Empty check
        if s.is_empty() {
            return Err(PathError::Empty);
        }

        // Leading/trailing slash check
        if s.starts_with('/') || s.ends_with('/') {
            return Err(PathError::LeadingTrailingSlash);
        }

        // Validate each segment
        for segment in s.split('/') {
            if segment.is_empty() {
                return Err(PathError::EmptySegment);
            }
            if segment == "." || segment == ".." {
                return Err(PathError::RelativeSegment);
            }
            if segment.chars().any(|c| c.is_ascii_control()) {
                return Err(PathError::ControlCharacter);
            }
        }

        Ok(Self(s.to_string()))
    }

    /// Get the path as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Path {
    type Error = PathError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<&str> for Path {
    type Error = PathError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl std::fmt::Display for Path {
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
    pub key: Path,
}

impl GetRequest {
    /// Create a new get request
    ///
    /// Returns a PathError if the key doesn't satisfy Path invariants
    pub fn new(key: impl AsRef<str>) -> Result<Self, PathError> {
        Ok(Self {
            key: Path::new(key)?,
        })
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
    pub key: Path,
    pub value: Bytes,
    pub condition: Option<Condition>,
    pub metadata: Option<Metadata>,
}

impl PutRequest {
    /// Create a new put request
    ///
    /// Returns a PathError if the key doesn't satisfy Path invariants
    pub fn new(key: impl AsRef<str>, value: Bytes) -> Result<Self, PathError> {
        Ok(Self {
            key: Path::new(key)?,
            value,
            condition: None,
            metadata: None,
        })
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

/// Request for a patch operation (update object metadata without touching data)
#[derive(Debug, Clone)]
pub struct PatchRequest {
    pub key: Path,
    pub metadata: Metadata,
    pub condition: Option<Condition>,
}

impl PatchRequest {
    /// Create a new patch request
    ///
    /// Returns a PathError if the key doesn't satisfy Path invariants
    pub fn new(key: impl AsRef<str>, metadata: Metadata) -> Result<Self, PathError> {
        Ok(Self {
            key: Path::new(key)?,
            metadata,
            condition: None,
        })
    }

    /// Set the condition to only patch if the current version matches
    pub fn if_version_matches(mut self, version: Version) -> Self {
        self.condition = Some(Condition::IfVersionMatches(version));
        self
    }

    /// Execute the patch request against a client
    pub async fn execute(self, client: &Client) -> Result<PatchResponse, Error> {
        client.patch(self).await
    }
}

/// Response from a patch operation
#[derive(Debug, Clone)]
pub struct PatchResponse {
    /// The new version of the patched object
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

    /// Execute a patch operation (update object metadata without touching data)
    async fn patch(&self, request: PatchRequest) -> Result<PatchResponse, Error>;
}

/// Type alias for the object store client
///
/// Users should use this type to interact with the object store
pub type Client = Arc<dyn ObjectStore>;
