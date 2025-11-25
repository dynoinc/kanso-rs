use std::marker::PhantomData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kanso_client::{Client, CopyRequest, GetRequest, Metadata, PutRequest, Version};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use uuid::Uuid;

const OWNER_HEADER: &str = "x-kanso-lease-owner";
const EXPIRY_HEADER: &str = "x-kanso-lease-expiry";

/// Error type for lease operations
#[derive(Debug, Error)]
pub enum LeaseError {
    #[error("lease is held by another owner")]
    LeaseHeld,

    #[error("conflict during update")]
    Conflict,

    #[error("path not found")]
    NotFound,

    #[error("storage error: {0}")]
    Storage(#[from] kanso_client::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("invalid metadata: {0}")]
    InvalidMetadata(String),
}

/// Builder for acquiring a lease
pub struct AcquireRequest<T> {
    path: String,
    owner: String,
    ttl: Duration,
    init_value: T,
}

impl<T: Serialize + DeserializeOwned> AcquireRequest<T> {
    /// Create a new acquire request with default owner (UUID) and 60s TTL
    pub fn new(path: impl Into<String>, init_value: T) -> Self {
        Self {
            path: path.into(),
            owner: Uuid::new_v4().to_string(),
            ttl: Duration::from_secs(60),
            init_value,
        }
    }

    /// Set the owner identifier
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    /// Set the lease TTL (time-to-live)
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Execute the acquire request
    ///
    /// Returns a tuple of (Lease, current_value) where current_value is either:
    /// - The init_value if we initialized a new lease
    /// - The existing value if we took over an expired lease
    ///
    /// Returns an error if the lease is currently held by another owner
    pub async fn execute(self, client: &Client) -> Result<(Lease<T>, T), LeaseError> {
        // Try to get existing value
        let existing = GetRequest::new(&self.path).execute(client).await?;

        let (value, version) = match existing {
            None => {
                // Path doesn't exist - initialize with init_value
                let value_bytes = serde_json::to_vec(&self.init_value)?;
                let expiry = current_timestamp() + self.ttl.as_secs();
                let mut metadata = Metadata::new();
                metadata.insert(OWNER_HEADER, &self.owner);
                metadata.insert(EXPIRY_HEADER, expiry.to_string());

                let response = PutRequest::new(&self.path, Bytes::from(value_bytes))
                    .if_absent()
                    .metadata(metadata)
                    .execute(client)
                    .await?;

                (self.init_value, response.version)
            }
            Some(resp) => {
                // Path exists - check if lease is alive
                let expiry = get_expiry(&resp.metadata)?;
                let current_owner = get_owner(&resp.metadata)?;

                // If lease is alive and we don't own it, fail
                if is_lease_alive(expiry) && current_owner != self.owner {
                    return Err(LeaseError::LeaseHeld);
                }

                // Either lease is expired or we own it - take it over/renew using copy
                let value: T = serde_json::from_slice(&resp.value)?;
                let expiry = current_timestamp() + self.ttl.as_secs();
                let mut metadata = Metadata::new();
                metadata.insert(OWNER_HEADER, &self.owner);
                metadata.insert(EXPIRY_HEADER, expiry.to_string());

                let response = CopyRequest::new(&self.path, metadata)
                    .if_version_matches(resp.version)
                    .execute(client)
                    .await
                    .map_err(|_| LeaseError::Conflict)?;

                (value, response.version)
            }
        };

        Ok((
            Lease {
                client: client.clone(),
                path: self.path,
                owner: self.owner,
                ttl: self.ttl,
                version,
                _phantom: PhantomData,
            },
            value,
        ))
    }
}

/// A lease on a path in the object store
///
/// The lease tracks the version internally and ensures atomic updates.
/// Users are expected to track the value themselves after acquiring the lease.
pub struct Lease<T> {
    client: Client,
    path: String,
    owner: String,
    ttl: Duration,
    version: Version,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Lease<T> {
    /// Update the value atomically
    ///
    /// This will fail if the version has changed (someone else modified it)
    /// or if the lease has expired.
    pub async fn update(&mut self, value: &T) -> Result<(), LeaseError> {
        let value_bytes = serde_json::to_vec(value)?;
        let expiry = current_timestamp() + self.ttl.as_secs();
        let mut metadata = Metadata::new();
        metadata.insert(OWNER_HEADER, &self.owner);
        metadata.insert(EXPIRY_HEADER, expiry.to_string());

        let response = PutRequest::new(&self.path, Bytes::from(value_bytes))
            .if_version_matches(self.version.clone())
            .metadata(metadata)
            .execute(&self.client)
            .await
            .map_err(|_| LeaseError::Conflict)?;

        self.version = response.version;
        Ok(())
    }

    /// Renew the lease without changing the value
    ///
    /// This extends the lease expiry time without modifying the stored value.
    /// Uses copy operation to update metadata without fetching the value.
    pub async fn renew(&mut self) -> Result<(), LeaseError> {
        // Update expiry with our tracked version using copy
        // If version doesn't match, someone else modified it (Conflict)
        let expiry = current_timestamp() + self.ttl.as_secs();
        let mut metadata = Metadata::new();
        metadata.insert(OWNER_HEADER, &self.owner);
        metadata.insert(EXPIRY_HEADER, expiry.to_string());

        let response = CopyRequest::new(&self.path, metadata)
            .if_version_matches(self.version.clone())
            .execute(&self.client)
            .await
            .map_err(|_| LeaseError::Conflict)?;

        self.version = response.version;
        Ok(())
    }

    /// Release the lease
    ///
    /// This sets the expiry to a past time and clears the owner,
    /// making the lease available for others to acquire.
    pub async fn release(self) -> Result<(), LeaseError> {
        // Get current value
        let resp = GetRequest::new(&self.path)
            .execute(&self.client)
            .await?
            .ok_or(LeaseError::NotFound)?;

        // Set expiry to past and clear owner
        let mut metadata = Metadata::new();
        metadata.insert(OWNER_HEADER, "");
        metadata.insert(EXPIRY_HEADER, "0");

        PutRequest::new(&self.path, resp.value)
            .if_version_matches(resp.version)
            .metadata(metadata)
            .execute(&self.client)
            .await
            .map_err(|_| LeaseError::Conflict)?;

        Ok(())
    }
}

// Helper functions

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn get_expiry(metadata: &Metadata) -> Result<u64, LeaseError> {
    metadata
        .get(EXPIRY_HEADER)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| LeaseError::InvalidMetadata("missing or invalid expiry".to_string()))
}

fn get_owner(metadata: &Metadata) -> Result<String, LeaseError> {
    metadata
        .get(OWNER_HEADER)
        .cloned()
        .ok_or_else(|| LeaseError::InvalidMetadata("missing owner".to_string()))
}

fn is_lease_alive(expiry: u64) -> bool {
    expiry > current_timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kanso_inmemory::InMemoryStore;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        count: u32,
    }

    #[tokio::test]
    async fn test_lease_happy_path() {
        let store: Arc<dyn kanso_client::ObjectStore> = Arc::new(InMemoryStore::new());

        // Test acquire with init value
        let (mut lease, value) = AcquireRequest::new("test-key", TestData { count: 0 })
            .owner("test-owner")
            .ttl(Duration::from_secs(60))
            .execute(&store)
            .await
            .unwrap();
        assert_eq!(value.count, 0);

        // Test update
        lease.update(&TestData { count: 1 }).await.unwrap();

        // Test renew
        lease.renew().await.unwrap();

        // Test that we can't acquire while lease is held by another owner
        let result = AcquireRequest::new("test-key", TestData { count: 999 })
            .owner("different-owner")
            .execute(&store)
            .await;
        assert!(matches!(result, Err(LeaseError::LeaseHeld)));

        // Test that we CAN re-acquire if we own the lease
        let (_lease2, value2) = AcquireRequest::new("test-key", TestData { count: 888 })
            .owner("test-owner")
            .execute(&store)
            .await
            .unwrap();
        assert_eq!(value2.count, 1); // Should get existing value, not init value

        // Test release
        _lease2.release().await.unwrap();

        // Test acquire of expired lease (takeover)
        let (_lease3, value3) = AcquireRequest::new("test-key", TestData { count: 999 })
            .owner("new-owner")
            .execute(&store)
            .await
            .unwrap();
        assert_eq!(value3.count, 1); // Should get the existing value
    }
}
