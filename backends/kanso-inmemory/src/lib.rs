use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use kanso_client::{
    Condition, GetRequest, GetResponse, Metadata, ObjectStore, PatchRequest, PatchResponse,
    PutRequest, PutResponse, Version,
};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
struct StoredObject {
    value: Bytes,
    version: Version,
    metadata: Metadata,
}

/// In-memory implementation of ObjectStore for testing
#[derive(Debug, Clone)]
pub struct InMemoryStore {
    data: Arc<RwLock<HashMap<String, StoredObject>>>,
    version_counter: Arc<RwLock<u64>>,
}

impl InMemoryStore {
    /// Create a new empty in-memory store
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            version_counter: Arc::new(RwLock::new(0)),
        }
    }

    async fn next_version(&self) -> Version {
        let mut counter = self.version_counter.write().await;
        *counter += 1;
        Version::new(counter.to_string())
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ObjectStore for InMemoryStore {
    async fn get(&self, request: GetRequest) -> Result<Option<GetResponse>, kanso_client::Error> {
        let data = self.data.read().await;
        Ok(data.get(request.key.as_str()).map(|obj| GetResponse {
            value: obj.value.clone(),
            version: obj.version.clone(),
            metadata: obj.metadata.clone(),
        }))
    }

    async fn put(&self, request: PutRequest) -> Result<PutResponse, kanso_client::Error> {
        let mut data = self.data.write().await;

        // Check conditions
        if let Some(condition) = &request.condition {
            match condition {
                Condition::IfAbsent => {
                    if data.contains_key(request.key.as_str()) {
                        return Err(kanso_client::Error::ConditionFailed {
                            condition: condition.clone(),
                        });
                    }
                }
                Condition::IfVersionMatches(expected_version) => {
                    match data.get(request.key.as_str()) {
                        Some(obj) if &obj.version == expected_version => {
                            // Version matches, continue
                        }
                        _ => {
                            // Version mismatch or key doesn't exist
                            return Err(kanso_client::Error::ConditionFailed {
                                condition: condition.clone(),
                            });
                        }
                    }
                }
            }
        }

        let version = self.next_version().await;
        let metadata = request.metadata.unwrap_or_default();

        data.insert(
            request.key.as_str().to_string(),
            StoredObject {
                value: request.value,
                version: version.clone(),
                metadata,
            },
        );

        Ok(PutResponse { version })
    }

    async fn patch(&self, request: PatchRequest) -> Result<PatchResponse, kanso_client::Error> {
        let mut data = self.data.write().await;

        // Get the existing object and clone the value we need
        let value = data
            .get(request.key.as_str())
            .map(|obj| obj.value.clone())
            .ok_or(kanso_client::Error::NotFound)?;

        // Check condition if present
        if let Some(condition) = &request.condition {
            match condition {
                Condition::IfAbsent => {
                    // This doesn't make sense for patch (object must exist), but handle it
                    return Err(kanso_client::Error::ConditionFailed {
                        condition: condition.clone(),
                    });
                }
                Condition::IfVersionMatches(expected_version) => {
                    let obj = data.get(request.key.as_str()).unwrap(); // Safe: we just checked it exists
                    if &obj.version != expected_version {
                        return Err(kanso_client::Error::ConditionFailed {
                            condition: condition.clone(),
                        });
                    }
                }
            }
        }

        // Create new version and update metadata, keep the value
        let version = self.next_version().await;
        data.insert(
            request.key.as_str().to_string(),
            StoredObject {
                value,
                version: version.clone(),
                metadata: request.metadata,
            },
        );

        Ok(PatchResponse { version })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compliance() {
        let store: kanso_client::Client = Arc::new(InMemoryStore::new());
        kanso_backends_test_suite::run_compliance_tests(&store, "").await;
    }
}
