use async_trait::async_trait;
use kanso_client::{
    Condition, Error, GetRequest, GetResponse, Metadata, ObjectStore, PatchRequest, PatchResponse,
    Path, PutRequest, PutResponse, Version,
};
use std::sync::Arc;

/// GCS implementation of ObjectStore using direct JSON API calls
///
/// Path format: "bucket-name/path/to/object"
/// The bucket is parsed from the first path component.
#[derive(Clone)]
pub struct GcsStore {
    client: reqwest::Client,
    auth: Option<Arc<dyn gcp_auth::TokenProvider>>,
    endpoint: String,
}

impl GcsStore {
    /// Create a new GcsStore with default credentials
    pub async fn new() -> Result<Self, Error> {
        let auth = gcp_auth::provider()
            .await
            .map_err(|e| Error::Other(format!("failed to create auth provider: {e}")))?;
        Ok(Self {
            client: reqwest::Client::new(),
            auth: Some(auth),
            endpoint: "https://storage.googleapis.com".into(),
        })
    }

    /// Create a new GcsStore with a custom endpoint (for testing with fake-gcs-server)
    pub fn with_endpoint(endpoint: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            auth: None, // No auth needed for fake-gcs-server
            endpoint: endpoint.into(),
        }
    }

    async fn get_token(&self) -> Result<Option<String>, Error> {
        match &self.auth {
            Some(provider) => {
                let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];
                let token = provider
                    .token(scopes)
                    .await
                    .map_err(|e| Error::Other(format!("token error: {e}")))?;
                Ok(Some(token.as_str().to_string()))
            }
            None => Ok(None),
        }
    }
}

/// Parse bucket and key from a path
/// Path format: "bucket/key/path"
fn parse_path(path: &Path) -> Result<(&str, &str), Error> {
    let s = path.as_str();
    let slash_pos = s
        .find('/')
        .ok_or_else(|| Error::Other("path must include bucket: expected 'bucket/key'".into()))?;
    Ok((&s[..slash_pos], &s[slash_pos + 1..]))
}

#[async_trait]
impl ObjectStore for GcsStore {
    async fn get(&self, request: GetRequest) -> Result<Option<GetResponse>, Error> {
        let (bucket, key) = parse_path(&request.key)?;
        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media",
            self.endpoint,
            urlencoding::encode(bucket),
            urlencoding::encode(key)
        );

        let mut req = self.client.get(&url);
        if let Some(token) = self.get_token().await? {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| Error::Other(format!("request error: {e}")))?;

        match resp.status().as_u16() {
            404 => Ok(None),
            200 => {
                // Extract version from header
                let generation = resp
                    .headers()
                    .get("x-goog-generation")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| Error::Other("missing generation header".into()))?;
                let version = Version::new(generation);

                // Extract custom metadata from x-goog-meta-* headers
                let mut metadata = Metadata::new();
                for (name, value) in resp.headers() {
                    if let Some(key) = name.as_str().strip_prefix("x-goog-meta-")
                        && let Ok(v) = value.to_str()
                    {
                        metadata.insert(key, v);
                    }
                }

                // Read body
                let value = resp
                    .bytes()
                    .await
                    .map_err(|e| Error::Other(format!("read error: {e}")))?;

                Ok(Some(GetResponse {
                    value,
                    version,
                    metadata,
                }))
            }
            status => Err(Error::Other(format!("GCS get error: status {status}"))),
        }
    }

    async fn put(&self, request: PutRequest) -> Result<PutResponse, Error> {
        let (bucket, key) = parse_path(&request.key)?;
        let mut url = format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.endpoint,
            urlencoding::encode(bucket),
            urlencoding::encode(key)
        );

        // Add condition query params
        if let Some(condition) = &request.condition {
            match condition {
                Condition::IfAbsent => url.push_str("&ifGenerationMatch=0"),
                Condition::IfVersionMatches(v) => {
                    url.push_str(&format!("&ifGenerationMatch={}", v.as_str()));
                }
            }
        }

        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(request.value.clone());

        if let Some(token) = self.get_token().await? {
            req = req.bearer_auth(token);
        }

        // Add custom metadata as x-goog-meta-* headers
        if let Some(metadata) = &request.metadata {
            for (k, v) in &metadata.headers {
                req = req.header(format!("x-goog-meta-{k}"), v);
            }
        }

        let resp = req
            .send()
            .await
            .map_err(|e| Error::Other(format!("request error: {e}")))?;

        match resp.status().as_u16() {
            200 => {
                let body: serde_json::Value = resp
                    .json()
                    .await
                    .map_err(|e| Error::Other(format!("json error: {e}")))?;
                let generation = body["generation"]
                    .as_str()
                    .ok_or_else(|| Error::Other("missing generation".into()))?;
                Ok(PutResponse {
                    version: Version::new(generation),
                })
            }
            412 => Err(Error::ConditionFailed {
                condition: request.condition.unwrap(),
            }),
            status => Err(Error::Other(format!("GCS put error: status {status}"))),
        }
    }

    async fn patch(&self, request: PatchRequest) -> Result<PatchResponse, Error> {
        let (bucket, key) = parse_path(&request.key)?;
        let mut url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            urlencoding::encode(bucket),
            urlencoding::encode(key)
        );

        // Add condition query params
        if let Some(Condition::IfVersionMatches(v)) = &request.condition {
            url.push_str(&format!("?ifGenerationMatch={}", v.as_str()));
        }

        // PATCH body with metadata
        let body = serde_json::json!({
            "metadata": request.metadata.headers
        });

        let mut req = self
            .client
            .patch(&url)
            .header("Content-Type", "application/json")
            .json(&body);

        if let Some(token) = self.get_token().await? {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| Error::Other(format!("request error: {e}")))?;

        match resp.status().as_u16() {
            200 => {
                let body: serde_json::Value = resp
                    .json()
                    .await
                    .map_err(|e| Error::Other(format!("json error: {e}")))?;
                let generation = body["generation"]
                    .as_str()
                    .ok_or_else(|| Error::Other("missing generation".into()))?;
                Ok(PatchResponse {
                    version: Version::new(generation),
                })
            }
            404 => Err(Error::NotFound),
            412 => Err(Error::ConditionFailed {
                condition: request.condition.unwrap(),
            }),
            status => Err(Error::Other(format!("GCS patch error: status {status}"))),
        }
    }
}
