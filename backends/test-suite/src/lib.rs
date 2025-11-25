use bytes::Bytes;
use kanso_client::{Client, Condition, Error, GetRequest, Metadata, PatchRequest, PutRequest};

/// Run compliance tests against an ObjectStore implementation.
///
/// The `path_prefix` is prepended to all keys, allowing backends to use
/// isolated paths (e.g., "bucket/" for GCS).
pub async fn run_compliance_tests(client: &Client, path_prefix: &str) {
    let key = format!("{path_prefix}test/key");

    // Get non-existent returns None
    assert!(
        GetRequest::new(&key)
            .unwrap()
            .execute(client)
            .await
            .unwrap()
            .is_none()
    );

    // Put creates object, put if_absent on existing fails
    let v1 = PutRequest::new(&key, Bytes::from("v1"))
        .unwrap()
        .metadata(Metadata::with("k", "v"))
        .execute(client)
        .await
        .unwrap()
        .version;
    assert!(matches!(
        PutRequest::new(&key, Bytes::from("x"))
            .unwrap()
            .if_absent()
            .execute(client)
            .await,
        Err(Error::ConditionFailed {
            condition: Condition::IfAbsent
        })
    ));

    // Get returns correct data/version/metadata
    let resp = GetRequest::new(&key)
        .unwrap()
        .execute(client)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resp.value, Bytes::from("v1"));
    assert_eq!(resp.version, v1);
    assert_eq!(resp.metadata.get("k"), Some(&"v".to_string()));

    // Put with version match succeeds, wrong version fails
    let v2 = PutRequest::new(&key, Bytes::from("v2"))
        .unwrap()
        .if_version_matches(v1.clone())
        .execute(client)
        .await
        .unwrap()
        .version;
    assert!(
        PutRequest::new(&key, Bytes::from("x"))
            .unwrap()
            .if_version_matches(v1)
            .execute(client)
            .await
            .is_err()
    );

    // Patch updates metadata, wrong version fails
    let _v3 = PatchRequest::new(&key, Metadata::with("k2", "v2"))
        .unwrap()
        .if_version_matches(v2.clone())
        .execute(client)
        .await
        .unwrap()
        .version;
    assert!(
        PatchRequest::new(&key, Metadata::new())
            .unwrap()
            .if_version_matches(v2)
            .execute(client)
            .await
            .is_err()
    );
    let resp = GetRequest::new(&key)
        .unwrap()
        .execute(client)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resp.value, Bytes::from("v2"));
    assert_eq!(resp.metadata.get("k2"), Some(&"v2".to_string()));

    // Patch non-existent returns NotFound
    let bad_key = format!("{path_prefix}nonexistent");
    assert!(matches!(
        PatchRequest::new(&bad_key, Metadata::new())
            .unwrap()
            .execute(client)
            .await,
        Err(Error::NotFound)
    ));
}
