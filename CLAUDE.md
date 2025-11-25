# Kanso Project Guidelines

This document captures the design decisions and guidelines for the Kanso project.

## Project Structure

- **Workspace**: `kanso` - Rust workspace using edition 2024
- **Crates**:
  - `kanso-client` - Object store client trait and types

## Design Principles

### Naming Conventions

- **Avoid stuttering**: Don't repeat the crate name in type names
  - ❌ `kanso_client::KansoClient`
  - ✅ `kanso_client::ObjectStore` with type alias `kanso_client::Client`

### API Patterns

#### Request Builder Pattern

Use a request builder pattern for all operations (Get/Put) rather than direct method calls:

```rust
// Get request
let result = client.get("my-key").execute().await?;

// Put request (unconditional)
let version = client.put("my-key", data).execute().await?;

// Put request with condition (if-absent)
let version = client.put("my-key", data)
    .if_absent()
    .execute().await?;

// Put request with condition (if-version-matches)
let version = client.put("my-key", data)
    .if_version_matches(current_version)
    .execute().await?;
```

#### Client Usage

Users should always use the client via the `Client` type alias:

```rust
use kanso_client::Client;
use std::sync::Arc;

let client: Client = Arc::new(implementation);
```

## Object Store API

### Get Operation

- Returns `Result<Option<Bytes>, Error>`
- `None` indicates the key does not exist (not an error)
- Uses `Bytes` from the `bytes` crate for efficient data handling

### Put Operation

- Returns `Result<Version, Error>` containing the new version of the stored object
- Supports conditional writes via builder methods:
  - **Put-if-absent**: Only write if the key doesn't exist
  - **Put-if-version-matches**: Only write if the current version matches (compare-and-swap)

### Versioning

- Each object has a `Version` (string-based etag/version)
- Returned by Put operations
- Used for conditional writes (optimistic concurrency control)

### Error Handling

- Use `thiserror` for error type definitions
- Start with minimal error variants and expand as implementations are added
- Errors should be descriptive and actionable

## Async Support

- All I/O operations are async
- Use `async-trait` for trait definitions
- Trait requires `Send + Sync` bounds for thread-safe usage in Arc

## Dependencies

- `bytes` - Efficient byte buffer handling
- `thiserror` - Error type derivation
- `async-trait` - Async trait support

## Code Quality

- Must pass `cargo check`
- Must pass `cargo clippy` with no warnings
- Must be formatted with `cargo fmt`
