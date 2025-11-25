# Kanso Project Guidelines

Design decisions and guidelines for the Kanso project.

## Naming Conventions

- **Avoid stuttering**: Don't repeat the crate name in type names
  - ❌ `kanso_client::KansoClient`
  - ✅ `kanso_client::ObjectStore` with type alias `Client`

## API Design

### Request/Response Pattern

- Operations use request/response types: `GetRequest`/`GetResponse`, `PutRequest`/`PutResponse`
- Trait methods take requests and return responses
- Request types provide builder methods and an `execute(&Client)` convenience method
- Get returns `Result<Option<GetResponse>, Error>` - `None` means key not found

### Versioning

- Every object has a `Version` (string-based etag)
- Get returns value + version together in `GetResponse`
- Put returns new version in `PutResponse`
- Conditional writes use version for compare-and-swap

### Conditional Writes

- `PutRequest::if_absent()` - Only write if key doesn't exist
- `PutRequest::if_version_matches(v)` - Only write if version matches (CAS)

## Type Guidelines

- Use `Bytes` for values (efficient, zero-copy)
- Use `String` for keys
- Use `Arc<dyn ObjectStore>` via `Client` type alias
- All trait methods are async with `Send + Sync` bounds

## Code Quality

- Must pass `cargo check`
- Must pass `cargo clippy` with `-D warnings`
- Must be formatted with `cargo fmt`
