# lancedb-rgw-store Test Suite

This directory contains comprehensive tests for the lancedb-rgw-store crate,
which provides LanceDB integration with Ceph RGW via the native SAL API.

## Test Categories

### 1. Unit Tests (mock_sal_tests.rs)

Comprehensive unit tests using the mock-sal feature. These tests verify:

- **ObjectStore Operations**: put, put_opts (conditional writes), get, delete, copy, copy_if_not_exists, list, head
- **Multipart Upload**: init, put_part, complete, abort
- **Provider Tests**: URL parsing, path extraction, store creation
- **Session Tests**: Configuration, creation, cache sizes
- **C API Tests**: FFI function behavior, null handling
- **Error Handling**: errno to error mapping
- **Concurrency Tests**: Send+Sync traits, concurrent operations
- **Edge Cases**: Unicode paths, special characters, large data

### 2. Stateful Mock Tests (stateful_mock_tests.rs)

Tests using an in-memory storage backend to simulate real operations:

- **Lifecycle Tests**: Put/Get/Delete sequences
- **Stress Tests**: High concurrency, mixed operations
- **Performance Tests**: Latency consistency, throughput
- **Edge Cases**: Empty bucket names, long keys, binary data

## Running Tests

### Quick Start

```bash
# Run all tests with mock SAL (no Ceph required)
cargo test --features mock-sal

# Run specific test module
cargo test --features mock-sal object_store_tests

# Run with verbose output
cargo test --features mock-sal -- --nocapture
```

### Test Categories

```bash
# Unit tests only
cargo test --features mock-sal --test mock_sal_tests

# Stateful mock tests
cargo test --features mock-sal --test stateful_mock_tests

# C API tests
cargo test --features mock-sal c_api_tests

# Concurrency tests
cargo test --features mock-sal concurrency_tests

# Performance tests
cargo test --features mock-sal performance_tests
```

### Running Without Mock (Requires Ceph)

When running without the mock-sal feature, tests will attempt to use the real
RGW SAL C API, which requires linking against Ceph libraries.

```bash
# Build with real SAL (requires Ceph development environment)
cargo build

# Tests will need valid driver/dpp pointers - typically run via Ceph test framework
```

## Test Coverage

| Module | Description | Coverage |
|--------|-------------|----------|
| store.rs | ObjectStore trait implementation | High |
| provider.rs | ObjectStoreProvider implementation | High |
| session.rs | Session creation and configuration | High |
| ffi.rs | FFI structures and mock implementations | High |
| lib.rs | C API functions | High |

## Mock SAL Behavior

When the `mock-sal` feature is enabled, the FFI functions behave as follows:

| Function | Mock Behavior |
|----------|---------------|
| `rgw_put_object` | Always returns success (0) |
| `rgw_put_object_conditional` | Returns success, sets canceled=0 |
| `rgw_get_object` | Returns ENOENT (-2) |
| `rgw_delete_object` | Always returns success (0) |
| `rgw_head_object` | Returns ENOENT (-2) |
| `rgw_list_objects` | Returns empty list |
| `rgw_copy_object` | Always returns success (0) |
| `rgw_copy_object_conditional` | Always returns success (0) |
| `rgw_delete_objects` | Always returns success (0) |
| `rgw_init_multipart` | Returns success (0) |
| `rgw_multipart_put_part` | Returns success (0) |
| `rgw_multipart_complete` | Returns success (0) |
| `rgw_multipart_abort` | Returns success (0) |

## Writing New Tests

### Adding a New Test

```rust
#[tokio::test]
async fn test_my_new_feature() {
    let store = create_test_store("test-bucket");
    // Test implementation
}
```

### Testing Error Conditions

```rust
#[tokio::test]
async fn test_error_handling() {
    let store = create_test_store("test-bucket");
    let result = store.get(&Path::from("nonexistent")).await;
    assert!(matches!(result.unwrap_err(), object_store::Error::NotFound { .. }));
}
```

### Testing Concurrency

```rust
#[tokio::test]
async fn test_concurrent_operations() {
    let store = Arc::new(create_test_store("test-bucket"));
    let mut handles = vec![];

    for i in 0..10 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            // Concurrent operation
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
```

## CI Integration

These tests are designed to run in CI without requiring a Ceph cluster:

```yaml
# Example CI configuration
test:
  script:
    - cd rust/lancedb-rgw-store
    - cargo test --features mock-sal
```

## Related C++ Tests

C++ unit tests for the SAL wrapper are located at:
`src/test/rgw/test_rgw_sal_wrapper.cc`

These tests verify:
- Structure layout compatibility with Rust
- Null pointer handling
- Parameter validation
- Boundary conditions
