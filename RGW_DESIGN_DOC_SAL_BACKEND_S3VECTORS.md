# Design Document: SAL Backend for RGW S3 Vectors (LanceDB)

## Overview

This document describes the implementation of a Storage Abstraction Layer (SAL) backend for RGW S3 Vectors using LanceDB. The SAL backend allows LanceDB to access Ceph RGW storage directly via the native SAL API instead of going through the S3 HTTP protocol, eliminating network overhead and improving performance for vector operations.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              RGW S3 Vector API                                │
│                          (rgw_rest_s3vector.cc)                              │
└────────────────────────────────────┬─────────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           S3 Vector Operations                                │
│                             (rgw_s3vector.cc)                                │
│  ┌─────────────────────┐   ┌─────────────────────┐                           │
│  │   LOCAL Backend     │   │     S3 Backend      │                           │
│  │  (filesystem path)  │   │  (local RGW or      │                           │
│  │                     │   │   external S3)      │                           │
│  └─────────────────────┘   └──────────┬──────────┘                           │
└────────────────────────────────────────┼─────────────────────────────────────┘
                                         │
              ┌──────────────────────────┴────────────────────────────┐
              │                                                       │
              ▼                                                       ▼
┌─────────────────────────────┐                    ┌─────────────────────────────┐
│    External S3 Backend      │                    │    Local RGW SAL Backend    │
│   (HTTP S3 Protocol)        │                    │   (Direct SAL API)          │
│                             │                    │                             │
│  LanceDB → AWS SDK → S3     │                    │  LanceDB → ceph-lancedb-rgw │
└─────────────────────────────┘                    │          ↓                  │
                                                   │     rgw_sal_wrapper.cc      │
                                                   │          ↓                  │
                                                   │     RGW SAL API             │
                                                   │          ↓                  │
                                                   │     RADOS/OSD               │
                                                   └─────────────────────────────┘
```

## Components

### 1. Rust Crate: ceph-lancedb-rgw

**Location:** `rust/ceph-lancedb-rgw/`

A Rust crate that implements Apache Arrow's `object_store::ObjectStore` trait, routing all I/O operations through Ceph's RGW SAL C API.

#### Key Files:

| File | Description |
|------|-------------|
| `src/lib.rs` | Crate entry point and C FFI exports |
| `src/store.rs` | `RGWObjectStore` - ObjectStore trait implementation |
| `src/provider.rs` | `RGWStoreProvider` - creates stores from S3 URLs |
| `src/session.rs` | Session management and LanceDB integration |
| `src/ffi.rs` | FFI bindings to C SAL wrapper functions |
| `include/ceph_lancedb_rgw.h` | C header for FFI interface |

#### Core Classes:

**RGWObjectStore** (`src/store.rs:58-917`)
- Implements `object_store::ObjectStore` trait
- Holds raw pointers to RGW driver and DoutPrefixProvider
- Supports all object operations: put, get, delete, copy, list, head
- Full multipart upload support

**RGWStoreProvider** (`src/provider.rs:23-127`)
- Implements `ObjectStoreProvider` trait
- Creates `RGWObjectStore` instances for s3:// URLs
- Extracts bucket and path prefix from URLs
- Registered in Lance's ObjectStoreRegistry

### 2. C++ SAL Wrapper

**Location:** `src/rgw/rgw_sal_wrapper.cc`, `src/rgw/rgw_sal_wrapper.h`

C wrapper functions that expose RGW SAL operations for FFI consumption by Rust.

#### Exposed Functions:

| Function | Description |
|----------|-------------|
| `rgw_put_object` | Write an object to RGW storage |
| `rgw_get_object` | Read an object (supports range reads) |
| `rgw_delete_object` | Delete an object |
| `rgw_head_object` | Get object metadata |
| `rgw_list_objects` | List objects with prefix/delimiter |
| `rgw_copy_object` | Copy object within/between buckets |
| `rgw_delete_objects` | Batch delete objects |
| `rgw_init_multipart` | Initialize multipart upload |
| `rgw_multipart_put_part` | Upload a part |
| `rgw_multipart_complete` | Complete multipart upload |
| `rgw_multipart_abort` | Abort multipart upload |
| `rgw_free_buffer` | Free memory allocated by get operations |
| `rgw_free_object_meta` | Free metadata structures |
| `rgw_free_list_result` | Free list result structures |

#### Data Structures:

```c
typedef struct RGWBuffer {
    uint8_t* data;      // Pointer to data
    size_t len;         // Length of valid data
    size_t capacity;    // Allocated capacity
} RGWBuffer;

typedef struct RGWObjectMeta {
    uint64_t size;          // Object size in bytes
    char* etag;             // ETag (MD5 hash)
    char* content_type;     // Content type
    int64_t last_modified;  // Unix timestamp
} RGWObjectMeta;

typedef struct RGWListEntry {
    char* key;              // Object key
    uint64_t size;          // Object size
    int64_t last_modified;  // Unix timestamp
} RGWListEntry;

typedef struct RGWListResult {
    RGWListEntry* entries;  // Array of entries
    size_t count;           // Number of entries
    int is_truncated;       // More results available
    char* next_marker;      // Pagination marker
} RGWListResult;
```

### 3. LanceDB Session API

**Location:** `rust/ceph-lancedb-rgw/include/ceph_lancedb_rgw.h`

C API for creating LanceDB sessions configured to use RGW SAL.

```c
// Create session with default cache sizes (256MB index, 128MB metadata)
CephLanceDBSession* ceph_lancedb_create_session(void* driver, const void* dpp);

// Create session with custom cache sizes
CephLanceDBSession* ceph_lancedb_create_session_with_cache(
    void* driver,
    const void* dpp,
    size_t index_cache_size,
    size_t metadata_cache_size
);

// Free session
void ceph_lancedb_session_free(CephLanceDBSession* session);

// Get session pointer for lancedb-c
const void* ceph_lancedb_session_as_ptr(const CephLanceDBSession* session);
```

### 4. S3 Vector Integration

**Location:** `src/rgw/rgw_s3vector.cc`, `src/rgw/rgw_s3vector.h`

Integration of SAL backend into the S3 Vector operations.

#### Backend Types:

```cpp
enum class BackendType {
  LOCAL,  // Local filesystem storage (default)
  S3      // S3 storage backend (local RGW via SAL or external S3)
};
```

#### Connection Logic (`rgw_s3vector.cc:87-210`):

```cpp
LanceDBConnection* connect(rgw::sal::Driver* driver, DoutPrefixProvider* dpp,
                           const std::string& vector_bucket_name) {
    if (backend == "s3") {
        // Use configured S3 bucket with vector bucket name as prefix
        uri = fmt::format("s3://{}/{}/", s3_bucket, vector_bucket_name);

        if (is_local_rgw_backend(cct)) {
            // Create SAL session for local RGW
            CephLanceDBSession* sal_session = ceph_lancedb_create_session(driver, dpp);
            builder = lancedb_connect_builder_session_ptr(builder, sal_session);
        } else {
            // External S3: use storage options for credentials
            lancedb_connect_builder_storage_option(builder, "endpoint", s3_endpoint);
            lancedb_connect_builder_storage_option(builder, "aws_access_key_id", access_key);
            // ... etc
        }
    } else {
        // Local filesystem backend
        uri = fmt::format("{}/{}", local_path, vector_bucket_name);
    }
}
```

## Configuration Options

New configuration options in `rgw.yaml.in`:

| Option | Type | Description |
|--------|------|-------------|
| `rgw_s3vector_backend` | str | Backend type: "local" or "s3" |
| `rgw_s3vector_local_path` | str | Local filesystem path for "local" backend |
| `rgw_s3vector_s3_endpoint` | str | S3 endpoint URL (empty/localhost = SAL) |
| `rgw_s3vector_s3_region` | str | AWS region (default: us-east-1) |
| `rgw_s3vector_s3_access_key` | str | S3 access key |
| `rgw_s3vector_s3_secret_key` | str | S3 secret key |
| `rgw_s3vector_s3_allow_http` | bool | Allow HTTP endpoints |
| `rgw_s3vector_s3_bucket` | str | Single S3 bucket for all vector data |

### Backend Selection Logic

```
rgw_s3vector_backend = "local"  → Filesystem storage
rgw_s3vector_backend = "s3"     → S3 storage
    ├─ endpoint empty/"localhost"/"127.0.0.1" → SAL backend (direct)
    └─ endpoint = external URL                 → HTTP S3 protocol
```

## Data Storage Model

### Single Bucket Architecture

All vector data is stored in a single S3 bucket configured via `rgw_s3vector_s3_bucket`:

```
s3://configured-s3-bucket/
├── vector-bucket-1/           # Vector bucket as prefix
│   ├── index-1/               # LanceDB table (index)
│   │   ├── _versions/
│   │   ├── _indices/
│   │   └── *.lance
│   └── index-2/
│       └── ...
└── vector-bucket-2/
    └── ...
```

Benefits:
- Simplified permissions (single bucket ACL)
- Consistent storage location
- Easier migration and backup
- Per-vector-bucket isolation via prefixes

## Build System Integration

### CMake Integration (`src/CMakeLists.txt`, `src/rgw/CMakeLists.txt`)

```cmake
# Build Rust crate
add_custom_target(ceph-lancedb-rgw
    COMMAND cargo build --release --manifest-path ${CMAKE_SOURCE_DIR}/rust/ceph-lancedb-rgw/Cargo.toml
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/rust/ceph-lancedb-rgw
)

# Link library
set(RGW_LANCEDB_LIBS ${CMAKE_SOURCE_DIR}/rust/ceph-lancedb-rgw/target/release/libceph_lancedb_rgw.so)
```

### Cargo Configuration (`rust/ceph-lancedb-rgw/Cargo.toml`)

Key dependencies:
- `lance` - Lance columnar format
- `lancedb` - LanceDB database
- `object_store` - Apache Arrow ObjectStore trait
- `arrow` - Apache Arrow data types
- `async-trait` - Async trait support
- `tokio` - Async runtime

## Testing

### Unit Tests (Rust)

```bash
# Run with mock SAL (no Ceph required)
cargo test --features mock-sal

# Specific test modules
cargo test --features mock-sal object_store_tests
cargo test --features mock-sal concurrency_tests
```

Test categories:
- ObjectStore operations (put, get, delete, copy, list, head)
- Provider tests (URL parsing, path extraction)
- Session tests (configuration, cache sizes)
- C API tests (FFI functions, null handling)
- Concurrency tests (Send+Sync traits)

### Integration Tests (C++)

**Location:** `src/test/rgw/test_rgw_sal_wrapper.cc`

Tests:
- Structure layout compatibility
- Null pointer handling
- Parameter validation
- Boundary conditions

### SAL Wrapper Endpoint Test

**Location:** `src/rgw/rgw_rest_sal_wrapper_test.cc`

HTTP endpoint for testing SAL wrapper from running RGW:

```bash
# Run tests via HTTP
curl -X POST "http://localhost:8000/sal-wrapper-test?sal-wrapper-test" \
  -H "Content-Type: application/json" \
  -d '{"test": "all", "iterations": 10}'
```

### Python Integration Tests

**Location:** `src/test/rgw/sal_wrapper/test_sal_wrapper_endpoint.py`

End-to-end tests using pytest.

## Commits Summary (Top 8)

| Commit | Description |
|--------|-------------|
| `059dbd92ee3` | Fix list_indexes and add prefix support to SAL wrapper |
| `5b1c2a271d5` | Update lancedb-c submodule with session pointer support |
| `c480253fe20` | Update build system for SAL wrapper integration |
| `22b8c53c5f1` | Use single S3 bucket for vector data storage |
| `80bee185336` | Rename lancedb wrapper files to sal_wrapper |
| `d7d5b341b4a` | Add comprehensive tests for ceph-lancedb-rgw SAL backend |
| `311f7c7154a` | Add ceph-lancedb-rgw crate and SAL wrapper |
| `f335adc5c08` | LANCEDB Backend options - minimal changes |

## Known Limitations

### Current Limitations

1. **Multipart Upload ETag Handling**
   - Placeholder ETags used in multipart complete
   - Proper ETag tracking requires Arc<Mutex<Vec<String>>> pattern
   - Location: `src/store.rs:740-783`

2. **Metadata Support**
   - Vector metadata not yet fully implemented
   - Non-filterable metadata keys stored but not retrieved
   - Location: `rgw_s3vector.cc` - various TODO comments

3. **Policy Operations**
   - `put_vector_bucket_policy` and `get_vector_bucket_policy` are stubs
   - `delete_vector_bucket_policy` returns success without action
   - Location: `rgw_s3vector.cc:901-905`

4. **Prefix Filtering in list_indexes**
   - Client-side prefix filtering due to LanceDB limitation
   - Issue: https://github.com/lancedb/lancedb/issues/2895
   - Location: `rgw_s3vector.cc:813-820`

5. **Vector Index Creation**
   - Vector index only created after vectors are added
   - Scalar index on 'key' column created immediately
   - Location: `rgw_s3vector.cc:547-559`

## Pending TODOs

### High Priority

1. **Implement Metadata Queries**
   - Add metadata column support to Arrow schema
   - Implement metadata-based filtering in queries
   - Files: `rgw_s3vector.cc` lines with `// metadata TODO`

2. **Implement Vector Bucket Policies**
   - Store and retrieve bucket policies
   - Apply policy checks on operations
   - File: `rgw_s3vector.cc:901-928`

3. **Proper Multipart ETag Tracking**
   - Use thread-safe ETag collection
   - File: `src/store.rs:740-783`

### Medium Priority

4. **Force Delete Flag for Vector Buckets**
   - Add confirmation flag for non-empty bucket deletion
   - Location: `rgw_s3vector.cc:873`

5. **Improved Error Messages**
   - Map LanceDB errors to more descriptive messages
   - Location: `rgw_s3vector.cc:23-57`

6. **Session Lifecycle Management**
   - Proper session cleanup on connection close
   - Location: `rgw_s3vector.cc:207-209`

### Low Priority

7. **Performance Optimization**
   - Batch operations where possible
   - Connection pooling
   - Cache tuning

8. **Documentation**
   - User guide for configuration
   - Performance tuning guide
   - Migration guide from local to S3 backend

## Security Considerations

1. **Pointer Safety**
   - Raw pointers marked unsafe with clear lifetime requirements
   - Send+Sync implemented with safety comments
   - Location: `src/store.rs:69-72`

2. **Credential Handling**
   - S3 credentials passed via configuration
   - No credential caching in Rust layer
   - SAL backend uses RGW's existing auth

3. **Input Validation**
   - Bucket/key names validated before use
   - Null checks on all FFI boundaries
   - Size limits enforced

## Performance Characteristics

### SAL Backend Advantages

- **No HTTP overhead**: Direct RADOS access
- **No auth per-request**: Uses existing RGW session
- **Reduced latency**: ~10-50ms per operation vs ~50-200ms HTTP
- **No serialization**: Direct memory access

### Memory Considerations

- Default index cache: 256 MB
- Default metadata cache: 128 MB
- Configurable via `ceph_lancedb_create_session_with_cache()`

## Future Enhancements

1. **Async Operations**: Full async/await support
2. **Streaming Uploads**: Support for very large vectors
3. **Replication**: Multi-site vector replication
4. **Tiering**: Automatic data tiering for cold vectors
5. **Compression**: Native vector compression support
