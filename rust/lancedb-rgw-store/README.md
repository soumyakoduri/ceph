# lancedb-rgw-store

Rust crate that provides a SAL (Storage Abstraction Layer) wrapper for LanceDB to access Ceph RGW storage directly via the SAL API instead of going through S3 HTTP protocol.

## Overview

This crate implements the `object_store::ObjectStore` trait from Apache Arrow, routing all I/O operations through Ceph's RGW SAL C API. This allows LanceDB to store vector indexes directly in Ceph without the overhead of S3 HTTP requests.

## Building

### During Ceph Build

The crate is built automatically as part of the Ceph build process when `WITH_RADOSGW_LANCEDB` is enabled:

```bash
cd ceph/build
ninja radosgw
```

### Manual Rebuild (Development)

When making changes to this crate during development:

```bash
# Option 1: Use ninja to rebuild and copy the library
cd ceph/build
ninja lancedb-rgw-store
# NOTE: Option 1 may not rebuild if BUILD_ALWAYS is set to "OFF" in
# src/CMakeLists.txt. Ensure BUILD_ALWAYS is "ON" for the lancedb-rgw-store
# ExternalProject_Add target, or use Option 2 instead.

# Option 2: Build with cargo directly (faster iteration)
cd ceph/rust/lancedb-rgw-store
cargo build --release

# If using Option 2, ensure the build directory has a symlink to the target:
# cd ceph/build/lib
# ln -sf ../../rust/lancedb-rgw-store/target/release/liblancedb_rgw_store.so .
```

After rebuilding, restart the RGW daemon to pick up the changes.

## Testing

### Unit Tests (with mock SAL)

```bash
cargo test --features mock-sal
```

### Integration Tests

Integration tests require a running Ceph cluster with RGW. See `src/test/rgw/s3vectors/` for the test suite.

## Architecture

- `src/lib.rs` - Crate entry point and C FFI exports
- `src/store.rs` - `RGWObjectStore` implementation of `ObjectStore` trait
- `src/provider.rs` - `RGWStoreProvider` for creating stores from S3 URLs
- `src/ffi.rs` - FFI bindings to C SAL wrapper functions
- `include/lancedb_rgw_store.h` - C header for FFI interface
