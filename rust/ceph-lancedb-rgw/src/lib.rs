// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Ceph Authors

//! LanceDB RGW Backend Integration
//!
//! This crate provides integration between LanceDB and Ceph RGW, allowing
//! LanceDB to use RGW's native SAL API instead of the S3 HTTP protocol.
//!
//! # Architecture
//!
//! All S3 operations are intercepted at the ObjectStoreRegistry level and
//! routed through RGW's SAL C API. This provides:
//!
//! - Lower latency (no HTTP overhead)
//! - Direct access to RADOS
//! - Consistent authentication/authorization with RGW
//!
//! # Zero Upstream Changes
//!
//! This crate requires NO modifications to lancedb, lance, or lance-io.
//! It uses the existing extension points:
//! - `ObjectStoreRegistry::insert()` to override the "s3" scheme
//! - `lancedb_session_new_with_registry()` to create sessions with custom registries
//! - `lancedb_connect_builder_session()` to pass sessions to connections
//!
//! # Usage from C++
//!
//! ```cpp
//! #include "ceph_lancedb_rgw.h"
//! #include "lancedb.h"
//!
//! // Create registry with RGW backend
//! CephLanceDBRegistry* registry = ceph_lancedb_create_registry(driver, dpp);
//!
//! // Create session with custom cache sizes using lancedb-c API
//! LanceDBSessionOptions options = {
//!     .index_cache_bytes = 512 * 1024 * 1024,    // 512 MB
//!     .metadata_cache_bytes = 256 * 1024 * 1024  // 256 MB
//! };
//! LanceDBSession* session = lancedb_session_new_with_registry(&options, registry);
//! // Note: registry ownership transferred to session
//!
//! // Use session with connection
//! LanceDBConnectBuilder* builder = lancedb_connect("s3://mybucket/vectors");
//! builder = lancedb_connect_builder_session(builder, session);
//! LanceDBConnection* db = lancedb_connect_builder_execute(builder);
//!
//! // ... use db ...
//!
//! // Cleanup
//! lancedb_connection_free(db);
//! lancedb_session_free(session);
//! ```

/// FFI bindings to Ceph's rgw_sal_wrapper.cc
pub mod ffi;
mod provider;
mod session;
mod store;

// Re-export main types for Rust users
pub use provider::RGWStoreProvider;
pub use session::{
    create_rgw_registry, create_rgw_session, create_rgw_session_with_cache, RGWSessionConfig,
    DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE,
};
pub use store::RGWObjectStore;

use std::os::raw::c_void;
use std::sync::Arc;

//=============================================================================
// C API - Registry for use with lancedb_session_new_with_registry()
//=============================================================================

/// Opaque handle to an ObjectStoreRegistry
pub type CephLanceDBRegistry = c_void;

/// Create an ObjectStoreRegistry configured to route S3 URLs through RGW SAL
///
/// This registry can be passed to lancedb_session_new_with_registry() to create
/// a LanceDB session with full control over session options (cache sizes, etc.)
/// while still routing all s3:// URLs through RGW SAL.
///
/// # Safety
/// - `driver` must be a valid pointer to rgw::sal::Driver
/// - `dpp` must be a valid pointer to DoutPrefixProvider (can be NULL)
/// - Both pointers must remain valid for the lifetime of the registry
///
/// # Returns
/// Opaque pointer to registry, or NULL on failure.
/// Caller must either:
/// - Pass to lancedb_session_new_with_registry() (transfers ownership)
/// - Free with ceph_lancedb_registry_free()
///
/// # Example (from C++)
/// ```cpp
/// // Create registry with RGW backend
/// void* registry = ceph_lancedb_create_registry(driver, dpp);
///
/// // Create session with custom cache sizes using lancedb-c API
/// LanceDBSessionOptions options = {
///     .index_cache_bytes = 512 * 1024 * 1024,    // 512 MB
///     .metadata_cache_bytes = 256 * 1024 * 1024  // 256 MB
/// };
/// LanceDBSession* session = lancedb_session_new_with_registry(&options, registry);
/// // Note: registry ownership transferred to session
///
/// // Use session with connection
/// auto* builder = lancedb_connect("s3://mybucket/vectors");
/// builder = lancedb_connect_builder_session(builder, session);
/// auto* db = lancedb_connect_builder_execute(builder);
///
/// // Cleanup
/// lancedb_connection_free(db);
/// lancedb_session_free(session);
/// ```
#[no_mangle]
pub unsafe extern "C" fn ceph_lancedb_create_registry(
    driver: *mut c_void,
    dpp: *const c_void,
) -> *mut CephLanceDBRegistry {
    if driver.is_null() {
        return std::ptr::null_mut();
    }

    let registry = create_rgw_registry(driver, dpp);

    // Convert Arc to raw pointer - transfers ownership to caller
    Arc::into_raw(registry) as *mut CephLanceDBRegistry
}

/// Free a registry created by ceph_lancedb_create_registry
///
/// Only call this if the registry was NOT passed to lancedb_session_new_with_registry().
/// If it was passed to that function, ownership was transferred and you must NOT
/// call this function.
///
/// # Safety
/// - `registry` must be a valid pointer returned by ceph_lancedb_create_registry
/// - Must not be called if registry was passed to lancedb_session_new_with_registry()
/// - Must not be called more than once for the same registry
#[no_mangle]
pub unsafe extern "C" fn ceph_lancedb_registry_free(registry: *mut CephLanceDBRegistry) {
    if !registry.is_null() {
        // Reconstruct Arc and let it drop
        let _ = Arc::from_raw(registry as *const lance_io::object_store::ObjectStoreRegistry);
    }
}

//=============================================================================
// Cache size defaults
//=============================================================================

/// Get the default index cache size in bytes
#[no_mangle]
pub extern "C" fn ceph_lancedb_default_index_cache_size() -> usize {
    DEFAULT_INDEX_CACHE_SIZE
}

/// Get the default metadata cache size in bytes
#[no_mangle]
pub extern "C" fn ceph_lancedb_default_metadata_cache_size() -> usize {
    DEFAULT_METADATA_CACHE_SIZE
}

//=============================================================================
// Version information
//=============================================================================

/// Get the version string for this library
#[no_mangle]
pub extern "C" fn ceph_lancedb_version() -> *const std::os::raw::c_char {
    // Static string with null terminator
    static VERSION: &[u8] = b"0.1.0\0";
    VERSION.as_ptr() as *const std::os::raw::c_char
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_size_defaults() {
        assert_eq!(ceph_lancedb_default_index_cache_size(), 256 * 1024 * 1024);
        assert_eq!(ceph_lancedb_default_metadata_cache_size(), 128 * 1024 * 1024);
    }

    #[test]
    fn test_version() {
        let version = ceph_lancedb_version();
        assert!(!version.is_null());
        let version_str = unsafe { std::ffi::CStr::from_ptr(version).to_str().unwrap() };
        assert_eq!(version_str, "0.1.0");
    }

    #[test]
    fn test_create_registry_null_driver() {
        let registry =
            unsafe { ceph_lancedb_create_registry(std::ptr::null_mut(), std::ptr::null()) };
        assert!(registry.is_null());
    }

    #[test]
    fn test_registry_lifecycle() {
        // Use a dummy non-null pointer for testing
        let fake_driver = 0x1234usize as *mut c_void;
        let fake_dpp = 0x5678usize as *const c_void;

        let registry = unsafe { ceph_lancedb_create_registry(fake_driver, fake_dpp) };
        assert!(!registry.is_null());

        // Free should not crash
        unsafe { ceph_lancedb_registry_free(registry) };
    }

    #[test]
    fn test_registry_free_null_safe() {
        // Should not crash when passed NULL
        unsafe { ceph_lancedb_registry_free(std::ptr::null_mut()) };
    }
}
