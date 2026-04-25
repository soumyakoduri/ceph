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
//! - `Session::new()` to create sessions with custom registries
//! - `connect().session()` to pass custom sessions
//!
//! # Usage from C++
//!
//! ```cpp
//! #include "ceph_lancedb_rgw.h"
//!
//! // Create session during RGW initialization
//! void* session = ceph_lancedb_create_session(driver, dpp);
//!
//! // Use with LanceDB C API
//! auto* builder = lancedb_connect("s3://mybucket/vectors");
//! lancedb_connect_builder_session(builder, session);
//! auto* db = lancedb_connect_builder_execute(builder);
//!
//! // ... use db ...
//!
//! // Cleanup
//! lancedb_connection_free(db);
//! ceph_lancedb_session_free(session);
//! ```

mod ffi;
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
// C API - For calling from RGW C++ code
//=============================================================================

/// Opaque handle to a Lance Session
pub type CephLanceDBSession = c_void;

/// Create a LanceDB session configured to use RGW as the S3 backend
///
/// This session should be passed to lancedb_connect_builder_session() when
/// connecting to a database. All s3:// URLs will be routed through RGW SAL.
///
/// # Safety
/// - `driver` must be a valid pointer to rgw::sal::Driver
/// - `dpp` must be a valid pointer to DoutPrefixProvider
/// - Both pointers must remain valid for the lifetime of the session
///
/// # Returns
/// Opaque pointer to session, or NULL on failure.
/// Caller must free with ceph_lancedb_session_free().
#[no_mangle]
pub unsafe extern "C" fn ceph_lancedb_create_session(
    driver: *mut c_void,
    dpp: *const c_void,
) -> *mut CephLanceDBSession {
    if driver.is_null() {
        return std::ptr::null_mut();
    }

    let session = create_rgw_session(driver, dpp);

    // Convert Arc to raw pointer
    Arc::into_raw(session) as *mut CephLanceDBSession
}

/// Create a LanceDB session with custom cache sizes
///
/// # Safety
/// - `driver` must be a valid pointer to rgw::sal::Driver
/// - `dpp` must be a valid pointer to DoutPrefixProvider (can be NULL)
/// - Both pointers must remain valid for the lifetime of the session
///
/// # Arguments
/// * `driver` - Pointer to RGW driver
/// * `dpp` - Pointer to DoutPrefixProvider
/// * `index_cache_size` - Size of index cache in bytes (0 to disable)
/// * `metadata_cache_size` - Size of metadata cache in bytes (0 to disable)
///
/// # Returns
/// Opaque pointer to session, or NULL on failure.
#[no_mangle]
pub unsafe extern "C" fn ceph_lancedb_create_session_with_cache(
    driver: *mut c_void,
    dpp: *const c_void,
    index_cache_size: usize,
    metadata_cache_size: usize,
) -> *mut CephLanceDBSession {
    if driver.is_null() {
        return std::ptr::null_mut();
    }

    let session = create_rgw_session_with_cache(driver, dpp, index_cache_size, metadata_cache_size);
    Arc::into_raw(session) as *mut CephLanceDBSession
}

/// Free a session created by ceph_lancedb_create_session
///
/// # Safety
/// - `session` must be a valid pointer returned by ceph_lancedb_create_session
/// - Must not be called more than once for the same session
/// - Session must not be in use when freed
#[no_mangle]
pub unsafe extern "C" fn ceph_lancedb_session_free(session: *mut CephLanceDBSession) {
    if !session.is_null() {
        // Reconstruct Arc and let it drop
        let _ = Arc::from_raw(session as *const lance::session::Session);
    }
}

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

/// Get the raw session pointer for use with lancedb-c
///
/// This returns the same pointer that was passed to the C API,
/// but cast to the type expected by lancedb_connect_builder_session().
///
/// # Safety
/// - `session` must be a valid pointer returned by ceph_lancedb_create_session
/// - The returned pointer is only valid as long as the session is alive
#[no_mangle]
pub unsafe extern "C" fn ceph_lancedb_session_as_ptr(
    session: *const CephLanceDBSession,
) -> *const c_void {
    session as *const c_void
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
    fn test_create_session_null_driver() {
        let session =
            unsafe { ceph_lancedb_create_session(std::ptr::null_mut(), std::ptr::null()) };
        assert!(session.is_null());
    }

    #[test]
    fn test_session_lifecycle() {
        // Use a dummy non-null pointer for testing
        let fake_driver = 0x1234usize as *mut c_void;
        let fake_dpp = 0x5678usize as *const c_void;

        let session = unsafe { ceph_lancedb_create_session(fake_driver, fake_dpp) };
        assert!(!session.is_null());

        // Free should not crash
        unsafe { ceph_lancedb_session_free(session) };
    }

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
}
