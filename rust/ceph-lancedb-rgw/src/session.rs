// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Ceph Authors

//! Session factory for creating LanceDB sessions with RGW backend
//!
//! This module provides convenient functions to create properly configured
//! Lance Sessions that use RGW as the storage backend for S3 URLs.

use crate::provider::RGWStoreProvider;
use lance::session::Session;
use lance_io::object_store::ObjectStoreRegistry;
use std::os::raw::c_void;
use std::sync::Arc;

/// Default index cache size (256 MB)
pub const DEFAULT_INDEX_CACHE_SIZE: usize = 256 * 1024 * 1024;

/// Default metadata cache size (128 MB)
pub const DEFAULT_METADATA_CACHE_SIZE: usize = 128 * 1024 * 1024;

/// Create a Lance Session configured to use RGW for S3 URLs
///
/// This creates an ObjectStoreRegistry with the "s3" scheme overridden
/// to use RGWStoreProvider instead of the default AWS provider.
///
/// # Safety
/// The caller must ensure that `driver` and `dpp` pointers remain valid
/// for the lifetime of the returned Session and any datasets opened with it.
///
/// # Arguments
/// * `driver` - Pointer to RGW driver (env.driver in RGW handlers)
/// * `dpp` - Pointer to DoutPrefixProvider for logging
///
/// # Returns
/// An Arc-wrapped Session ready to be used with lancedb::connect().session()
///
/// # Example (conceptual - actual usage is from C++)
/// ```ignore
/// let session = create_rgw_session(driver, dpp);
/// let db = lancedb::connect("s3://mybucket/vectors")
///     .session(session)
///     .execute()
///     .await?;
/// ```
pub unsafe fn create_rgw_session(driver: *mut c_void, dpp: *const c_void) -> Arc<Session> {
    create_rgw_session_with_cache(
        driver,
        dpp,
        DEFAULT_INDEX_CACHE_SIZE,
        DEFAULT_METADATA_CACHE_SIZE,
    )
}

/// Create a Lance Session with custom cache sizes
///
/// # Safety
/// The caller must ensure that `driver` and `dpp` pointers remain valid
/// for the lifetime of the returned Session.
///
/// # Arguments
/// * `driver` - Pointer to RGW driver
/// * `dpp` - Pointer to DoutPrefixProvider
/// * `index_cache_size` - Size of the index cache in bytes (0 to disable)
/// * `metadata_cache_size` - Size of the metadata cache in bytes (0 to disable)
pub unsafe fn create_rgw_session_with_cache(
    driver: *mut c_void,
    dpp: *const c_void,
    index_cache_size: usize,
    metadata_cache_size: usize,
) -> Arc<Session> {
    // Create registry with default providers
    let registry = create_rgw_registry(driver, dpp);

    // Create session with custom registry
    Arc::new(Session::new(index_cache_size, metadata_cache_size, registry))
}

/// Create just the ObjectStoreRegistry with RGW provider
///
/// Useful if you want to manage the Session yourself or need access
/// to the registry for other purposes.
///
/// # Safety
/// The caller must ensure that `driver` and `dpp` pointers remain valid
/// for the lifetime of the registry and any stores created from it.
pub unsafe fn create_rgw_registry(
    driver: *mut c_void,
    dpp: *const c_void,
) -> Arc<ObjectStoreRegistry> {
    let registry = Arc::new(ObjectStoreRegistry::default());
    let rgw_provider = Arc::new(RGWStoreProvider::new(driver, dpp));

    // Override "s3" - all s3:// URLs will now use RGW
    registry.insert("s3", rgw_provider.clone());

    // Also register "rgw" scheme for explicit usage
    registry.insert("rgw", rgw_provider);

    registry
}

/// Configuration for creating RGW sessions
#[derive(Debug, Clone)]
pub struct RGWSessionConfig {
    /// Size of the index cache in bytes
    pub index_cache_size: usize,
    /// Size of the metadata cache in bytes
    pub metadata_cache_size: usize,
}

impl Default for RGWSessionConfig {
    fn default() -> Self {
        Self {
            index_cache_size: DEFAULT_INDEX_CACHE_SIZE,
            metadata_cache_size: DEFAULT_METADATA_CACHE_SIZE,
        }
    }
}

impl RGWSessionConfig {
    /// Create a new config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the index cache size
    pub fn with_index_cache_size(mut self, size: usize) -> Self {
        self.index_cache_size = size;
        self
    }

    /// Set the metadata cache size
    pub fn with_metadata_cache_size(mut self, size: usize) -> Self {
        self.metadata_cache_size = size;
        self
    }

    /// Disable all caching
    pub fn no_cache(mut self) -> Self {
        self.index_cache_size = 0;
        self.metadata_cache_size = 0;
        self
    }

    /// Create a session with this configuration
    ///
    /// # Safety
    /// The caller must ensure that `driver` and `dpp` pointers remain valid
    /// for the lifetime of the returned Session.
    pub unsafe fn build(self, driver: *mut c_void, dpp: *const c_void) -> Arc<Session> {
        create_rgw_session_with_cache(
            driver,
            dpp,
            self.index_cache_size,
            self.metadata_cache_size,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_cache_sizes() {
        assert_eq!(DEFAULT_INDEX_CACHE_SIZE, 256 * 1024 * 1024);
        assert_eq!(DEFAULT_METADATA_CACHE_SIZE, 128 * 1024 * 1024);
    }

    #[test]
    fn test_session_config_default() {
        let config = RGWSessionConfig::default();
        assert_eq!(config.index_cache_size, DEFAULT_INDEX_CACHE_SIZE);
        assert_eq!(config.metadata_cache_size, DEFAULT_METADATA_CACHE_SIZE);
    }

    #[test]
    fn test_session_config_builder() {
        let config = RGWSessionConfig::new()
            .with_index_cache_size(100)
            .with_metadata_cache_size(200);

        assert_eq!(config.index_cache_size, 100);
        assert_eq!(config.metadata_cache_size, 200);
    }

    #[test]
    fn test_session_config_no_cache() {
        let config = RGWSessionConfig::new().no_cache();
        assert_eq!(config.index_cache_size, 0);
        assert_eq!(config.metadata_cache_size, 0);
    }
}
