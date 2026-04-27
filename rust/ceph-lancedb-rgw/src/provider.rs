// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Ceph Authors

//! ObjectStoreProvider that creates RGWObjectStore instances
//!
//! This provider is registered in the ObjectStoreRegistry to handle
//! specific URL schemes (e.g., "s3" or "rgw").

use crate::store::RGWObjectStore;
use async_trait::async_trait;
use lance_core::Result;
use lance_io::object_store::{ObjectStore, ObjectStoreParams};
use object_store::path::Path;
use std::os::raw::c_void;
use std::sync::Arc;
use url::Url;

/// Provider that creates RGWObjectStore instances for s3:// or rgw:// URLs
///
/// This provider is designed to replace the default AWS S3 provider when
/// running inside Ceph RGW, routing all S3 operations through the native
/// SAL API instead of the S3 HTTP protocol.
#[derive(Debug)]
pub struct RGWStoreProvider {
    /// Pointer to RGW driver
    driver: *mut c_void,
    /// Pointer to DoutPrefixProvider
    dpp: *const c_void,
}

// Safety: These pointers are thread-safe in Ceph's RGW architecture
unsafe impl Send for RGWStoreProvider {}
unsafe impl Sync for RGWStoreProvider {}

impl RGWStoreProvider {
    /// Create a new RGWStoreProvider
    ///
    /// # Safety
    /// The caller must ensure that `driver` and `dpp` pointers remain valid
    /// for the lifetime of this provider and all stores it creates.
    ///
    /// # Arguments
    /// * `driver` - Pointer to rgw::sal::Driver (typically env.driver in RGW handlers)
    /// * `dpp` - Pointer to DoutPrefixProvider for logging
    pub unsafe fn new(driver: *mut c_void, dpp: *const c_void) -> Self {
        Self { driver, dpp }
    }

    /// Get the driver pointer
    pub fn driver(&self) -> *mut c_void {
        self.driver
    }

    /// Get the dpp pointer
    pub fn dpp(&self) -> *const c_void {
        self.dpp
    }
}

/// Trait that defines how to create ObjectStore instances for a given URL scheme.
/// This trait is defined in lance-io::object_store::providers
#[async_trait]
impl lance_io::object_store::ObjectStoreProvider for RGWStoreProvider {
    /// Create a new ObjectStore for the given URL
    ///
    /// Extracts the bucket name and path prefix from the URL and creates an
    /// RGWObjectStore configured to operate on that bucket with the path prefix.
    /// For example: s3://bucket/vector-bucket/ -> bucket="bucket", prefix="vector-bucket/"
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        // Extract bucket from URL: s3://bucket/path -> bucket
        let bucket = match base_path.host_str() {
            Some(b) => b,
            None => {
                return Err(lance_core::Error::io(
                    format!("URL '{}' must have a bucket/host component", base_path),
                    snafu::location!(),
                ));
            }
        };

        // Extract path prefix from URL: s3://bucket/path/ -> "path/"
        // The path includes the leading slash, so we trim it
        let path = base_path.path().trim_start_matches('/');
        // Ensure the prefix ends with a slash if non-empty (for proper path concatenation)
        let prefix = if path.is_empty() {
            String::new()
        } else if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };

        // Create RGW ObjectStore with bucket and prefix
        // Note: The prefix is stored but not used for path manipulation
        // since Lance's ObjectStore wrapper handles the base path
        let inner = Arc::new(unsafe { RGWObjectStore::new(self.driver, self.dpp, bucket, &prefix) });

        // Build ObjectStore with the inner RGW store
        // Use sensible defaults for optional parameters
        Ok(ObjectStore::new(
            inner,
            base_path,
            params.block_size,
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            params.list_is_lexically_ordered.unwrap_or(true),
            4,  // io_parallelism
            3,  // download_retry_count
            params.storage_options.as_ref(),
        ))
    }

    /// Extract the path relative to the bucket
    ///
    /// For s3://bucket/path/to/file, returns "path/to/file"
    fn extract_path(&self, url: &Url) -> Result<Path> {
        let path = url.path().trim_start_matches('/');
        Path::parse(path).map_err(|e| {
            lance_core::Error::io(
                format!("Invalid path in URL '{}': {}", url, e),
                snafu::location!(),
            )
        })
    }

}

// Note: calculate_object_store_prefix uses the default trait implementation
// which returns "{scheme}${authority}" (e.g., "s3$mybucket")

#[cfg(test)]
mod tests {
    use super::*;
    use lance_io::object_store::ObjectStoreProvider;

    #[test]
    fn test_extract_path() {
        let provider =
            unsafe { RGWStoreProvider::new(std::ptr::null_mut(), std::ptr::null()) };

        let url = Url::parse("s3://mybucket/path/to/file.lance").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.as_ref(), "path/to/file.lance");

        let url = Url::parse("s3://mybucket/").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.as_ref(), "");
    }

    // Note: calculate_object_store_prefix uses the default trait implementation
    // when available. Test omitted since method availability depends on lance-io version.

    #[test]
    fn test_provider_debug() {
        let provider =
            unsafe { RGWStoreProvider::new(std::ptr::null_mut(), std::ptr::null()) };
        let debug_str = format!("{:?}", provider);
        assert!(debug_str.contains("RGWStoreProvider"));
    }
}
