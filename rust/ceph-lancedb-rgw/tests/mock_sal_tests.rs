// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Ceph Authors

//! Comprehensive unit tests for ceph-lancedb-rgw using mock SAL
//!
//! These tests run with the mock-sal feature enabled, allowing testing
//! without a real Ceph environment. Run with:
//! ```bash
//! cargo test --features mock-sal
//! ```

use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use std::os::raw::c_void;
use std::sync::Arc;

use ceph_lancedb_rgw::{
    create_rgw_registry, create_rgw_session, create_rgw_session_with_cache,
    ffi::{RGWBuffer, RGWListResult, RGWObjectMeta},
    RGWObjectStore, RGWSessionConfig, RGWStoreProvider, DEFAULT_INDEX_CACHE_SIZE,
    DEFAULT_METADATA_CACHE_SIZE,
};

//=============================================================================
// Test Fixtures
//=============================================================================

/// Create a test RGWObjectStore with mock pointers
fn create_test_store(bucket: &str) -> RGWObjectStore {
    create_test_store_with_prefix(bucket, "")
}

fn create_test_store_with_prefix(bucket: &str, prefix: &str) -> RGWObjectStore {
    // Use non-null but invalid pointers for testing
    let fake_driver = 0x1000usize as *mut c_void;
    let fake_dpp = 0x2000usize as *const c_void;
    unsafe { RGWObjectStore::new(fake_driver, fake_dpp, bucket, prefix) }
}

/// Create a test RGWStoreProvider with mock pointers
fn create_test_provider() -> RGWStoreProvider {
    let fake_driver = 0x1000usize as *mut c_void;
    let fake_dpp = 0x2000usize as *const c_void;
    unsafe { RGWStoreProvider::new(fake_driver, fake_dpp) }
}

//=============================================================================
// RGWObjectStore Tests
//=============================================================================

mod object_store_tests {
    use super::*;

    #[test]
    fn test_store_creation() {
        let store = create_test_store("test-bucket");
        assert_eq!(format!("{}", store), "RGWObjectStore(bucket=test-bucket)");
    }

    #[test]
    fn test_store_debug_format() {
        let store = create_test_store("my-bucket");
        let debug = format!("{:?}", store);
        assert!(debug.contains("RGWObjectStore"));
        assert!(debug.contains("my-bucket"));
        assert!(debug.contains("driver"));
    }

    #[test]
    fn test_store_with_special_bucket_name() {
        let store = create_test_store("bucket-with-dashes-123");
        assert_eq!(
            format!("{}", store),
            "RGWObjectStore(bucket=bucket-with-dashes-123)"
        );
    }

    #[test]
    fn test_store_with_empty_bucket_name() {
        let store = create_test_store("");
        assert_eq!(format!("{}", store), "RGWObjectStore(bucket=)");
    }

    #[tokio::test]
    async fn test_put_object_success() {
        let store = create_test_store("test-bucket");
        let location = Path::from("test/object.txt");
        let data = Bytes::from("Hello, World!");
        let payload = PutPayload::from(data);

        let result = store.put(&location, payload).await;
        // Mock implementation returns success
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_object_with_empty_data() {
        let store = create_test_store("test-bucket");
        let location = Path::from("empty-object");
        let data = Bytes::new();
        let payload = PutPayload::from(data);

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_object_not_found() {
        let store = create_test_store("test-bucket");
        let location = Path::from("nonexistent/object.txt");

        let result = store.get(&location).await;
        // Mock implementation returns ENOENT (-2)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, object_store::Error::NotFound { .. }),
            "Expected NotFound error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn test_delete_object_success() {
        let store = create_test_store("test-bucket");
        let location = Path::from("object-to-delete.txt");

        let result = store.delete(&location).await;
        // Delete should succeed even for non-existent objects (ENOENT treated as success)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_head_object_not_found() {
        let store = create_test_store("test-bucket");
        let location = Path::from("nonexistent.txt");

        let result = store.head(&location).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_empty_bucket() {
        let store = create_test_store("empty-bucket");
        let prefix = Some(Path::from(""));

        let stream = store.list(prefix.as_ref());
        let results: Vec<_> = stream.collect().await;

        // Mock returns empty list
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_list_with_prefix() {
        let store = create_test_store("test-bucket");
        let prefix = Some(Path::from("data/vectors/"));

        let stream = store.list(prefix.as_ref());
        let results: Vec<_> = stream.collect().await;

        // Mock returns empty list
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let store = create_test_store("test-bucket");
        let prefix = Some(Path::from("data/"));

        let result = store.list_with_delimiter(prefix.as_ref()).await;
        assert!(result.is_ok());

        let list_result = result.unwrap();
        assert!(list_result.objects.is_empty());
        assert!(list_result.common_prefixes.is_empty());
    }

    #[tokio::test]
    async fn test_copy_object_success() {
        let store = create_test_store("test-bucket");
        let from = Path::from("source.txt");
        let to = Path::from("destination.txt");

        let result = store.copy(&from, &to).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_copy_if_not_exists_when_head_returns_not_found() {
        let store = create_test_store("test-bucket");
        let from = Path::from("source.txt");
        let to = Path::from("destination.txt");

        // Since head returns ENOENT (mock), copy_if_not_exists proceeds with copy
        let result = store.copy_if_not_exists(&from, &to).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rename_object() {
        let store = create_test_store("test-bucket");
        let from = Path::from("old-name.txt");
        let to = Path::from("new-name.txt");

        let result = store.rename(&from, &to).await;
        // Rename = copy + delete, both succeed in mock
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_path_with_unicode() {
        let store = create_test_store("test-bucket");
        let location = Path::from("données/fichier-测试.txt");
        let data = Bytes::from("Unicode content");
        let payload = PutPayload::from(data);

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_path_with_special_chars() {
        let store = create_test_store("test-bucket");
        let location = Path::from("path/with spaces/and+plus.txt");
        let data = Bytes::from("Special chars");
        let payload = PutPayload::from(data);

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_deeply_nested_path() {
        let store = create_test_store("test-bucket");
        let location = Path::from("a/b/c/d/e/f/g/h/i/j/deep-file.txt");
        let data = Bytes::from("Deep content");
        let payload = PutPayload::from(data);

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }
}

//=============================================================================
// Multipart Upload Tests
//=============================================================================

mod multipart_tests {
    use super::*;

    #[tokio::test]
    async fn test_init_multipart_not_supported() {
        let store = create_test_store("test-bucket");
        let location = Path::from("large-file.bin");

        let result = store.put_multipart(&location).await;
        // Mock returns ENOSYS for multipart operations
        assert!(result.is_err());
    }
}

//=============================================================================
// Provider Tests
//=============================================================================

mod provider_tests {
    use super::*;
    use lance_io::object_store::ObjectStoreProvider;
    use url::Url;

    #[test]
    fn test_provider_creation() {
        let provider = create_test_provider();
        let debug = format!("{:?}", provider);
        assert!(debug.contains("RGWStoreProvider"));
    }

    #[test]
    fn test_extract_path_basic() {
        let provider = create_test_provider();
        let url = Url::parse("s3://mybucket/path/to/file.lance").unwrap();

        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.as_ref(), "path/to/file.lance");
    }

    #[test]
    fn test_extract_path_root() {
        let provider = create_test_provider();
        let url = Url::parse("s3://mybucket/").unwrap();

        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.as_ref(), "");
    }

    #[test]
    fn test_extract_path_no_trailing_slash() {
        let provider = create_test_provider();
        let url = Url::parse("s3://mybucket").unwrap();

        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.as_ref(), "");
    }

    #[test]
    fn test_extract_path_deep() {
        let provider = create_test_provider();
        let url = Url::parse("s3://bucket/a/b/c/d/e/file.parquet").unwrap();

        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.as_ref(), "a/b/c/d/e/file.parquet");
    }

    #[test]
    fn test_extract_path_with_encoded_chars() {
        let provider = create_test_provider();
        let url = Url::parse("s3://bucket/path%20with%20spaces/file.txt").unwrap();

        let path = provider.extract_path(&url).unwrap();
        // URL decoding should happen
        assert!(path.as_ref().contains("path"));
    }

    #[test]
    fn test_driver_accessor() {
        let fake_driver = 0x1234usize as *mut c_void;
        let fake_dpp = 0x5678usize as *const c_void;
        let provider = unsafe { RGWStoreProvider::new(fake_driver, fake_dpp) };

        assert_eq!(provider.driver(), fake_driver);
        assert_eq!(provider.dpp(), fake_dpp);
    }

    #[tokio::test]
    async fn test_new_store_creates_object_store() {
        let provider = create_test_provider();
        let url = Url::parse("s3://test-bucket/vectors").unwrap();
        let params = lance_io::object_store::ObjectStoreParams::default();

        let result = provider.new_store(url, &params).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_new_store_no_bucket_fails() {
        let provider = create_test_provider();
        // file:// URLs don't have a host component
        let url = Url::parse("file:///path/to/file").unwrap();
        let params = lance_io::object_store::ObjectStoreParams::default();

        let result = provider.new_store(url, &params).await;
        // Should fail because no bucket/host in URL
        assert!(result.is_err());
    }
}

//=============================================================================
// Session Tests
//=============================================================================

mod session_tests {
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
            .with_index_cache_size(100 * 1024 * 1024)
            .with_metadata_cache_size(50 * 1024 * 1024);

        assert_eq!(config.index_cache_size, 100 * 1024 * 1024);
        assert_eq!(config.metadata_cache_size, 50 * 1024 * 1024);
    }

    #[test]
    fn test_session_config_no_cache() {
        let config = RGWSessionConfig::new().no_cache();
        assert_eq!(config.index_cache_size, 0);
        assert_eq!(config.metadata_cache_size, 0);
    }

    #[test]
    fn test_create_session() {
        let fake_driver = 0x1000usize as *mut c_void;
        let fake_dpp = 0x2000usize as *const c_void;

        let session = unsafe { create_rgw_session(fake_driver, fake_dpp) };
        assert!(Arc::strong_count(&session) == 1);
    }

    #[test]
    fn test_create_session_with_cache() {
        let fake_driver = 0x1000usize as *mut c_void;
        let fake_dpp = 0x2000usize as *const c_void;

        let session =
            unsafe { create_rgw_session_with_cache(fake_driver, fake_dpp, 1024 * 1024, 512 * 1024) };
        assert!(Arc::strong_count(&session) == 1);
    }

    #[test]
    fn test_create_registry() {
        let fake_driver = 0x1000usize as *mut c_void;
        let fake_dpp = 0x2000usize as *const c_void;

        let registry = unsafe { create_rgw_registry(fake_driver, fake_dpp) };
        assert!(Arc::strong_count(&registry) == 1);
    }

    #[test]
    fn test_session_config_build() {
        let fake_driver = 0x1000usize as *mut c_void;
        let fake_dpp = 0x2000usize as *const c_void;

        let config = RGWSessionConfig::new()
            .with_index_cache_size(64 * 1024 * 1024)
            .with_metadata_cache_size(32 * 1024 * 1024);

        let session = unsafe { config.build(fake_driver, fake_dpp) };
        assert!(Arc::strong_count(&session) == 1);
    }
}

//=============================================================================
// C API Tests
//=============================================================================

mod c_api_tests {
    use super::*;
    use ceph_lancedb_rgw::{
        ceph_lancedb_create_registry, ceph_lancedb_registry_free,
        ceph_lancedb_default_index_cache_size, ceph_lancedb_default_metadata_cache_size,
        ceph_lancedb_version,
    };
    use std::ffi::CStr;

    #[test]
    fn test_c_api_null_driver() {
        let registry = unsafe { ceph_lancedb_create_registry(std::ptr::null_mut(), std::ptr::null()) };
        assert!(registry.is_null());
    }

    #[test]
    fn test_c_api_registry_lifecycle() {
        let fake_driver = 0x1234usize as *mut c_void;
        let fake_dpp = 0x5678usize as *const c_void;

        let registry = unsafe { ceph_lancedb_create_registry(fake_driver, fake_dpp) };
        assert!(!registry.is_null());

        // Free should not crash
        unsafe { ceph_lancedb_registry_free(registry) };
    }

    #[test]
    fn test_c_api_default_cache_sizes() {
        let index_size = ceph_lancedb_default_index_cache_size();
        let metadata_size = ceph_lancedb_default_metadata_cache_size();

        assert_eq!(index_size, 256 * 1024 * 1024);
        assert_eq!(metadata_size, 128 * 1024 * 1024);
    }

    #[test]
    fn test_c_api_version() {
        let version = ceph_lancedb_version();
        assert!(!version.is_null());

        let version_str = unsafe { CStr::from_ptr(version).to_str().unwrap() };
        assert_eq!(version_str, "0.1.0");
    }

    #[test]
    fn test_c_api_free_null_registry() {
        // Freeing null should not crash
        unsafe { ceph_lancedb_registry_free(std::ptr::null_mut()) };
    }
}

//=============================================================================
// Error Handling Tests
//=============================================================================

mod error_tests {
    use super::*;

    #[tokio::test]
    async fn test_errno_enoent_mapped_to_not_found() {
        let store = create_test_store("test-bucket");
        let location = Path::from("nonexistent.txt");

        let err = store.get(&location).await.unwrap_err();
        match err {
            object_store::Error::NotFound { path, .. } => {
                assert!(path.contains("nonexistent.txt"));
            }
            _ => panic!("Expected NotFound error, got: {:?}", err),
        }
    }

    #[tokio::test]
    async fn test_head_error_propagation() {
        let store = create_test_store("test-bucket");
        let location = Path::from("missing.txt");

        let err = store.head(&location).await.unwrap_err();
        match err {
            object_store::Error::NotFound { path, .. } => {
                assert!(path.contains("missing.txt"));
            }
            _ => panic!("Expected NotFound error, got: {:?}", err),
        }
    }

    #[test]
    fn test_errno_mapping_coverage() {
        // Verify that errno_to_error produces correct error types for all mapped errnos
        let store = create_test_store("test-bucket");
        let path = Path::from("test.txt");

        // ENOENT -> NotFound
        let err = store.errno_to_error_for_test(-2, &path, "get");
        assert!(matches!(err, object_store::Error::NotFound { .. }));

        // EEXIST -> AlreadyExists
        let err = store.errno_to_error_for_test(-17, &path, "put");
        assert!(matches!(err, object_store::Error::AlreadyExists { .. }));

        // EPERM -> Generic
        let err = store.errno_to_error_for_test(-1, &path, "put");
        assert!(matches!(err, object_store::Error::Generic { .. }));

        // EACCES -> Generic
        let err = store.errno_to_error_for_test(-13, &path, "put");
        assert!(matches!(err, object_store::Error::Generic { .. }));

        // EINVAL -> Generic
        let err = store.errno_to_error_for_test(-22, &path, "put");
        assert!(matches!(err, object_store::Error::Generic { .. }));

        // ENOSPC -> Generic
        let err = store.errno_to_error_for_test(-28, &path, "put");
        assert!(matches!(err, object_store::Error::Generic { .. }));

        // ENAMETOOLONG -> Generic (with key too long message)
        let err = store.errno_to_error_for_test(-36, &path, "put");
        assert!(matches!(err, object_store::Error::Generic { .. }));
        assert!(err.to_string().contains("key too long"));

        // Unknown errno -> Generic
        let err = store.errno_to_error_for_test(-999, &path, "put");
        assert!(matches!(err, object_store::Error::Generic { .. }));
    }
}

//=============================================================================
// Concurrency Tests
//=============================================================================

mod concurrency_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_concurrent_puts() {
        let store = Arc::new(create_test_store("test-bucket"));
        let counter = Arc::new(AtomicUsize::new(0));

        let mut join_set = JoinSet::new();

        for i in 0..10 {
            let store = store.clone();
            let counter = counter.clone();

            join_set.spawn(async move {
                let location = Path::from(format!("concurrent/file-{}.txt", i));
                let data = Bytes::from(format!("Data for file {}", i));
                let payload = PutPayload::from(data);

                let result = store.put(&location, payload).await;
                if result.is_ok() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });
        }

        while join_set.join_next().await.is_some() {}

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_concurrent_deletes() {
        let store = Arc::new(create_test_store("test-bucket"));
        let counter = Arc::new(AtomicUsize::new(0));

        let mut join_set = JoinSet::new();

        for i in 0..10 {
            let store = store.clone();
            let counter = counter.clone();

            join_set.spawn(async move {
                let location = Path::from(format!("delete/file-{}.txt", i));
                let result = store.delete(&location).await;
                if result.is_ok() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });
        }

        while join_set.join_next().await.is_some() {}

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn test_store_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RGWObjectStore>();
    }

    #[test]
    fn test_provider_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RGWStoreProvider>();
    }
}

//=============================================================================
// Range Read Tests
//=============================================================================

mod range_read_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_ranges_empty() {
        let store = create_test_store("test-bucket");
        let location = Path::from("file.txt");

        let result = store.get_ranges(&location, &[]).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_opts_with_offset() {
        let store = create_test_store("test-bucket");
        let location = Path::from("file.txt");
        let opts = object_store::GetOptions {
            range: Some(object_store::GetRange::Offset(100)),
            ..Default::default()
        };

        let result = store.get_opts(&location, opts).await;
        // Mock returns ENOENT, so this fails
        assert!(result.is_err());
    }
}

//=============================================================================
// FFI Structure Tests
//=============================================================================

mod ffi_tests {
    use super::*;

    #[test]
    fn test_rgw_buffer_default() {
        let buffer = RGWBuffer::default();
        assert!(buffer.data.is_null());
        assert_eq!(buffer.len, 0);
        assert_eq!(buffer.capacity, 0);
    }

    #[test]
    fn test_rgw_object_meta_default() {
        let meta = RGWObjectMeta::default();
        assert_eq!(meta.size, 0);
        assert!(meta.etag.is_null());
        assert!(meta.content_type.is_null());
        assert_eq!(meta.last_modified, 0);
    }

    #[test]
    fn test_rgw_list_result_default() {
        let result = RGWListResult::default();
        assert!(result.entries.is_null());
        assert_eq!(result.count, 0);
        assert_eq!(result.is_truncated, 0);
        assert!(result.next_marker.is_null());
    }

    #[test]
    fn test_owned_buffer_empty_to_bytes() {
        use ceph_lancedb_rgw::ffi::OwnedRGWBuffer;
        let buffer = OwnedRGWBuffer(RGWBuffer::default());
        let bytes = buffer.to_bytes();
        assert!(bytes.is_empty());
    }
}

//=============================================================================
// Large Data Tests
//=============================================================================

mod large_data_tests {
    use super::*;

    #[tokio::test]
    async fn test_put_1mb_data() {
        let store = create_test_store("test-bucket");
        let location = Path::from("large-file.bin");

        // Create 1MB of data
        let data = vec![0xABu8; 1024 * 1024];
        let payload = PutPayload::from(Bytes::from(data));

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_with_various_sizes() {
        let store = create_test_store("test-bucket");

        let sizes = [0, 1, 100, 1024, 10 * 1024, 100 * 1024];

        for size in sizes {
            let location = Path::from(format!("size-test/{}.bin", size));
            let data = vec![0xCDu8; size];
            let payload = PutPayload::from(Bytes::from(data));

            let result = store.put(&location, payload).await;
            assert!(result.is_ok(), "Failed for size {}", size);
        }
    }
}

//=============================================================================
// Path Edge Case Tests
//=============================================================================

mod path_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_root_level_object() {
        let store = create_test_store("test-bucket");
        let location = Path::from("root-file.txt");
        let payload = PutPayload::from(Bytes::from("root content"));

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_object_with_dots_in_name() {
        let store = create_test_store("test-bucket");
        let location = Path::from("file.with.many.dots.txt");
        let payload = PutPayload::from(Bytes::from("dotted"));

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_object_with_underscore_prefix() {
        let store = create_test_store("test-bucket");
        let location = Path::from("_hidden/private.txt");
        let payload = PutPayload::from(Bytes::from("hidden"));

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_very_long_path() {
        let store = create_test_store("test-bucket");
        // Create a path with many segments
        let segments: Vec<String> = (0..50).map(|i| format!("seg{}", i)).collect();
        let path = segments.join("/") + "/file.txt";
        let location = Path::from(path);
        let payload = PutPayload::from(Bytes::from("long path"));

        let result = store.put(&location, payload).await;
        assert!(result.is_ok());
    }
}
