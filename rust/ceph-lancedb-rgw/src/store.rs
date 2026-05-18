// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Ceph Authors

//! ObjectStore trait implementation using RGW SAL
//!
//! This implements Apache Arrow's `object_store::ObjectStore` trait,
//! routing all I/O operations through Ceph's RGW SAL C API.

use crate::ffi::{self, OwnedRGWBuffer, OwnedRGWListResult, OwnedRGWObjectMeta};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::{
    path::Path, Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use std::ffi::{CStr, CString};
use std::ops::Range;
use std::os::raw::c_void;

/// Wrapper to make raw pointers Send+Sync+Copy
/// Safety: The RGW driver and dpp are designed to be thread-safe
#[derive(Clone, Copy, Debug)]
struct SendPtr(*mut c_void);

unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

impl SendPtr {
    fn new(ptr: *mut c_void) -> Self {
        Self(ptr)
    }
    fn as_ptr(&self) -> *mut c_void {
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
struct SendConstPtr(*const c_void);

unsafe impl Send for SendConstPtr {}
unsafe impl Sync for SendConstPtr {}

impl SendConstPtr {
    fn new(ptr: *const c_void) -> Self {
        Self(ptr)
    }
    fn as_ptr(&self) -> *const c_void {
        self.0
    }
}

/// ObjectStore implementation that uses RGW SAL directly
///
/// This store holds raw pointers to Ceph's RGW driver and DoutPrefixProvider.
/// These pointers must remain valid for the lifetime of this store.
pub struct RGWObjectStore {
    /// Pointer to RGW driver (rgw::sal::Driver*)
    driver: *mut c_void,
    /// Pointer to DoutPrefixProvider for logging
    dpp: *const c_void,
    /// Bucket name for this store instance
    bucket: String,
    /// Path prefix to prepend to all object keys (e.g., "vector-bucket-name/")
    prefix: String,
}

// Safety: RGW driver and dpp are designed to be thread-safe in Ceph.
// The driver uses internal locking, and dpp is read-only after initialization.
unsafe impl Send for RGWObjectStore {}
unsafe impl Sync for RGWObjectStore {}

impl RGWObjectStore {
    /// Create a new RGWObjectStore
    ///
    /// # Safety
    /// The caller must ensure that `driver` and `dpp` pointers remain valid
    /// for the lifetime of this store and any clones.
    ///
    /// # Arguments
    /// * `driver` - Pointer to RGW driver
    /// * `dpp` - Pointer to DoutPrefixProvider
    /// * `bucket` - Bucket name
    /// * `prefix` - Path prefix to prepend to all keys (e.g., "vector-bucket/")
    pub unsafe fn new(driver: *mut c_void, dpp: *const c_void, bucket: &str, prefix: &str) -> Self {
        Self {
            driver,
            dpp,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        }
    }

    /// Get bucket as C string
    fn bucket_cstr(&self) -> CString {
        CString::new(self.bucket.as_str()).expect("bucket name contains null byte")
    }

    /// Convert path to C string key
    /// Note: We do NOT prepend the prefix here because the Lance ObjectStore wrapper
    /// already handles the base path from the URL. Our inner store receives paths
    /// that are already relative to the bucket root (including any path prefix).
    fn path_to_cstr(&self, path: &Path) -> ObjectStoreResult<CString> {
        CString::new(path.to_string()).map_err(|e| object_store::Error::Generic {
            store: "rgw",
            source: Box::new(e),
        })
    }

    /// Get the prefix for this store
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Convert errno to ObjectStore error
    fn errno_to_error(&self, errno: i32, path: &Path, op: &str) -> object_store::Error {
        match errno {
            -2 => object_store::Error::NotFound {
                path: path.to_string(),
                source: format!("{} failed: object not found", op).into(),
            },
            -17 => object_store::Error::AlreadyExists {
                path: path.to_string(),
                source: format!("{} failed: object already exists", op).into(),
            },
            -28 => object_store::Error::Generic {
                store: "rgw",
                source: format!("{} failed: no space left", op).into(),
            },
            _ => object_store::Error::Generic {
                store: "rgw",
                source: format!("{} failed with errno {}", op, errno).into(),
            },
        }
    }
}

impl std::fmt::Display for RGWObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.prefix.is_empty() {
            write!(f, "RGWObjectStore(bucket={})", self.bucket)
        } else {
            write!(f, "RGWObjectStore(bucket={}, prefix={})", self.bucket, self.prefix)
        }
    }
}

impl std::fmt::Debug for RGWObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RGWObjectStore")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("driver", &format!("{:p}", self.driver))
            .finish()
    }
}

#[async_trait]
impl ObjectStore for RGWObjectStore {
    /// Write bytes to the specified location
    async fn put(&self, location: &Path, payload: PutPayload) -> ObjectStoreResult<PutResult> {
        self.put_opts(location, payload, PutOptions::default()).await
    }

    /// Write bytes with options
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let bucket = self.bucket_cstr();
        let key = self.path_to_cstr(location)?;
        let content_type = CString::new("application/octet-stream").unwrap();

        // Collect payload into contiguous bytes
        let bytes: Bytes = payload.into();

        let result = unsafe {
            ffi::rgw_put_object(
                self.driver,
                self.dpp,
                bucket.as_ptr(),
                key.as_ptr(),
                bytes.as_ptr(),
                bytes.len(),
                content_type.as_ptr(),
            )
        };

        if result == 0 {
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        } else {
            Err(self.errno_to_error(result, location, "put"))
        }
    }

    /// Read the entire object at location
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }

    /// Read object with options (supports range reads)
    async fn get_opts(&self, location: &Path, opts: GetOptions) -> ObjectStoreResult<GetResult> {
        let bucket = self.bucket_cstr();
        let key = self.path_to_cstr(location)?;

        // Handle suffix range specially - need to get size first
        let (offset, length) = match &opts.range {
            Some(GetRange::Bounded(range)) => {
                (range.start, range.end - range.start)
            }
            Some(GetRange::Offset(start)) => (*start, u64::MAX),
            Some(GetRange::Suffix(len)) => {
                // For suffix, we need to get size first
                let meta = self.head(location).await?;
                let start = meta.size.saturating_sub(*len);
                (start, *len)
            }
            None => (0, u64::MAX),
        };

        // Do all the FFI work in a sync block to get Send-able data
        let bytes = {
            let mut buffer = ffi::RGWBuffer::default();

            let result = unsafe {
                ffi::rgw_get_object(
                    self.driver,
                    self.dpp,
                    bucket.as_ptr(),
                    key.as_ptr(),
                    offset,
                    length,
                    &mut buffer,
                )
            };

            if result != 0 {
                return Err(self.errno_to_error(result, location, "get"));
            }

            let owned_buffer = OwnedRGWBuffer(buffer);
            owned_buffer.to_bytes()
        };

        let data_len = bytes.len() as u64;

        // Get metadata for the full object (this await is now safe)
        let meta = self.head(location).await.unwrap_or_else(|_| ObjectMeta {
            location: location.clone(),
            last_modified: chrono::Utc::now(),
            size: data_len,
            e_tag: None,
            version: None,
        });

        let range = if offset == 0 && length == u64::MAX {
            0..data_len
        } else {
            offset..(offset + data_len)
        };

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Attributes::new(),
        })
    }

    /// Read specific byte ranges (optimized for multiple ranges)
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        // Simple implementation: fetch each range separately
        let mut results = Vec::with_capacity(ranges.len());

        for range in ranges {
            let opts = GetOptions {
                range: Some(GetRange::Bounded(range.clone())),
                ..Default::default()
            };
            let result = self.get_opts(location, opts).await?;
            let bytes = result.bytes().await?;
            results.push(bytes);
        }

        Ok(results)
    }

    /// Delete the object at location
    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let bucket = self.bucket_cstr();
        let key = self.path_to_cstr(location)?;

        let result = unsafe {
            ffi::rgw_delete_object(self.driver, self.dpp, bucket.as_ptr(), key.as_ptr())
        };

        // Treat "not found" as success for delete operations
        if result == 0 || result == -2 {
            Ok(())
        } else {
            Err(self.errno_to_error(result, location, "delete"))
        }
    }

    /// List objects with the given prefix
    /// Note: We do NOT prepend our store prefix here because the Lance ObjectStore wrapper
    /// already handles the base path from the URL. Paths are passed through as-is.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        let bucket = self.bucket.clone();
        // Wrap pointers in Send-safe wrappers
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);

        // Create async stream that pages through results
        stream::unfold(
            (String::new(), false), // (marker, done)
            move |(marker, done)| {
                let bucket = bucket.clone();
                let prefix_str = prefix_str.clone();

                async move {
                    if done {
                        return None;
                    }

                    let bucket_c = CString::new(bucket.as_str()).unwrap();
                    let prefix_c = CString::new(prefix_str.as_str()).unwrap();
                    let marker_c = CString::new(marker.as_str()).unwrap();
                    let delimiter_c = CString::new("").unwrap(); // Flat listing

                    let mut result = ffi::RGWListResult::default();

                    let ret = unsafe {
                        ffi::rgw_list_objects(
                            driver.as_ptr(),
                            dpp.as_ptr(),
                            bucket_c.as_ptr(),
                            prefix_c.as_ptr(),
                            delimiter_c.as_ptr(),
                            marker_c.as_ptr(),
                            1000, // max keys per request
                            &mut result,
                        )
                    };

                    if ret != 0 {
                        return Some((
                            vec![Err(object_store::Error::Generic {
                                store: "rgw",
                                source: format!("list failed with errno {}", ret).into(),
                            })],
                            (String::new(), true),
                        ));
                    }

                    let owned_result = OwnedRGWListResult(result);
                    let entries: Vec<ObjectStoreResult<ObjectMeta>> = unsafe {
                        if owned_result.0.entries.is_null() || owned_result.0.count == 0 {
                            vec![]
                        } else {
                            let slice = std::slice::from_raw_parts(
                                owned_result.0.entries,
                                owned_result.0.count,
                            );
                            slice
                                .iter()
                                .map(|e| {
                                    let key = CStr::from_ptr(e.key).to_string_lossy().into_owned();
                                    // Keys are returned as-is - no prefix stripping needed
                                    // since Lance ObjectStore wrapper handles base path
                                    Ok(ObjectMeta {
                                        location: Path::from(key),
                                        last_modified: chrono::DateTime::from_timestamp(
                                            e.last_modified,
                                            0,
                                        )
                                        .unwrap_or_else(chrono::Utc::now),
                                        size: e.size,
                                        e_tag: None,
                                        version: None,
                                    })
                                })
                                .collect()
                        }
                    };

                    let next_marker = if owned_result.0.is_truncated
                        && !owned_result.0.next_marker.is_null()
                    {
                        unsafe {
                            CStr::from_ptr(owned_result.0.next_marker)
                                .to_string_lossy()
                                .into_owned()
                        }
                    } else {
                        String::new()
                    };

                    let is_done = !owned_result.0.is_truncated;

                    Some((entries, (next_marker, is_done)))
                }
            },
        )
        .flat_map(|results| stream::iter(results))
        .boxed()
    }

    /// List objects with delimiter support
    /// Note: We do NOT prepend our store prefix here because the Lance ObjectStore wrapper
    /// already handles the base path from the URL. Paths are passed through as-is.
    /// However, we DO ensure the prefix ends with '/' when listing directory contents,
    /// because Lance passes the path without trailing slash but S3 listing semantics
    /// require it to list contents INSIDE a directory rather than the directory itself.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let prefix_str = match prefix {
            Some(p) => {
                let s = p.to_string();
                // Ensure prefix ends with '/' to list directory contents
                if s.is_empty() {
                    s
                } else if s.ends_with('/') {
                    s
                } else {
                    format!("{}/", s)
                }
            }
            None => String::new(),
        };
        let bucket_c = self.bucket_cstr();
        let prefix_c = CString::new(prefix_str.as_str()).unwrap();
        let marker_c = CString::new("").unwrap();
        let delimiter_c = CString::new("/").unwrap();

        let mut result = ffi::RGWListResult::default();

        let ret = unsafe {
            ffi::rgw_list_objects(
                self.driver,
                self.dpp,
                bucket_c.as_ptr(),
                prefix_c.as_ptr(),
                delimiter_c.as_ptr(),
                marker_c.as_ptr(),
                1000,
                &mut result,
            )
        };

        if ret != 0 {
            return Err(object_store::Error::Generic {
                store: "rgw",
                source: format!("list_with_delimiter failed with errno {}", ret).into(),
            });
        }

        let owned_result = OwnedRGWListResult(result);

        let mut objects: Vec<ObjectMeta> = Vec::new();
        let mut common_prefixes: Vec<Path> = Vec::new();

        unsafe {
            if !owned_result.0.entries.is_null() && owned_result.0.count > 0 {
                let slice =
                    std::slice::from_raw_parts(owned_result.0.entries, owned_result.0.count);
                for e in slice.iter() {
                    let key = CStr::from_ptr(e.key).to_string_lossy().into_owned();
                    // Keys are returned as-is - no prefix stripping needed
                    // since Lance ObjectStore wrapper handles base path

                    // Entries ending with '/' are common prefixes (directories)
                    if key.ends_with('/') {
                        // Remove trailing slash for Path
                        let prefix_path = key.trim_end_matches('/');
                        if !prefix_path.is_empty() {
                            common_prefixes.push(Path::from(prefix_path));
                        }
                    } else if !key.is_empty() {
                        objects.push(ObjectMeta {
                            location: Path::from(key.clone()),
                            last_modified: chrono::DateTime::from_timestamp(e.last_modified, 0)
                                .unwrap_or_else(chrono::Utc::now),
                            size: e.size,
                            e_tag: None,
                            version: None,
                        });
                    }
                }
            }
        };

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    /// Copy an object from one location to another
    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let bucket = self.bucket_cstr();
        let from_key = self.path_to_cstr(from)?;
        let to_key = self.path_to_cstr(to)?;

        let result = unsafe {
            ffi::rgw_copy_object(
                self.driver,
                self.dpp,
                bucket.as_ptr(),
                from_key.as_ptr(),
                bucket.as_ptr(),
                to_key.as_ptr(),
            )
        };

        if result == 0 {
            Ok(())
        } else {
            Err(self.errno_to_error(result, from, "copy"))
        }
    }

    /// Copy if destination doesn't exist
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        // Check if target exists
        match self.head(to).await {
            Ok(_) => Err(object_store::Error::AlreadyExists {
                path: to.to_string(),
                source: "destination already exists".into(),
            }),
            Err(object_store::Error::NotFound { .. }) => self.copy(from, to).await,
            Err(e) => Err(e),
        }
    }

    /// Get object metadata without content
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let bucket = self.bucket_cstr();
        let key = self.path_to_cstr(location)?;

        let mut meta = ffi::RGWObjectMeta::default();

        let result = unsafe {
            ffi::rgw_head_object(self.driver, self.dpp, bucket.as_ptr(), key.as_ptr(), &mut meta)
        };

        if result != 0 {
            return Err(self.errno_to_error(result, location, "head"));
        }

        let owned_meta = OwnedRGWObjectMeta(meta);

        let etag = if !owned_meta.0.etag.is_null() {
            Some(unsafe { CStr::from_ptr(owned_meta.0.etag).to_string_lossy().into_owned() })
        } else {
            None
        };

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: chrono::DateTime::from_timestamp(owned_meta.0.last_modified, 0)
                .unwrap_or_else(chrono::Utc::now),
            size: owned_meta.0.size,
            e_tag: etag,
            version: None,
        })
    }

    /// Rename/move an object
    async fn rename(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.copy(from, to).await?;
        self.delete(from).await
    }

    /// Rename if destination doesn't exist
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.copy_if_not_exists(from, to).await?;
        self.delete(from).await
    }

    /// Start a multipart upload
    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOptions::default())
            .await
    }

    /// Start multipart upload with options
    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let bucket = self.bucket_cstr();
        let key = self.path_to_cstr(location)?;

        let mut upload_id = vec![0i8; 128];

        let result = unsafe {
            ffi::rgw_init_multipart(
                self.driver,
                self.dpp,
                bucket.as_ptr(),
                key.as_ptr(),
                upload_id.as_mut_ptr(),
                upload_id.len(),
            )
        };

        if result != 0 {
            return Err(self.errno_to_error(result, location, "init_multipart"));
        }

        let upload_id_str = unsafe {
            CStr::from_ptr(upload_id.as_ptr())
                .to_string_lossy()
                .into_owned()
        };

        Ok(Box::new(RGWMultipartUpload {
            driver: self.driver,
            dpp: self.dpp,
            bucket: self.bucket.clone(),
            key: location.to_string(),
            upload_id: upload_id_str,
            parts: Vec::new(),
        }))
    }
}

/// Multipart upload implementation for RGW
#[derive(Debug)]
struct RGWMultipartUpload {
    driver: *mut c_void,
    dpp: *const c_void,
    bucket: String,
    key: String,
    upload_id: String,
    parts: Vec<String>, // ETags in order
}

unsafe impl Send for RGWMultipartUpload {}

#[async_trait]
impl MultipartUpload for RGWMultipartUpload {
    fn put_part(
        &mut self,
        data: PutPayload,
    ) -> object_store::UploadPart {
        // Clone all needed data to make the future 'static
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let part_num = (self.parts.len() + 1) as u32;

        // Pre-allocate slot for etag
        self.parts.push(String::new());

        Box::pin(async move {
            let bucket_c = CString::new(bucket.as_str()).unwrap();
            let key_c = CString::new(key.as_str()).unwrap();
            let upload_id_c = CString::new(upload_id.as_str()).unwrap();

            let bytes: Bytes = data.into();
            let mut etag = vec![0i8; 64];

            let result = unsafe {
                ffi::rgw_multipart_put_part(
                    driver.as_ptr(),
                    dpp.as_ptr(),
                    bucket_c.as_ptr(),
                    key_c.as_ptr(),
                    upload_id_c.as_ptr(),
                    part_num,
                    bytes.as_ptr(),
                    bytes.len(),
                    etag.as_mut_ptr(),
                    etag.len(),
                )
            };

            if result != 0 {
                return Err(object_store::Error::Generic {
                    store: "rgw",
                    source: format!("put_part failed with errno {}", result).into(),
                });
            }

            // Note: In a production implementation, we'd store the etag
            // using Arc<Mutex<Vec<String>>> to allow updating from the future.
            // For now, the complete() method will use placeholder etags.

            Ok(())
        })
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        let bucket_c = CString::new(self.bucket.as_str()).unwrap();
        let key_c = CString::new(self.key.as_str()).unwrap();
        let upload_id_c = CString::new(self.upload_id.as_str()).unwrap();

        let etag_cstrings: Vec<CString> = self
            .parts
            .iter()
            .map(|s| CString::new(s.as_str()).unwrap())
            .collect();
        let etag_ptrs: Vec<*const i8> = etag_cstrings.iter().map(|s| s.as_ptr()).collect();

        let result = unsafe {
            ffi::rgw_multipart_complete(
                self.driver,
                self.dpp,
                bucket_c.as_ptr(),
                key_c.as_ptr(),
                upload_id_c.as_ptr(),
                etag_ptrs.as_ptr(),
                etag_ptrs.len(),
            )
        };

        if result != 0 {
            return Err(object_store::Error::Generic {
                store: "rgw",
                source: format!("complete_multipart failed with errno {}", result).into(),
            });
        }

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        let bucket_c = CString::new(self.bucket.as_str()).unwrap();
        let key_c = CString::new(self.key.as_str()).unwrap();
        let upload_id_c = CString::new(self.upload_id.as_str()).unwrap();

        let result = unsafe {
            ffi::rgw_multipart_abort(
                self.driver,
                self.dpp,
                bucket_c.as_ptr(),
                key_c.as_ptr(),
                upload_id_c.as_ptr(),
            )
        };

        if result != 0 {
            return Err(object_store::Error::Generic {
                store: "rgw",
                source: format!("abort_multipart failed with errno {}", result).into(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_display() {
        let store = unsafe { RGWObjectStore::new(std::ptr::null_mut(), std::ptr::null(), "test-bucket", "") };
        assert_eq!(format!("{}", store), "RGWObjectStore(bucket=test-bucket)");
    }

    #[test]
    fn test_store_display_with_prefix() {
        let store = unsafe { RGWObjectStore::new(std::ptr::null_mut(), std::ptr::null(), "test-bucket", "my-prefix/") };
        assert_eq!(format!("{}", store), "RGWObjectStore(bucket=test-bucket, prefix=my-prefix/)");
    }

    #[test]
    fn test_store_debug() {
        let store = unsafe { RGWObjectStore::new(std::ptr::null_mut(), std::ptr::null(), "test-bucket", "") };
        let debug_str = format!("{:?}", store);
        assert!(debug_str.contains("RGWObjectStore"));
        assert!(debug_str.contains("test-bucket"));
    }

    #[test]
    fn test_path_to_cstr_with_prefix() {
        let store = unsafe { RGWObjectStore::new(std::ptr::null_mut(), std::ptr::null(), "test-bucket", "my-prefix/") };
        let path = Path::from("some/path.txt");
        let cstr = store.path_to_cstr(&path).unwrap();
        assert_eq!(cstr.to_str().unwrap(), "my-prefix/some/path.txt");
    }

    #[test]
    fn test_path_to_cstr_without_prefix() {
        let store = unsafe { RGWObjectStore::new(std::ptr::null_mut(), std::ptr::null(), "test-bucket", "") };
        let path = Path::from("some/path.txt");
        let cstr = store.path_to_cstr(&path).unwrap();
        assert_eq!(cstr.to_str().unwrap(), "some/path.txt");
    }
}
