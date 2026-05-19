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
    path::Path, Attributes, Error as ObjectStoreError, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    UpdateVersion,
};
use std::ffi::{CStr, CString};
use std::ops::Range;
use std::os::raw::{c_int, c_void};

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

/// Chunk size for streaming reads (8 MB).
/// Objects larger than this are read in multiple chunks to bound memory usage.
const STREAM_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

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

    /// Convert errno to ObjectStore error (test-only public accessor)
    #[cfg(any(test, feature = "mock-sal"))]
    pub fn errno_to_error_for_test(&self, errno: i32, path: &Path, op: &str) -> object_store::Error {
        self.errno_to_error(errno, path, op)
    }

    /// Convert errno to ObjectStore error
    ///
    /// Maps common POSIX errno values to appropriate ObjectStore error types.
    /// Negative errno values are expected (e.g., -2 for ENOENT).
    fn errno_to_error(&self, errno: i32, path: &Path, op: &str) -> object_store::Error {
        match errno {
            -2 => object_store::Error::NotFound { // ENOENT
                path: path.to_string(),
                source: format!("{} failed: object not found", op).into(),
            },
            -1 => object_store::Error::Generic { // EPERM
                store: "rgw",
                source: format!("{} failed: operation not permitted", op).into(),
            },
            -13 => object_store::Error::Generic { // EACCES
                store: "rgw",
                source: format!("{} failed: permission denied", op).into(),
            },
            -17 => object_store::Error::AlreadyExists { // EEXIST
                path: path.to_string(),
                source: format!("{} failed: object already exists", op).into(),
            },
            -22 => object_store::Error::Generic { // EINVAL
                store: "rgw",
                source: format!("{} failed: invalid argument", op).into(),
            },
            -28 => object_store::Error::Generic { // ENOSPC
                store: "rgw",
                source: format!("{} failed: no space left on device", op).into(),
            },
            -36 => object_store::Error::Generic { // ENAMETOOLONG
                store: "rgw",
                source: format!("{} failed: object key too long", op).into(),
            },
            -38 => object_store::Error::NotSupported { // ENOSYS
                source: format!("{} not supported by SAL backend", op).into(),
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

    /// Write bytes with options (supports conditional writes)
    ///
    /// Supports three modes:
    /// - `PutMode::Overwrite` (default): Unconditional write, overwrites any existing object.
    /// - `PutMode::Create`: Atomic create-if-not-exists. Returns `AlreadyExists` if the
    ///   object already exists. Uses SAL's `if_nomatch="*"` precondition.
    /// - `PutMode::Update(version)`: Compare-and-swap. Only writes if the existing object's
    ///   ETag matches `version.e_tag`. Returns `Precondition` error otherwise.
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let bucket = self.bucket_cstr();
        let key = self.path_to_cstr(location)?;
        let content_type = CString::new("application/octet-stream").unwrap();

        // Collect payload into contiguous bytes
        let bytes: Bytes = payload.into();

        match opts.mode {
            PutMode::Overwrite => {
                // Unconditional write - use the simple put API
                let result = unsafe {
                    ffi::rgw_put_object(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
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
            PutMode::Create => {
                // Create-if-not-exists: use if_nomatch="*"
                let if_nomatch = CString::new("*").unwrap();
                let mut canceled: c_int = 0;

                let result = unsafe {
                    ffi::rgw_put_object_conditional(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
                        bucket.as_ptr(),
                        key.as_ptr(),
                        bytes.as_ptr(),
                        bytes.len(),
                        content_type.as_ptr(),
                        std::ptr::null(),       // if_match: not used
                        if_nomatch.as_ptr(),     // if_nomatch: "*"
                        &mut canceled,
                    )
                };

                if result != 0 {
                    return Err(self.errno_to_error(result, location, "put (create)"));
                }
                if canceled != 0 {
                    return Err(ObjectStoreError::AlreadyExists {
                        path: location.to_string(),
                        source: "object already exists (conditional create failed)".into(),
                    });
                }
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                })
            }
            PutMode::Update(UpdateVersion { e_tag, .. }) => {
                // Compare-and-swap: only write if existing ETag matches
                let etag_str = e_tag.ok_or_else(|| ObjectStoreError::Generic {
                    store: "rgw",
                    source: "PutMode::Update requires e_tag".into(),
                })?;
                let if_match = CString::new(etag_str.as_str()).map_err(|_| {
                    ObjectStoreError::Generic {
                        store: "rgw",
                        source: "invalid etag string".into(),
                    }
                })?;
                let mut canceled: c_int = 0;

                let result = unsafe {
                    ffi::rgw_put_object_conditional(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
                        bucket.as_ptr(),
                        key.as_ptr(),
                        bytes.as_ptr(),
                        bytes.len(),
                        content_type.as_ptr(),
                        if_match.as_ptr(),       // if_match: expected ETag
                        std::ptr::null(),         // if_nomatch: not used
                        &mut canceled,
                    )
                };

                if result != 0 {
                    return Err(self.errno_to_error(result, location, "put (update)"));
                }
                if canceled != 0 {
                    return Err(ObjectStoreError::Precondition {
                        path: location.to_string(),
                        source: "object ETag does not match (conditional update failed)".into(),
                    });
                }
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                })
            }
        }
    }

    /// Read the entire object at location
    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }

    /// Read object with options (supports range reads and streaming)
    ///
    /// For reads larger than STREAM_CHUNK_SIZE (8 MB), data is returned as a
    /// multi-chunk stream so that only one chunk is held in memory at a time.
    /// The C API already supports offset+length, so chunked streaming is done
    /// by issuing multiple bounded reads.
    async fn get_opts(&self, location: &Path, opts: GetOptions) -> ObjectStoreResult<GetResult> {
        // Get metadata first — we need the object size for range calculations
        // and for the GetResult metadata field
        let meta = self.head(location).await?;
        let obj_size = meta.size;

        // Resolve the byte range to read
        let (range_start, range_end) = match &opts.range {
            Some(GetRange::Bounded(range)) => (range.start, range.end.min(obj_size)),
            Some(GetRange::Offset(start)) => (*start, obj_size),
            Some(GetRange::Suffix(len)) => {
                let start = obj_size.saturating_sub(*len);
                (start, obj_size)
            }
            None => (0, obj_size),
        };

        let total_len = range_end.saturating_sub(range_start);

        if total_len == 0 {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::once(async { Ok(Bytes::new()) }).boxed(),
                ),
                meta,
                range: range_start..range_end,
                attributes: Attributes::new(),
            });
        }

        // For small reads (≤ one chunk), use a single FFI call — no overhead
        if total_len <= STREAM_CHUNK_SIZE {
            let bucket = self.bucket_cstr();
            let key = self.path_to_cstr(location)?;

            let bytes = {
                let mut buffer = ffi::RGWBuffer::default();
                let result = unsafe {
                    ffi::rgw_get_object(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
                        bucket.as_ptr(),
                        key.as_ptr(),
                        range_start,
                        total_len,
                        &mut buffer,
                    )
                };
                if result != 0 {
                    return Err(self.errno_to_error(result, location, "get"));
                }
                OwnedRGWBuffer(buffer).to_bytes()
            };

            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::once(async move { Ok(bytes) }).boxed(),
                ),
                meta,
                range: range_start..range_end,
                attributes: Attributes::new(),
            });
        }

        // For large reads, stream in STREAM_CHUNK_SIZE chunks.
        // Each chunk issues its own rgw_get_object(offset, chunk_len) call,
        // so only one chunk buffer is live at a time.
        let bucket_name = self.bucket.clone();
        let key_str = location.to_string();
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);

        let chunk_stream = stream::unfold(range_start, move |offset| {
            let bucket_name = bucket_name.clone();
            let key_str = key_str.clone();

            async move {
                if offset >= range_end {
                    return None;
                }

                let chunk_len = STREAM_CHUNK_SIZE.min(range_end - offset);
                let bucket_c = CString::new(bucket_name.as_str()).unwrap();
                let key_c = CString::new(key_str.as_str()).unwrap();

                let mut buffer = ffi::RGWBuffer::default();
                let result = unsafe {
                    ffi::rgw_get_object(
                        driver.as_ptr(),
                        dpp.as_ptr(),
                        std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
                        bucket_c.as_ptr(),
                        key_c.as_ptr(),
                        offset,
                        chunk_len,
                        &mut buffer,
                    )
                };

                if result != 0 {
                    return Some((
                        Err(object_store::Error::Generic {
                            store: "rgw",
                            source: format!(
                                "get chunk at offset {} failed with errno {}",
                                offset, result
                            )
                            .into(),
                        }),
                        range_end, // stop iteration
                    ));
                }

                let bytes = OwnedRGWBuffer(buffer).to_bytes();
                let next_offset = offset + bytes.len() as u64;
                Some((Ok(bytes), next_offset))
            }
        });

        Ok(GetResult {
            payload: GetResultPayload::Stream(chunk_stream.boxed()),
            meta,
            range: range_start..range_end,
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
            ffi::rgw_delete_object(self.driver, self.dpp, std::ptr::null_mut(), bucket.as_ptr(), key.as_ptr())
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
                            std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
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

                    let next_marker = if owned_result.0.is_truncated != 0
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

                    let is_done = owned_result.0.is_truncated == 0;

                    // If no entries were returned, we're done regardless
                    let is_done = is_done || entries.is_empty();

                    Some((entries, (next_marker, is_done)))
                }
            },
        )
        .flat_map(|results| stream::iter(results))
        .boxed()
    }

    /// List objects with delimiter support (paginated)
    ///
    /// Fetches all pages using marker-based pagination so that buckets with
    /// more than 1000 entries are fully enumerated.
    ///
    /// Note: We do NOT prepend our store prefix here because the Lance ObjectStore
    /// wrapper already handles the base path from the URL. Paths are passed through
    /// as-is. However, we DO ensure the prefix ends with '/' when listing directory
    /// contents, because Lance passes the path without trailing slash but S3 listing
    /// semantics require it to list contents INSIDE a directory rather than the
    /// directory itself.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let prefix_str = match prefix {
            Some(p) => {
                let s = p.to_string();
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

        let mut objects: Vec<ObjectMeta> = Vec::new();
        let mut common_prefixes: Vec<Path> = Vec::new();
        let mut marker = String::new();

        loop {
            let bucket_c = self.bucket_cstr();
            let prefix_c = CString::new(prefix_str.as_str()).unwrap();
            let marker_c = CString::new(marker.as_str()).unwrap();
            let delimiter_c = CString::new("/").unwrap();

            let mut result = ffi::RGWListResult::default();

            let ret = unsafe {
                ffi::rgw_list_objects(
                    self.driver,
                    self.dpp,
                    std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
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

            unsafe {
                if !owned_result.0.entries.is_null() && owned_result.0.count > 0 {
                    let slice = std::slice::from_raw_parts(
                        owned_result.0.entries,
                        owned_result.0.count,
                    );
                    for e in slice.iter() {
                        let key = CStr::from_ptr(e.key).to_string_lossy().into_owned();

                        // Entries ending with '/' are common prefixes (directories)
                        if key.ends_with('/') {
                            let prefix_path = key.trim_end_matches('/');
                            if !prefix_path.is_empty() {
                                common_prefixes.push(Path::from(prefix_path));
                            }
                        } else if !key.is_empty() {
                            objects.push(ObjectMeta {
                                location: Path::from(key.clone()),
                                last_modified: chrono::DateTime::from_timestamp(
                                    e.last_modified,
                                    0,
                                )
                                .unwrap_or_else(chrono::Utc::now),
                                size: e.size,
                                e_tag: None,
                                version: None,
                            });
                        }
                    }
                }
            }

            // Check if there are more pages
            if owned_result.0.is_truncated == 0 {
                break;
            }

            // Get the next marker for pagination
            if !owned_result.0.next_marker.is_null() {
                marker = unsafe {
                    CStr::from_ptr(owned_result.0.next_marker)
                        .to_string_lossy()
                        .into_owned()
                };
            } else {
                // No next_marker means we're done even if is_truncated was set
                break;
            }

            // Safety: if we got zero entries, stop to prevent infinite loop
            if owned_result.0.count == 0 {
                break;
            }
        }

        // Deduplicate common_prefixes (same prefix could appear in multiple pages)
        common_prefixes.sort();
        common_prefixes.dedup();

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
                std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
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

    /// Copy if destination doesn't exist (atomic via SAL precondition)
    ///
    /// Uses `rgw_copy_object_conditional` with `if_nomatch="*"` so the
    /// existence check and copy are performed atomically by the SAL backend,
    /// eliminating the race window of head-then-copy.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let bucket = self.bucket_cstr();
        let from_key = self.path_to_cstr(from)?;
        let to_key = self.path_to_cstr(to)?;
        let if_nomatch = CString::new("*").unwrap();

        let result = unsafe {
            ffi::rgw_copy_object_conditional(
                self.driver,
                self.dpp,
                std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
                bucket.as_ptr(),
                from_key.as_ptr(),
                bucket.as_ptr(),
                to_key.as_ptr(),
                std::ptr::null(),       // if_match: not used
                if_nomatch.as_ptr(),     // if_nomatch: "*" = copy-if-not-exists
            )
        };

        if result == 0 {
            Ok(())
        } else if result == -17 {
            // -EEXIST: destination already exists
            Err(ObjectStoreError::AlreadyExists {
                path: to.to_string(),
                source: "destination already exists".into(),
            })
        } else {
            Err(self.errno_to_error(result, from, "copy_if_not_exists"))
        }
    }

    /// Get object metadata without content
    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let bucket = self.bucket_cstr();
        let key = self.path_to_cstr(location)?;

        let mut meta = ffi::RGWObjectMeta::default();

        let result = unsafe {
            ffi::rgw_head_object(self.driver, self.dpp, std::ptr::null_mut(), bucket.as_ptr(), key.as_ptr(), &mut meta)
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
                std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
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
                    std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
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
                std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
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
                std::ptr::null_mut(),  // yield_ctx: NULL for Tokio threads
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
