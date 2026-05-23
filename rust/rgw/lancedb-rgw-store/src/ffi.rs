/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! FFI bindings to Ceph's rgw_sal_wrapper.cc
//!
//! These functions are implemented in Ceph's C++ code and resolved at runtime.
//! The Rust code calls these functions to perform actual storage operations.

use std::os::raw::{c_char, c_int, c_void};

/// Object identifier (key + optional version)
#[repr(C)]
pub struct RGWObject {
    /// Object key, null-terminated
    pub key: *const c_char,
    /// Version ID, null-terminated (NULL for current version)
    pub version_id: *const c_char,
}

impl RGWObject {
    /// Create an RGWObject for the current version (no version_id)
    pub fn from_key(key: *const c_char) -> Self {
        Self {
            key,
            version_id: std::ptr::null(),
        }
    }
}

impl Default for RGWObject {
    fn default() -> Self {
        Self {
            key: std::ptr::null(),
            version_id: std::ptr::null(),
        }
    }
}

/// Buffer for receiving data from RGW
#[repr(C)]
pub struct RGWBuffer {
    /// Pointer to data (allocated by RGW, freed by rgw_free_buffer)
    pub data: *mut u8,
    /// Length of data
    pub len: usize,
}

impl Default for RGWBuffer {
    fn default() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
        }
    }
}

/// Object metadata returned by head operations
#[repr(C)]
pub struct RGWObjectMeta {
    /// Object size in bytes
    pub size: u64,
    /// ETag (MD5 hash), null-terminated string
    pub etag: *mut c_char,
    /// Content type, null-terminated string
    pub content_type: *mut c_char,
    /// Last modified timestamp (Unix epoch seconds)
    pub last_modified: i64,
}

impl Default for RGWObjectMeta {
    fn default() -> Self {
        Self {
            size: 0,
            etag: std::ptr::null_mut(),
            content_type: std::ptr::null_mut(),
            last_modified: 0,
        }
    }
}

/// Single entry in a list operation result
#[repr(C)]
pub struct RGWListEntry {
    /// Object key, null-terminated string
    pub key: *mut c_char,
    /// Object size in bytes
    pub size: u64,
    /// Last modified timestamp (Unix epoch seconds)
    pub last_modified: i64,
}

/// Result of a list objects operation
/// Note: is_truncated is c_int (not bool) to match the C struct exactly
#[repr(C)]
pub struct RGWListResult {
    /// Array of list entries
    pub entries: *mut RGWListEntry,
    /// Number of entries
    pub count: usize,
    /// True (1) if there are more results, 0 otherwise
    pub is_truncated: c_int,
    /// Marker for next page, null-terminated string
    pub next_marker: *mut c_char,
}

impl Default for RGWListResult {
    fn default() -> Self {
        Self {
            entries: std::ptr::null_mut(),
            count: 0,
            is_truncated: 0,
            next_marker: std::ptr::null_mut(),
        }
    }
}

// When mock-sal feature is enabled, use stub implementations
#[cfg(feature = "mock-sal")]
mod mock_impl {
    use super::*;

    pub unsafe fn rgw_put_object(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
        _data: *const u8,
        _len: usize,
        _content_type: *const c_char,
    ) -> c_int {
        0 // Success
    }

    pub unsafe fn rgw_put_object_conditional(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
        _data: *const u8,
        _len: usize,
        _content_type: *const c_char,
        _if_match: *const c_char,
        _if_nomatch: *const c_char,
        canceled: *mut c_int,
    ) -> c_int {
        if !canceled.is_null() {
            *canceled = 0;
        }
        0 // Success
    }

    pub unsafe fn rgw_get_object(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
        _offset: u64,
        _length: u64,
        buffer: *mut RGWBuffer,
    ) -> c_int {
        (*buffer).data = std::ptr::null_mut();
        (*buffer).len = 0;
        -2 // ENOENT - not found
    }

    pub unsafe fn rgw_delete_object(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
    ) -> c_int {
        0 // Success
    }

    pub unsafe fn rgw_head_object(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
        _meta: *mut RGWObjectMeta,
    ) -> c_int {
        -2 // ENOENT
    }

    pub unsafe fn rgw_list_objects(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _prefix: *const c_char,
        _delimiter: *const c_char,
        _marker: *const c_char,
        _max_keys: u32,
        result: *mut RGWListResult,
    ) -> c_int {
        (*result).entries = std::ptr::null_mut();
        (*result).count = 0;
        (*result).is_truncated = 0;
        (*result).next_marker = std::ptr::null_mut();
        0 // Success with empty result
    }

    pub unsafe fn rgw_copy_object(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _src_bucket: *const c_char,
        _src_obj: *const RGWObject,
        _dst_bucket: *const c_char,
        _dst_obj: *const RGWObject,
    ) -> c_int {
        0 // Success
    }

    pub unsafe fn rgw_copy_object_conditional(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _src_bucket: *const c_char,
        _src_obj: *const RGWObject,
        _dst_bucket: *const c_char,
        _dst_obj: *const RGWObject,
        _if_match: *const c_char,
        _if_nomatch: *const c_char,
    ) -> c_int {
        0 // Success
    }

    pub unsafe fn rgw_init_multipart(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
        _upload_id: *mut c_char,
        _upload_id_len: usize,
    ) -> c_int {
        0 // Success
    }

    pub unsafe fn rgw_multipart_put_part(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
        _upload_id: *const c_char,
        _part_num: u32,
        _data: *const u8,
        _len: usize,
        _etag: *mut c_char,
        _etag_len: usize,
    ) -> c_int {
        0 // Success
    }

    pub unsafe fn rgw_multipart_complete(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
        _upload_id: *const c_char,
        _etags: *const *const c_char,
        _count: usize,
    ) -> c_int {
        0 // Success
    }

    pub unsafe fn rgw_multipart_abort(
        _driver: *mut c_void,
        _dpp: *const c_void,
        _yield_ctx: *mut c_void,
        _bucket: *const c_char,
        _obj: *const RGWObject,
        _upload_id: *const c_char,
    ) -> c_int {
        0 // Success
    }

    pub unsafe fn rgw_free_buffer(_buffer: *mut RGWBuffer) {
        // Nothing to free in mock
    }

    pub unsafe fn rgw_free_object_meta(_meta: *mut RGWObjectMeta) {
        // Nothing to free in mock
    }

    pub unsafe fn rgw_free_list_result(_result: *mut RGWListResult) {
        // Nothing to free in mock
    }

    pub unsafe fn rgw_get_max_chunk_size(_driver: *mut c_void) -> u64 {
        4 * 1024 * 1024 // 4 MB default
    }
}

// Real FFI declarations - used when not in mock mode
#[cfg(not(feature = "mock-sal"))]
extern "C" {
    //=========================================================================
    // Core Object Operations
    //=========================================================================

    /// Write an object to RGW storage
    pub fn rgw_put_object(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
        data: *const u8,
        len: usize,
        content_type: *const c_char,
    ) -> c_int;

    /// Write an object with conditional preconditions
    pub fn rgw_put_object_conditional(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
        data: *const u8,
        len: usize,
        content_type: *const c_char,
        if_match: *const c_char,
        if_nomatch: *const c_char,
        canceled: *mut c_int,
    ) -> c_int;

    /// Read an object from RGW storage
    pub fn rgw_get_object(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
        offset: u64,
        length: u64,
        buffer: *mut RGWBuffer,
    ) -> c_int;

    /// Delete an object from RGW storage
    pub fn rgw_delete_object(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
    ) -> c_int;

    /// Get object metadata without reading content
    pub fn rgw_head_object(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
        meta: *mut RGWObjectMeta,
    ) -> c_int;

    /// List objects in a bucket
    pub fn rgw_list_objects(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        prefix: *const c_char,
        delimiter: *const c_char,
        marker: *const c_char,
        max_keys: u32,
        result: *mut RGWListResult,
    ) -> c_int;

    /// Copy an object within or between buckets
    pub fn rgw_copy_object(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        src_bucket: *const c_char,
        src_obj: *const RGWObject,
        dst_bucket: *const c_char,
        dst_obj: *const RGWObject,
    ) -> c_int;

    /// Copy an object with conditional preconditions
    pub fn rgw_copy_object_conditional(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        src_bucket: *const c_char,
        src_obj: *const RGWObject,
        dst_bucket: *const c_char,
        dst_obj: *const RGWObject,
        if_match: *const c_char,
        if_nomatch: *const c_char,
    ) -> c_int;

    //=========================================================================
    // Multipart Upload Operations
    //=========================================================================

    /// Initialize a multipart upload
    pub fn rgw_init_multipart(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
        upload_id: *mut c_char,
        upload_id_len: usize,
    ) -> c_int;

    /// Upload a part in a multipart upload
    pub fn rgw_multipart_put_part(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
        upload_id: *const c_char,
        part_num: u32,
        data: *const u8,
        len: usize,
        etag: *mut c_char,
        etag_len: usize,
    ) -> c_int;

    /// Complete a multipart upload
    pub fn rgw_multipart_complete(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
        upload_id: *const c_char,
        etags: *const *const c_char,
        count: usize,
    ) -> c_int;

    /// Abort a multipart upload
    pub fn rgw_multipart_abort(
        driver: *mut c_void,
        dpp: *const c_void,
        yield_ctx: *mut c_void,
        bucket: *const c_char,
        obj: *const RGWObject,
        upload_id: *const c_char,
    ) -> c_int;

    //=========================================================================
    // Memory Management
    //=========================================================================

    /// Free a buffer allocated by rgw_get_object
    pub fn rgw_free_buffer(buffer: *mut RGWBuffer);

    /// Free metadata allocated by rgw_head_object
    pub fn rgw_free_object_meta(meta: *mut RGWObjectMeta);

    /// Free list result allocated by rgw_list_objects
    pub fn rgw_free_list_result(result: *mut RGWListResult);

    //=========================================================================
    // Configuration
    //=========================================================================

    /// Get the configured rgw_max_chunk_size (in bytes)
    pub fn rgw_get_max_chunk_size(driver: *mut c_void) -> u64;
}

//=============================================================================
// Safe Rust Wrappers
//=============================================================================

/// RAII wrapper for RGWBuffer that automatically frees on drop
pub struct OwnedRGWBuffer(pub RGWBuffer);

impl Drop for OwnedRGWBuffer {
    fn drop(&mut self) {
        if !self.0.data.is_null() {
            #[cfg(feature = "mock-sal")]
            unsafe {
                mock_impl::rgw_free_buffer(&mut self.0);
            }
            #[cfg(not(feature = "mock-sal"))]
            unsafe {
                rgw_free_buffer(&mut self.0);
            }
        }
    }
}

impl OwnedRGWBuffer {
    /// Convert to Bytes, copying the data
    pub fn to_bytes(&self) -> bytes::Bytes {
        if self.0.data.is_null() || self.0.len == 0 {
            bytes::Bytes::new()
        } else {
            unsafe {
                let slice = std::slice::from_raw_parts(self.0.data, self.0.len);
                bytes::Bytes::copy_from_slice(slice)
            }
        }
    }
}

/// RAII wrapper for RGWObjectMeta
pub struct OwnedRGWObjectMeta(pub RGWObjectMeta);

impl Drop for OwnedRGWObjectMeta {
    fn drop(&mut self) {
        #[cfg(feature = "mock-sal")]
        unsafe {
            mock_impl::rgw_free_object_meta(&mut self.0);
        }
        #[cfg(not(feature = "mock-sal"))]
        unsafe {
            rgw_free_object_meta(&mut self.0);
        }
    }
}

/// RAII wrapper for RGWListResult
pub struct OwnedRGWListResult(pub RGWListResult);

impl Drop for OwnedRGWListResult {
    fn drop(&mut self) {
        #[cfg(feature = "mock-sal")]
        unsafe {
            mock_impl::rgw_free_list_result(&mut self.0);
        }
        #[cfg(not(feature = "mock-sal"))]
        unsafe {
            rgw_free_list_result(&mut self.0);
        }
    }
}

#[cfg(feature = "mock-sal")]
pub use mock_impl::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_default() {
        let buf = RGWBuffer::default();
        assert!(buf.data.is_null());
        assert_eq!(buf.len, 0);
    }

    #[test]
    fn test_object_meta_default() {
        let meta = RGWObjectMeta::default();
        assert_eq!(meta.size, 0);
        assert!(meta.etag.is_null());
        assert!(meta.content_type.is_null());
        assert_eq!(meta.last_modified, 0);
    }

    #[test]
    fn test_list_result_default() {
        let result = RGWListResult::default();
        assert!(result.entries.is_null());
        assert_eq!(result.count, 0);
        assert_eq!(result.is_truncated, 0);
        assert!(result.next_marker.is_null());
    }

    #[test]
    fn test_owned_buffer_empty() {
        let buf = OwnedRGWBuffer(RGWBuffer::default());
        let bytes = buf.to_bytes();
        assert!(bytes.is_empty());
    }
}
