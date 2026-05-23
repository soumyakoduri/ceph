// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 *
 * This file provides C wrapper functions for RGW SAL that are mainly
 * called by Rust crates via FFI. Any updates to these functions should
 * reflected in the FFI bindings defined in the corresponding Rust code as well.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
* Any updates to below interfaces should be done in ffi.rs as well
* Should we add versioning?
*/

/**
* Buffer for receiving data from RGW
*/
typedef struct RGWBuffer {
  uint8_t* data;      /* Pointer to data (allocated by RGW) */
  size_t len;         /* Length of valid data */
  size_t capacity;    /* Allocated capacity */
} RGWBuffer;

/**
* Object metadata returned by head operations
*/
typedef struct RGWObjectMeta {
  uint64_t size;          /* Object size in bytes */
  char* etag;             /* ETag (MD5 hash), null-terminated */
  char* content_type;     /* Content type, null-terminated */
  int64_t last_modified;  /* Last modified timestamp (Unix epoch seconds) */
} RGWObjectMeta;

/**
* Single entry in a list operation result
*/
typedef struct RGWListEntry {
  char* key;              /* Object key, null-terminated */
  uint64_t size;          /* Object size in bytes */
  int64_t last_modified;  /* Last modified timestamp (Unix epoch seconds) */
} RGWListEntry;

/**
* Result of a list objects operation
*/
typedef struct RGWListResult {
  RGWListEntry* entries;  /* Array of list entries */
  size_t count;           /* Number of entries */
  int is_truncated;       /* True (1) if there are more results */
  char* next_marker;      /* Marker for next page, null-terminated */
} RGWListResult;

/*==========================================================================
* Thread Safety and Yield Context
*=========================================================================
* These functions are NOT thread-safe for concurrent operations on the same
* bucket/object. The caller (like LANCEDB) must serialize operations
* when multiple threads access the same objects. Different threads may
* safely operate on different objects/buckets concurrently.
*
* yield_ctx parameter (present on all SAL operations):
*
*   All functions accept a `void* yield_ctx` parameter, which is an opaque
*   pointer to an `optional_yield` (C++) value.
*
*   - NULL: Uses null_yield, blocking the calling thread until the SAL
*     operation completes. This is the correct choice when called from,
*     say, Rust/Tokio's thread pool (via spawn_blocking), because Tokio's
*     blocking threads are designed to be blocked.
*
*   - Non-NULL: Cast to `optional_yield*` and dereferenced. The SAL
*     operation will yield the calling Boost.ASIO coroutine while waiting
*     for I/O, allowing other coroutines to run on the same thread. This
*     is the correct choice when called directly from an RGW Beast handler
*     or any ASIO coroutine context.
*
*=========================================================================*/

/*==========================================================================
* Memory Ownership Convention
*=========================================================================
* Input parameters:
*   - All string parameters (bucket, key, etc.) are borrowed (caller retains
*     ownership). They must remain valid for the duration of the call.
*   - data/len in put operations are borrowed (caller retains ownership).
*
* Output parameters:
*   - RGWBuffer: Allocated by rgw_get_object, caller must free with
*     rgw_free_buffer().
*   - RGWObjectMeta: Allocated by rgw_head_object, caller must free with
*     rgw_free_object_meta() (frees etag and content_type strings).
*   - RGWListResult: Allocated by rgw_list_objects, caller must free with
*     rgw_free_list_result() (frees all entry keys and next_marker).
*   - upload_id/etag out-buffers in multipart: Caller provides the buffer,
*     function writes into it. No separate free needed.
*=========================================================================*/

/*==========================================================================
* Core Object Operations
*
* XXX: versioned objects are not handled yet
*=========================================================================*/

/**
* Write an object to RGW storage
*
* @param driver        RGW driver pointer (rgw::sal::Driver*)
* @param dpp           DoutPrefixProvider for logging
* @param yield_ctx     optional_yield pointer (NULL for null_yield)
* @param bucket        Bucket name (null-terminated)
* @param key           Object key (null-terminated)
* @param data          Pointer to data bytes
* @param len           Length of data
* @param content_type  MIME type (null-terminated)
*
* @return 0 on success, negative errno on failure
*/
int rgw_put_object(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key,
  const uint8_t* data,
  size_t len,
  const char* content_type
);

/**
* Write an object with conditional preconditions
*
* Supports atomic create-if-not-exists and update-if-match semantics
* via the if_match/if_nomatch parameters.
*
* @param driver        RGW driver pointer (rgw::sal::Driver*)
* @param dpp           DoutPrefixProvider for logging
* @param yield_ctx     optional_yield pointer (NULL for null_yield)
* @param bucket        Bucket name (null-terminated)
* @param key           Object key (null-terminated)
* @param data          Pointer to data bytes
* @param len           Length of data
* @param content_type  MIME type (null-terminated)
* @param if_match      Only write if existing ETag matches (NULL to skip).
* @param if_nomatch    Only write if existing ETag does NOT match (NULL to skip).
*                      Pass "*" for create-if-not-exists (fails if object exists).
* @param canceled      Output: set to 1 if write was rejected due to precondition
*                      failure, 0 otherwise. May be NULL if caller doesn't need it.
*
* @return 0 on success (including when canceled=1), negative errno on failure.
*         When canceled=1 and return is 0, the object was NOT written.
*/
int rgw_put_object_conditional(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key,
  const uint8_t* data,
  size_t len,
  const char* content_type,
  const char* if_match,
  const char* if_nomatch,
  int* canceled
);

/**
* Read an object from RGW storage
*
* @param driver    RGW driver pointer
* @param dpp       DoutPrefixProvider for logging
* @param yield_ctx optional_yield pointer (NULL for null_yield)
* @param bucket    Bucket name
* @param key       Object key
* @param offset    Start offset for range read
* @param length    Number of bytes to read (UINT64_MAX for entire object)
* @param buffer    Output buffer (caller must free with rgw_free_buffer)
*
* @return 0 on success, -ENOENT if not found, other negative errno on failure
*/
int rgw_get_object(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key,
  uint64_t offset,
  uint64_t length,
  RGWBuffer* buffer
);

/**
* Delete an object from RGW storage
*
* @param driver    RGW driver pointer
* @param dpp       DoutPrefixProvider for logging
* @param yield_ctx optional_yield pointer (NULL for null_yield)
* @param bucket    Bucket name
* @param key       Object key
*
* @return 0 on success, -ENOENT if not found (treated as success),
*         other negative errno on failure
*/
int rgw_delete_object(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key
);

/**
* Get object metadata without reading content
*
* @param driver    RGW driver pointer
* @param dpp       DoutPrefixProvider for logging
* @param yield_ctx optional_yield pointer (NULL for null_yield)
* @param bucket    Bucket name
* @param key       Object key
* @param meta      Output metadata (caller must free with rgw_free_object_meta)
*
* @return 0 on success, -ENOENT if not found, other negative errno on failure
*/
int rgw_head_object(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key,
  RGWObjectMeta* meta
);

/**
* List objects in a bucket
*
* @param driver     RGW driver pointer
* @param dpp        DoutPrefixProvider for logging
* @param yield_ctx  optional_yield pointer (NULL for null_yield)
* @param bucket     Bucket name
* @param prefix     Filter by prefix (empty string for all)
* @param delimiter  Delimiter for hierarchy (empty for flat listing)
* @param marker     Start after this key (empty for beginning)
* @param max_keys   Maximum number of results
* @param result     Output result (caller must free with rgw_free_list_result)
*
* @return 0 on success, negative errno on failure
*/
int rgw_list_objects(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* prefix,
  const char* delimiter,
  const char* marker,
  uint32_t max_keys,
  RGWListResult* result
);

/**
* Copy an object within or between buckets
*
* @param driver        RGW driver pointer
* @param dpp           DoutPrefixProvider for logging
* @param yield_ctx     optional_yield pointer (NULL for null_yield)
* @param src_bucket    Source bucket name
* @param src_key       Source object key
* @param dst_bucket    Destination bucket name
* @param dst_key       Destination object key
*
* @return 0 on success, negative errno on failure
*/
int rgw_copy_object(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* src_bucket,
  const char* src_key,
  const char* dst_bucket,
  const char* dst_key
);

/**
* Copy an object conditionally (atomic copy-if-not-exists)
*
* @param driver        RGW driver pointer
* @param dpp           DoutPrefixProvider for logging
* @param yield_ctx     optional_yield pointer (NULL for null_yield)
* @param src_bucket    Source bucket name
* @param src_key       Source object key
* @param dst_bucket    Destination bucket name
* @param dst_key       Destination object key
* @param if_match      Only copy if destination ETag matches (NULL to skip)
* @param if_nomatch    Only copy if destination ETag does NOT match (NULL to skip).
*                      Pass "*" for copy-if-not-exists.
*
* @return 0 on success, -EEXIST if precondition failed (object exists when
*         if_nomatch="*"), -ENOENT if source not found, other negative errno.
*/
int rgw_copy_object_conditional(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* src_bucket,
  const char* src_key,
  const char* dst_bucket,
  const char* dst_key,
  const char* if_match,
  const char* if_nomatch
);

/**
* Delete multiple objects atomically
*
* @param driver    RGW driver pointer
* @param dpp       DoutPrefixProvider for logging
* @param yield_ctx optional_yield pointer (NULL for null_yield)
* @param bucket    Bucket name
* @param keys      Array of null-terminated key strings
* @param count     Number of keys
*
* @return 0 on success, negative errno on failure
*/
int rgw_delete_objects(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* const* keys,
  size_t count
);

/*==========================================================================
* Multipart Upload Operations
*=========================================================================*/

/**
* Initialize a multipart upload
*
* @param driver        RGW driver pointer
* @param dpp           DoutPrefixProvider for logging
* @param yield_ctx     optional_yield pointer (NULL for null_yield)
* @param bucket        Bucket name
* @param key           Object key
* @param upload_id     Output buffer for upload ID (must be at least 64 bytes)
* @param upload_id_len Size of upload_id buffer
*
* @return 0 on success, negative errno on failure
*/
int rgw_init_multipart(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key,
  char* upload_id,
  size_t upload_id_len
);

/**
* Upload a part in a multipart upload
*
* @param driver        RGW driver pointer
* @param dpp           DoutPrefixProvider for logging
* @param yield_ctx     optional_yield pointer (NULL for null_yield)
* @param bucket        Bucket name
* @param key           Object key
* @param upload_id     Upload ID from rgw_init_multipart
* @param part_num      Part number (1-10000)
* @param data          Part data
* @param len           Data length
* @param etag          Output buffer for part ETag (must be at least 64 bytes)
* @param etag_len      Size of etag buffer
*
* @return 0 on success, negative errno on failure
*/
int rgw_multipart_put_part(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key,
  const char* upload_id,
  uint32_t part_num,
  const uint8_t* data,
  size_t len,
  char* etag,
  size_t etag_len
);

/**
* Complete a multipart upload
*
* @param driver    RGW driver pointer
* @param dpp       DoutPrefixProvider for logging
* @param yield_ctx optional_yield pointer (NULL for null_yield)
* @param bucket    Bucket name
* @param key       Object key
* @param upload_id Upload ID from rgw_init_multipart
* @param etags     Array of part ETags in order
* @param count     Number of parts
*
* @return 0 on success, negative errno on failure
*/
int rgw_multipart_complete(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key,
  const char* upload_id,
  const char* const* etags,
  size_t count
);

/**
* Abort a multipart upload
*
* @param driver    RGW driver pointer
* @param dpp       DoutPrefixProvider for logging
* @param yield_ctx optional_yield pointer (NULL for null_yield)
* @param bucket    Bucket name
* @param key       Object key
* @param upload_id Upload ID from rgw_init_multipart
*
* @return 0 on success, negative errno on failure
*/
int rgw_multipart_abort(
  void* driver,
  const void* dpp,
  void* yield_ctx,
  const char* bucket,
  const char* key,
  const char* upload_id
);

/*==========================================================================
* Memory Management
*=========================================================================*/

/**
* Free a buffer allocated by rgw_get_object
*/
void rgw_free_buffer(RGWBuffer* buffer);

/**
* Free metadata allocated by rgw_head_object
*/
void rgw_free_object_meta(RGWObjectMeta* meta);

/**
* Free list result allocated by rgw_list_objects
*/
void rgw_free_list_result(RGWListResult* result);

#ifdef __cplusplus
}
#endif
