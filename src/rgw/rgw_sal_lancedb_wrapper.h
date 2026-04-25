// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * SPDX-License-Identifier: Apache-2.0
 * SPDX-FileCopyrightText: Copyright The Ceph Authors
 *
 * This file provides C wrapper functions for RGW SAL that are called
 * by the ceph-lancedb-rgw Rust crate. These functions match the FFI
 * bindings defined in ceph-lancedb-rgw/src/ffi.rs.
 */

#ifndef RGW_SAL_LANCEDB_WRAPPER_H
#define RGW_SAL_LANCEDB_WRAPPER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Buffer for receiving data from RGW
 * Must match the RGWBuffer struct in ffi.rs
 */
typedef struct RGWBuffer {
    uint8_t* data;      /* Pointer to data (allocated by RGW) */
    size_t len;         /* Length of valid data */
    size_t capacity;    /* Allocated capacity */
} RGWBuffer;

/**
 * Object metadata returned by head operations
 * Must match the RGWObjectMeta struct in ffi.rs
 */
typedef struct RGWObjectMeta {
    uint64_t size;          /* Object size in bytes */
    char* etag;             /* ETag (MD5 hash), null-terminated */
    char* content_type;     /* Content type, null-terminated */
    int64_t last_modified;  /* Last modified timestamp (Unix epoch seconds) */
} RGWObjectMeta;

/**
 * Single entry in a list operation result
 * Must match the RGWListEntry struct in ffi.rs
 */
typedef struct RGWListEntry {
    char* key;              /* Object key, null-terminated */
    uint64_t size;          /* Object size in bytes */
    int64_t last_modified;  /* Last modified timestamp (Unix epoch seconds) */
} RGWListEntry;

/**
 * Result of a list objects operation
 * Must match the RGWListResult struct in ffi.rs
 */
typedef struct RGWListResult {
    RGWListEntry* entries;  /* Array of list entries */
    size_t count;           /* Number of entries */
    int is_truncated;       /* True (1) if there are more results */
    char* next_marker;      /* Marker for next page, null-terminated */
} RGWListResult;

/*==========================================================================
 * Core Object Operations
 *=========================================================================*/

/**
 * Write an object to RGW storage
 *
 * @param driver        RGW driver pointer (rgw::sal::Driver*)
 * @param dpp           DoutPrefixProvider for logging
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
    const char* bucket,
    const char* key,
    const uint8_t* data,
    size_t len,
    const char* content_type
);

/**
 * Read an object from RGW storage
 *
 * @param driver    RGW driver pointer
 * @param dpp       DoutPrefixProvider for logging
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
 * @param bucket    Bucket name
 * @param key       Object key
 *
 * @return 0 on success, -ENOENT if not found (treated as success),
 *         other negative errno on failure
 */
int rgw_delete_object(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key
);

/**
 * Get object metadata without reading content
 *
 * @param driver    RGW driver pointer
 * @param dpp       DoutPrefixProvider for logging
 * @param bucket    Bucket name
 * @param key       Object key
 * @param meta      Output metadata (caller must free with rgw_free_object_meta)
 *
 * @return 0 on success, -ENOENT if not found, other negative errno on failure
 */
int rgw_head_object(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    RGWObjectMeta* meta
);

/**
 * List objects in a bucket
 *
 * @param driver     RGW driver pointer
 * @param dpp        DoutPrefixProvider for logging
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
    const char* src_bucket,
    const char* src_key,
    const char* dst_bucket,
    const char* dst_key
);

/**
 * Delete multiple objects atomically
 *
 * @param driver    RGW driver pointer
 * @param dpp       DoutPrefixProvider for logging
 * @param bucket    Bucket name
 * @param keys      Array of null-terminated key strings
 * @param count     Number of keys
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_delete_objects(
    void* driver,
    const void* dpp,
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
 * @param bucket    Bucket name
 * @param key       Object key
 * @param upload_id Upload ID from rgw_init_multipart
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_abort(
    void* driver,
    const void* dpp,
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

#endif /* RGW_SAL_LANCEDB_WRAPPER_H */
