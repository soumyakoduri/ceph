// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

/**
 * @file rgw_sal_unified.h
 * @brief Unified SAL C API with TRUE 1-1 mapping for object store operations
 *
 * This header provides a simplified C interface to RGW SAL with TRUE 1-1 mapping.
 * Each object store operation maps to exactly ONE C function that handles all
 * internal logic (getting buckets, creating writers, etc.).
 *
 * Design Principles:
 * - ONE function per ObjectStore method
 * - Driver + bucket name passed to each function
 * - All internal resource management hidden
 * - Simple, clean API surface
 */

#ifndef CEPH_RGW_SAL_UNIFIED_H
#define CEPH_RGW_SAL_UNIFIED_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * Opaque Types (Minimal Surface Area)
 * ======================================================================== */

/**
 * Context for SAL operations (wraps CephContext)
 */
typedef struct sal_context_t sal_context_t;

/**
 * SAL driver handle (wraps rgw::sal::Driver)
 */
typedef struct sal_driver_t sal_driver_t;

/* ========================================================================
 * Data Structures
 * ======================================================================== */

/**
 * Object metadata
 */
typedef struct {
    uint64_t size;
    int64_t mtime_sec;
    int64_t mtime_nsec;
    char* etag;  // Caller must free with sal_free_string
} sal_object_meta_t;

/**
 * Single entry in a list result
 */
typedef struct {
    char* key;
    uint64_t size;
    int64_t mtime_sec;
    int64_t mtime_nsec;
    char* etag;
} sal_list_entry_t;

/**
 * Listing result with pagination
 */
typedef struct {
    sal_list_entry_t* entries;
    size_t count;
    char** common_prefixes;
    size_t prefix_count;
    char* next_marker;
} sal_list_result_t;

/**
 * Byte range
 */
typedef struct {
    uint64_t start;
    uint64_t end;
} sal_byte_range_t;

/**
 * Data for a single range
 */
typedef struct {
    char* data;
    uint64_t len;
    sal_byte_range_t range;
} sal_range_data_t;

/**
 * Delete result
 */
typedef struct {
    char* key;
    int error_code;
} sal_delete_result_t;

/**
 * Conditionals for GET
 */
typedef struct {
    const char* if_match;
    const char* if_none_match;
    int64_t if_modified_since;
    int64_t if_unmodified_since;
} sal_conditionals_t;

/* ========================================================================
 * Enumerations
 * ======================================================================== */

/**
 * Put mode
 */
typedef enum {
    SAL_PUT_MODE_CREATE = 1,
    SAL_PUT_MODE_OVERWRITE = 2,
    SAL_PUT_MODE_UPDATE = 3
} sal_put_mode_t;

/* ========================================================================
 * Initialization
 * ======================================================================== */

sal_context_t* sal_ctx_create(const char* cluster, const char* user, const char* conf);
void sal_ctx_destroy(sal_context_t* ctx);
sal_driver_t* sal_driver_create_rados(sal_context_t* ctx);
void sal_driver_destroy(sal_driver_t* driver);

/* ========================================================================
 * TRUE 1-1 MAPPED APIs
 * Each ObjectStore method = ONE C function
 * ======================================================================== */

/**
 * PUT object (ObjectStore::put_opts)
 *
 * Internally handles: get bucket, create writer, prepare, write, complete
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param key Object key
 * @param data Data to write
 * @param len Data length
 * @param mode PUT mode (create/overwrite/update)
 * @param etag Output ETag (NULL if not needed, caller frees)
 * @return 0 on success, negative errno on failure
 */
int sal_put_object(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    const char* data,
    uint64_t len,
    sal_put_mode_t mode,
    char** etag
);

/**
 * GET object (ObjectStore::get_opts)
 *
 * Internally handles: get bucket, get object, get metadata, read data
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param key Object key
 * @param offset Offset to start reading
 * @param len Length to read (0 = entire object)
 * @param conds Conditionals (NULL to ignore)
 * @param buffer Output buffer (caller frees with sal_free_buffer)
 * @param bytes_read Actual bytes read
 * @param meta Output metadata (caller frees with sal_object_meta_free)
 * @return 0 on success, negative errno on failure
 */
int sal_get_object(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    uint64_t offset,
    uint64_t len,
    sal_conditionals_t* conds,
    char** buffer,
    uint64_t* bytes_read,
    sal_object_meta_t* meta
);

/**
 * DELETE object (ObjectStore::delete)
 *
 * Internally handles: get bucket, get object, delete
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param key Object key
 * @return 0 on success, negative errno on failure
 */
int sal_delete_object(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key
);

/**
 * LIST objects (ObjectStore::list)
 *
 * Internally handles: get bucket, list with no delimiter
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param prefix Prefix filter (NULL for none)
 * @param marker Pagination marker (NULL to start)
 * @param max_keys Max keys to return
 * @param result Output result (caller frees with sal_list_result_free)
 * @return 0 on success, negative errno on failure
 */
int sal_list_objects(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* prefix,
    const char* marker,
    int max_keys,
    sal_list_result_t* result
);

/**
 * LIST objects with delimiter (ObjectStore::list_with_delimiter)
 *
 * Internally handles: get bucket, list with delimiter
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param prefix Prefix filter (NULL for none)
 * @param delimiter Delimiter (e.g., "/")
 * @param marker Pagination marker (NULL to start)
 * @param max_keys Max keys to return
 * @param result Output result (caller frees with sal_list_result_free)
 * @return 0 on success, negative errno on failure
 */
int sal_list_objects_with_delimiter(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* prefix,
    const char* delimiter,
    const char* marker,
    int max_keys,
    sal_list_result_t* result
);

/**
 * COPY object (ObjectStore::copy_opts)
 *
 * Internally handles: get buckets, get objects, copy
 *
 * @param driver Driver handle
 * @param src_bucket Source bucket name
 * @param src_key Source key
 * @param dst_bucket Destination bucket name
 * @param dst_key Destination key
 * @return 0 on success, negative errno on failure
 */
int sal_copy_object(
    sal_driver_t* driver,
    const char* src_bucket,
    const char* src_key,
    const char* dst_bucket,
    const char* dst_key
);

/**
 * DELETE multiple objects (ObjectStore::delete_stream)
 *
 * Internally handles: get bucket, delete each object
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param keys Array of keys to delete
 * @param count Number of keys
 * @param results Output results (caller frees with sal_delete_results_free)
 * @param result_count Number of results
 * @return 0 on success, negative errno on failure
 */
int sal_delete_objects(
    sal_driver_t* driver,
    const char* bucket_name,
    const char** keys,
    size_t count,
    sal_delete_result_t** results,
    size_t* result_count
);

/**
 * GET object ranges (ObjectStore::get_ranges)
 *
 * Internally handles: get bucket, get object, read multiple ranges
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param key Object key
 * @param ranges Array of ranges
 * @param range_count Number of ranges
 * @param results Output results (caller frees with sal_range_data_free)
 * @param result_count Number of results
 * @return 0 on success, negative errno on failure
 */
int sal_get_object_ranges(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    sal_byte_range_t* ranges,
    size_t range_count,
    sal_range_data_t** results,
    size_t* result_count
);

/**
 * INIT multipart upload (ObjectStore::put_multipart_opts)
 *
 * Internally handles: get bucket, create multipart upload
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param key Object key
 * @param upload_id Output upload ID (caller frees with sal_free_string)
 * @return 0 on success, negative errno on failure
 */
int sal_init_multipart(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    char** upload_id
);

/**
 * PUT multipart part (MultipartUpload::put_part)
 *
 * Internally handles: get bucket, upload part
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param key Object key
 * @param upload_id Upload ID
 * @param part_num Part number (1-indexed)
 * @param data Part data
 * @param len Data length
 * @param etag Output part ETag (caller frees with sal_free_string)
 * @return 0 on success, negative errno on failure
 */
int sal_multipart_put_part(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    const char* upload_id,
    int part_num,
    const char* data,
    uint64_t len,
    char** etag
);

/**
 * COMPLETE multipart upload (MultipartUpload::complete)
 *
 * Internally handles: get bucket, complete multipart
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param key Object key
 * @param upload_id Upload ID
 * @param part_etags Array of part ETags (in order)
 * @param num_parts Number of parts
 * @param final_etag Output final ETag (NULL if not needed, caller frees)
 * @return 0 on success, negative errno on failure
 */
int sal_multipart_complete(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    const char* upload_id,
    const char** part_etags,
    int num_parts,
    char** final_etag
);

/**
 * ABORT multipart upload (MultipartUpload::abort)
 *
 * Internally handles: get bucket, abort multipart
 *
 * @param driver Driver handle
 * @param bucket_name Bucket name
 * @param key Object key
 * @param upload_id Upload ID
 * @return 0 on success, negative errno on failure
 */
int sal_multipart_abort(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    const char* upload_id
);

/* ========================================================================
 * Memory Management
 * ======================================================================== */

void sal_free_string(char* str);
void sal_free_buffer(char* buf);
void sal_object_meta_free(sal_object_meta_t* meta);
void sal_list_result_free(sal_list_result_t* result);
void sal_range_data_free(sal_range_data_t* results, size_t count);
void sal_delete_results_free(sal_delete_result_t* results, size_t count);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_RGW_SAL_UNIFIED_H */
