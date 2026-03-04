// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* XXX: Add version?
 
#define RGW_SAL_VER_MAJOR 1
#define RGW_SAL_VER_MINOR 1
#define RGW_SAL_VER_EXTRA 0

#define RGW_SAL_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)
#define RGW_SAL_VERSION_CODE RGW_SAL_VERSION(RGW_SAL_VER_MAJOR, RGW_SAL_VER_MINOR, RGW_SAL_VER_EXTRA)
*/

// Conditionals for PUT operations
typedef struct {
    const char* if_match;        // Only write if ETag matches
    const char* if_none_match;   // Only write if ETag doesn't match
} RGWPutConditionals;

/**
 * PUT object
 *
 * Internally handles: get bucket, create writer, prepare, write, complete
 *
 * @param driver_ptr Driver handle
 * @param bucket Bucket name
 * @param key Object key
 * @param data Data to write
 * @param data_len Data length
 * @param conds put conditionals (optional)
 * @param obj_attributes  object attributes (optional)
 * @param etag Output ETag (caller must free with rgw_free_string)
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_put_object(void* driver_ptr, const void* dpp_ptr, const char* bucket,
                   const char* key, const char* data, uint64_t data_len,
                   void* obj_attributes,
                   const RGWPutConditionals* conds, char** etag);

// Conditionals for GET operations
typedef struct {
    const char* if_match;
    const char* if_none_match;
    int64_t if_modified_since;
    int64_t if_unmodified_since;
} RGWGetConditionals;

/**
 * Object metadata
 */
typedef struct {
    uint64_t size;
    int64_t mtime_sec;
    int64_t mtime_nsec;
    char* etag;  // Caller must free with rgw_free_string
} RGWObjectMeta;

/**
 * GET object from RGW
 *
 * @param driver_ptr RGW Driver pointer
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param offset Byte offset to start reading from
 * @param len Length to read (0 = entire object)
 * @param conds Conditional parameters
 * @param buffer Output buffer pointer (caller must free with rgw_free_buffer)
 * @param bytes_read Actual bytes read
 * @param meta Output metadata output
 *
 * @return 0 on success, negative errno on failure
 */

int rgw_get_object(void* driver_ptr, const void*  dpp_ptr, const char* bucket,
                   const char* key, uint64_t offset,
                   uint64_t len, RGWGetConditionals* conds,
                   char** buffer, uint64_t* bytes_read,
                   RGWObjectMeta* meta);

/**
 * DELETE object from RGW
 *
 * @param driver_ptr RGW Driver pointer
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_delete_object(void* driver_ptr, const void* dpp_ptr, const char* bucket,
                      const char* key);

/**
 * Single entry in a list result
 */
typedef struct {
    char* key;
    char* etag;
    uint64_t size;
    int64_t mtime_sec;
    int32_t mtime_nsec;
} RGWObjectEntry;

/**
 * Listing result
 */
typedef struct {
    RGWObjectEntry* entries;
    uint32_t num_objects;
    char** common_prefixes;
    uint32_t num_common_prefixes;
    char* next_marker;
    int32_t is_truncated;
} RGWListResult;


/**
 * LIST objects in RGW bucket
 *
 * @param driver_ptr RGW Driver pointer
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param prefix Object key Prefix filter (optional)
 * @param delimiter Delimiter (optional)
 * @param marker Pagination marker (optional)
 * @param max_keys Max keys to return
 * @param result List result output (caller must free with rgw_free_list_result)
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_list_objects(void *driver_ptr, const void* dpp_ptr, const char* bucket,
                     const char* prefix, const char* delimiter,
                     const char* marker,
                     int max_keys, RGWListResult* result);


/**
 * COPY object
 *
 * @param driver_ptr RGW Driver pointer
 * @param dpp_ptr DPP pointer
 * @param src_bucket Source bucket name
 * @param src_key Source key
 * @param dst_bucket Destination bucket name
 * @param dst_key Destination key
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_copy_object(void *driver_ptr, const void* dpp_ptr,
                    const char* src_bucket, const char* src_key,
                    const char* dst_bucket, const char* dst_key);

/**
 * Delete result
 */
typedef struct {
    char* key;
    int error_code;
} rgw_delete_result_t;


/**
 * DELETE multiple objects
 *
 * @param driver_ptr Driver handle
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param keys Array of keys to delete
 * @param count Number of keys
 * @param results Output results (caller must free with rgw_free_delete_results)
 * @param result_count Number of results XXXXXXXXXXXXXXXXXX
 *
 * @return 0 if all deletes succeeded, negative errno if any failed
 */
int rgw_delete_objects(void* driver_ptr, const void* dpp_ptr, const char* bucket,
                       const char** keys, uint32_t num_keys);
// XXXXXXXXXXX      rgw_delete_result_t** results, size_t* result_count);

/**
 * Byte range
 */
typedef struct {
    uint64_t start;
    uint64_t end;
} RGWRange;

/**
 * Result for a single range read
 */
typedef struct {
    char* data;
    uint64_t len;
} RGWRangeResult;

/**
 * GET multiple ranges from an object in a single operation
 *
 * @param driver_ptr Driver handle
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param ranges Array of ranges to read
 * @param num_ranges Number of ranges in the array
 * @param results Output results (caller must free with rgw_free_ranges_data)
 * @param result_count Number of results
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_get_object_ranges(void* driver_ptr, const void* dpp_ptr, const char* bucket,
                          const char* key, const RGWRange* ranges,
                          uint32_t num_ranges, RGWRangeResult** results,
                          uint32_t* result_count);

/**
 * Initialize multipart upload
 *
 * @param driver_ptr Driver handle
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param upload_id Output upload ID (caller must free with rgw_free_string)
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_init_multipart(void* driver_ptr, const void* dpp_ptr, const char* bucket,
                       const char* key, char** upload_id);


/**
 * Upload a part in multipart upload
 *
 * @param driver_ptr Driver handle
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param upload_id Upload ID
 * @param part_num Part number
 * @param data Part data
 * @param data_len Part data length
 * @param etag Output part ETag (caller must free with rgw_free_string)
 *
 * @return 0 on success, negative errno on failure
 */

int rgw_multipart_put_part(void *driver_ptr, const void* dpp_ptr, const char* bucket,
                           const char* key, const char* upload_id,
                           uint64_t part_num, const char* data,
                           uint64_t len, char** etag);

/**
 * complete multipart upload
 *
 * @param driver_ptr Driver handle
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param upload_id Upload ID
 * @param part_etags Array of part ETags (in order)
 * @param num_parts Number of parts uploaded
 * @param final_etag Output final ETag (NULL if not needed, caller frees)
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_complete(void* driver_ptr, const void* dpp_ptr, const char* bucket,
                           const char* key, const char* upload_id,
                           const char** part_etags, uint32_t num_parts,
                           char** final_etag);

/**
 * Abort multipart upload
 *
 * @param driver_ptr Driver handle
 * @param dpp_ptr DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param upload_id Upload ID
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_abort(void* driver_ptr, const void* dpp_ptr, const char* bucket,
                        const char* key, const char* upload_id);


/**
 * Free buffer allocated by RGW
 */
void rgw_free_buffer(char* buffer);

/**
 * Free string allocated by RGW
 */
void rgw_free_string(char* str);

/**
 * Free list result
 */
void rgw_list_result_free(RGWListResult* result);

/**
 * Free object metadata
 */
void rgw_object_meta_free(RGWObjectMeta* meta);

/**
 * Free range results
 *
 * @param results Array of range results
 * @param num_ranges Number of ranges
 */
void rgw_free_ranges(RGWRangeResult* results, uint32_t num_ranges);

#ifdef __cplusplus
}
#endif

