// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Arrow Authors

#ifndef CEPH_RGW_OBJECT_STORE_H
#define CEPH_RGW_OBJECT_STORE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Conditionals for GET operations
typedef struct {
    const char* if_match;
    const char* if_none_match;
    int64_t if_modified_since;
    int64_t if_unmodified_since;
} RGWConditionals;

// Conditionals for PUT operations
typedef struct {
    const char* if_match;        // Only write if ETag matches
    const char* if_none_match;   // Only write if ETag doesn't match
    int if_not_exists;           // Only write if object doesn't exist (1 = true, 0 = false)
} RGWPutConditionals;

// Object metadata
typedef struct {
    char* etag;
    uint64_t size;
    int64_t mtime_sec;
    int32_t mtime_nsec;
} RGWObjectMeta;

// Object entry for list operations
typedef struct {
    char* key;
    char* etag;
    uint64_t size;
    int64_t mtime_sec;
    int32_t mtime_nsec;
} RGWObjectEntry;

// List result
typedef struct {
    RGWObjectEntry* objects;
    uint32_t num_objects;
    char** common_prefixes;
    uint32_t num_common_prefixes;
    char* next_marker;
    int32_t is_truncated;
} RGWListResult;

/**
 * Get object from RGW
 *
 * @param driver RGW driver pointer (sal::Driver*)
 * @param dpp DPP pointer (DoutPrefixProvider*), can be null
 * @param bucket Bucket name
 * @param key Object key
 * @param offset Byte offset to start reading from
 * @param length Number of bytes to read (0 = read to end)
 * @param conds Conditional parameters (can be null)
 * @param buffer Output buffer pointer (caller must free with rgw_free_buffer)
 * @param bytes_read Number of bytes actually read
 * @param meta Object metadata output
 * @return 0 on success, negative errno on failure
 */
int rgw_get_object(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    uint64_t offset,
    uint64_t length,
    const RGWConditionals* conds,
    char** buffer,
    uint64_t* bytes_read,
    RGWObjectMeta* meta
);

/**
 * Callback function type for streaming object data
 * @param data Pointer to data chunk
 * @param len Length of data chunk
 * @param user_data User-provided context pointer
 * @return 0 to continue, negative to abort
 */
typedef int (*rgw_data_callback_t)(
    const char* data,
    uint64_t len,
    void* user_data
);

/**
 * Get object with streaming callback (memory-efficient for large objects)
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param offset Byte offset to start reading from
 * @param length Number of bytes to read (0 = read to end)
 * @param callback Callback function for data chunks
 * @param user_data User data passed to callback
 * @param total_read Total bytes read
 * @return 0 on success, negative errno on failure
 */
int rgw_get_object_callback(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    uint64_t offset,
    uint64_t length,
    rgw_data_callback_t callback,
    void* user_data,
    uint64_t* total_read
);

/**
 * Range specification for get_ranges
 */
typedef struct {
    uint64_t offset;
    uint64_t length;
} RGWRange;

/**
 * Result for a single range read
 */
typedef struct {
    char* data;
    uint64_t length;
} RGWRangeResult;

/**
 * Get multiple ranges from an object in a single operation
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param ranges Array of ranges to read
 * @param num_ranges Number of ranges in the array
 * @param results Output array of range results (caller must free with rgw_free_ranges)
 * @return 0 on success, negative errno on failure
 */
int rgw_get_ranges(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    const RGWRange* ranges,
    uint32_t num_ranges,
    RGWRangeResult** results
);

/**
 * Delete multiple objects in a single operation
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param keys Array of object keys to delete
 * @param num_keys Number of keys in the array
 * @param results Array of result codes for each delete (0 = success, negative = error)
 * @return 0 if all deletes succeeded, negative errno if any failed
 */
int rgw_delete_objects(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char** keys,
    uint32_t num_keys,
    int* results
);

/**
 * Put object to RGW
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param data Data to write
 * @param data_len Length of data
 * @param content_type Optional content type (can be null)
 * @param conds Optional put conditionals (can be null)
 * @param etag Output etag (caller must free with rgw_free_string)
 * @return 0 on success, negative errno on failure
 */
int rgw_put_object(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    const char* data,
    uint64_t data_len,
    const char* content_type,
    const RGWPutConditionals* conds,
    char** etag
);

/**
 * Head object (get metadata only)
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param meta Object metadata output
 * @return 0 on success, negative errno on failure
 */
int rgw_head_object(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    RGWObjectMeta* meta
);

/**
 * Delete object from RGW
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @return 0 on success, negative errno on failure
 */
int rgw_delete_object(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key
);

/**
 * List objects in RGW bucket
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param prefix Object key prefix filter (can be null)
 * @param delimiter Delimiter for grouping (can be null)
 * @param marker Pagination marker (can be null)
 * @param max_keys Maximum number of keys to return
 * @param result List result output (caller must free with rgw_list_result_free)
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
 * Copy object within RGW
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param src_bucket Source bucket name
 * @param src_key Source object key
 * @param dst_bucket Destination bucket name
 * @param dst_key Destination object key
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
 * Initialize multipart upload
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param upload_id Output upload ID (caller must free with rgw_free_string)
 * @return 0 on success, negative errno on failure
 */
int rgw_init_multipart(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    char** upload_id
);

/**
 * Upload a part in multipart upload
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param upload_id Upload ID from init_multipart
 * @param part_num Part number (starting from 1)
 * @param data Part data
 * @param data_len Length of part data
 * @param etag Output etag (caller must free with rgw_free_string)
 * @return 0 on success, negative errno on failure
 */
int rgw_put_multipart_part(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    const char* upload_id,
    uint32_t part_num,
    const char* data,
    uint64_t data_len,
    char** etag
);

/**
 * Complete multipart upload
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param upload_id Upload ID
 * @param num_parts Number of parts uploaded
 * @return 0 on success, negative errno on failure
 */
int rgw_complete_multipart(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    const char* upload_id,
    uint32_t num_parts
);

/**
 * Abort multipart upload
 *
 * @param driver RGW driver pointer
 * @param dpp DPP pointer
 * @param bucket Bucket name
 * @param key Object key
 * @param upload_id Upload ID
 * @return 0 on success, negative errno on failure
 */
int rgw_abort_multipart(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    const char* upload_id
);

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

#endif // CEPH_RGW_OBJECT_STORE_H
