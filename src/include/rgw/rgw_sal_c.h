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
 * @file rgw_sal_c.h
 * @brief C bindings for RGW Storage Abstraction Layer (SAL)
 *
 * This header provides a C interface to the RGW SAL, enabling
 * integration with applications written in C or other languages
 * that can interface with C.
 *
 * The SAL is backend-agnostic and supports multiple storage backends:
 * - rados: RADOS-based storage (default)
 * - dbstore: Database-backed storage (SQLite, etc.)
 * - posix: POSIX filesystem-backed storage
 *
 * Example usage:
 * @code
 * rgw_sal_driver_t driver;
 * rgw_sal_driver_create("dbstore", "/etc/ceph/ceph.conf", &driver);
 * rgw_sal_driver_initialize(driver);
 * // ... use driver ...
 * rgw_sal_driver_destroy(driver);
 * @endcode
 */

#ifndef CEPH_RGW_SAL_C_H
#define CEPH_RGW_SAL_C_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

#define RGW_SAL_C_VER_MAJOR 1
#define RGW_SAL_C_VER_MINOR 0
#define RGW_SAL_C_VER_EXTRA 0

#define RGW_SAL_C_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)
#define RGW_SAL_C_VERSION_CODE RGW_SAL_C_VERSION(RGW_SAL_C_VER_MAJOR, RGW_SAL_C_VER_MINOR, RGW_SAL_C_VER_EXTRA)

/* Opaque types */
typedef void* rgw_sal_driver_t;
typedef void* rgw_sal_user_t;
typedef void* rgw_sal_bucket_t;
typedef void* rgw_sal_object_t;
typedef void* rgw_sal_writer_t;

/* Forward declarations */
struct rgw_sal_cluster_stat {
  uint64_t kb;
  uint64_t kb_used;
  uint64_t kb_avail;
  uint64_t num_objects;
};

struct rgw_sal_bucket_info {
  char* name;
  char* tenant;
  char* marker;
  char* bucket_id;
  uint64_t size;
  uint64_t size_rounded;
  uint64_t creation_time;
  char* owner_id;
};

struct rgw_sal_user_info {
  char* user_id;
  char* tenant;
  char* display_name;
  char* email;
  uint32_t max_buckets;
  uint8_t suspended;
};

struct rgw_sal_object_info {
  char* name;
  char* instance;
  uint64_t size;
  uint64_t mtime;
  char* etag;
  char* content_type;
  char* owner_id;
};

struct rgw_sal_bucket_list {
  struct rgw_sal_bucket_info* buckets;
  size_t count;
  char* next_marker;
  int is_truncated;
};

struct rgw_sal_object_list {
  struct rgw_sal_object_info* objects;
  size_t count;
  char* next_marker;
  int is_truncated;
};

/* Callback types */
typedef int (*rgw_sal_data_cb)(const void* data, size_t len, uint64_t offset, void* user_data);
typedef void (*rgw_sal_completion_cb)(int ret, void* user_data);

/* ============================================
 * Driver Operations
 * ============================================ */

/**
 * Create a SAL driver instance
 * @param driver_name Name of the driver (e.g., "rados", "dbstore", "posix")
 * @param cct_path Path to ceph.conf (can be NULL)
 * @param driver Output parameter for the driver handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_driver_create(const char* driver_name, const char* cct_path, rgw_sal_driver_t* driver);

/**
 * Initialize a SAL driver
 * @param driver The driver handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_driver_initialize(rgw_sal_driver_t driver);

/**
 * Get the name of the driver
 * @param driver The driver handle
 * @return Driver name string (do not free)
 */
const char* rgw_sal_driver_get_name(rgw_sal_driver_t driver);

/**
 * Get cluster statistics
 * @param driver The driver handle
 * @param stats Output parameter for cluster stats
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_driver_get_cluster_stat(rgw_sal_driver_t driver, struct rgw_sal_cluster_stat* stats);

/**
 * Get cluster ID
 * @param driver The driver handle
 * @param cluster_id Output buffer for cluster ID
 * @param cluster_id_len Size of cluster_id buffer
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_driver_get_cluster_id(rgw_sal_driver_t driver, char* cluster_id, size_t cluster_id_len);

/**
 * Destroy a SAL driver instance
 * @param driver The driver handle
 */
void rgw_sal_driver_destroy(rgw_sal_driver_t driver);

/* ============================================
 * User Operations
 * ============================================ */

/**
 * Get a user by user ID
 * @param driver The driver handle
 * @param user_id User ID string
 * @param tenant Tenant name (can be NULL)
 * @param user Output parameter for user handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_get_user(rgw_sal_driver_t driver, const char* user_id, const char* tenant, rgw_sal_user_t* user);

/**
 * Get a user by access key
 * @param driver The driver handle
 * @param access_key Access key string
 * @param user Output parameter for user handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_get_user_by_access_key(rgw_sal_driver_t driver, const char* access_key, rgw_sal_user_t* user);

/**
 * Get a user by email
 * @param driver The driver handle
 * @param email Email address
 * @param user Output parameter for user handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_get_user_by_email(rgw_sal_driver_t driver, const char* email, rgw_sal_user_t* user);

/**
 * Load user information
 * @param user The user handle
 * @param info Output parameter for user info
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_user_load(rgw_sal_user_t user, struct rgw_sal_user_info* info);

/**
 * Store user information
 * @param user The user handle
 * @param info User info to store
 * @param exclusive If true, fail if user already exists
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_user_store(rgw_sal_user_t user, const struct rgw_sal_user_info* info, int exclusive);

/**
 * Remove a user
 * @param user The user handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_user_remove(rgw_sal_user_t user);

/**
 * Get user ID
 * @param user The user handle
 * @param user_id Output buffer for user ID
 * @param user_id_len Size of user_id buffer
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_user_get_id(rgw_sal_user_t user, char* user_id, size_t user_id_len);

/**
 * Get user display name
 * @param user The user handle
 * @param display_name Output buffer for display name
 * @param display_name_len Size of display_name buffer
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_user_get_display_name(rgw_sal_user_t user, char* display_name, size_t display_name_len);

/**
 * Destroy a user handle
 * @param user The user handle
 */
void rgw_sal_user_destroy(rgw_sal_user_t user);

/* ============================================
 * Bucket Operations
 * ============================================ */

/**
 * Get a bucket by name
 * @param driver The driver handle
 * @param bucket_name Bucket name
 * @param tenant Tenant name (can be NULL)
 * @param bucket Output parameter for bucket handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_get_bucket(rgw_sal_driver_t driver, const char* bucket_name, const char* tenant, rgw_sal_bucket_t* bucket);

/**
 * Create a bucket
 * @param driver The driver handle
 * @param bucket_name Bucket name
 * @param tenant Tenant name (can be NULL)
 * @param owner_id Owner user ID
 * @param bucket Output parameter for bucket handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_create_bucket(rgw_sal_driver_t driver, const char* bucket_name, const char* tenant, const char* owner_id, rgw_sal_bucket_t* bucket);

/**
 * Load bucket information
 * @param bucket The bucket handle
 * @param info Output parameter for bucket info
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_bucket_load(rgw_sal_bucket_t bucket, struct rgw_sal_bucket_info* info);

/**
 * List buckets for a user
 * @param driver The driver handle
 * @param owner_id Owner user ID
 * @param tenant Tenant name (can be NULL)
 * @param marker Marker for pagination (can be NULL)
 * @param max Maximum number of buckets to return
 * @param list Output parameter for bucket list
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_list_buckets(rgw_sal_driver_t driver, const char* owner_id, const char* tenant, const char* marker, uint64_t max, struct rgw_sal_bucket_list* list);

/**
 * Free a bucket list
 * @param list The bucket list to free
 */
void rgw_sal_bucket_list_free(struct rgw_sal_bucket_list* list);

/**
 * Remove a bucket
 * @param bucket The bucket handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_bucket_remove(rgw_sal_bucket_t bucket);

/**
 * Get bucket name
 * @param bucket The bucket handle
 * @param name Output buffer for bucket name
 * @param name_len Size of name buffer
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_bucket_get_name(rgw_sal_bucket_t bucket, char* name, size_t name_len);

/**
 * Destroy a bucket handle
 * @param bucket The bucket handle
 */
void rgw_sal_bucket_destroy(rgw_sal_bucket_t bucket);

/* ============================================
 * Object Operations
 * ============================================ */

/**
 * Get an object from a bucket
 * @param bucket The bucket handle
 * @param object_name Object name
 * @param instance Object instance (can be NULL for current version)
 * @param object Output parameter for object handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_bucket_get_object(rgw_sal_bucket_t bucket, const char* object_name, const char* instance, rgw_sal_object_t* object);

/**
 * Create an object in a bucket
 * @param bucket The bucket handle
 * @param object_name Object name
 * @param object Output parameter for object handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_bucket_create_object(rgw_sal_bucket_t bucket, const char* object_name, rgw_sal_object_t* object);

/**
 * Load object information
 * @param object The object handle
 * @param info Output parameter for object info
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_object_load(rgw_sal_object_t object, struct rgw_sal_object_info* info);

/**
 * Read object data
 * @param object The object handle
 * @param offset Starting offset
 * @param len Number of bytes to read
 * @param data_cb Callback function for data chunks
 * @param user_data User data passed to callback
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_object_read(rgw_sal_object_t object, uint64_t offset, uint64_t len, rgw_sal_data_cb data_cb, void* user_data);

/**
 * Write object data
 * @param object The object handle
 * @param data Data to write
 * @param len Length of data
 * @param offset Starting offset
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_object_write(rgw_sal_object_t object, const void* data, size_t len, uint64_t offset);

/**
 * Delete an object
 * @param object The object handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_object_delete(rgw_sal_object_t object);

/**
 * List objects in a bucket
 * @param bucket The bucket handle
 * @param prefix Object name prefix (can be NULL)
 * @param marker Marker for pagination (can be NULL)
 * @param max Maximum number of objects to return
 * @param list Output parameter for object list
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_bucket_list_objects(rgw_sal_bucket_t bucket, const char* prefix, const char* marker, uint64_t max, struct rgw_sal_object_list* list);

/**
 * Free an object list
 * @param list The object list to free
 */
void rgw_sal_object_list_free(struct rgw_sal_object_list* list);

/**
 * Get object name
 * @param object The object handle
 * @param name Output buffer for object name
 * @param name_len Size of name buffer
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_object_get_name(rgw_sal_object_t object, char* name, size_t name_len);

/**
 * Get object size
 * @param object The object handle
 * @param size Output parameter for object size
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_object_get_size(rgw_sal_object_t object, uint64_t* size);

/**
 * Destroy an object handle
 * @param object The object handle
 */
void rgw_sal_object_destroy(rgw_sal_object_t object);

/* ============================================
 * Writer Operations (for async writes)
 * ============================================ */

/**
 * Get an atomic writer for an object
 * @param driver The driver handle
 * @param bucket The bucket handle
 * @param object_name Object name
 * @param owner_id Owner user ID
 * @param writer Output parameter for writer handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_get_atomic_writer(rgw_sal_driver_t driver, rgw_sal_bucket_t bucket, const char* object_name, const char* owner_id, rgw_sal_writer_t* writer);

/**
 * Prepare a writer for writing
 * @param writer The writer handle
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_writer_prepare(rgw_sal_writer_t writer);

/**
 * Write data using a writer
 * @param writer The writer handle
 * @param data Data to write
 * @param len Length of data
 * @param offset Starting offset
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_writer_write(rgw_sal_writer_t writer, const void* data, size_t len, uint64_t offset);

/**
 * Complete a write operation
 * @param writer The writer handle
 * @param etag ETag for the object (can be NULL)
 * @return 0 on success, negative error code on failure
 */
int rgw_sal_writer_complete(rgw_sal_writer_t writer, const char* etag);

/**
 * Destroy a writer handle
 * @param writer The writer handle
 */
void rgw_sal_writer_destroy(rgw_sal_writer_t writer);

/* ============================================
 * Utility Functions
 * ============================================ */

/**
 * Get version information
 * @param major Output parameter for major version
 * @param minor Output parameter for minor version
 * @param extra Output parameter for extra version
 * @return Version string
 */
const char* rgw_sal_version(int* major, int* minor, int* extra);

/**
 * Free a user info structure
 * @param info The user info to free
 */
void rgw_sal_user_info_free(struct rgw_sal_user_info* info);

/**
 * Free a bucket info structure
 * @param info The bucket info to free
 */
void rgw_sal_bucket_info_free(struct rgw_sal_bucket_info* info);

/**
 * Free an object info structure
 * @param info The object info to free
 */
void rgw_sal_object_info_free(struct rgw_sal_object_info* info);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_RGW_SAL_C_H */
