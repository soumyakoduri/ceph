/*
 * SPDX-License-Identifier: Apache-2.0
 * SPDX-FileCopyrightText: Copyright The Ceph Authors
 *
 * LanceDB RGW Backend Integration - C API
 *
 * This header provides the C API for creating LanceDB sessions that use
 * Ceph RGW's native SAL API instead of the S3 HTTP protocol.
 *
 * Usage:
 *   1. Create a session with ceph_lancedb_create_session()
 *   2. Pass the session to lancedb_connect_builder_session()
 *   3. Use standard LanceDB C API for all operations
 *   4. Free the session with ceph_lancedb_session_free()
 *
 * All s3:// URLs will automatically be routed through RGW SAL.
 */

#ifndef CEPH_LANCEDB_RGW_H
#define CEPH_LANCEDB_RGW_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Opaque handle to a Lance Session configured for RGW.
 *
 * This session can be passed to lancedb_connect_builder_session() to make
 * all s3:// URLs route through RGW's native SAL API.
 */
typedef void CephLanceDBSession;

/**
 * Create a LanceDB session configured to use RGW as the S3 backend.
 *
 * This function creates a Lance Session with the ObjectStoreRegistry
 * configured to route all s3:// URLs through RGW's SAL API instead of
 * making HTTP requests to S3.
 *
 * The session uses default cache sizes:
 *   - Index cache: 256 MB
 *   - Metadata cache: 128 MB
 *
 * @param driver  Pointer to rgw::sal::Driver (env.driver in RGW handlers)
 * @param dpp     Pointer to DoutPrefixProvider for logging (can be NULL)
 *
 * @return Opaque session pointer on success, NULL on failure
 *
 * @note Caller must free with ceph_lancedb_session_free()
 * @note Both driver and dpp must remain valid for session lifetime
 *
 * Example:
 * @code
 *   // In RGW operation handler
 *   CephLanceDBSession* session = ceph_lancedb_create_session(
 *       env.driver,
 *       this  // RGWOp is a DoutPrefixProvider
 *   );
 *
 *   LanceDBConnectBuilder* builder = lancedb_connect("s3://mybucket/vectors");
 *   lancedb_connect_builder_session(builder, session);
 *   LanceDBConnection* db = lancedb_connect_builder_execute(builder);
 *
 *   // ... use db ...
 *
 *   lancedb_connection_free(db);
 *   ceph_lancedb_session_free(session);
 * @endcode
 */
CephLanceDBSession* ceph_lancedb_create_session(void* driver, const void* dpp);

/**
 * Create a LanceDB session with custom cache sizes.
 *
 * This function allows fine-grained control over cache sizes for
 * memory-constrained environments or high-performance scenarios.
 *
 * @param driver              Pointer to rgw::sal::Driver
 * @param dpp                 Pointer to DoutPrefixProvider (can be NULL)
 * @param index_cache_size    Size of index cache in bytes (0 to disable)
 * @param metadata_cache_size Size of metadata cache in bytes (0 to disable)
 *
 * @return Opaque session pointer on success, NULL on failure
 *
 * Example:
 * @code
 *   // Create session with 512MB index cache, 256MB metadata cache
 *   CephLanceDBSession* session = ceph_lancedb_create_session_with_cache(
 *       driver,
 *       dpp,
 *       512 * 1024 * 1024,  // 512 MB
 *       256 * 1024 * 1024   // 256 MB
 *   );
 * @endcode
 */
CephLanceDBSession* ceph_lancedb_create_session_with_cache(
    void* driver,
    const void* dpp,
    size_t index_cache_size,
    size_t metadata_cache_size
);

/**
 * Free a session created by ceph_lancedb_create_session.
 *
 * This function releases all resources associated with the session,
 * including cached indices and metadata.
 *
 * @param session  Session pointer to free (safe to pass NULL)
 *
 * @warning Must not be called while session is in use by any connection
 * @warning Must not be called more than once for the same session
 */
void ceph_lancedb_session_free(CephLanceDBSession* session);

/**
 * Get the raw session pointer for use with lancedb-c.
 *
 * This function returns the session pointer in a form suitable for
 * passing to lancedb_connect_builder_session().
 *
 * @param session  Session created by ceph_lancedb_create_session
 *
 * @return Pointer suitable for lancedb_connect_builder_session()
 */
const void* ceph_lancedb_session_as_ptr(const CephLanceDBSession* session);

/**
 * Get the default index cache size in bytes.
 *
 * @return Default index cache size (256 MB)
 */
size_t ceph_lancedb_default_index_cache_size(void);

/**
 * Get the default metadata cache size in bytes.
 *
 * @return Default metadata cache size (128 MB)
 */
size_t ceph_lancedb_default_metadata_cache_size(void);

/**
 * Get the version string for this library.
 *
 * @return Null-terminated version string (e.g., "0.1.0")
 */
const char* ceph_lancedb_version(void);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_LANCEDB_RGW_H */
