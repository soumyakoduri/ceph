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
 *   1. Create a registry with ceph_lancedb_create_registry()
 *   2. Create a session with lancedb_session_new_with_registry() (from lancedb.h)
 *   3. Pass the session to lancedb_connect_builder_session()
 *   4. Use standard LanceDB C API for all operations
 *   5. Free the session with lancedb_session_free()
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

/*===========================================================================
 * Registry API - For use with lancedb_session_new_with_registry()
 *
 * This API provides flexibility by separating registry creation from
 * session creation. This allows using standard lancedb-c session options
 * (cache sizes, etc.) while still routing S3 URLs through RGW SAL.
 *===========================================================================*/

/**
 * Opaque handle to an ObjectStoreRegistry configured for RGW.
 */
typedef void CephLanceDBRegistry;

/**
 * Create an ObjectStoreRegistry configured to route S3 URLs through RGW SAL.
 *
 * This registry can be passed to lancedb_session_new_with_registry() to create
 * a LanceDB session with full control over session options (cache sizes, etc.)
 * while still routing all s3:// URLs through RGW SAL.
 *
 * @param driver  Pointer to rgw::sal::Driver (env.driver in RGW handlers)
 * @param dpp     Pointer to DoutPrefixProvider for logging (can be NULL)
 *
 * @return Opaque registry pointer on success, NULL on failure
 *
 * @note Caller must either:
 *   - Pass to lancedb_session_new_with_registry() (transfers ownership), OR
 *   - Free with ceph_lancedb_registry_free()
 * @note Both driver and dpp must remain valid for registry lifetime
 *
 * Example:
 * @code
 *   #include "ceph_lancedb_rgw.h"
 *   #include "lancedb.h"
 *
 *   // Create registry with RGW backend
 *   CephLanceDBRegistry* registry = ceph_lancedb_create_registry(driver, dpp);
 *
 *   // Create session with custom cache sizes using lancedb-c API
 *   LanceDBSessionOptions options = {
 *       .index_cache_bytes = 512 * 1024 * 1024,    // 512 MB
 *       .metadata_cache_bytes = 256 * 1024 * 1024  // 256 MB
 *   };
 *   LanceDBSession* session = lancedb_session_new_with_registry(&options, registry);
 *   // Note: registry ownership transferred to session
 *
 *   // Use session with connection
 *   LanceDBConnectBuilder* builder = lancedb_connect("s3://mybucket/vectors");
 *   builder = lancedb_connect_builder_session(builder, session);
 *   LanceDBConnection* db = lancedb_connect_builder_execute(builder);
 *
 *   // ... use db ...
 *
 *   // Cleanup
 *   lancedb_connection_free(db);
 *   lancedb_session_free(session);
 * @endcode
 */
CephLanceDBRegistry* ceph_lancedb_create_registry(void* driver, const void* dpp);

/**
 * Free a registry created by ceph_lancedb_create_registry.
 *
 * Only call this if the registry was NOT passed to lancedb_session_new_with_registry().
 * If it was passed to that function, ownership was transferred and you must NOT
 * call this function.
 *
 * @param registry  Registry pointer to free (safe to pass NULL)
 *
 * @warning Must not be called if registry was passed to lancedb_session_new_with_registry()
 * @warning Must not be called more than once for the same registry
 */
void ceph_lancedb_registry_free(CephLanceDBRegistry* registry);

/*===========================================================================
 * Cache Size Defaults
 *===========================================================================*/

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

/*===========================================================================
 * Version Information
 *===========================================================================*/

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
