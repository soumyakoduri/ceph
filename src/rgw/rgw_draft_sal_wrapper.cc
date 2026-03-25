// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

/**
 * @file rgw_draft_sal_wrapper.cc
 * @brief C FFI wrapper implementation for RGW SAL (Storage Abstraction Layer)
 *
 * This file implements C-compatible wrapper functions around the RGW SAL layer.
 * The implementations follow the patterns used in rgw_op.cc for S3 operations,
 * including:
 * - Using AtomicObjectProcessor for PUT operations
 * - Sending notifications for object lifecycle events
 * - Proper error handling and state management
 *
 * DESIGN PRINCIPLE: This is a thin FFI wrapper layer
 * ----------------------------------------------------
 * - Conditional checks (if-match, if-none-match, if-modified-since, etc.) are
 *   delegated to the SAL layer via operation parameters
 * - Business logic is NOT duplicated in this wrapper
 * - The wrapper focuses on:
 *   * C/C++ boundary conversion (void* handles, string conversions, etc.)
 *   * Memory management for C callers
 *   * Parameter marshaling to SAL interfaces
 * - The SAL layer handles all storage logic, conditionals, and validations
 */

#include "include/rgw/rgw_sal_c.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_rados.h"
#include "rgw_putobj_processor.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_compression.h"
#include "rgw_notify_event_type.h"
#include "rgw_sal_rados.h"
#include "common/armor.h"
#include "common/errno.h"
#include "common/ceph_crypto.h"

#include <memory>
#include <string>
#include <cstring>
#include <errno.h>

using namespace std;
using namespace rgw;
using namespace rgw::sal;

#define dout_subsys ceph_subsys_rgw

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Helper to get Driver from void pointer
 */
static inline Driver* get_driver(void* driver_ptr) {
  return static_cast<Driver*>(driver_ptr);
}

/**
 * Helper to get DoutPrefixProvider from void pointer
 */
static inline const DoutPrefixProvider* get_dpp(const void* dpp_ptr) {
  if (dpp_ptr) {
    return static_cast<const DoutPrefixProvider*>(dpp_ptr);
  }
  // Return nullptr if no DPP provided - operations will need to handle this
  return nullptr;
}

/**
 * Helper to duplicate a string for C API return
 */
static char* rgw_strdup(const std::string& str) {
  if (str.empty()) {
    return nullptr;
  }
  char* result = (char*)malloc(str.length() + 1);
  if (result) {
    memcpy(result, str.c_str(), str.length());
    result[str.length()] = '\0';
  }
  return result;
}

/**
 * Helper to duplicate a buffer for C API return
 */
static char* rgw_bufdup(const char* data, size_t len) {
  if (!data || len == 0) {
    return nullptr;
  }
  char* result = (char*)malloc(len);
  if (result) {
    memcpy(result, data, len);
  }
  return result;
}

// ============================================================================
// PUT Object Implementation
// ============================================================================

extern "C" {

int rgw_put_object(void* driver_ptr, const void* dpp_ptr, const char* bucket_name,
                   const char* key, const char* data, uint64_t data_len,
                   void* obj_attributes,
                   const RGWPutConditionals* conds, char** etag) {
  // Validate parameters
  if (!bucket_name || !key || !data || !etag) {
    return -EINVAL;
  }

  Driver* driver = get_driver(driver_ptr);
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);

  if (!driver) {
    return -EINVAL;
  }

  try {
    // Load bucket
    std::unique_ptr<Bucket> bucket;
    rgw_bucket b;
    b.name = bucket_name;
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load bucket: " << bucket_name
                        << " ret=" << ret << dendl;
      return ret;
    }

    // Create object
    std::unique_ptr<Object> obj = bucket->get_object(rgw_obj_key(key));
    if (Object::empty(obj.get())) {
      ldpp_dout(dpp, 0) << "ERROR: failed to create object" << dendl;
      return -EINVAL;
    }

    // Conditionals will be handled by SAL layer in processor->complete()
    const char* if_match_param = nullptr;
    const char* if_none_match_param = nullptr;

    if (conds) {
      if_match_param = conds->if_match;
      if_none_match_param = conds->if_none_match;
    }

    // Prepare attributes
    rgw::sal::Attrs attrs;
    if (obj_attributes) {
      attrs = *static_cast<std::map<std::string, bufferlist>*>(obj_attributes);
    }

    // Set content type if not provided
    if (attrs.find(RGW_ATTR_CONTENT_TYPE) == attrs.end()) {
      bufferlist ct_bl;
      ct_bl.append("application/octet-stream");
      attrs[RGW_ATTR_CONTENT_TYPE] = ct_bl;
    }

    // Create atomic object processor (like S3 PUT operations do)
    std::unique_ptr<ObjectProcessor> processor;

    rgw_placement_rule dest_placement;
    dest_placement.name = bucket->get_info().placement_rule.name;
    dest_placement.storage_class = bucket->get_info().placement_rule.storage_class;

    ACLOwner owner;
    // Extract owner from variant - only supports rgw_user for now
    if (const rgw_user* uid = std::get_if<rgw_user>(&bucket->get_info().owner)) {
      owner.id = *uid;
      owner.display_name = uid->to_str();
    } else {
      ldpp_dout(dpp, 0) << "ERROR: account ownership not yet supported" << dendl;
      return -ENOTSUP;
    }

    processor = driver->get_atomic_writer(dpp, null_yield,
                                          obj.get(),
                                          owner,
                                          &dest_placement,
                                          0, // olh_epoch
                                          ""); // unique_tag

    if (!processor) {
      ldpp_dout(dpp, 0) << "ERROR: failed to create atomic processor" << dendl;
      return -EIO;
    }

    // Prepare processor
    ret = processor->prepare(null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: processor prepare failed: " << ret << dendl;
      return ret;
    }

    // Write data
    bufferlist bl;
    bl.append(data, data_len);

    ret = processor->process(std::move(bl), 0);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: processor write failed: " << ret << dendl;
      return ret;
    }

    // Flush remaining data
    bufferlist empty;
    ret = processor->process(std::move(empty), data_len);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: processor flush failed: " << ret << dendl;
      return ret;
    }

    // Calculate MD5 for ETag
    ceph::crypto::MD5 hash;
    hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    hash.Update((const unsigned char*)data, data_len);
    unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
    hash.Final(m);

    char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
    std::string etag_str = calc_md5;

    // Set ETag in attributes
    bufferlist etag_bl;
    etag_bl.append(etag_str);
    attrs[RGW_ATTR_ETAG] = etag_bl;

    // Complete the operation
    // SAL layer will handle conditional checks (if_match, if_none_match)
    ceph::real_time mtime = ceph::real_clock::now();
    bool canceled = false;
    req_context rctx{dpp, null_yield, nullptr};

    ret = processor->complete(data_len, etag_str, &mtime, mtime, attrs,
                             std::nullopt, // cksum
                             ceph::real_time(), // delete_at
                             if_match_param,
                             if_none_match_param,
                             nullptr, // user_data
                             nullptr, // zones_trace
                             &canceled,
                             rctx,
                             0); // flags

    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: processor complete failed: " << ret << dendl;
      return ret;
    }

    if (canceled) {
      ldpp_dout(dpp, 0) << "ERROR: operation was canceled" << dendl;
      return -ECANCELED;
    }

    // Send notification (using no-req_state variant)
    rgw::notify::EventTypeList event_types;
    event_types.push_back(rgw::notify::ObjectCreatedPut);
    // Extract user_id from owner (which is a variant)
    std::string user_id;
    std::string user_tenant;
    if (const rgw_user* uid = std::get_if<rgw_user>(&bucket->get_info().owner)) {
      user_id = uid->to_str();
      user_tenant = uid->tenant;
    }
    std::string req_id = "ffi-wrapper";

    std::unique_ptr<Notification> notif = driver->get_notification(
      dpp, obj.get(), nullptr, event_types, bucket.get(),
      user_id, user_tenant, req_id, null_yield);

    if (notif) {
      ret = notif->publish_reserve(dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << "WARNING: notification reserve failed: " << ret << dendl;
        // Don't fail the operation, just log
      } else {
        ret = notif->publish_commit(dpp, data_len, mtime, etag_str,
                                   obj->get_instance());
        if (ret < 0) {
          ldpp_dout(dpp, 1) << "WARNING: notification commit failed: " << ret << dendl;
        }
      }
    }

    // Return ETag to caller
    *etag = rgw_strdup(etag_str);
    if (!*etag) {
      return -ENOMEM;
    }

    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: exception in rgw_put_object: " << e.what() << dendl;
    return -EIO;
  }
}

// ============================================================================
// GET Object Implementation
// ============================================================================

int rgw_get_object(void* driver_ptr, const void* dpp_ptr, const char* bucket_name,
                   const char* key, uint64_t offset,
                   uint64_t len, RGWGetConditionals* conds,
                   char** buffer, uint64_t* bytes_read,
                   RGWObjectMeta* meta) {
  // Validate parameters
  if (!bucket_name || !key || !buffer || !bytes_read) {
    return -EINVAL;
  }

  Driver* driver = get_driver(driver_ptr);
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);

  if (!driver) {
    return -EINVAL;
  }

  *buffer = nullptr;
  *bytes_read = 0;

  try {
    // Load bucket
    std::unique_ptr<Bucket> bucket;
    rgw_bucket b;
    b.name = bucket_name;
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load bucket: " << bucket_name
                        << " ret=" << ret << dendl;
      return ret;
    }

    // Get object
    std::unique_ptr<Object> obj = bucket->get_object(rgw_obj_key(key));
    if (Object::empty(obj.get())) {
      ldpp_dout(dpp, 0) << "ERROR: failed to create object" << dendl;
      return -EINVAL;
    }

    // Create read operation
    std::unique_ptr<Object::ReadOp> read_op = obj->get_read_op();

    // Set conditional parameters - SAL layer will handle the checks in prepare()
    ceph::real_time mod_time, unmod_time; // Keep alive during prepare()

    if (conds) {
      read_op->params.if_match = conds->if_match;
      read_op->params.if_nomatch = conds->if_none_match;

      if (conds->if_modified_since > 0) {
        mod_time = ceph::real_clock::zero();
        mod_time += std::chrono::seconds(conds->if_modified_since);
        read_op->params.mod_ptr = &mod_time;
      }
      if (conds->if_unmodified_since > 0) {
        unmod_time = ceph::real_clock::zero();
        unmod_time += std::chrono::seconds(conds->if_unmodified_since);
        read_op->params.unmod_ptr = &unmod_time;
      }
    }

    // Prepare the read - SAL layer checks conditionals here
    ret = read_op->prepare(null_yield, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: read prepare failed: " << ret << dendl;
      return ret;
    }

    // Get object size and metadata
    uint64_t obj_size = obj->get_size();

    // Calculate read range
    uint64_t read_offset = offset;
    uint64_t read_len = len;

    if (len == 0 || (offset + len) > obj_size) {
      read_len = obj_size - offset;
    }

    if (read_offset >= obj_size) {
      ldpp_dout(dpp, 10) << "Offset beyond object size" << dendl;
      return -EINVAL;
    }

    // Read data
    bufferlist bl;
    ret = read_op->read(read_offset, read_len, bl, null_yield, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: read failed: " << ret << dendl;
      return ret;
    }

    // Copy data to output buffer
    *bytes_read = bl.length();
    if (*bytes_read > 0) {
      *buffer = rgw_bufdup(bl.c_str(), *bytes_read);
      if (!*buffer) {
        return -ENOMEM;
      }
    }

    // Fill metadata if requested
    if (meta) {
      meta->size = obj_size;

      // Get mtime from object
      ceph::real_time mtime = obj->get_mtime();
      if (mtime != ceph::real_clock::zero()) {
        auto duration = mtime.time_since_epoch();
        auto secs = std::chrono::duration_cast<std::chrono::seconds>(duration);
        auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - secs);
        meta->mtime_sec = secs.count();
        meta->mtime_nsec = nsecs.count();
      } else {
        meta->mtime_sec = 0;
        meta->mtime_nsec = 0;
      }

      // Get ETag
      bufferlist etag_bl;
      ret = read_op->get_attr(dpp, RGW_ATTR_ETAG, etag_bl, null_yield);
      if (ret >= 0) {
        meta->etag = rgw_strdup(etag_bl.to_str());
      } else {
        meta->etag = nullptr;
      }
    }

    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: exception in rgw_get_object: " << e.what() << dendl;
    if (*buffer) {
      free(*buffer);
      *buffer = nullptr;
    }
    return -EIO;
  }
}

// ============================================================================
// DELETE Object Implementation
// ============================================================================

int rgw_delete_object(void* driver_ptr, const void* dpp_ptr, const char* bucket_name,
                      const char* key) {
  // Validate parameters
  if (!bucket_name || !key) {
    return -EINVAL;
  }

  Driver* driver = get_driver(driver_ptr);
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);

  if (!driver) {
    return -EINVAL;
  }

  try {
    // Load bucket
    std::unique_ptr<Bucket> bucket;
    rgw_bucket b;
    b.name = bucket_name;
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load bucket: " << bucket_name
                        << " ret=" << ret << dendl;
      return ret;
    }

    // Get object
    std::unique_ptr<Object> obj = bucket->get_object(rgw_obj_key(key));
    if (Object::empty(obj.get())) {
      ldpp_dout(dpp, 0) << "ERROR: failed to create object" << dendl;
      return -EINVAL;
    }

    // Load object state to check if it exists
    ret = obj->load_obj_state(dpp, null_yield, true);
    if (ret < 0) {
      if (ret == -ENOENT) {
        ldpp_dout(dpp, 10) << "Object does not exist: " << key << dendl;
        return -ENOENT;
      }
      ldpp_dout(dpp, 0) << "ERROR: failed to load object state: " << ret << dendl;
      return ret;
    }

    // Get object size and etag for notification
    uint64_t obj_size = obj->get_size();
    std::string etag;
    bufferlist etag_bl;
    if (obj->get_attr(RGW_ATTR_ETAG, etag_bl)) {
      etag = etag_bl.to_str();
    }

    // Send notification before delete (using no-req_state variant)
    rgw::notify::EventTypeList event_types;
    event_types.push_back(rgw::notify::ObjectRemovedDelete);
    // Extract user_id from owner (which is a variant)
    std::string user_id;
    std::string user_tenant;
    if (const rgw_user* uid = std::get_if<rgw_user>(&bucket->get_info().owner)) {
      user_id = uid->to_str();
      user_tenant = uid->tenant;
    }
    std::string req_id = "ffi-wrapper";

    std::unique_ptr<Notification> notif = driver->get_notification(
      dpp, obj.get(), nullptr, event_types, bucket.get(),
      user_id, user_tenant, req_id, null_yield);

    if (notif) {
      ret = notif->publish_reserve(dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << "WARNING: notification reserve failed: " << ret << dendl;
      }
    }

    // Delete the object
    std::unique_ptr<Object::DeleteOp> del_op = obj->get_delete_op();
    del_op->params.bucket_owner = bucket->get_info().owner;
    del_op->params.versioning_status = 0; // No versioning for now

    ret = del_op->delete_obj(dpp, null_yield, 0);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: delete failed: " << ret << dendl;
      return ret;
    }

    // Commit notification
    if (notif) {
      ceph::real_time mtime = ceph::real_clock::now();
      ret = notif->publish_commit(dpp, obj_size, mtime, etag, obj->get_instance());
      if (ret < 0) {
        ldpp_dout(dpp, 1) << "WARNING: notification commit failed: " << ret << dendl;
      }
    }

    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: exception in rgw_delete_object: " << e.what() << dendl;
    return -EIO;
  }
}

// ============================================================================
// LIST Objects Implementation
// ============================================================================

int rgw_list_objects(void* driver_ptr, const void* dpp_ptr, const char* bucket_name,
                     const char* prefix, const char* delimiter,
                     const char* marker, int max_keys, RGWListResult* result) {
  // Validate parameters
  if (!bucket_name || !result) {
    return -EINVAL;
  }

  Driver* driver = get_driver(driver_ptr);
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);

  if (!driver) {
    return -EINVAL;
  }

  // Initialize result
  memset(result, 0, sizeof(RGWListResult));

  try {
    // Load bucket
    std::unique_ptr<Bucket> bucket;
    rgw_bucket b;
    b.name = bucket_name;
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load bucket: " << bucket_name
                        << " ret=" << ret << dendl;
      return ret;
    }

    // Setup list parameters
    Bucket::ListParams params;
    if (prefix) {
      params.prefix = prefix;
    }
    if (delimiter) {
      params.delim = delimiter;
    }
    if (marker) {
      params.marker.name = marker;
    }
    params.list_versions = false;
    params.allow_unordered = false;

    // Perform listing
    Bucket::ListResults results;
    ret = bucket->list(dpp, params, max_keys > 0 ? max_keys : 1000,
                      results, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: list failed: " << ret << dendl;
      return ret;
    }

    // Allocate result arrays
    result->num_objects = results.objs.size();
    if (result->num_objects > 0) {
      result->entries = (RGWObjectEntry*)calloc(result->num_objects,
                                                sizeof(RGWObjectEntry));
      if (!result->entries) {
        return -ENOMEM;
      }

      // Fill object entries
      for (size_t i = 0; i < results.objs.size(); i++) {
        const rgw_bucket_dir_entry& entry = results.objs[i];

        result->entries[i].key = rgw_strdup(entry.key.name);
        result->entries[i].etag = rgw_strdup(entry.meta.etag);
        result->entries[i].size = entry.meta.accounted_size;

        auto mtime_duration = entry.meta.mtime.time_since_epoch();
        auto secs = std::chrono::duration_cast<std::chrono::seconds>(mtime_duration);
        auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(
          mtime_duration - secs);
        result->entries[i].mtime_sec = secs.count();
        result->entries[i].mtime_nsec = nsecs.count();
      }
    }

    // Fill common prefixes (common_prefixes is a map<string, bool>)
    result->num_common_prefixes = results.common_prefixes.size();
    if (result->num_common_prefixes > 0) {
      result->common_prefixes = (char**)calloc(result->num_common_prefixes,
                                               sizeof(char*));
      if (!result->common_prefixes) {
        rgw_list_result_free(result);
        return -ENOMEM;
      }

      size_t i = 0;
      for (const auto& prefix_pair : results.common_prefixes) {
        result->common_prefixes[i++] = rgw_strdup(prefix_pair.first);
      }
    }

    // Set continuation marker
    if (!results.next_marker.empty()) {
      result->next_marker = rgw_strdup(results.next_marker.name);
      result->is_truncated = 1;
    } else {
      result->next_marker = nullptr;
      result->is_truncated = 0;
    }

    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: exception in rgw_list_objects: " << e.what() << dendl;
    rgw_list_result_free(result);
    return -EIO;
  }
}

// ============================================================================
// COPY Object Implementation
// ============================================================================

int rgw_copy_object(void* driver_ptr, const void* dpp_ptr,
                    const char* src_bucket_name, const char* src_key,
                    const char* dst_bucket_name, const char* dst_key) {
  // Validate parameters
  if (!src_bucket_name || !src_key || !dst_bucket_name || !dst_key) {
    return -EINVAL;
  }

  Driver* driver = get_driver(driver_ptr);
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);

  if (!driver) {
    return -EINVAL;
  }

  try {
    // Load source bucket
    std::unique_ptr<Bucket> src_bucket;
    rgw_bucket src_b;
    src_b.name = src_bucket_name;
    int ret = driver->load_bucket(dpp, src_b, &src_bucket, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load source bucket: "
                        << src_bucket_name << " ret=" << ret << dendl;
      return ret;
    }

    // Load destination bucket
    std::unique_ptr<Bucket> dst_bucket;
    rgw_bucket dst_b;
    dst_b.name = dst_bucket_name;
    ret = driver->load_bucket(dpp, dst_b, &dst_bucket, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load destination bucket: "
                        << dst_bucket_name << " ret=" << ret << dendl;
      return ret;
    }

    // Get source object
    std::unique_ptr<Object> src_obj = src_bucket->get_object(rgw_obj_key(src_key));
    if (Object::empty(src_obj.get())) {
      ldpp_dout(dpp, 0) << "ERROR: failed to create source object" << dendl;
      return -EINVAL;
    }

    // Load source object state
    ret = src_obj->load_obj_state(dpp, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load source object state: "
                        << ret << dendl;
      return ret;
    }

    if (!src_obj->exists()) {
      ldpp_dout(dpp, 0) << "ERROR: source object does not exist" << dendl;
      return -ENOENT;
    }

    // Get destination object
    std::unique_ptr<Object> dst_obj = dst_bucket->get_object(rgw_obj_key(dst_key));
    if (Object::empty(dst_obj.get())) {
      ldpp_dout(dpp, 0) << "ERROR: failed to create destination object" << dendl;
      return -EINVAL;
    }

    // TODO: copy_object has a complex signature requiring req_info and many parameters
    // For now, stub this out and return not implemented
    ldpp_dout(dpp, 0) << "WARNING: rgw_copy_object not fully implemented yet" << dendl;
    return -ENOSYS;

    /* TODO: Implement properly with full parameter set including:
     * - ACLOwner
     * - remote_user
     * - req_info
     * - zone_id
     * - placement_rule
     * - Many other parameters (29 total!)
     *
     * See rgw_sal.h:1223 for the full signature
     */

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: exception in rgw_copy_object: " << e.what() << dendl;
    return -EIO;
  }
}

// ============================================================================
// DELETE Multiple Objects Implementation
// ============================================================================

int rgw_delete_objects(void* driver_ptr, const void* dpp_ptr,
                       const char* bucket_name,
                       const char** keys, uint32_t num_keys) {
  // Validate parameters
  if (!bucket_name || !keys || num_keys == 0) {
    return -EINVAL;
  }

  Driver* driver = get_driver(driver_ptr);
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);

  if (!driver) {
    return -EINVAL;
  }

  try {
    // Load bucket
    std::unique_ptr<Bucket> bucket;
    rgw_bucket b;
    b.name = bucket_name;
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load bucket: " << bucket_name
                        << " ret=" << ret << dendl;
      return ret;
    }

    int error_count = 0;

    // Delete each object
    for (uint32_t i = 0; i < num_keys; i++) {
      std::unique_ptr<Object> obj = bucket->get_object(rgw_obj_key(keys[i]));
      if (Object::empty(obj.get())) {
        ldpp_dout(dpp, 1) << "WARNING: failed to create object: "
                          << keys[i] << dendl;
        error_count++;
        continue;
      }

      // Delete the object
      std::unique_ptr<Object::DeleteOp> del_op = obj->get_delete_op();
      del_op->params.bucket_owner = bucket->get_info().owner;

      ret = del_op->delete_obj(dpp, null_yield, 0);
      if (ret < 0 && ret != -ENOENT) {
        ldpp_dout(dpp, 1) << "WARNING: delete failed for " << keys[i]
                          << ": " << ret << dendl;
        error_count++;
      }
    }

    // Return error if any deletions failed
    return error_count > 0 ? -EIO : 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: exception in rgw_delete_objects: "
                      << e.what() << dendl;
    return -EIO;
  }
}

// ============================================================================
// GET Object Ranges Implementation
// ============================================================================

int rgw_get_object_ranges(void* driver_ptr, const void* dpp_ptr,
                          const char* bucket_name,
                          const char* key, const RGWRange* ranges,
                          uint32_t num_ranges, RGWRangeResult** results,
                          uint32_t* result_count) {
  // Validate parameters
  if (!bucket_name || !key || !ranges || !results || !result_count) {
    return -EINVAL;
  }

  Driver* driver = get_driver(driver_ptr);
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);

  if (!driver) {
    return -EINVAL;
  }

  *results = nullptr;
  *result_count = 0;

  try {
    // Load bucket
    std::unique_ptr<Bucket> bucket;
    rgw_bucket b;
    b.name = bucket_name;
    int ret = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to load bucket: " << bucket_name
                        << " ret=" << ret << dendl;
      return ret;
    }

    // Get object
    std::unique_ptr<Object> obj = bucket->get_object(rgw_obj_key(key));
    if (Object::empty(obj.get())) {
      ldpp_dout(dpp, 0) << "ERROR: failed to create object" << dendl;
      return -EINVAL;
    }

    // Allocate results array
    *results = (RGWRangeResult*)calloc(num_ranges, sizeof(RGWRangeResult));
    if (!*results) {
      return -ENOMEM;
    }
    *result_count = num_ranges;

    // Read each range
    for (uint32_t i = 0; i < num_ranges; i++) {
      std::unique_ptr<Object::ReadOp> read_op = obj->get_read_op();

      ret = read_op->prepare(null_yield, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: read prepare failed: " << ret << dendl;
        rgw_free_ranges(*results, i);
        *results = nullptr;
        *result_count = 0;
        return ret;
      }

      uint64_t range_len = ranges[i].end - ranges[i].start + 1;
      bufferlist bl;

      ret = read_op->read(ranges[i].start, range_len, bl, null_yield, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: read range failed: " << ret << dendl;
        rgw_free_ranges(*results, i);
        *results = nullptr;
        *result_count = 0;
        return ret;
      }

      (*results)[i].len = bl.length();
      (*results)[i].data = rgw_bufdup(bl.c_str(), bl.length());
      if (!(*results)[i].data && bl.length() > 0) {
        rgw_free_ranges(*results, i);
        *results = nullptr;
        *result_count = 0;
        return -ENOMEM;
      }
    }

    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: exception in rgw_get_object_ranges: "
                      << e.what() << dendl;
    if (*results) {
      rgw_free_ranges(*results, *result_count);
      *results = nullptr;
      *result_count = 0;
    }
    return -EIO;
  }
}

// ============================================================================
// Multipart Upload Implementation
// ============================================================================

// TODO: Multipart operations require more complex SAL API usage
// These are stubbed out for now and will be implemented incrementally

int rgw_init_multipart(void* driver_ptr, const void* dpp_ptr,
                       const char* bucket_name,
                       const char* key, char** upload_id) {
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);
  ldpp_dout(dpp, 0) << "WARNING: rgw_init_multipart not yet implemented" << dendl;
  return -ENOSYS;
}

int rgw_multipart_put_part(void* driver_ptr, const void* dpp_ptr,
                            const char* bucket_name,
                            const char* key, const char* upload_id,
                            uint64_t part_num, const char* data,
                            uint64_t data_len, char** etag) {
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);
  ldpp_dout(dpp, 0) << "WARNING: rgw_multipart_put_part not yet implemented" << dendl;
  return -ENOSYS;
}

int rgw_multipart_complete(void* driver_ptr, const void* dpp_ptr,
                           const char* bucket_name,
                           const char* key, const char* upload_id,
                           const char** part_etags, uint32_t num_parts,
                           char** final_etag) {
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);
  ldpp_dout(dpp, 0) << "WARNING: rgw_multipart_complete not yet implemented" << dendl;
  return -ENOSYS;
}

int rgw_multipart_abort(void* driver_ptr, const void* dpp_ptr,
                        const char* bucket_name,
                        const char* key, const char* upload_id) {
  const DoutPrefixProvider* dpp = get_dpp(dpp_ptr);
  ldpp_dout(dpp, 0) << "WARNING: rgw_multipart_abort not yet implemented" << dendl;
  return -ENOSYS;
}

// ============================================================================
// Memory Management Functions
// ============================================================================

void rgw_free_buffer(char* buffer) {
  if (buffer) {
    free(buffer);
  }
}

void rgw_free_string(char* str) {
  if (str) {
    free(str);
  }
}

void rgw_list_result_free(RGWListResult* result) {
  if (!result) {
    return;
  }

  // Free object entries
  if (result->entries) {
    for (uint32_t i = 0; i < result->num_objects; i++) {
      free(result->entries[i].key);
      free(result->entries[i].etag);
    }
    free(result->entries);
  }

  // Free common prefixes
  if (result->common_prefixes) {
    for (uint32_t i = 0; i < result->num_common_prefixes; i++) {
      free(result->common_prefixes[i]);
    }
    free(result->common_prefixes);
  }

  // Free next marker
  free(result->next_marker);

  // Clear the structure
  memset(result, 0, sizeof(RGWListResult));
}

void rgw_object_meta_free(RGWObjectMeta* meta) {
  if (!meta) {
    return;
  }

  free(meta->etag);
  memset(meta, 0, sizeof(RGWObjectMeta));
}

void rgw_free_ranges(RGWRangeResult* results, uint32_t num_ranges) {
  if (!results) {
    return;
  }

  for (uint32_t i = 0; i < num_ranges; i++) {
    free(results[i].data);
  }
  free(results);
}

} // extern "C"
