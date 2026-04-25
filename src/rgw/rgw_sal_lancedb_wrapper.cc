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
 * This file implements C wrapper functions for RGW SAL that are called
 * by the ceph-lancedb-rgw Rust crate via FFI.
 */

#include "rgw_sal_lancedb_wrapper.h"
#include "rgw/rgw_sal.h"
#include "rgw/rgw_bucket.h"
#include "common/dout.h"
#include "common/errno.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#define dout_subsys ceph_subsys_rgw

// Helper to get DoutPrefixProvider
static inline const DoutPrefixProvider* get_dpp(const void* dpp) {
    return reinterpret_cast<const DoutPrefixProvider*>(dpp);
}

// Helper to get driver
static inline rgw::sal::Driver* get_driver(void* driver) {
    return reinterpret_cast<rgw::sal::Driver*>(driver);
}

// Helper to get bucket
static int get_bucket(
    rgw::sal::Driver* driver,
    const DoutPrefixProvider* dpp,
    const char* bucket_name,
    std::unique_ptr<rgw::sal::Bucket>& bucket_out
) {
    if (!driver || !bucket_name) {
        return -EINVAL;
    }

    rgw_bucket bucket_id;
    bucket_id.name = bucket_name;

    int ret = driver->get_bucket(dpp, nullptr, bucket_id, &bucket_out, null_yield);
    if (ret < 0) {
        return ret;
    }

    return 0;
}

// Helper to allocate and copy string
static char* strdup_safe(const std::string& str) {
    char* result = static_cast<char*>(malloc(str.size() + 1));
    if (result) {
        memcpy(result, str.c_str(), str.size() + 1);
    }
    return result;
}

extern "C" {

int rgw_put_object(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* key,
    const uint8_t* data,
    size_t len,
    const char* content_type
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !key || (!data && len > 0)) {
        return -EINVAL;
    }

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Create object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -ENOMEM;
        }

        // Create writer
        std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
            dpp,
            null_yield,
            obj.get(),
            nullptr,  // owner
            nullptr,  // ptail_placement_rule
            0,        // olh_epoch
            ""        // unique_tag
        );

        if (!writer) {
            return -ENOMEM;
        }

        // Prepare write
        ret = writer->prepare(null_yield);
        if (ret < 0) {
            return ret;
        }

        // Write data
        bufferlist bl;
        bl.append(reinterpret_cast<const char*>(data), len);

        ret = writer->process(std::move(bl), 0);
        if (ret < 0) {
            return ret;
        }

        // Complete write
        bufferlist empty_bl;
        rgw::sal::Attrs attrs;
        if (content_type && strlen(content_type) > 0) {
            bufferlist ct_bl;
            ct_bl.append(content_type);
            attrs[RGW_ATTR_CONTENT_TYPE] = ct_bl;
        }

        ceph::real_time mtime = ceph::real_clock::now();

        ret = writer->complete(
            len,        // accounted_size
            "",         // etag
            &mtime,     // mtime
            mtime,      // set_mtime
            attrs,      // attrs
            ceph::real_time(),  // delete_at
            nullptr,    // if_match
            nullptr,    // if_nomatch
            nullptr,    // user_data
            nullptr,    // zones_trace
            nullptr,    // pcanceled
            null_yield
        );

        return ret;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_get_object(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* key,
    uint64_t offset,
    uint64_t length,
    RGWBuffer* buffer
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !key || !buffer) {
        return -EINVAL;
    }

    // Initialize buffer
    buffer->data = nullptr;
    buffer->len = 0;
    buffer->capacity = 0;

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -ENOMEM;
        }

        // Get object state
        RGWObjState* state = nullptr;
        ret = obj->get_obj_state(dpp, &state, null_yield);
        if (ret < 0) {
            return ret;
        }

        if (!state->exists) {
            return -ENOENT;
        }

        // Calculate actual read length
        uint64_t obj_size = state->size;
        uint64_t read_len = length;

        if (length == UINT64_MAX || offset + length > obj_size) {
            if (offset >= obj_size) {
                // Offset past end of object
                return 0;
            }
            read_len = obj_size - offset;
        }

        if (read_len == 0) {
            return 0;
        }

        // Allocate buffer
        buffer->data = static_cast<uint8_t*>(malloc(read_len));
        if (!buffer->data) {
            return -ENOMEM;
        }
        buffer->capacity = read_len;

        // Create read operation
        std::unique_ptr<rgw::sal::Object::ReadOp> read_op = obj->get_read_op();

        ret = read_op->prepare(null_yield, dpp);
        if (ret < 0) {
            free(buffer->data);
            buffer->data = nullptr;
            return ret;
        }

        // Read data
        bufferlist bl;
        ret = read_op->read(offset, read_len, bl, null_yield, dpp);
        if (ret < 0) {
            free(buffer->data);
            buffer->data = nullptr;
            return ret;
        }

        // Copy to output buffer
        size_t actual_len = bl.length();
        if (actual_len > read_len) {
            actual_len = read_len;
        }
        memcpy(buffer->data, bl.c_str(), actual_len);
        buffer->len = actual_len;

        return 0;

    } catch (const std::exception& e) {
        if (buffer->data) {
            free(buffer->data);
            buffer->data = nullptr;
        }
        return -EIO;
    } catch (...) {
        if (buffer->data) {
            free(buffer->data);
            buffer->data = nullptr;
        }
        return -EIO;
    }
}

int rgw_delete_object(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* key
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !key) {
        return -EINVAL;
    }

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -ENOMEM;
        }

        // Delete object
        std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();

        ret = del_op->delete_obj(dpp, null_yield, 0);

        // Treat ENOENT as success for delete operations
        if (ret == -ENOENT) {
            return 0;
        }

        return ret;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_head_object(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* key,
    RGWObjectMeta* meta
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !key || !meta) {
        return -EINVAL;
    }

    // Initialize meta
    meta->size = 0;
    meta->etag = nullptr;
    meta->content_type = nullptr;
    meta->last_modified = 0;

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -ENOMEM;
        }

        // Get object state
        RGWObjState* state = nullptr;
        ret = obj->get_obj_state(dpp, &state, null_yield);
        if (ret < 0) {
            return ret;
        }

        if (!state->exists) {
            return -ENOENT;
        }

        // Fill metadata
        meta->size = state->size;
        meta->last_modified = ceph::real_clock::to_time_t(state->mtime);

        // Get ETag from attributes
        auto etag_iter = state->attrset.find(RGW_ATTR_ETAG);
        if (etag_iter != state->attrset.end()) {
            meta->etag = strdup_safe(etag_iter->second.to_str());
        }

        // Get content type from attributes
        auto ct_iter = state->attrset.find(RGW_ATTR_CONTENT_TYPE);
        if (ct_iter != state->attrset.end()) {
            meta->content_type = strdup_safe(ct_iter->second.to_str());
        }

        return 0;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_list_objects(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* prefix,
    const char* delimiter,
    const char* marker,
    uint32_t max_keys,
    RGWListResult* result
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !result) {
        return -EINVAL;
    }

    // Initialize result
    result->entries = nullptr;
    result->count = 0;
    result->is_truncated = 0;
    result->next_marker = nullptr;

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Set up list parameters
        rgw::sal::Bucket::ListParams params;
        params.prefix = prefix ? prefix : "";
        params.delim = delimiter ? delimiter : "";
        params.marker = rgw_obj_key(marker ? marker : "");
        params.list_versions = false;
        params.allow_unordered = false;

        rgw::sal::Bucket::ListResults results;

        // Execute list
        ret = bucket->list(dpp, params, max_keys, results, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Allocate entries
        size_t count = results.objs.size();
        if (count > 0) {
            result->entries = static_cast<RGWListEntry*>(
                calloc(count, sizeof(RGWListEntry))
            );
            if (!result->entries) {
                return -ENOMEM;
            }

            for (size_t i = 0; i < count; i++) {
                const auto& obj = results.objs[i];
                result->entries[i].key = strdup_safe(obj.key.name);
                result->entries[i].size = obj.meta.size;
                result->entries[i].last_modified =
                    ceph::real_clock::to_time_t(obj.meta.mtime);
            }
        }

        result->count = count;
        result->is_truncated = results.is_truncated ? 1 : 0;

        if (results.is_truncated && !results.next_marker.name.empty()) {
            result->next_marker = strdup_safe(results.next_marker.name);
        }

        return 0;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_copy_object(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* src_bucket_name,
    const char* src_key,
    const char* dst_bucket_name,
    const char* dst_key
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !src_bucket_name || !src_key ||
        !dst_bucket_name || !dst_key) {
        return -EINVAL;
    }

    try {
        // Get source bucket
        std::unique_ptr<rgw::sal::Bucket> src_bucket;
        int ret = get_bucket(driver, dpp, src_bucket_name, src_bucket);
        if (ret < 0) {
            return ret;
        }

        // Get destination bucket
        std::unique_ptr<rgw::sal::Bucket> dst_bucket;
        ret = get_bucket(driver, dpp, dst_bucket_name, dst_bucket);
        if (ret < 0) {
            return ret;
        }

        // Get source object
        std::unique_ptr<rgw::sal::Object> src_obj =
            src_bucket->get_object(rgw_obj_key(src_key));
        if (!src_obj) {
            return -ENOMEM;
        }

        // Get destination object
        std::unique_ptr<rgw::sal::Object> dst_obj =
            dst_bucket->get_object(rgw_obj_key(dst_key));
        if (!dst_obj) {
            return -ENOMEM;
        }

        // Copy object
        RGWObjManifest* manifest = nullptr;
        rgw::sal::Attrs attrs;

        ret = src_obj->copy_object(
            nullptr,        // user
            nullptr,        // info
            nullptr,        // zone_group
            nullptr,        // dest_placement
            dst_bucket.get(),
            dst_obj.get(),
            src_bucket.get(),
            src_obj.get(),
            nullptr,        // dest_bucket_info
            nullptr,        // src_bucket_info
            ceph::real_time(),  // src_mtime
            nullptr,        // mod_ptr
            nullptr,        // unmod_ptr
            false,          // high_precision_time
            nullptr,        // if_match
            nullptr,        // if_nomatch
            ATTRSMOD_NONE,
            false,          // copy_if_newer
            attrs,
            RGWObjCategory::Main,
            0,              // olh_epoch
            ceph::real_time(),  // delete_at
            nullptr,        // version_id
            nullptr,        // tag
            nullptr,        // etag
            nullptr,        // petag
            dpp,
            null_yield
        );

        return ret;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_delete_objects(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* const* keys,
    size_t count
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || (!keys && count > 0)) {
        return -EINVAL;
    }

    if (count == 0) {
        return 0;
    }

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Delete each object
        for (size_t i = 0; i < count; i++) {
            if (!keys[i]) continue;

            std::unique_ptr<rgw::sal::Object> obj =
                bucket->get_object(rgw_obj_key(keys[i]));
            if (!obj) continue;

            std::unique_ptr<rgw::sal::Object::DeleteOp> del_op =
                obj->get_delete_op();

            // Ignore individual failures
            del_op->delete_obj(dpp, null_yield, 0);
        }

        return 0;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_init_multipart(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* key,
    char* upload_id,
    size_t upload_id_len
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !key || !upload_id || upload_id_len < 1) {
        return -EINVAL;
    }

    upload_id[0] = '\0';

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Create multipart upload
        std::unique_ptr<rgw::sal::MultipartUpload> upload =
            bucket->get_multipart_upload(key);
        if (!upload) {
            return -ENOMEM;
        }

        ACLOwner owner;
        rgw_placement_rule placement;

        ret = upload->init(dpp, null_yield, nullptr, owner, placement, nullptr);
        if (ret < 0) {
            return ret;
        }

        // Copy upload ID
        std::string id = upload->get_upload_id();
        if (id.size() >= upload_id_len) {
            return -ENOSPC;
        }
        strncpy(upload_id, id.c_str(), upload_id_len - 1);
        upload_id[upload_id_len - 1] = '\0';

        return 0;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_multipart_put_part(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* key,
    const char* upload_id,
    uint32_t part_num,
    const uint8_t* data,
    size_t len,
    char* etag,
    size_t etag_len
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !key || !upload_id ||
        !etag || etag_len < 1 || (!data && len > 0)) {
        return -EINVAL;
    }

    etag[0] = '\0';

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Get multipart upload
        std::unique_ptr<rgw::sal::MultipartUpload> upload =
            bucket->get_multipart_upload(key, upload_id);
        if (!upload) {
            return -ENOMEM;
        }

        // Get writer for part
        std::unique_ptr<rgw::sal::Writer> writer = upload->get_writer(
            dpp,
            null_yield,
            nullptr,  // head_obj
            nullptr,  // owner
            nullptr,  // ptail_placement_rule
            part_num,
            ""        // unique_tag
        );

        if (!writer) {
            return -ENOMEM;
        }

        // Prepare write
        ret = writer->prepare(null_yield);
        if (ret < 0) {
            return ret;
        }

        // Write data
        bufferlist bl;
        bl.append(reinterpret_cast<const char*>(data), len);

        ret = writer->process(std::move(bl), 0);
        if (ret < 0) {
            return ret;
        }

        // Complete write
        bufferlist empty_bl;
        rgw::sal::Attrs attrs;
        ceph::real_time mtime = ceph::real_clock::now();

        ret = writer->complete(
            len,        // accounted_size
            "",         // etag (will be computed)
            &mtime,
            mtime,
            attrs,
            ceph::real_time(),  // delete_at
            nullptr,    // if_match
            nullptr,    // if_nomatch
            nullptr,    // user_data
            nullptr,    // zones_trace
            nullptr,    // pcanceled
            null_yield
        );

        if (ret < 0) {
            return ret;
        }

        // TODO: Get actual ETag from writer
        // For now, return a placeholder
        strncpy(etag, "placeholder_etag", etag_len - 1);
        etag[etag_len - 1] = '\0';

        return 0;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_multipart_complete(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* key,
    const char* upload_id,
    const char* const* etags,
    size_t count
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !key || !upload_id ||
        (!etags && count > 0)) {
        return -EINVAL;
    }

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Get multipart upload
        std::unique_ptr<rgw::sal::MultipartUpload> upload =
            bucket->get_multipart_upload(key, upload_id);
        if (!upload) {
            return -ENOMEM;
        }

        // Build parts list
        std::list<rgw_obj_key> remove_objs;
        bool compressed = false;

        RGWCompressionInfo cs_info;
        off_t ofs = 0;
        uint64_t accounted_size = 0;
        std::string etag;

        ret = upload->complete(
            dpp,
            null_yield,
            nullptr,    // cs_info
            nullptr,    // ofs
            etag,
            nullptr,    // mtime
            0,          // set_mtime
            rgw::sal::Attrs(),
            ceph::real_time(),  // delete_at
            nullptr,    // if_match
            nullptr,    // if_nomatch
            nullptr,    // user_data
            nullptr,    // zones_trace
            nullptr     // pcanceled
        );

        return ret;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

int rgw_multipart_abort(
    void* driver_ptr,
    const void* dpp_ptr,
    const char* bucket_name,
    const char* key,
    const char* upload_id
) {
    auto* driver = get_driver(driver_ptr);
    auto* dpp = get_dpp(dpp_ptr);

    if (!driver || !bucket_name || !key || !upload_id) {
        return -EINVAL;
    }

    try {
        // Get bucket
        std::unique_ptr<rgw::sal::Bucket> bucket;
        int ret = get_bucket(driver, dpp, bucket_name, bucket);
        if (ret < 0) {
            return ret;
        }

        // Get multipart upload
        std::unique_ptr<rgw::sal::MultipartUpload> upload =
            bucket->get_multipart_upload(key, upload_id);
        if (!upload) {
            return -ENOMEM;
        }

        // Abort upload
        ret = upload->abort(dpp, nullptr, null_yield);

        return ret;

    } catch (const std::exception& e) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

void rgw_free_buffer(RGWBuffer* buffer) {
    if (buffer) {
        if (buffer->data) {
            free(buffer->data);
            buffer->data = nullptr;
        }
        buffer->len = 0;
        buffer->capacity = 0;
    }
}

void rgw_free_object_meta(RGWObjectMeta* meta) {
    if (meta) {
        if (meta->etag) {
            free(meta->etag);
            meta->etag = nullptr;
        }
        if (meta->content_type) {
            free(meta->content_type);
            meta->content_type = nullptr;
        }
        meta->size = 0;
        meta->last_modified = 0;
    }
}

void rgw_free_list_result(RGWListResult* result) {
    if (result) {
        if (result->entries) {
            for (size_t i = 0; i < result->count; i++) {
                if (result->entries[i].key) {
                    free(result->entries[i].key);
                }
            }
            free(result->entries);
            result->entries = nullptr;
        }
        if (result->next_marker) {
            free(result->next_marker);
            result->next_marker = nullptr;
        }
        result->count = 0;
        result->is_truncated = 0;
    }
}

} // extern "C"
