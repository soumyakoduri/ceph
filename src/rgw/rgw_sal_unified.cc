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
 * @file rgw_sal_unified.cc
 * @brief Implementation of Unified SAL C API with TRUE 1-1 mapping
 *
 * Each function handles ALL internal logic.
 * No exposed bucket/object/writer handles - all managed internally.
 */

#include "rgw/rgw_sal_unified.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <errno.h>

// RGW SAL headers
#include "rgw/rgw_sal.h"
#include "rgw/rgw_sal_rados.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/errno.h"

/* ========================================================================
 * Internal Structures
 * ======================================================================== */

struct sal_context_t {
    CephContext* cct;
    DoutPrefixProvider* dpp;

    sal_context_t(CephContext* c, DoutPrefixProvider* d) : cct(c), dpp(d) {}
    ~sal_context_t() {
        if (cct) {
            cct->put();
        }
        delete dpp;
    }
};

struct sal_driver_t {
    rgw::sal::Driver* driver;
    DoutPrefixProvider* dpp;

    sal_driver_t(rgw::sal::Driver* d, DoutPrefixProvider* dp) : driver(d), dpp(dp) {}
    ~sal_driver_t() {
        delete driver;
    }
};

/* ========================================================================
 * Helper Functions
 * ======================================================================== */

namespace {

// Simple DoutPrefixProvider implementation
class SimpleDPP : public DoutPrefixProvider {
public:
    CephContext* get_cct() const override { return nullptr; }
    unsigned get_subsys() const override { return ceph_subsys_rgw; }
    std::ostream& gen_prefix(std::ostream& out) const override { return out; }
};

// Convert C++ exceptions to errno codes
template<typename F>
int safe_call(F&& func) {
    try {
        return func();
    } catch (const std::bad_alloc&) {
        return -ENOMEM;
    } catch (const std::exception&) {
        return -EIO;
    } catch (...) {
        return -EIO;
    }
}

// Duplicate a C string
char* str_dup(const std::string& s) {
    if (s.empty()) {
        return nullptr;
    }
    char* dup = static_cast<char*>(malloc(s.length() + 1));
    if (dup) {
        std::memcpy(dup, s.c_str(), s.length() + 1);
    }
    return dup;
}

// Helper to get bucket (internally used by all functions)
std::unique_ptr<rgw::sal::Bucket> get_bucket_internal(
    sal_driver_t* driver,
    const char* bucket_name)
{
    if (!driver || !driver->driver || !bucket_name) {
        return nullptr;
    }

    try {
        rgw_bucket bucket_id(bucket_name);
        std::unique_ptr<rgw::sal::Bucket> bucket;

        int ret = driver->driver->get_bucket(
            driver->dpp,
            nullptr,  // user
            bucket_id,
            &bucket,
            null_yield
        );

        if (ret < 0 || !bucket) {
            return nullptr;
        }

        return bucket;
    } catch (...) {
        return nullptr;
    }
}

} // anonymous namespace

/* ========================================================================
 * Initialization
 * ======================================================================== */

extern "C" {

sal_context_t* sal_ctx_create(const char* cluster, const char* user, const char* conf) {
    if (!cluster || !user) {
        return nullptr;
    }

    try {
        std::vector<const char*> args;

        if (conf != nullptr) {
            args.push_back("--conf");
            args.push_back(conf);
        }

        args.push_back("--name");
        std::string user_str = std::string("client.") + user;
        args.push_back(user_str.c_str());

        CephContext* cct = common_preinit(
            static_cast<CephInitParameters>(CEPH_ENTITY_TYPE_CLIENT),
            static_cast<int>(args.size()),
            const_cast<char**>(args.data()),
            CINIT_FLAG_NO_DEFAULT_CONFIG_FILE
        );

        if (!cct) {
            return nullptr;
        }

        if (conf != nullptr) {
            cct->_conf.parse_config_files(conf, nullptr, 0);
        }

        cct->_conf.apply_changes(nullptr);

        DoutPrefixProvider* dpp = new SimpleDPP();

        return new sal_context_t(cct, dpp);
    } catch (...) {
        return nullptr;
    }
}

void sal_ctx_destroy(sal_context_t* ctx) {
    delete ctx;
}

sal_driver_t* sal_driver_create_rados(sal_context_t* ctx) {
    if (!ctx || !ctx->cct) {
        return nullptr;
    }

    try {
        rgw::sal::Driver* driver = rgw::sal::StoreManager::get_storage(
            ctx->dpp,
            ctx->cct,
            "rados",
            false,  // use data pool
            false   // run sync thread
        );

        if (!driver) {
            return nullptr;
        }

        return new sal_driver_t(driver, ctx->dpp);
    } catch (...) {
        return nullptr;
    }
}

void sal_driver_destroy(sal_driver_t* driver) {
    delete driver;
}

/* ========================================================================
 * TRUE 1-1 MAPPED APIs
 * ======================================================================== */

int sal_put_object(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    const char* data,
    uint64_t len,
    sal_put_mode_t mode,
    char** etag)
{
    if (!driver || !bucket_name || !key || !data) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -EINVAL;
        }

        // Check mode constraints
        if (mode == SAL_PUT_MODE_CREATE || mode == SAL_PUT_MODE_UPDATE) {
            int ret = obj->get_obj_state(driver->dpp, null_yield, false);
            bool exists = (ret == 0);

            if (mode == SAL_PUT_MODE_CREATE && exists) {
                return -EEXIST;
            }
            if (mode == SAL_PUT_MODE_UPDATE && !exists) {
                return -ENOENT;
            }
        }

        // Create atomic writer
        std::unique_ptr<rgw::sal::Writer> writer = driver->driver->get_atomic_writer(
            driver->dpp,
            null_yield,
            obj.release(),
            nullptr,  // owner
            nullptr,  // olh_epoch
            rgw_placement_rule()
        );

        if (!writer) {
            return -EIO;
        }

        // Prepare
        int ret = writer->prepare(null_yield);
        if (ret < 0) {
            return ret;
        }

        // Write data
        bufferlist bl;
        bl.append(data, len);
        ret = writer->process(std::move(bl), 0);
        if (ret < 0) {
            return ret;
        }

        // Complete
        rgw::sal::Attrs attrs;
        ceph::real_time mtime = ceph::real_clock::now();
        std::string tag;

        ret = writer->complete(
            len,
            tag,
            &mtime,
            ceph::real_time(),
            attrs,
            ceph::real_time(),
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            null_yield
        );

        if (ret == 0 && etag) {
            *etag = str_dup(tag);
        }

        return ret;
    });
}

int sal_get_object(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    uint64_t offset,
    uint64_t len,
    sal_conditionals_t* conds,
    char** buffer,
    uint64_t* bytes_read,
    sal_object_meta_t* meta)
{
    if (!driver || !bucket_name || !key || !buffer || !bytes_read || !meta) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -ENOENT;
        }

        // Get state/metadata
        int ret = obj->get_obj_state(driver->dpp, null_yield, false);
        if (ret < 0) {
            return ret;
        }

        uint64_t obj_size = obj->get_obj_size();
        rgw::sal::Attrs attrs = obj->get_attrs();
        ceph::real_time mtime = obj->get_mtime();
        std::string etag_str;

        auto iter = attrs.find(RGW_ATTR_ETAG);
        if (iter != attrs.end()) {
            etag_str = iter->second.to_str();
        }

        // Check conditionals if provided
        if (conds) {
            // if-match
            if (conds->if_match && !etag_str.empty()) {
                if (etag_str != conds->if_match) {
                    return -EPRECOND;
                }
            }

            // if-none-match
            if (conds->if_none_match && !etag_str.empty()) {
                if (etag_str == conds->if_none_match) {
                    return -EPRECOND;
                }
            }

            // if-modified-since
            if (conds->if_modified_since > 0) {
                time_t obj_mtime = ceph::real_clock::to_time_t(mtime);
                if (obj_mtime <= conds->if_modified_since) {
                    return -EPRECOND;
                }
            }

            // if-unmodified-since
            if (conds->if_unmodified_since > 0) {
                time_t obj_mtime = ceph::real_clock::to_time_t(mtime);
                if (obj_mtime > conds->if_unmodified_since) {
                    return -EPRECOND;
                }
            }
        }

        // Determine read length
        if (len == 0 || offset + len > obj_size) {
            len = obj_size - offset;
        }

        // Fill metadata
        meta->size = obj_size;
        meta->mtime_sec = static_cast<int64_t>(ceph::real_clock::to_time_t(mtime));
        meta->mtime_nsec = static_cast<int64_t>(mtime.time_since_epoch().count() % 1000000000);
        meta->etag = str_dup(etag_str);

        if (len == 0) {
            *buffer = nullptr;
            *bytes_read = 0;
            return 0;
        }

        // Allocate buffer
        *buffer = static_cast<char*>(malloc(len));
        if (!*buffer) {
            sal_free_string(meta->etag);
            meta->etag = nullptr;
            return -ENOMEM;
        }

        // Read data
        rgw::sal::Object::ReadParams params;
        params.offset = offset;
        params.length = len;

        bufferlist bl;
        ret = obj->read(driver->dpp, null_yield, params, bl);

        if (ret < 0) {
            free(*buffer);
            *buffer = nullptr;
            sal_free_string(meta->etag);
            meta->etag = nullptr;
            return ret;
        }

        // Copy to buffer
        *bytes_read = bl.length();
        if (*bytes_read > 0) {
            bl.begin().copy(*bytes_read, *buffer);
        }

        return 0;
    });
}

int sal_delete_object(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key)
{
    if (!driver || !bucket_name || !key) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -ENOENT;
        }

        // Delete
        rgw::sal::Object::DeleteParams params;
        rgw::sal::Object::DeleteResult result;

        return obj->delete_object(
            driver->dpp,
            null_yield,
            false,  // prevent_versioning
            params,
            result
        );
    });
}

int sal_list_objects(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* prefix,
    const char* marker,
    int max_keys,
    sal_list_result_t* result)
{
    return sal_list_objects_with_delimiter(
        driver, bucket_name, prefix, nullptr, marker, max_keys, result
    );
}

int sal_list_objects_with_delimiter(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* prefix,
    const char* delimiter,
    const char* marker,
    int max_keys,
    sal_list_result_t* result)
{
    if (!driver || !bucket_name || !result) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Initialize result
        result->entries = nullptr;
        result->count = 0;
        result->common_prefixes = nullptr;
        result->prefix_count = 0;
        result->next_marker = nullptr;

        // Setup list params
        rgw::sal::Bucket::ListParams params;
        params.prefix = prefix ? prefix : "";
        params.delim = delimiter ? delimiter : "";
        params.marker = marker ? rgw_obj_key(marker) : rgw_obj_key();
        params.list_versions = false;
        params.allow_unordered = false;

        rgw::sal::Bucket::ListResults results;

        int ret = bucket->list(
            driver->dpp,
            params,
            max_keys,
            results,
            null_yield
        );

        if (ret < 0) {
            return ret;
        }

        // Allocate entries
        if (!results.objs.empty()) {
            result->count = results.objs.size();
            result->entries = static_cast<sal_list_entry_t*>(
                calloc(result->count, sizeof(sal_list_entry_t))
            );

            if (!result->entries) {
                return -ENOMEM;
            }

            for (size_t i = 0; i < results.objs.size(); ++i) {
                auto& obj = results.objs[i];
                result->entries[i].key = str_dup(obj.key.name);
                result->entries[i].size = obj.meta.size;
                result->entries[i].mtime_sec = static_cast<int64_t>(obj.meta.mtime.sec());
                result->entries[i].mtime_nsec = static_cast<int64_t>(obj.meta.mtime.nsec());
                result->entries[i].etag = str_dup(obj.meta.etag);
            }
        }

        // Allocate common prefixes
        if (!results.common_prefixes.empty()) {
            result->prefix_count = results.common_prefixes.size();
            result->common_prefixes = static_cast<char**>(
                calloc(result->prefix_count, sizeof(char*))
            );

            if (!result->common_prefixes) {
                sal_list_result_free(result);
                return -ENOMEM;
            }

            for (size_t i = 0; i < results.common_prefixes.size(); ++i) {
                result->common_prefixes[i] = str_dup(results.common_prefixes[i]);
            }
        }

        // Set next marker
        if (results.is_truncated && !results.next_marker.empty()) {
            result->next_marker = str_dup(results.next_marker.name);
        }

        return 0;
    });
}

int sal_copy_object(
    sal_driver_t* driver,
    const char* src_bucket,
    const char* src_key,
    const char* dst_bucket,
    const char* dst_key)
{
    if (!driver || !src_bucket || !src_key || !dst_bucket || !dst_key) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get source bucket
        auto src_bkt = get_bucket_internal(driver, src_bucket);
        if (!src_bkt) {
            return -ENOENT;
        }

        // Get destination bucket
        auto dst_bkt = get_bucket_internal(driver, dst_bucket);
        if (!dst_bkt) {
            return -ENOENT;
        }

        // Get source object
        std::unique_ptr<rgw::sal::Object> src_obj = src_bkt->get_object(rgw_obj_key(src_key));
        if (!src_obj) {
            return -ENOENT;
        }

        // Get destination object
        std::unique_ptr<rgw::sal::Object> dst_obj = dst_bkt->get_object(rgw_obj_key(dst_key));
        if (!dst_obj) {
            return -EINVAL;
        }

        // Perform copy
        rgw::sal::Attrs attrs;
        ceph::real_time mtime;

        return src_obj->copy_object(
            driver->dpp,
            nullptr,  // user
            nullptr,  // info
            rgw_zone_id(),
            dst_obj.get(),
            dst_bkt.get(),
            src_bkt.get(),
            mtime,
            nullptr,
            nullptr,
            false,
            nullptr,
            attrs,
            RGWObjCategory::Main,
            0,
            ceph::real_time(),
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            null_yield
        );
    });
}

int sal_delete_objects(
    sal_driver_t* driver,
    const char* bucket_name,
    const char** keys,
    size_t count,
    sal_delete_result_t** results,
    size_t* result_count)
{
    if (!driver || !bucket_name || !keys || !results || !result_count) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        *results = static_cast<sal_delete_result_t*>(
            calloc(count, sizeof(sal_delete_result_t))
        );

        if (!*results) {
            return -ENOMEM;
        }

        *result_count = count;

        // Delete each object
        for (size_t i = 0; i < count; ++i) {
            (*results)[i].key = str_dup(keys[i]);

            std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(keys[i]));

            if (!obj) {
                (*results)[i].error_code = -ENOENT;
                continue;
            }

            rgw::sal::Object::DeleteParams params;
            rgw::sal::Object::DeleteResult result;

            int ret = obj->delete_object(
                driver->dpp,
                null_yield,
                false,
                params,
                result
            );

            (*results)[i].error_code = ret;
        }

        return 0;
    });
}

int sal_get_object_ranges(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    sal_byte_range_t* ranges,
    size_t range_count,
    sal_range_data_t** results,
    size_t* result_count)
{
    if (!driver || !bucket_name || !key || !ranges || !results || !result_count) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -ENOENT;
        }

        // Get state
        int ret = obj->get_obj_state(driver->dpp, null_yield, false);
        if (ret < 0) {
            return ret;
        }

        *results = static_cast<sal_range_data_t*>(
            calloc(range_count, sizeof(sal_range_data_t))
        );

        if (!*results) {
            return -ENOMEM;
        }

        *result_count = range_count;

        // Read each range
        for (size_t i = 0; i < range_count; ++i) {
            uint64_t len = ranges[i].end - ranges[i].start;
            char* buf = static_cast<char*>(malloc(len));

            if (!buf) {
                sal_range_data_free(*results, i);
                *results = nullptr;
                return -ENOMEM;
            }

            rgw::sal::Object::ReadParams params;
            params.offset = ranges[i].start;
            params.length = len;

            bufferlist bl;
            ret = obj->read(driver->dpp, null_yield, params, bl);

            if (ret < 0 && ret != -ENOENT) {
                free(buf);
                sal_range_data_free(*results, i);
                *results = nullptr;
                return ret;
            }

            uint64_t bytes_read = bl.length();
            if (bytes_read > 0) {
                bl.begin().copy(bytes_read, buf);
            }

            (*results)[i].data = buf;
            (*results)[i].len = bytes_read;
            (*results)[i].range = ranges[i];
        }

        return 0;
    });
}

int sal_init_multipart(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    char** upload_id)
{
    if (!driver || !bucket_name || !key || !upload_id) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -EINVAL;
        }

        // Create multipart upload
        std::unique_ptr<rgw::sal::MultipartUpload> mp = driver->driver->get_multipart_upload(
            obj.get(),
            ""  // upload_id will be generated
        );

        if (!mp) {
            return -EIO;
        }

        // Initialize
        ACLOwner owner;
        rgw::sal::Attrs attrs;
        ceph::real_time mtime = ceph::real_clock::now();

        int ret = mp->init(driver->dpp, null_yield, owner, rgw_placement_rule(), attrs, mtime);
        if (ret < 0) {
            return ret;
        }

        std::string uid = mp->get_upload_id();
        *upload_id = str_dup(uid);

        return 0;
    });
}

int sal_multipart_put_part(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    const char* upload_id,
    int part_num,
    const char* data,
    uint64_t len,
    char** etag)
{
    if (!driver || !bucket_name || !key || !upload_id || !data || !etag) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -EINVAL;
        }

        // Get multipart upload
        std::unique_ptr<rgw::sal::MultipartUpload> mp = driver->driver->get_multipart_upload(
            obj.get(),
            upload_id
        );

        if (!mp) {
            return -EINVAL;
        }

        // Create multipart writer
        std::unique_ptr<rgw::sal::Writer> writer = driver->driver->get_multipart_writer(
            driver->dpp,
            null_yield,
            mp.get(),
            rgw_obj_key(),
            nullptr,
            rgw_placement_rule(),
            part_num,
            upload_id
        );

        if (!writer) {
            return -EIO;
        }

        // Prepare
        int ret = writer->prepare(null_yield);
        if (ret < 0) {
            return ret;
        }

        // Write
        bufferlist bl;
        bl.append(data, len);
        ret = writer->process(std::move(bl), 0);
        if (ret < 0) {
            return ret;
        }

        // Complete
        rgw::sal::Attrs attrs;
        ceph::real_time mtime = ceph::real_clock::now();
        std::string tag;

        ret = writer->complete(
            len,
            tag,
            &mtime,
            ceph::real_time(),
            attrs,
            ceph::real_time(),
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            null_yield
        );

        if (ret == 0) {
            *etag = str_dup(tag);
        }

        return ret;
    });
}

int sal_multipart_complete(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    const char* upload_id,
    const char** part_etags,
    int num_parts,
    char** final_etag)
{
    if (!driver || !bucket_name || !key || !upload_id || !part_etags) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -EINVAL;
        }

        // Get multipart upload
        std::unique_ptr<rgw::sal::MultipartUpload> mp = driver->driver->get_multipart_upload(
            obj.get(),
            upload_id
        );

        if (!mp) {
            return -EINVAL;
        }

        // Complete multipart
        std::list<rgw_obj_index_key> remove_objs;
        uint64_t accounted_size = 0;
        bool compressed = false;
        RGWCompressionInfo cs_info;
        off_t off;
        std::string tag;
        ACLOwner owner;
        uint64_t olh_epoch = 0;
        rgw::sal::Attrs attrs;

        int ret = mp->complete(
            driver->dpp,
            null_yield,
            ceph::real_clock::now(),
            owner,
            olh_epoch,
            &remove_objs,
            &accounted_size,
            &compressed,
            &cs_info,
            &off,
            &tag,
            attrs
        );

        if (ret == 0 && final_etag) {
            *final_etag = str_dup(tag);
        }

        return ret;
    });
}

int sal_multipart_abort(
    sal_driver_t* driver,
    const char* bucket_name,
    const char* key,
    const char* upload_id)
{
    if (!driver || !bucket_name || !key || !upload_id) {
        return -EINVAL;
    }

    return safe_call([&]() -> int {
        // Get bucket
        auto bucket = get_bucket_internal(driver, bucket_name);
        if (!bucket) {
            return -ENOENT;
        }

        // Get object
        std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(key));
        if (!obj) {
            return -EINVAL;
        }

        // Get multipart upload
        std::unique_ptr<rgw::sal::MultipartUpload> mp = driver->driver->get_multipart_upload(
            obj.get(),
            upload_id
        );

        if (!mp) {
            return -EINVAL;
        }

        return mp->abort(driver->dpp, null_yield);
    });
}

/* ========================================================================
 * Memory Management
 * ======================================================================== */

void sal_free_string(char* str) {
    free(str);
}

void sal_free_buffer(char* buf) {
    free(buf);
}

void sal_object_meta_free(sal_object_meta_t* meta) {
    if (meta) {
        sal_free_string(meta->etag);
        meta->etag = nullptr;
    }
}

void sal_list_result_free(sal_list_result_t* result) {
    if (!result) {
        return;
    }

    if (result->entries) {
        for (size_t i = 0; i < result->count; ++i) {
            sal_free_string(result->entries[i].key);
            sal_free_string(result->entries[i].etag);
        }
        free(result->entries);
        result->entries = nullptr;
    }

    if (result->common_prefixes) {
        for (size_t i = 0; i < result->prefix_count; ++i) {
            sal_free_string(result->common_prefixes[i]);
        }
        free(result->common_prefixes);
        result->common_prefixes = nullptr;
    }

    sal_free_string(result->next_marker);
    result->next_marker = nullptr;

    result->count = 0;
    result->prefix_count = 0;
}

void sal_range_data_free(sal_range_data_t* results, size_t count) {
    if (!results) {
        return;
    }

    for (size_t i = 0; i < count; ++i) {
        sal_free_buffer(results[i].data);
    }

    free(results);
}

void sal_delete_results_free(sal_delete_result_t* results, size_t count) {
    if (!results) {
        return;
    }

    for (size_t i = 0; i < count; ++i) {
        sal_free_string(results[i].key);
    }

    free(results);
}

} // extern "C"
