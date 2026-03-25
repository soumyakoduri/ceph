// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Arrow Authors

#include "rgw_object_store.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"
#include "common/errno.h"
#include <cstring>
#include <memory>

using namespace rgw::sal;

namespace {

// Helper to allocate C string
char* alloc_cstring(const std::string& str) {
    if (str.empty()) {
        return nullptr;
    }
    char* result = static_cast<char*>(malloc(str.size() + 1));
    if (result) {
        memcpy(result, str.c_str(), str.size() + 1);
    }
    return result;
}

// Helper to get DPP
const DoutPrefixProvider* get_dpp(const void* dpp) {
    return dpp ? static_cast<const DoutPrefixProvider*>(dpp) : null_dpp();
}

} // anonymous namespace

extern "C" {

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
) {
    if (!driver || !bucket || !key || !buffer || !bytes_read || !meta) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Read object state
        std::string etag_str;
        ceph::real_time mtime;
        uint64_t obj_size = 0;

        ret = sal_object->get_obj_state(sal_dpp, &obj_size, &etag_str, &mtime, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Check conditionals
        if (conds) {
            // Check if-match condition
            if (conds->if_match && etag_str != conds->if_match) {
                return -EINVAL;  // ETag mismatch (precondition failed)
            }

            // Check if-none-match condition
            if (conds->if_none_match && etag_str == conds->if_none_match) {
                return -EEXIST;  // ETag matched (not modified)
            }

            // Check if-modified-since condition
            if (conds->if_modified_since > 0) {
                time_t obj_mtime = ceph::real_clock::to_time_t(mtime);
                if (obj_mtime <= conds->if_modified_since) {
                    return -EEXIST;  // Not modified
                }
            }

            // Check if-unmodified-since condition
            if (conds->if_unmodified_since > 0) {
                time_t obj_mtime = ceph::real_clock::to_time_t(mtime);
                if (obj_mtime > conds->if_unmodified_since) {
                    return -EINVAL;  // Modified (precondition failed)
                }
            }
        }

        // Set up range
        bufferlist bl;
        uint64_t read_offset = offset;
        uint64_t read_length = length;
        if (read_length == 0) {
            read_length = obj_size - read_offset;
        }

        // Read data
        ret = sal_object->read(sal_dpp, read_offset, read_length, bl, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Allocate buffer and copy data
        *bytes_read = bl.length();
        *buffer = static_cast<char*>(malloc(*bytes_read));
        if (!*buffer) {
            return -ENOMEM;
        }
        bl.begin().copy(*bytes_read, *buffer);

        // Fill metadata
        meta->etag = alloc_cstring(etag_str);
        meta->size = obj_size;
        auto mtime_tp = ceph::real_clock::to_timespec(mtime);
        meta->mtime_sec = mtime_tp.tv_sec;
        meta->mtime_nsec = mtime_tp.tv_nsec;

        return 0;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

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
) {
    if (!driver || !bucket || !key || !callback || !total_read) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Get object size
        uint64_t obj_size = 0;
        std::string etag_str;
        ceph::real_time mtime;

        ret = sal_object->get_obj_state(sal_dpp, &obj_size, &etag_str, &mtime, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Determine read range
        uint64_t read_offset = offset;
        uint64_t read_length = length;
        if (read_length == 0 || read_offset + read_length > obj_size) {
            read_length = obj_size - read_offset;
        }

        // Read object data in chunks
        const uint64_t CHUNK_SIZE = 4 * 1024 * 1024; // 4MB chunks
        uint64_t bytes_remaining = read_length;
        uint64_t current_offset = read_offset;
        *total_read = 0;

        while (bytes_remaining > 0) {
            uint64_t chunk_size = (bytes_remaining < CHUNK_SIZE) ? bytes_remaining : CHUNK_SIZE;

            bufferlist bl;
            ret = sal_object->read(sal_dpp, current_offset, chunk_size, bl, null_yield);
            if (ret < 0) {
                return ret;
            }

            // Call user callback with chunk
            uint64_t chunk_len = bl.length();
            if (chunk_len > 0) {
                // Copy to temporary buffer for callback
                std::vector<char> temp_buf(chunk_len);
                bl.begin().copy(chunk_len, temp_buf.data());

                ret = callback(temp_buf.data(), chunk_len, user_data);
                if (ret < 0) {
                    return ret; // Callback requested abort
                }

                *total_read += chunk_len;
            }

            bytes_remaining -= chunk_len;
            current_offset += chunk_len;

            // Break if we read less than expected (EOF)
            if (chunk_len < chunk_size) {
                break;
            }
        }

        return 0;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

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
) {
    if (!driver || !bucket || !key || !data || !etag) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Check if object exists (for conditionals)
        bool object_exists = false;
        std::string existing_etag;
        uint64_t obj_size = 0;
        ceph::real_time mtime;

        ret = sal_object->get_obj_state(sal_dpp, &obj_size, &existing_etag, &mtime, null_yield);
        if (ret == 0) {
            object_exists = true;
        } else if (ret != -ENOENT) {
            // Real error, not just "not found"
            return ret;
        }

        // Apply put conditionals
        if (conds) {
            // Check if-not-exists condition
            if (conds->if_not_exists && object_exists) {
                return -EEXIST;  // Object already exists
            }

            // Check if-match condition (only if object exists)
            if (conds->if_match && object_exists) {
                if (existing_etag != conds->if_match) {
                    return -EINVAL;  // ETag mismatch
                }
            }

            // Check if-none-match condition (only if object exists)
            if (conds->if_none_match && object_exists) {
                if (existing_etag == conds->if_none_match) {
                    return -EEXIST;  // ETag matched (object not modified)
                }
            }
        }

        // Create bufferlist from data
        bufferlist bl;
        bl.append(data, data_len);

        // Prepare writer
        std::unique_ptr<Writer> writer;
        ret = sal_object->get_obj_writer(sal_dpp, null_yield, &writer);
        if (ret < 0) {
            return ret;
        }

        // Set content type if provided
        if (content_type) {
            // TODO: Set content-type attribute through writer or object
            // This may require access to rgw_op.h or similar headers
        }

        // Write data
        ret = writer->write(sal_dpp, bl, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Complete write
        std::string etag_str;
        ret = writer->complete(sal_dpp, null_yield, &etag_str);
        if (ret < 0) {
            return ret;
        }

        *etag = alloc_cstring(etag_str);
        return 0;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

int rgw_head_object(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    RGWObjectMeta* meta
) {
    if (!driver || !bucket || !key || !meta) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Get object state
        std::string etag_str;
        ceph::real_time mtime;
        uint64_t obj_size = 0;

        ret = sal_object->get_obj_state(sal_dpp, &obj_size, &etag_str, &mtime, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Fill metadata
        meta->etag = alloc_cstring(etag_str);
        meta->size = obj_size;
        auto mtime_tp = ceph::real_clock::to_timespec(mtime);
        meta->mtime_sec = mtime_tp.tv_sec;
        meta->mtime_nsec = mtime_tp.tv_nsec;

        return 0;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

int rgw_delete_object(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key
) {
    if (!driver || !bucket || !key) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Delete object
        ret = sal_object->delete_object(sal_dpp, null_yield);
        return ret;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

int rgw_get_ranges(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    const RGWRange* ranges,
    uint32_t num_ranges,
    RGWRangeResult** results
) {
    if (!driver || !bucket || !key || !ranges || !results || num_ranges == 0) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Allocate results array
        *results = static_cast<RGWRangeResult*>(calloc(num_ranges, sizeof(RGWRangeResult)));
        if (!*results) {
            return -ENOMEM;
        }

        // Read each range
        for (uint32_t i = 0; i < num_ranges; ++i) {
            bufferlist bl;
            ret = sal_object->read(sal_dpp, ranges[i].offset, ranges[i].length, bl, null_yield);
            if (ret < 0) {
                // Free previously allocated ranges
                rgw_free_ranges(*results, i);
                *results = nullptr;
                return ret;
            }

            // Allocate and copy data for this range
            (*results)[i].length = bl.length();
            (*results)[i].data = static_cast<char*>(malloc(bl.length()));
            if (!(*results)[i].data) {
                rgw_free_ranges(*results, i);
                *results = nullptr;
                return -ENOMEM;
            }
            bl.begin().copy(bl.length(), (*results)[i].data);
        }

        return 0;
    } catch (const std::exception& e) {
        if (*results) {
            rgw_free_ranges(*results, num_ranges);
            *results = nullptr;
        }
        return -EIO;
    }
}

int rgw_delete_objects(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char** keys,
    uint32_t num_keys,
    int* results
) {
    if (!driver || !bucket || !keys || !results || num_keys == 0) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        int overall_ret = 0;

        // Delete each object
        for (uint32_t i = 0; i < num_keys; ++i) {
            if (!keys[i]) {
                results[i] = -EINVAL;
                overall_ret = -EINVAL;
                continue;
            }

            // Get object
            std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(keys[i]));
            if (!sal_object) {
                results[i] = -ENOENT;
                overall_ret = -ENOENT;
                continue;
            }

            // Delete object
            ret = sal_object->delete_object(sal_dpp, null_yield);
            results[i] = ret;
            if (ret < 0) {
                overall_ret = ret;
            }
        }

        return overall_ret;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

int rgw_list_objects(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* prefix,
    const char* delimiter,
    const char* marker,
    uint32_t max_keys,
    RGWListResult* result
) {
    if (!driver || !bucket || !result) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Set up list params
        ListParams params;
        params.prefix = prefix ? prefix : "";
        params.delim = delimiter ? delimiter : "";
        params.marker = rgw_obj_key(marker ? marker : "");
        params.list_versions = false;
        params.allow_unordered = false;

        ListResults results;
        ret = sal_bucket->list(sal_dpp, params, max_keys, results, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Allocate object array
        result->num_objects = results.objs.size();
        result->objects = nullptr;
        if (result->num_objects > 0) {
            result->objects = static_cast<RGWObjectEntry*>(
                calloc(result->num_objects, sizeof(RGWObjectEntry))
            );
            if (!result->objects) {
                return -ENOMEM;
            }

            for (size_t i = 0; i < results.objs.size(); ++i) {
                const auto& obj = results.objs[i];
                result->objects[i].key = alloc_cstring(obj.key.name);
                result->objects[i].etag = alloc_cstring(obj.meta.etag);
                result->objects[i].size = obj.meta.size;
                auto mtime_tp = ceph::real_clock::to_timespec(obj.meta.mtime);
                result->objects[i].mtime_sec = mtime_tp.tv_sec;
                result->objects[i].mtime_nsec = mtime_tp.tv_nsec;
            }
        }

        // Allocate common prefixes
        result->num_common_prefixes = results.common_prefixes.size();
        result->common_prefixes = nullptr;
        if (result->num_common_prefixes > 0) {
            result->common_prefixes = static_cast<char**>(
                calloc(result->num_common_prefixes, sizeof(char*))
            );
            if (!result->common_prefixes) {
                rgw_list_result_free(result);
                return -ENOMEM;
            }

            for (size_t i = 0; i < results.common_prefixes.size(); ++i) {
                result->common_prefixes[i] = alloc_cstring(results.common_prefixes[i]);
            }
        }

        // Set truncation info
        result->is_truncated = results.is_truncated ? 1 : 0;
        result->next_marker = results.is_truncated ?
            alloc_cstring(results.next_marker.name) : nullptr;

        return 0;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

int rgw_copy_object(
    void* driver,
    const void* dpp,
    const char* src_bucket,
    const char* src_key,
    const char* dst_bucket,
    const char* dst_key
) {
    if (!driver || !src_bucket || !src_key || !dst_bucket || !dst_key) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get source bucket and object
        std::unique_ptr<Bucket> src_sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, src_bucket, &src_sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        std::unique_ptr<Object> src_object = src_sal_bucket->get_object(rgw_obj_key(src_key));
        if (!src_object) {
            return -ENOENT;
        }

        // Get destination bucket and object
        std::unique_ptr<Bucket> dst_sal_bucket;
        ret = sal_driver->get_bucket(sal_dpp, nullptr, dst_bucket, &dst_sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        std::unique_ptr<Object> dst_object = dst_sal_bucket->get_object(rgw_obj_key(dst_key));
        if (!dst_object) {
            return -ENOENT;
        }

        // Perform copy
        ret = src_object->copy_object(sal_dpp, dst_object.get(), null_yield);
        return ret;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

int rgw_init_multipart(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    char** upload_id
) {
    if (!driver || !bucket || !key || !upload_id) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Initialize multipart upload
        std::unique_ptr<MultipartUpload> upload;
        ret = sal_object->create_multipart_upload(sal_dpp, null_yield, &upload);
        if (ret < 0) {
            return ret;
        }

        *upload_id = alloc_cstring(upload->get_upload_id());
        return 0;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

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
) {
    if (!driver || !bucket || !key || !upload_id || !data || !etag) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Get multipart upload
        std::unique_ptr<MultipartUpload> upload;
        ret = sal_object->get_multipart_upload(sal_dpp, upload_id, &upload, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Create bufferlist
        bufferlist bl;
        bl.append(data, data_len);

        // Upload part
        std::unique_ptr<Writer> writer;
        ret = upload->get_writer(sal_dpp, null_yield, part_num, &writer);
        if (ret < 0) {
            return ret;
        }

        ret = writer->write(sal_dpp, bl, null_yield);
        if (ret < 0) {
            return ret;
        }

        std::string etag_str;
        ret = writer->complete(sal_dpp, null_yield, &etag_str);
        if (ret < 0) {
            return ret;
        }

        *etag = alloc_cstring(etag_str);
        return 0;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

int rgw_complete_multipart(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    const char* upload_id,
    uint32_t num_parts
) {
    if (!driver || !bucket || !key || !upload_id) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Get multipart upload
        std::unique_ptr<MultipartUpload> upload;
        ret = sal_object->get_multipart_upload(sal_dpp, upload_id, &upload, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Complete upload
        ret = upload->complete(sal_dpp, num_parts, null_yield);
        return ret;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

int rgw_abort_multipart(
    void* driver,
    const void* dpp,
    const char* bucket,
    const char* key,
    const char* upload_id
) {
    if (!driver || !bucket || !key || !upload_id) {
        return -EINVAL;
    }

    Driver* sal_driver = static_cast<Driver*>(driver);
    const DoutPrefixProvider* sal_dpp = get_dpp(dpp);

    try {
        // Get bucket
        std::unique_ptr<Bucket> sal_bucket;
        int ret = sal_driver->get_bucket(sal_dpp, nullptr, bucket, &sal_bucket, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Get object
        std::unique_ptr<Object> sal_object = sal_bucket->get_object(rgw_obj_key(key));
        if (!sal_object) {
            return -ENOENT;
        }

        // Get multipart upload
        std::unique_ptr<MultipartUpload> upload;
        ret = sal_object->get_multipart_upload(sal_dpp, upload_id, &upload, null_yield);
        if (ret < 0) {
            return ret;
        }

        // Abort upload
        ret = upload->abort(sal_dpp, null_yield);
        return ret;
    } catch (const std::exception& e) {
        return -EIO;
    }
}

void rgw_free_buffer(char* buffer) {
    free(buffer);
}

void rgw_free_string(char* str) {
    free(str);
}

void rgw_list_result_free(RGWListResult* result) {
    if (!result) {
        return;
    }

    // Free objects
    if (result->objects) {
        for (uint32_t i = 0; i < result->num_objects; ++i) {
            free(result->objects[i].key);
            free(result->objects[i].etag);
        }
        free(result->objects);
        result->objects = nullptr;
    }

    // Free common prefixes
    if (result->common_prefixes) {
        for (uint32_t i = 0; i < result->num_common_prefixes; ++i) {
            free(result->common_prefixes[i]);
        }
        free(result->common_prefixes);
        result->common_prefixes = nullptr;
    }

    // Free next marker
    if (result->next_marker) {
        free(result->next_marker);
        result->next_marker = nullptr;
    }

    result->num_objects = 0;
    result->num_common_prefixes = 0;
    result->is_truncated = 0;
}

void rgw_object_meta_free(RGWObjectMeta* meta) {
    if (!meta) {
        return;
    }

    if (meta->etag) {
        free(meta->etag);
        meta->etag = nullptr;
    }

    meta->size = 0;
    meta->mtime_sec = 0;
    meta->mtime_nsec = 0;
}

void rgw_free_ranges(RGWRangeResult* results, uint32_t num_ranges) {
    if (!results) {
        return;
    }

    for (uint32_t i = 0; i < num_ranges; ++i) {
        if (results[i].data) {
            free(results[i].data);
        }
    }

    free(results);
}

} // extern "C"
