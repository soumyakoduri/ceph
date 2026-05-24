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
 * Unit tests for rgw_sal_wrapper.h/cc
 * Tests the C wrapper functions that interface with the Rust ceph-lancedb-rgw crate.
 */

#include "gtest/gtest.h"
#include "rgw_sal_wrapper.h"
#include <cstring>
#include <string>
#include <vector>

namespace {

//=============================================================================
// RGWBuffer Tests
//=============================================================================

class RGWBufferTest : public ::testing::Test {
protected:
    void SetUp() override {
        memset(&buffer, 0, sizeof(buffer));
    }

    void TearDown() override {
        // Ensure buffer is freed if test allocated it
        if (buffer.data != nullptr) {
            rgw_free_buffer(&buffer);
        }
    }

    RGWBuffer buffer;
};

TEST_F(RGWBufferTest, DefaultInitialization) {
    EXPECT_EQ(buffer.data, nullptr);
    EXPECT_EQ(buffer.len, 0);
    EXPECT_EQ(buffer.capacity, 0);
}

TEST_F(RGWBufferTest, FreeNullBuffer) {
    // Should not crash when freeing a null buffer
    RGWBuffer null_buffer = {nullptr, 0, 0};
    rgw_free_buffer(&null_buffer);
    SUCCEED();
}

//=============================================================================
// RGWObjectMeta Tests
//=============================================================================

class RGWObjectMetaTest : public ::testing::Test {
protected:
    void SetUp() override {
        memset(&meta, 0, sizeof(meta));
    }

    void TearDown() override {
        rgw_free_object_meta(&meta);
    }

    RGWObjectMeta meta;
};

TEST_F(RGWObjectMetaTest, DefaultInitialization) {
    EXPECT_EQ(meta.size, 0);
    EXPECT_EQ(meta.etag, nullptr);
    EXPECT_EQ(meta.content_type, nullptr);
    EXPECT_EQ(meta.last_modified, 0);
}

TEST_F(RGWObjectMetaTest, FreeNullMeta) {
    RGWObjectMeta null_meta = {0, nullptr, nullptr, 0};
    rgw_free_object_meta(&null_meta);
    SUCCEED();
}

//=============================================================================
// RGWListResult Tests
//=============================================================================

class RGWListResultTest : public ::testing::Test {
protected:
    void SetUp() override {
        memset(&result, 0, sizeof(result));
    }

    void TearDown() override {
        rgw_free_list_result(&result);
    }

    RGWListResult result;
};

TEST_F(RGWListResultTest, DefaultInitialization) {
    EXPECT_EQ(result.entries, nullptr);
    EXPECT_EQ(result.count, 0);
    EXPECT_EQ(result.is_truncated, 0);
    EXPECT_EQ(result.next_marker, nullptr);
}

TEST_F(RGWListResultTest, FreeNullResult) {
    RGWListResult null_result = {nullptr, 0, 0, nullptr};
    rgw_free_list_result(&null_result);
    SUCCEED();
}

//=============================================================================
// Mock Driver Tests
//
// These tests use null pointers for driver/dpp since we're testing the
// wrapper structure, not actual SAL operations.
//=============================================================================

class RGWSALWrapperTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Using null pointers - actual SAL operations will fail gracefully
        driver = nullptr;
        dpp = nullptr;
    }

    void* driver;
    const void* dpp;
};

// Test that put_object handles null driver gracefully
TEST_F(RGWSALWrapperTest, PutObjectNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";
    const uint8_t data[] = "test data";
    const char* content_type = "application/octet-stream";

    int result = rgw_put_object(driver, dpp, nullptr, bucket, key, data, sizeof(data), content_type);
    // Should return an error (negative errno) for null driver
    EXPECT_LT(result, 0);
}

// Test that get_object handles null driver gracefully
TEST_F(RGWSALWrapperTest, GetObjectNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";
    RGWBuffer buffer = {nullptr, 0, 0};

    int result = rgw_get_object(driver, dpp, nullptr, bucket, key, 0, UINT64_MAX, &buffer);
    EXPECT_LT(result, 0);

    rgw_free_buffer(&buffer);
}

// Test that delete_object handles null driver gracefully
TEST_F(RGWSALWrapperTest, DeleteObjectNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";

    int result = rgw_delete_object(driver, dpp, nullptr, bucket, key);
    EXPECT_LT(result, 0);
}

// Test that head_object handles null driver gracefully
TEST_F(RGWSALWrapperTest, HeadObjectNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";
    RGWObjectMeta meta = {0, nullptr, nullptr, 0};

    int result = rgw_head_object(driver, dpp, nullptr, bucket, key, &meta);
    EXPECT_LT(result, 0);

    rgw_free_object_meta(&meta);
}

// Test that list_objects handles null driver gracefully
TEST_F(RGWSALWrapperTest, ListObjectsNullDriver) {
    const char* bucket = "test-bucket";
    const char* prefix = "";
    const char* delimiter = "";
    const char* marker = "";
    RGWListResult result_struct = {nullptr, 0, 0, nullptr};

    int result = rgw_list_objects(driver, dpp, nullptr, bucket, prefix, delimiter, marker, 1000, &result_struct);
    EXPECT_LT(result, 0);

    rgw_free_list_result(&result_struct);
}

// Test that copy_object handles null driver gracefully
TEST_F(RGWSALWrapperTest, CopyObjectNullDriver) {
    const char* src_bucket = "src-bucket";
    const char* src_key = "src-key";
    const char* dst_bucket = "dst-bucket";
    const char* dst_key = "dst-key";

    int result = rgw_copy_object(driver, dpp, nullptr, src_bucket, src_key, dst_bucket, dst_key);
    EXPECT_LT(result, 0);
}

// Test that put_object_conditional handles null driver gracefully
TEST_F(RGWSALWrapperTest, PutObjectConditionalNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";
    const uint8_t data[] = "test data";
    const char* content_type = "application/octet-stream";
    int canceled = 0;

    // Test with if_match (update-if-match semantics)
    int result = rgw_put_object_conditional(driver, dpp, nullptr, bucket, key,
                                            data, sizeof(data), content_type,
                                            "existing-etag", nullptr, &canceled);
    EXPECT_LT(result, 0);

    // Test with if_nomatch="*" (create-if-not-exists semantics)
    result = rgw_put_object_conditional(driver, dpp, nullptr, bucket, key,
                                        data, sizeof(data), content_type,
                                        nullptr, "*", &canceled);
    EXPECT_LT(result, 0);

    // Test with both if_match and if_nomatch
    result = rgw_put_object_conditional(driver, dpp, nullptr, bucket, key,
                                        data, sizeof(data), content_type,
                                        "etag1", "etag2", &canceled);
    EXPECT_LT(result, 0);

    // Test with null canceled pointer (should still work)
    result = rgw_put_object_conditional(driver, dpp, nullptr, bucket, key,
                                        data, sizeof(data), content_type,
                                        nullptr, "*", nullptr);
    EXPECT_LT(result, 0);
}

// Test that copy_object_conditional handles null driver gracefully
TEST_F(RGWSALWrapperTest, CopyObjectConditionalNullDriver) {
    const char* src_bucket = "src-bucket";
    const char* src_key = "src-key";
    const char* dst_bucket = "dst-bucket";
    const char* dst_key = "dst-key";

    // Test with if_nomatch="*" (copy-if-not-exists semantics)
    int result = rgw_copy_object_conditional(driver, dpp, nullptr,
                                             src_bucket, src_key,
                                             dst_bucket, dst_key,
                                             nullptr, "*");
    EXPECT_LT(result, 0);

    // Test with if_match (copy-if-match semantics)
    result = rgw_copy_object_conditional(driver, dpp, nullptr,
                                         src_bucket, src_key,
                                         dst_bucket, dst_key,
                                         "existing-etag", nullptr);
    EXPECT_LT(result, 0);

    // Test with both null (unconditional copy, same as rgw_copy_object)
    result = rgw_copy_object_conditional(driver, dpp, nullptr,
                                         src_bucket, src_key,
                                         dst_bucket, dst_key,
                                         nullptr, nullptr);
    EXPECT_LT(result, 0);
}

// Test that delete_objects (bulk delete) handles null driver gracefully
TEST_F(RGWSALWrapperTest, DeleteObjectsNullDriver) {
    const char* bucket = "test-bucket";
    const char* keys[] = {"key1", "key2", "key3"};

    int result = rgw_delete_objects(driver, dpp, nullptr, bucket, keys, 3);
    EXPECT_LT(result, 0);
}

// Test delete_objects with empty array
TEST_F(RGWSALWrapperTest, DeleteObjectsEmptyArray) {
    const char* bucket = "test-bucket";
    const char** keys = nullptr;

    int result = rgw_delete_objects(driver, dpp, nullptr, bucket, keys, 0);
    EXPECT_LT(result, 0);
}

// Test multipart init with null driver
TEST_F(RGWSALWrapperTest, InitMultipartNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";
    char upload_id[128];
    memset(upload_id, 0, sizeof(upload_id));

    int result = rgw_init_multipart(driver, dpp, nullptr, bucket, key, upload_id, sizeof(upload_id));
    EXPECT_LT(result, 0);
}

// Test multipart put part with null driver
TEST_F(RGWSALWrapperTest, MultipartPutPartNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";
    const char* upload_id = "fake-upload-id";
    const uint8_t data[] = "part data";
    char etag[64];
    memset(etag, 0, sizeof(etag));

    int result = rgw_multipart_put_part(
        driver, dpp, nullptr, bucket, key, upload_id, 1,
        data, sizeof(data), etag, sizeof(etag)
    );
    EXPECT_LT(result, 0);
}

// Test multipart complete with null driver
TEST_F(RGWSALWrapperTest, MultipartCompleteNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";
    const char* upload_id = "fake-upload-id";
    const char* etags[] = {"etag1", "etag2"};

    int result = rgw_multipart_complete(driver, dpp, nullptr, bucket, key, upload_id, etags, 2);
    EXPECT_LT(result, 0);
}

// Test multipart abort with null driver
TEST_F(RGWSALWrapperTest, MultipartAbortNullDriver) {
    const char* bucket = "test-bucket";
    const char* key = "test-key";
    const char* upload_id = "fake-upload-id";

    int result = rgw_multipart_abort(driver, dpp, nullptr, bucket, key, upload_id);
    EXPECT_LT(result, 0);
}

//=============================================================================
// Parameter Validation Tests
//
// These tests verify that null/invalid parameters are rejected gracefully.
// We use nullptr for driver to avoid undefined behavior from dereferencing
// invalid pointers - the wrapper checks for null driver first.
//=============================================================================

class ParameterValidationTest : public ::testing::Test {
protected:
    // Use nullptr for driver - wrapper should check this first before
    // attempting any operations that would dereference it
    void* null_driver = nullptr;
    const void* null_dpp = nullptr;
};

TEST_F(ParameterValidationTest, PutObjectNullBucket) {
    const uint8_t data[] = "test";
    int result = rgw_put_object(null_driver, null_dpp, nullptr, nullptr, "key",
                                data, sizeof(data), "text/plain");
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, PutObjectNullKey) {
    const uint8_t data[] = "test";
    int result = rgw_put_object(null_driver, null_dpp, nullptr, "bucket", nullptr,
                                data, sizeof(data), "text/plain");
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, GetObjectNullBuffer) {
    int result = rgw_get_object(null_driver, null_dpp, nullptr, "bucket", "key",
                                0, UINT64_MAX, nullptr);
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, HeadObjectNullMeta) {
    int result = rgw_head_object(null_driver, null_dpp, nullptr, "bucket", "key", nullptr);
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, ListObjectsNullResult) {
    int result = rgw_list_objects(null_driver, null_dpp, nullptr, "bucket", "", "", "", 1000, nullptr);
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, PutObjectConditionalNullBucket) {
    const uint8_t data[] = "test";
    int canceled = 0;
    int result = rgw_put_object_conditional(null_driver, null_dpp, nullptr, nullptr, "key",
                                            data, sizeof(data), "text/plain",
                                            nullptr, "*", &canceled);
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, PutObjectConditionalNullKey) {
    const uint8_t data[] = "test";
    int canceled = 0;
    int result = rgw_put_object_conditional(null_driver, null_dpp, nullptr, "bucket", nullptr,
                                            data, sizeof(data), "text/plain",
                                            nullptr, "*", &canceled);
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, CopyObjectConditionalNullSrcBucket) {
    int result = rgw_copy_object_conditional(null_driver, null_dpp, nullptr,
                                             nullptr, "src-key",
                                             "dst-bucket", "dst-key",
                                             nullptr, "*");
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, CopyObjectConditionalNullDstKey) {
    int result = rgw_copy_object_conditional(null_driver, null_dpp, nullptr,
                                             "src-bucket", "src-key",
                                             "dst-bucket", nullptr,
                                             nullptr, "*");
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, DeleteObjectsNullBucket) {
    const char* keys[] = {"key1", "key2"};
    int result = rgw_delete_objects(null_driver, null_dpp, nullptr, nullptr, keys, 2);
    EXPECT_LT(result, 0);
}

TEST_F(ParameterValidationTest, DeleteObjectsNullKeys) {
    int result = rgw_delete_objects(null_driver, null_dpp, nullptr, "bucket", nullptr, 2);
    EXPECT_LT(result, 0);
}

//=============================================================================
// Structure Layout Tests
//
// Verify that structure layouts match what the Rust code expects
//=============================================================================

TEST(StructureLayoutTest, RGWBufferSize) {
    // RGWBuffer should be 3 pointers/size_t in size
    EXPECT_EQ(sizeof(RGWBuffer), sizeof(uint8_t*) + 2 * sizeof(size_t));
}

TEST(StructureLayoutTest, RGWBufferAlignment) {
    // Check field offsets are as expected
    EXPECT_EQ(offsetof(RGWBuffer, data), 0);
    EXPECT_EQ(offsetof(RGWBuffer, len), sizeof(uint8_t*));
    EXPECT_EQ(offsetof(RGWBuffer, capacity), sizeof(uint8_t*) + sizeof(size_t));
}

TEST(StructureLayoutTest, RGWObjectMetaSize) {
    // RGWObjectMeta: uint64_t + char* + char* + int64_t
    size_t expected = sizeof(uint64_t) + sizeof(char*) + sizeof(char*) + sizeof(int64_t);
    EXPECT_EQ(sizeof(RGWObjectMeta), expected);
}

TEST(StructureLayoutTest, RGWListEntrySize) {
    // RGWListEntry: char* + uint64_t + int64_t
    size_t expected = sizeof(char*) + sizeof(uint64_t) + sizeof(int64_t);
    EXPECT_EQ(sizeof(RGWListEntry), expected);
}

TEST(StructureLayoutTest, RGWListResultSize) {
    // RGWListResult: RGWListEntry* + size_t + int + char*
    // Note: may have padding due to alignment
    EXPECT_GE(sizeof(RGWListResult),
              sizeof(RGWListEntry*) + sizeof(size_t) + sizeof(int) + sizeof(char*));
}

//=============================================================================
// Boundary Tests
//
// These tests verify handling of boundary conditions like zero-length data,
// large data, range reads, and special characters. We use nullptr for driver
// to avoid undefined behavior - the wrapper will return an error but won't
// crash due to dereferencing invalid pointers.
//=============================================================================

class BoundaryTest : public ::testing::Test {
protected:
    // Use nullptr to avoid undefined behavior from dereferencing fake pointers
    void* null_driver = nullptr;
    const void* null_dpp = nullptr;
};

TEST_F(BoundaryTest, PutObjectZeroLength) {
    const uint8_t data[] = "";
    int result = rgw_put_object(null_driver, null_dpp, nullptr, "bucket", "key",
                                data, 0, "application/octet-stream");
    // With null driver, should return error without crashing
    EXPECT_LT(result, 0);
}

TEST_F(BoundaryTest, PutObjectMaxLength) {
    // Test with a large but reasonable size - verifies no issues with size handling
    std::vector<uint8_t> large_data(1024 * 1024); // 1MB
    int result = rgw_put_object(null_driver, null_dpp, nullptr, "bucket", "key",
                                large_data.data(), large_data.size(),
                                "application/octet-stream");
    // With null driver, should return error without crashing
    EXPECT_LT(result, 0);
}

TEST_F(BoundaryTest, GetObjectRangeRead) {
    RGWBuffer buffer = {nullptr, 0, 0};

    // Test range read parameters are handled correctly
    int result = rgw_get_object(null_driver, null_dpp, nullptr, "bucket", "key",
                                100, 50, &buffer);
    EXPECT_LT(result, 0);
    rgw_free_buffer(&buffer);
}

TEST_F(BoundaryTest, ListObjectsMaxKeys) {
    RGWListResult result_struct = {nullptr, 0, 0, nullptr};

    // Test with max_keys = 0
    int result = rgw_list_objects(null_driver, null_dpp, nullptr, "bucket", "", "", "", 0, &result_struct);
    EXPECT_LT(result, 0);
    rgw_free_list_result(&result_struct);

    // Test with max_keys = UINT32_MAX
    result = rgw_list_objects(null_driver, null_dpp, nullptr, "bucket", "", "", "", UINT32_MAX, &result_struct);
    EXPECT_LT(result, 0);
    rgw_free_list_result(&result_struct);
}

TEST_F(BoundaryTest, MultipartPartNumberBoundary) {
    const uint8_t data[] = "test";
    char etag[64];
    memset(etag, 0, sizeof(etag));

    // Part numbers are 1-10000 in S3, test boundaries
    int result;

    // Part number 0 (invalid per S3 spec)
    result = rgw_multipart_put_part(null_driver, null_dpp, nullptr, "bucket", "key",
                                    "upload-id", 0, data, sizeof(data),
                                    etag, sizeof(etag));
    EXPECT_LT(result, 0);

    // Part number 1 (valid minimum)
    result = rgw_multipart_put_part(null_driver, null_dpp, nullptr, "bucket", "key",
                                    "upload-id", 1, data, sizeof(data),
                                    etag, sizeof(etag));
    EXPECT_LT(result, 0);

    // Part number 10000 (valid maximum)
    result = rgw_multipart_put_part(null_driver, null_dpp, nullptr, "bucket", "key",
                                    "upload-id", 10000, data, sizeof(data),
                                    etag, sizeof(etag));
    EXPECT_LT(result, 0);

    // Part number 10001 (invalid per S3 spec)
    result = rgw_multipart_put_part(null_driver, null_dpp, nullptr, "bucket", "key",
                                    "upload-id", 10001, data, sizeof(data),
                                    etag, sizeof(etag));
    EXPECT_LT(result, 0);
}

//=============================================================================
// Unicode and Special Character Tests
//
// Verify that unicode and special characters in keys are handled correctly
// at the parameter level (actual storage would require a real driver).
//=============================================================================

TEST_F(BoundaryTest, PutObjectUnicodeKey) {
    const uint8_t data[] = "test";
    int result = rgw_put_object(null_driver, null_dpp, nullptr, "bucket",
                                "données/fichier-测试.txt",
                                data, sizeof(data), "text/plain");
    // Should handle unicode in key without crashing
    EXPECT_LT(result, 0);
}

TEST_F(BoundaryTest, PutObjectSpecialCharsKey) {
    const uint8_t data[] = "test";
    int result = rgw_put_object(null_driver, null_dpp, nullptr, "bucket",
                                "path/with spaces/and+plus/file.txt",
                                data, sizeof(data), "text/plain");
    // Should handle special chars in key without crashing
    EXPECT_LT(result, 0);
}

TEST_F(BoundaryTest, ListObjectsUnicodePrefix) {
    RGWListResult result_struct = {nullptr, 0, 0, nullptr};

    int result = rgw_list_objects(null_driver, null_dpp, nullptr, "bucket",
                                  "données/", "", "", 1000, &result_struct);
    // Should handle unicode in prefix without crashing
    EXPECT_LT(result, 0);
    rgw_free_list_result(&result_struct);
}

//=============================================================================
// Conditional Write Tests
//
// Test various combinations of conditional write parameters
//=============================================================================

TEST_F(BoundaryTest, PutObjectConditionalEmptyEtag) {
    const uint8_t data[] = "test";
    int canceled = 0;

    // Empty string for if_match (different from NULL)
    int result = rgw_put_object_conditional(null_driver, null_dpp, nullptr,
                                            "bucket", "key",
                                            data, sizeof(data), "text/plain",
                                            "", nullptr, &canceled);
    EXPECT_LT(result, 0);
}

TEST_F(BoundaryTest, PutObjectConditionalLongEtag) {
    const uint8_t data[] = "test";
    int canceled = 0;

    // Very long etag string
    std::string long_etag(256, 'a');
    int result = rgw_put_object_conditional(null_driver, null_dpp, nullptr,
                                            "bucket", "key",
                                            data, sizeof(data), "text/plain",
                                            long_etag.c_str(), nullptr, &canceled);
    EXPECT_LT(result, 0);
}

TEST_F(BoundaryTest, CopyObjectConditionalSameSrcDst) {
    // Copy to same location with conditional
    int result = rgw_copy_object_conditional(null_driver, null_dpp, nullptr,
                                             "bucket", "key",
                                             "bucket", "key",
                                             nullptr, "*");
    EXPECT_LT(result, 0);
}

TEST_F(BoundaryTest, DeleteObjectsLargeCount) {
    // Test with a large number of keys
    std::vector<const char*> keys;
    std::vector<std::string> key_storage;
    for (int i = 0; i < 1000; i++) {
        key_storage.push_back("key" + std::to_string(i));
    }
    for (const auto& k : key_storage) {
        keys.push_back(k.c_str());
    }

    int result = rgw_delete_objects(null_driver, null_dpp, nullptr,
                                    "bucket", keys.data(), keys.size());
    EXPECT_LT(result, 0);
}

TEST_F(BoundaryTest, DeleteObjectsSingleKey) {
    const char* keys[] = {"single-key"};
    int result = rgw_delete_objects(null_driver, null_dpp, nullptr, "bucket", keys, 1);
    EXPECT_LT(result, 0);
}

} // namespace
