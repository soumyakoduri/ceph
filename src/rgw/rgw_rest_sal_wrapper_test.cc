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
 * Implementation of REST endpoint for testing the SAL wrapper.
 */

#include "rgw_rest_sal_wrapper_test.h"
#include "rgw_sal_wrapper.h"
#include "rgw_process_env.h"
#include "rgw_rest_s3.h"
#include "common/errno.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <random>
#include <sstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace {

// Limits to prevent abuse
static constexpr int MAX_TEST_ITERATIONS = 1000;
static constexpr size_t MAX_TEST_OBJECT_SIZE = 64 * 1024 * 1024; // 64MB

// Test configuration from request
struct TestConfig {
  std::string test_type = "all";  // "all", "put_get", "list", "copy"
  int iterations = 10;
  size_t object_size = 1024;

  void decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("test", test_type, obj);
    JSONDecoder::decode_json("iterations", iterations, obj);
    int64_t size = object_size;
    JSONDecoder::decode_json("object_size", size, obj);
    object_size = static_cast<size_t>(size);

    // Clamp to safe bounds
    if (iterations < 1) iterations = 1;
    if (iterations > MAX_TEST_ITERATIONS) iterations = MAX_TEST_ITERATIONS;
    if (object_size > MAX_TEST_OBJECT_SIZE) object_size = MAX_TEST_OBJECT_SIZE;
  }
};

// Single test result
struct TestResult {
  std::string name;
  bool passed;
  std::string error;
  double duration_ms;

  void dump(Formatter* f) const {
    f->open_object_section("test");
    encode_json("name", name, f);
    encode_json("passed", passed, f);
    if (!passed) {
      encode_json("error", error, f);
    }
    f->dump_float("duration_ms", duration_ms);
    f->close_section();
  }
};

// Overall test results
struct TestResults {
  bool success = true;
  int tests_run = 0;
  int tests_passed = 0;
  int tests_failed = 0;
  std::vector<TestResult> results;

  void add_result(const TestResult& r) {
    results.push_back(r);
    tests_run++;
    if (r.passed) {
      tests_passed++;
    } else {
      tests_failed++;
      success = false;
    }
  }

  void dump(Formatter* f) const {
    f->open_object_section("results");
    encode_json("success", success, f);
    encode_json("tests_run", tests_run, f);
    encode_json("tests_passed", tests_passed, f);
    encode_json("tests_failed", tests_failed, f);
    f->open_array_section("details");
    for (const auto& r : results) {
      r.dump(f);
    }
    f->close_section();
    f->close_section();
  }
};

// Helper to generate random data
std::vector<uint8_t> generate_random_data(size_t size) {
  std::vector<uint8_t> data(size);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(0, 255);

  for (size_t i = 0; i < size; i++) {
    data[i] = static_cast<uint8_t>(distrib(gen));
  }
  return data;
}

// Helper to generate unique key under a dedicated test namespace
// Uses __sal_test__/ prefix to avoid collisions with real objects
std::string generate_unique_key(const std::string& prefix) {
  static std::atomic<int> counter{0};
  return "__sal_test__/" + prefix + "_" + std::to_string(counter++) + "_" +
         std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
}

// Test runner class
class SALWrapperTester {
  void* driver_;
  const void* dpp_;
  const char* bucket_;
  TestResults& results_;

public:
  SALWrapperTester(void* driver, const void* dpp, const char* bucket, TestResults& results)
    : driver_(driver), dpp_(dpp), bucket_(bucket), results_(results) {}

  void run_all_tests(const TestConfig& config) {
    run_put_get_tests(config);
    run_delete_tests(config);
    run_head_tests(config);
    run_list_tests(config);
    run_copy_tests(config);
    run_range_read_tests(config);
  }

  void run_put_get_tests(const TestConfig& config) {
    TestResult result;
    result.name = "put_get_basic";

    auto start = std::chrono::high_resolution_clock::now();

    try {
      for (int i = 0; i < config.iterations; i++) {
        std::string key = generate_unique_key("test_put_get");
        auto data = generate_random_data(config.object_size);

        // Put object
        int ret = rgw_put_object(driver_, dpp_, nullptr, bucket_, key.c_str(),
                                 data.data(), data.size(), "application/octet-stream");
        if (ret != 0) {
          result.passed = false;
          result.error = "Put failed with errno: " + std::to_string(ret);
          break;
        }

        // Get object
        RGWBuffer buffer = {nullptr, 0, 0};
        ret = rgw_get_object(driver_, dpp_, nullptr, bucket_, key.c_str(),
                             0, UINT64_MAX, &buffer);
        if (ret != 0) {
          result.passed = false;
          result.error = "Get failed with errno: " + std::to_string(ret);
          rgw_free_buffer(&buffer);
          break;
        }

        // Verify content
        if (buffer.len != data.size() ||
            memcmp(buffer.data, data.data(), data.size()) != 0) {
          result.passed = false;
          result.error = "Data mismatch: expected " + std::to_string(data.size()) +
                        " bytes, got " + std::to_string(buffer.len);
          rgw_free_buffer(&buffer);
          break;
        }

        rgw_free_buffer(&buffer);

        // Cleanup
        rgw_delete_object(driver_, dpp_, nullptr, bucket_, key.c_str());

        result.passed = true;
      }
    } catch (const std::exception& e) {
      result.passed = false;
      result.error = std::string("Exception: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    results_.add_result(result);
  }

  void run_delete_tests(const TestConfig& config) {
    TestResult result;
    result.name = "delete_basic";

    auto start = std::chrono::high_resolution_clock::now();

    try {
      std::string key = generate_unique_key("test_delete");
      auto data = generate_random_data(100);

      // Create object
      int ret = rgw_put_object(driver_, dpp_, nullptr, bucket_, key.c_str(),
                               data.data(), data.size(), "text/plain");
      if (ret != 0) {
        result.passed = false;
        result.error = "Put failed: " + std::to_string(ret);
      } else {
        // Delete object
        ret = rgw_delete_object(driver_, dpp_, nullptr, bucket_, key.c_str());
        if (ret != 0) {
          result.passed = false;
          result.error = "Delete failed: " + std::to_string(ret);
        } else {
          // Verify deleted
          RGWObjectMeta meta = {0, nullptr, nullptr, 0};
          ret = rgw_head_object(driver_, dpp_, nullptr, bucket_, key.c_str(), &meta);
          rgw_free_object_meta(&meta);

          if (ret != -ENOENT) {
            result.passed = false;
            result.error = "Object still exists after delete";
          } else {
            result.passed = true;
          }
        }
      }
    } catch (const std::exception& e) {
      result.passed = false;
      result.error = std::string("Exception: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    results_.add_result(result);
  }

  void run_head_tests(const TestConfig& config) {
    TestResult result;
    result.name = "head_basic";

    auto start = std::chrono::high_resolution_clock::now();

    try {
      std::string key = generate_unique_key("test_head");
      auto data = generate_random_data(config.object_size);

      // Create object
      int ret = rgw_put_object(driver_, dpp_, nullptr, bucket_, key.c_str(),
                               data.data(), data.size(), "application/json");
      if (ret != 0) {
        result.passed = false;
        result.error = "Put failed: " + std::to_string(ret);
      } else {
        // Head object
        RGWObjectMeta meta = {0, nullptr, nullptr, 0};
        ret = rgw_head_object(driver_, dpp_, nullptr, bucket_, key.c_str(), &meta);

        if (ret != 0) {
          result.passed = false;
          result.error = "Head failed: " + std::to_string(ret);
        } else if (meta.size != data.size()) {
          result.passed = false;
          result.error = "Size mismatch: expected " + std::to_string(data.size()) +
                        ", got " + std::to_string(meta.size);
        } else {
          result.passed = true;
        }

        rgw_free_object_meta(&meta);
        rgw_delete_object(driver_, dpp_, nullptr, bucket_, key.c_str());
      }
    } catch (const std::exception& e) {
      result.passed = false;
      result.error = std::string("Exception: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    results_.add_result(result);
  }

  void run_list_tests(const TestConfig& config) {
    TestResult result;
    result.name = "list_basic";

    auto start = std::chrono::high_resolution_clock::now();

    try {
      std::string prefix = generate_unique_key("list_test");
      std::vector<std::string> keys;

      // Create multiple objects
      for (int i = 0; i < 5; i++) {
        std::string key = prefix + "/obj_" + std::to_string(i);
        keys.push_back(key);
        auto data = generate_random_data(100);
        rgw_put_object(driver_, dpp_, nullptr, bucket_, key.c_str(),
                       data.data(), data.size(), "text/plain");
      }

      // List objects
      RGWListResult list_result = {nullptr, 0, 0, nullptr};
      std::string list_prefix = prefix + "/";
      int ret = rgw_list_objects(driver_, dpp_, nullptr, bucket_,
                                  list_prefix.c_str(), "", "", 100, &list_result);

      if (ret != 0) {
        result.passed = false;
        result.error = "List failed: " + std::to_string(ret);
      } else if (list_result.count != 5) {
        result.passed = false;
        result.error = "Expected 5 objects, got " + std::to_string(list_result.count);
      } else {
        result.passed = true;
      }

      rgw_free_list_result(&list_result);

      // Cleanup
      for (const auto& key : keys) {
        rgw_delete_object(driver_, dpp_, nullptr, bucket_, key.c_str());
      }
    } catch (const std::exception& e) {
      result.passed = false;
      result.error = std::string("Exception: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    results_.add_result(result);
  }

  void run_copy_tests(const TestConfig& config) {
    TestResult result;
    result.name = "copy_basic";

    auto start = std::chrono::high_resolution_clock::now();

    try {
      std::string src_key = generate_unique_key("copy_src");
      std::string dst_key = generate_unique_key("copy_dst");
      auto data = generate_random_data(config.object_size);

      // Create source
      int ret = rgw_put_object(driver_, dpp_, nullptr, bucket_, src_key.c_str(),
                               data.data(), data.size(), "application/octet-stream");
      if (ret != 0) {
        result.passed = false;
        result.error = "Put source failed: " + std::to_string(ret);
      } else {
        // Copy
        ret = rgw_copy_object(driver_, dpp_, nullptr, bucket_, src_key.c_str(),
                              bucket_, dst_key.c_str());
        if (ret != 0) {
          result.passed = false;
          result.error = "Copy failed: " + std::to_string(ret);
        } else {
          // Verify destination
          RGWBuffer buffer = {nullptr, 0, 0};
          ret = rgw_get_object(driver_, dpp_, nullptr, bucket_, dst_key.c_str(),
                               0, UINT64_MAX, &buffer);

          if (ret != 0) {
            result.passed = false;
            result.error = "Get destination failed: " + std::to_string(ret);
          } else if (buffer.len != data.size() ||
                     memcmp(buffer.data, data.data(), data.size()) != 0) {
            result.passed = false;
            result.error = "Copied data mismatch";
          } else {
            result.passed = true;
          }

          rgw_free_buffer(&buffer);
        }

        // Cleanup
        rgw_delete_object(driver_, dpp_, nullptr, bucket_, src_key.c_str());
        rgw_delete_object(driver_, dpp_, nullptr, bucket_, dst_key.c_str());
      }
    } catch (const std::exception& e) {
      result.passed = false;
      result.error = std::string("Exception: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    results_.add_result(result);
  }

  void run_range_read_tests(const TestConfig& config) {
    TestResult result;
    result.name = "range_read";

    auto start = std::chrono::high_resolution_clock::now();

    try {
      std::string key = generate_unique_key("range_test");
      auto data = generate_random_data(10 * 1024);  // 10KB

      // Create object
      int ret = rgw_put_object(driver_, dpp_, nullptr, bucket_, key.c_str(),
                               data.data(), data.size(), "application/octet-stream");
      if (ret != 0) {
        result.passed = false;
        result.error = "Put failed: " + std::to_string(ret);
      } else {
        // Read range [1024, 3072)
        RGWBuffer buffer = {nullptr, 0, 0};
        ret = rgw_get_object(driver_, dpp_, nullptr, bucket_, key.c_str(),
                             1024, 2048, &buffer);

        if (ret != 0) {
          result.passed = false;
          result.error = "Range read failed: " + std::to_string(ret);
        } else if (buffer.len != 2048) {
          result.passed = false;
          result.error = "Range read size mismatch: expected 2048, got " +
                        std::to_string(buffer.len);
        } else if (memcmp(buffer.data, data.data() + 1024, 2048) != 0) {
          result.passed = false;
          result.error = "Range read data mismatch";
        } else {
          result.passed = true;
        }

        rgw_free_buffer(&buffer);
        rgw_delete_object(driver_, dpp_, nullptr, bucket_, key.c_str());
      }
    } catch (const std::exception& e) {
      result.passed = false;
      result.error = std::string("Exception: ") + e.what();
    }

    auto end = std::chrono::high_resolution_clock::now();
    result.duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    results_.add_result(result);
  }
};

} // anonymous namespace

//=============================================================================
// RGWSALWrapperTestInfo implementation (declared in header)
//=============================================================================

void RGWSALWrapperTestInfo::execute(optional_yield y) {
  op_ret = 0;
}

void RGWSALWrapperTestInfo::send_response() {
  dump_errno(s);
  end_header(s, this, "application/json");

  dump_start(s);
  Formatter* f = s->formatter;

  f->open_object_section("sal_wrapper_test");
  encode_json("version", "1.0", f);
  encode_json("description", "SAL Wrapper Test Endpoint", f);

  f->open_object_section("usage");
  encode_json("method", "POST", f);
  encode_json("path", "/{bucket}?sal-wrapper-test", f);
  f->close_section();

  f->open_object_section("config_options");
  encode_json("test", "all | put_get | list | copy | multipart", f);
  encode_json("iterations", "number of iterations (default: 10)", f);
  encode_json("object_size", "size of test objects in bytes (default: 1024)", f);
  f->close_section();

  encode_json("note", "Requires admin privileges", f);

  f->close_section();

  rgw_flush_formatter_and_reset(s, f);
}

//=============================================================================
// RGWSALWrapperTest implementation (declared in header)
//=============================================================================

// Store implementation details in a pimpl-like structure
struct RGWSALWrapperTestImpl {
  bufferlist in_data;
  TestConfig config;
  TestResults results;
};

RGWSALWrapperTest::RGWSALWrapperTest() : impl_(new RGWSALWrapperTestImpl()) {}
RGWSALWrapperTest::~RGWSALWrapperTest() { delete impl_; }

int RGWSALWrapperTest::verify_permission(optional_yield y) {
  // Only allow admin/system users to run SAL wrapper tests
  // since tests write/delete objects in the bucket
  if (!s->auth.identity->is_admin()) {
    ldpp_dout(this, 1) << "ERROR: SAL wrapper test requires admin privileges" << dendl;
    return -EACCES;
  }
  return 0;
}

void RGWSALWrapperTest::pre_exec() {}

const char* RGWSALWrapperTest::name() const { return "sal_wrapper_test"; }
RGWOpType RGWSALWrapperTest::get_type() { return RGW_OP_UNKNOWN; }
uint32_t RGWSALWrapperTest::op_mask() { return RGW_OP_TYPE_WRITE; }

int RGWSALWrapperTest::init_processing(optional_yield y) {
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int ret = 0;
  std::tie(ret, impl_->in_data) = read_all_input(s, max_size, false);
  if (ret < 0) {
    return ret;
  }

  if (impl_->in_data.length() > 0) {
    JSONParser parser;
    if (parser.parse(impl_->in_data.c_str(), impl_->in_data.length())) {
      try {
        impl_->config.decode_json(&parser);
      } catch (const JSONDecoder::err& e) {
        ldpp_dout(this, 1) << "ERROR: failed to parse test config: " << e.what() << dendl;
      }
    }
  }

  return 0;
}

void RGWSALWrapperTest::execute(optional_yield y) {
  if (!s->bucket) {
    op_ret = -EINVAL;
    ldpp_dout(this, 1) << "ERROR: bucket required for SAL wrapper tests" << dendl;
    return;
  }

  ldpp_dout(this, 10) << "Running SAL wrapper tests on bucket: "
                      << s->bucket->get_name() << dendl;

  SALWrapperTester tester(
    driver,
    this,
    s->bucket->get_name().c_str(),
    impl_->results
  );

  if (impl_->config.test_type == "all") {
    tester.run_all_tests(impl_->config);
  } else if (impl_->config.test_type == "put_get") {
    tester.run_put_get_tests(impl_->config);
  } else if (impl_->config.test_type == "list") {
    tester.run_list_tests(impl_->config);
  } else if (impl_->config.test_type == "copy") {
    tester.run_copy_tests(impl_->config);
  } else {
    tester.run_all_tests(impl_->config);
  }

  op_ret = impl_->results.success ? 0 : -EIO;
}

void RGWSALWrapperTest::send_response() {
  if (op_ret < 0 && impl_->results.tests_run == 0) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, "application/json");

  dump_start(s);
  impl_->results.dump(s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

