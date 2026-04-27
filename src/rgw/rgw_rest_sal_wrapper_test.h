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
 * REST endpoint for testing the SAL wrapper.
 * This allows testing the SAL wrapper from within RGW with real driver/dpp.
 *
 * Usage:
 *   POST /{bucket}?sal-wrapper-test
 *
 *   Request body (JSON):
 *   {
 *     "test": "all" | "put_get" | "list" | "copy" | "multipart",
 *     "iterations": 10,
 *     "object_size": 1024
 *   }
 *
 *   Response (JSON):
 *   {
 *     "success": true,
 *     "tests_run": 5,
 *     "tests_passed": 5,
 *     "tests_failed": 0,
 *     "results": [...]
 *   }
 */

#pragma once

#include "rgw_op.h"
#include "rgw_rest.h"

// Forward declaration
struct RGWSALWrapperTestImpl;

// SAL wrapper test operation
class RGWSALWrapperTest : public RGWOp {
  RGWSALWrapperTestImpl* impl_;

public:
  RGWSALWrapperTest();
  ~RGWSALWrapperTest() override;

  int verify_permission(optional_yield y) override;
  void pre_exec() override;
  const char* name() const override;
  RGWOpType get_type() override;
  uint32_t op_mask() override;
  int init_processing(optional_yield y) override;
  void execute(optional_yield y) override;
  void send_response() override;
};

class RGWHandler_REST_SALWrapperTest : public RGWHandler_REST {
public:
  RGWHandler_REST_SALWrapperTest() = default;
  ~RGWHandler_REST_SALWrapperTest() override = default;

  int init_permissions(RGWOp* op, optional_yield y) override { return 0; }
  int read_permissions(RGWOp* op, optional_yield y) override { return 0; }
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
  int postauth_init(optional_yield y) override { return 0; }

  RGWOp* op_post() override;
  RGWOp* op_get() override;
};

RGWHandler_REST* make_sal_wrapper_test_handler(rgw::sal::Driver* driver,
                                                req_state* s,
                                                const rgw::auth::StrategyRegistry& auth_registry);
