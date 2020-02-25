// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal.h"

#include "store/dbstore/dbstore_mgr.h"

namespace rgw { namespace sal {

class RGWDBStoreManager : public RGWStore {
private:
    class DBstoreManager *dbsm;
public:
    RGWDBStoreManager(): dbsm(nullptr) {}
    ~RGWDBStoreManager() { delete dbsm; }

    void finalize(void) override;
};

class RGWStoreManager {
public:
  RGWStoreManager() {}
  static rgw::sal::RGWRadosStore *get_storage(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads,
			       bool run_sync_thread, bool run_reshard_thread, bool use_cache = true) {
    rgw::sal::RGWRadosStore *store = init_storage_provider(cct, use_gc_thread, use_lc_thread,
	quota_threads, run_sync_thread, run_reshard_thread, use_cache);
    return store;
  }
  static rgw::sal::RGWRadosStore *get_raw_storage(CephContext *cct) {
    rgw::sal::RGWRadosStore *rados = init_raw_storage_provider(cct);
    return rados;
  }
  static rgw::sal::RGWRadosStore *init_storage_provider(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_metadata_cache);
  static rgw::sal::RGWRadosStore *init_raw_storage_provider(CephContext *cct);
  static void close_storage(rgw::sal::RGWRadosStore *store);
};

} } // namespace rgw::sal
