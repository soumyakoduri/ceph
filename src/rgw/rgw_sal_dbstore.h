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

  class RGWDBStore;

  class RGWDBStore : public RGWStore {
    private:
      /* DBStoreManager is used in case multiple
       * connections are needed one for each tenant.
       */
      DBStoreManager *dbsm;
      /* default dbstore (single connection). If needed
       * multiple db handles (for eg., one for each tenant),
       * use dbsm->getDBStore(tenant) */
      DBStore *dbstore;
      string luarocks_path;

    public:
      RGWDBStore(): dbsm(nullptr) {}
      ~RGWDBStore() { delete dbsm; }

      virtual std::unique_ptr<RGWUser> get_user(const rgw_user& u) override;
      virtual int get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<RGWUser>* user) override;
      virtual int get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<RGWUser>* user) override;
      virtual int get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<RGWUser>* user) override;
      virtual std::unique_ptr<RGWObject> get_object(const rgw_obj_key& k) override;
      virtual int get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const rgw_bucket& b, std::unique_ptr<RGWBucket>* bucket, optional_yield y) override;
      virtual int get_bucket(RGWUser* u, const RGWBucketInfo& i, std::unique_ptr<RGWBucket>* bucket) override;
      virtual int get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const std::string& tenant, const std::string&name, std::unique_ptr<RGWBucket>* bucket, optional_yield y) override;
      virtual int create_bucket(const DoutPrefixProvider *dpp, 
                                RGWUser& u, const rgw_bucket& b,
                                const std::string& zonegroup_id,
                                rgw_placement_rule& placement_rule,
                                std::string& swift_ver_location,
                                const RGWQuotaInfo * pquota_info,
                                const RGWAccessControlPolicy& policy,
                                RGWAttrs& attrs,
                                 RGWBucketInfo& info,
                                 obj_version& ep_objv,
                                 bool exclusive,
                                 bool obj_lock_enabled,
                                 bool *existed,
                                 req_info& req_info,
                                 std::unique_ptr<RGWBucket>* bucket,
                                 optional_yield y) override;
      virtual bool is_meta_master() override;
      virtual int forward_request_to_master(RGWUser* user, obj_version *objv,
                          bufferlist& in_data, JSONParser *jp, req_info& info,
                          optional_yield y) override;
      virtual int defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, RGWBucket* bucket, RGWObject* obj,
                           optional_yield y) override;
      virtual Zone* get_zone() { return NULL; }
      virtual std::string zone_unique_id(uint64_t unique_num) override;
      virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
      virtual int cluster_stat(RGWClusterStat& stats) override;
      virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
      virtual std::unique_ptr<Completions> get_completions(void) override;
      virtual std::unique_ptr<Notification> get_notification(rgw::sal::RGWObject* obj, struct req_state* s, rgw::notify::EventType event_type) override;
      virtual std::unique_ptr<GCChain> get_gc_chain(rgw::sal::RGWObject* obj) override;
      virtual std::unique_ptr<Writer> get_writer(Aio *aio, rgw::sal::RGWBucket* bucket,
                                                 RGWObjectCtx& obj_ctx, std::unique_ptr<rgw::sal::RGWObject> _head_obj,
                                                 const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual RGWLC* get_rgwlc(void) override { return NULL; }
      virtual RGWCtl* get_ctl(void) override { return NULL; }
      virtual RGWCoroutinesManagerRegistry* get_cr_registry() override { return NULL; }
      virtual int delete_raw_obj(const rgw_raw_obj& obj) override;
      virtual int delete_raw_obj_aio(const rgw_raw_obj& obj, Completions* aio) override;
      virtual void get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj) override;
      virtual int get_raw_chunk_size(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, uint64_t* chunk_size) override;

      virtual int log_usage(map<rgw_user_bucket, RGWUsageBatch>& usage_info) override;
      virtual int log_op(string& oid, bufferlist& bl) override;
      virtual int register_to_service_map(const string& daemon_type,
                                          const map<string, string>& meta) override;
      virtual void get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota) override;
      virtual int list_raw_objects(const rgw_pool& pool, const string& prefix_filter,
                                   int max, RGWListRawObjsCtx& ctx, std::list<string>& oids,
                                    bool *is_truncated) override;
      virtual int set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled) override;
      virtual uint64_t get_new_req_id() override { return 0; }
      virtual int get_sync_policy_handler(const DoutPrefixProvider *dpp,
                                          std::optional<rgw_zone_id> zone,
                                          std::optional<rgw_bucket> bucket,
                                          RGWBucketSyncPolicyHandlerRef *phandler,
                                          optional_yield y) override;
      virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
      virtual void wakeup_meta_sync_shards(set<int>& shard_ids) override { return; }
      virtual void wakeup_data_sync_shards(const rgw_zone_id& source_zone, map<int, set<string> >& shard_ids) override { return; }
      virtual int clear_usage() override { return 0; }
      virtual int get_config_key_val(string name, bufferlist *bl) override;
      virtual int put_system_obj(const rgw_pool& pool, const string& oid,
                                 bufferlist& data, bool exclusive,
                                 RGWObjVersionTracker *objv_tracker, real_time set_mtime,
                                 optional_yield y, map<string, bufferlist> *pattrs = nullptr)
                                 override;
      virtual int get_system_obj(const DoutPrefixProvider *dpp,
                                 const rgw_pool& pool, const string& key,
                                 bufferlist& bl,
                                 RGWObjVersionTracker *objv_tracker, real_time *pmtime,
                                 optional_yield y, map<string, bufferlist> *pattrs = nullptr,
                                 rgw_cache_entry_info *cache_info = nullptr,
                                 boost::optional<obj_version> refresh_version = boost::none) override;
      virtual int delete_system_obj(const rgw_pool& pool, const string& oid,
                                    RGWObjVersionTracker *objv_tracker, optional_yield y) override;
      virtual int meta_list_keys_init(const string& section, const string& marker, void** phandle) override;
      virtual int meta_list_keys_next(void* handle, int max, list<string>& keys, bool* truncated) override;
      virtual void meta_list_keys_complete(void* handle) override;
      virtual std::string meta_get_marker(void *handle) override;
      virtual int meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y) override;

      virtual const RGWSyncModuleInstanceRef& get_sync_module() { return NULL; }
      virtual std::string get_host_id() { return ""; }

      virtual void finalize(void) override;

      virtual CephContext *ctx(void) override {
        return NULL;  // return dbstore->ctx();
      }

      virtual const std::string& get_luarocks_path() const override {
        return luarocks_path;
      }

      virtual void set_luarocks_path(const std::string& path) override {
        luarocks_path = path;
      }

      /* Unique to DBStore */
      void setDBStoreManager(DBStoreManager *stm) { dbsm = stm; }
      DBStoreManager *getDBStoreManager(void) { return dbsm; }

      void setDBStore(DBStore * st) { dbstore = st; }
      DBStore *getDBStore(void) { return dbstore; }

      DBStore *getDBStore(string tenant) { return dbsm->getDBStore(tenant, false); }
  };

} } // namespace rgw::sal
