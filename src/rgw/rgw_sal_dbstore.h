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

#include "store/dbstore/common/dbstore.h"
#include "store/dbstore/dbstore_mgr.h"

namespace rgw { namespace sal {

  class RGWDBStore;

  class DBUser : public User {
    private:
      RGWDBStore *store;

    public:
      DBUser(RGWDBStore *_st, const rgw_user& _u) : User(_u), store(_st) { }
      DBUser(RGWDBStore *_st, const RGWUserInfo& _i) : User(_i), store(_st) { }
      DBUser(RGWDBStore *_st) : store(_st) { }
      DBUser(DBUser& _o) = default;
      DBUser() {}

      virtual std::unique_ptr<User> clone() override {
        return std::unique_ptr<User>(new DBUser(*this));
      }
      int list_buckets(const DoutPrefixProvider *dpp, const std::string& marker, const std::string& end_marker,
	  	     uint64_t max, bool need_stats, BucketList& buckets,
		     optional_yield y) override;
      virtual Bucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time) override;
      virtual int read_attrs(const DoutPrefixProvider *dpp, optional_yield y, Attrs* uattrs, RGWObjVersionTracker* tracker) override;
      virtual int read_stats(optional_yield y, RGWStorageStats* stats,
			   ceph::real_time *last_stats_sync = nullptr,
			   ceph::real_time *last_stats_update = nullptr) override;
      virtual int read_stats_async(RGWGetUserStats_CB *cb) override;
      virtual int complete_flush_stats(optional_yield y) override;
      virtual int read_usage(uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
               bool *is_truncated, RGWUsageIter& usage_iter,
               map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_usage(uint64_t start_epoch, uint64_t end_epoch) override;

      /* Placeholders */
      virtual int load_by_id(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int store_info(const DoutPrefixProvider *dpp, optional_yield y, const RGWUserCtl::PutParams& params = {}) override;
      virtual int remove_info(const DoutPrefixProvider *dpp, optional_yield y, const RGWUserCtl::RemoveParams& params = {}) override;

      friend class DBBucket;
  };

class DBBucket : public Bucket {
  private:
    RGWDBStore *store;
    RGWAccessControlPolicy acls;

  public:
    DBBucket(RGWDBStore *_st)
      : store(_st),
        acls() {
    }

    DBBucket(RGWDBStore *_st, User* _u)
      : Bucket(_u),
	store(_st),
        acls() {
    }

    DBBucket(RGWDBStore *_st, const rgw_bucket& _b)
      : Bucket(_b),
	store(_st),
        acls() {
    }

    DBBucket(RGWDBStore *_st, const RGWBucketEnt& _e)
      : Bucket(_e),
	store(_st),
        acls() {
    }

    DBBucket(RGWDBStore *_st, const RGWBucketInfo& _i)
      : Bucket(_i),
	store(_st),
        acls() {
    }

    DBBucket(RGWDBStore *_st, const rgw_bucket& _b, User* _u)
      : Bucket(_b, _u),
	store(_st),
        acls() {
    }

    DBBucket(RGWDBStore *_st, const RGWBucketEnt& _e, User* _u)
      : Bucket(_e, _u),
	store(_st),
        acls() {
    }

    DBBucket(RGWDBStore *_st, const RGWBucketInfo& _i, User* _u)
      : Bucket(_i, _u),
	store(_st),
        acls() {
    }

    ~DBBucket() { }

    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
    virtual int list(const DoutPrefixProvider *dpp, ListParams&, int, ListResults&, optional_yield y) override;
    Object* create_object(const rgw_obj_key& key /* Attributes */) override;
    virtual int remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, std::string prefix, std::string delimiter, bool forward_to_master, req_info* req_info, optional_yield y) override;
    virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
    virtual int set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy& acl, optional_yield y) override;
    virtual int get_bucket_info(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int get_bucket_stats(int shard_id,
				 std::string *bucket_ver, std::string *master_ver,
				 std::map<RGWObjCategory, RGWStorageStats>& stats,
				 std::string *max_marker = nullptr,
				 bool *syncstopped = nullptr) override;
    virtual int get_bucket_stats_async(int shard_id, RGWGetBucketStats_CB *ctx) override;
    virtual int read_bucket_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int sync_user_stats(optional_yield y) override;
    virtual int update_container_stats(const DoutPrefixProvider *dpp) override;
    virtual int check_bucket_shards(const DoutPrefixProvider *dpp) override;
    virtual int link(const DoutPrefixProvider *dpp, User* new_user, optional_yield y, bool update_entrypoint, RGWObjVersionTracker* objv) override;
    virtual int unlink(const DoutPrefixProvider *dpp, User* new_user, optional_yield y, bool update_entrypoint = true) override;
    virtual int chown(const DoutPrefixProvider *dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker = nullptr) override;
    virtual int put_instance_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time mtime) override;
    virtual int remove_entrypoint(const DoutPrefixProvider *dpp, RGWObjVersionTracker* objv, optional_yield y) override;
    virtual int remove_instance_info(const DoutPrefixProvider *dpp, RGWObjVersionTracker* objv, optional_yield y) override;
    virtual bool is_owner(User* user) override;
    virtual int check_empty(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int check_quota(RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
    virtual int set_instance_attrs(const DoutPrefixProvider *dpp, Attrs& attrs, optional_yield y) override;
    virtual int try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime) override;
    virtual int read_usage(uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   bool *is_truncated, RGWUsageIter& usage_iter,
			   map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
    virtual int trim_usage(uint64_t start_epoch, uint64_t end_epoch) override;
    virtual int remove_objs_from_index(std::list<rgw_obj_index_key>& objs_to_unlink) override;
    virtual int check_index(std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
    virtual int rebuild_index() override;
    virtual int set_tag_timeout(uint64_t timeout) override;
    virtual int purge_instance(const DoutPrefixProvider *dpp) override;
    virtual std::unique_ptr<Bucket> clone() override {
      return std::make_unique<DBBucket>(*this);
    }

    friend class RGWDBStore;
};

  class RGWDBStore : public Store {
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

      virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
      virtual int get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) override;
      virtual int get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) override;
      virtual int get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) override;
      virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
      virtual int get_bucket(const DoutPrefixProvider *dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
      virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
      virtual int get_bucket(const DoutPrefixProvider *dpp, User* u, const std::string& tenant, const std::string&name, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
      virtual int create_bucket(const DoutPrefixProvider *dpp, 
                                User& u, const rgw_bucket& b,
                                const std::string& zonegroup_id,
                                rgw_placement_rule& placement_rule,
                                std::string& swift_ver_location,
                                const RGWQuotaInfo * pquota_info,
                                const RGWAccessControlPolicy& policy,
                                Attrs& attrs,
                                 RGWBucketInfo& info,
                                 obj_version& ep_objv,
                                 bool exclusive,
                                 bool obj_lock_enabled,
                                 bool *existed,
                                 req_info& req_info,
                                 std::unique_ptr<Bucket>* bucket,
                                 optional_yield y) override;
      virtual bool is_meta_master() override;
      virtual int forward_request_to_master(User* user, obj_version *objv,
                          bufferlist& in_data, JSONParser *jp, req_info& info,
                          optional_yield y) override;
      virtual int defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, Bucket* bucket, Object* obj,
                           optional_yield y) override;
      virtual Zone* get_zone() { return NULL; }
      virtual std::string zone_unique_id(uint64_t unique_num) override;
      virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
      virtual int cluster_stat(RGWClusterStat& stats) override;
      virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
      virtual std::unique_ptr<Completions> get_completions(void) override;
      virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, struct req_state* s, rgw::notify::EventType event_type) override;
      virtual std::unique_ptr<GCChain> get_gc_chain(rgw::sal::Object* obj) override;
      virtual std::unique_ptr<Writer> get_writer(Aio *aio, rgw::sal::Bucket* bucket,
                                                 RGWObjectCtx& obj_ctx, std::unique_ptr<rgw::sal::Object> _head_obj,
                                                 const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual RGWLC* get_rgwlc(void) override { return NULL; }
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
      virtual int read_all_usage(uint64_t start_epoch, uint64_t end_epoch,
                   uint32_t max_entries, bool *is_truncated,
                   RGWUsageIter& usage_iter,
                   map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_all_usage(uint64_t start_epoch, uint64_t end_epoch) override;
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
        return dbstore->ctx();
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
