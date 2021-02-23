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

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "common/Clock.h"
#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_dbstore.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

  int RGWDBUser::list_buckets(const DoutPrefixProvider *dpp, const string& marker,
			       const string& end_marker, uint64_t max, bool need_stats,
			       RGWBucketList &buckets, optional_yield y)
  {
    return 0;
  }

  RGWBucket* RGWDBUser::create_bucket(rgw_bucket& bucket,
        				       ceph::real_time creation_time)
  {
    return NULL;
  }

  int RGWDBUser::read_attrs(const DoutPrefixProvider *dpp, optional_yield y, RGWAttrs* uattrs, RGWObjVersionTracker* tracker)
  {
    return 0;
  }

  int RGWDBUser::read_stats(optional_yield y, RGWStorageStats* stats,
  			     ceph::real_time *last_stats_sync,
			     ceph::real_time *last_stats_update)
  {
    return 0;
  }

  int RGWDBUser::read_stats_async(RGWGetUserStats_CB *cb)
  {
    return 0;
  }

  int RGWDBUser::complete_flush_stats(optional_yield y)
  {
    return 0;
  }

  int RGWDBUser::read_usage(uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
               bool *is_truncated, RGWUsageIter& usage_iter,
               map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int RGWDBUser::trim_usage(uint64_t start_epoch, uint64_t end_epoch)
  {
    return 0;
  }

  int RGWDBUser::load_by_id(const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int RGWDBUser::store_info(const DoutPrefixProvider *dpp, optional_yield y, const RGWUserCtl::PutParams& params)
  {
    return 0;
  }

  int RGWDBUser::remove_info(const DoutPrefixProvider *dpp, optional_yield y, const RGWUserCtl::RemoveParams& params)
  {
    return 0;
  }

  void RGWDBStore::finalize(void)
  {
    if (dbsm)
      dbsm->destroyAllHandles();
  }

  std::unique_ptr<RGWUser> RGWDBStore::get_user(const rgw_user &u)
  {
    // return std::unique_ptr<RGWUser>(new RGWDBUser(this, u));
    return NULL;
  }

  int RGWDBStore::get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<RGWUser>* user)
  {
    return 0;
  }

  int RGWDBStore::get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<RGWUser>* user)
  {
    return 0;
  }

  int RGWDBStore::get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<RGWUser>* user)
  {
    return 0;
  }

  std::unique_ptr<RGWObject> RGWDBStore::get_object(const rgw_obj_key& k)
  {
    // return std::unique_ptr<RGWObject>(new RGWDBObject(this, k));
    return NULL;
  }


  int RGWDBStore::get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const rgw_bucket& b, std::unique_ptr<RGWBucket>* bucket, optional_yield y)
  {
    return 0;
  }

  int RGWDBStore::get_bucket(RGWUser* u, const RGWBucketInfo& i, std::unique_ptr<RGWBucket>* bucket)
  {
    return 0;
  }

  int RGWDBStore::get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const std::string& tenant, const std::string& name, std::unique_ptr<RGWBucket>* bucket, optional_yield y)
  {
    return 0;
  }

  int RGWDBStore::create_bucket(const DoutPrefixProvider *dpp,
                                RGWUser& u, const rgw_bucket& b,
                                const string& zonegroup_id,
                                rgw_placement_rule& placement_rule,
                                string& swift_ver_location,
                                const RGWQuotaInfo * pquota_info,
                                const RGWAccessControlPolicy& policy,
                                RGWAttrs& attrs,
                                RGWBucketInfo& info,
                                obj_version& ep_objv,
                                bool exclusive,
                                bool obj_lock_enabled,
                                bool *existed,
                                req_info& req_info,
                                std::unique_ptr<RGWBucket>* bucket_out,
                                optional_yield y)
  {
    return 0;
  }

  bool RGWDBStore::is_meta_master()
  {
    // return svc()->zone->is_meta_master();
    return 0;
  }

  int RGWDBStore::forward_request_to_master(RGWUser* user, obj_version *objv,
                                            bufferlist& in_data,
                                            JSONParser *jp, req_info& info,
                                            optional_yield y)
  {
    return 0;
  }

  int RGWDBStore::defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, RGWBucket* bucket, RGWObject* obj, optional_yield y)
  {
    return 0;
  }

  std::string RGWDBStore::zone_unique_id(uint64_t unique_num)
  {
    //  return svc()->zone->zone_id();
    return "";
  }

  std::string RGWDBStore::zone_unique_trans_id(const uint64_t unique_num)
  {
    //  return svc()->zone->get_realm();
    return "";
  }

  int RGWDBStore::cluster_stat(RGWClusterStat& stats)
  {
    return 0;
  }

  std::unique_ptr<Lifecycle> RGWDBStore::get_lifecycle(void)
  {
    //  return std::unique_ptr<Lifecycle>(new DBLifecycle(this));
    return 0;
  }

  std::unique_ptr<Completions> RGWDBStore::get_completions(void)
  {
    //  return std::unique_ptr<Completions>(new DBCompletions());
    return 0;
  }

  std::unique_ptr<Notification> RGWDBStore::get_notification(rgw::sal::RGWObject* obj,
                                                             struct req_state* s,
                                                             rgw::notify::EventType event_type)
  {
    //  return std::unique_ptr<Reservation>(new DBReservation(this, obj, s, event_type));
    return 0;
  }

  std::unique_ptr<GCChain> RGWDBStore::get_gc_chain(rgw::sal::RGWObject* obj)
  {
    //  return std::unique_ptr<GCChain>(new DBGCChain(this, obj));
    return 0;
  }

  std::unique_ptr<Writer> RGWDBStore::get_writer(Aio *aio, rgw::sal::RGWBucket* bucket,
                                                 RGWObjectCtx& obj_ctx, std::unique_ptr<rgw::sal::RGWObject> _head_obj,
                                                 const DoutPrefixProvider *dpp, optional_yield y)
  {
    //  return std::unique_ptr<Writer>(new DBWriter(aio, this, bucket, obj_ctx, std::move(_head_obj), dpp, y));
    return 0;
  }

  int RGWDBStore::delete_raw_obj(const rgw_raw_obj& obj)
  {
    //  return rados->delete_raw_obj(obj);
    return 0;
  }

  int RGWDBStore::delete_raw_obj_aio(const rgw_raw_obj& obj, Completions* aio)
  {
    /* DBCompletions *raio = static_cast<DBCompletions*>(aio);

       return rados->delete_raw_obj_aio(obj, raio->handles);*/
    return 0;
  }

  void RGWDBStore::get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj)
  {
    //    rados->obj_to_raw(placement_rule, obj, raw_obj);
    return;
  }

  int RGWDBStore::get_raw_chunk_size(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, uint64_t* chunk_size)
  {
    //  return rados->get_max_chunk_size(obj.pool, chunk_size);
    return 0;
  }

  int RGWDBStore::log_usage(map<rgw_user_bucket, RGWUsageBatch>& usage_info)
  {
    //    return rados->log_usage(usage_info);
    return 0;
  }

  int RGWDBStore::log_op(string& oid, bufferlist& bl)
  {
    return 0;
  }

  int RGWDBStore::register_to_service_map(const string& daemon_type,
      const map<string, string>& meta)
  {
    //  return rados->register_to_service_map(daemon_type, meta);
    return 0;
  }

  void RGWDBStore::get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota)
  {
    /*  bucket_quota = svc()->quota->get_bucket_quota();
        user_quota = svc()->quota->get_user_quota();*/
    return;
  }

  int RGWDBStore::list_raw_objects(const rgw_pool& pool, const string& prefix_filter,
                                   int max, RGWListRawObjsCtx& ctx, list<string>& oids,
                                   bool *is_truncated)
  {
    //    return rados->list_raw_objects(pool, prefix_filter, max, ctx, oids, is_truncated);
    return 0;
  }

  int RGWDBStore::set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled)
  {
    //    return rados->set_buckets_enabled(buckets, enabled);
    return 0;
  }

  int RGWDBStore::get_sync_policy_handler(const DoutPrefixProvider *dpp,
                                          std::optional<rgw_zone_id> zone,
                                          std::optional<rgw_bucket> bucket,
                                          RGWBucketSyncPolicyHandlerRef *phandler,
                                          optional_yield y)
  {
    //  return ctl()->bucket->get_sync_policy_handler(zone, bucket, phandler, y);
    return 0;
  }

  RGWDataSyncStatusManager* RGWDBStore::get_data_sync_manager(const rgw_zone_id& source_zone)
  {
    //  return rados->get_data_sync_manager(source_zone);
    return 0;
  }

  int RGWDBStore::read_all_usage(uint64_t start_epoch, uint64_t end_epoch, 
                   uint32_t max_entries, bool *is_truncated,
                   RGWUsageIter& usage_iter,
                   map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int RGWDBStore::trim_all_usage(uint64_t start_epoch, uint64_t end_epoch)
  {
    return 0;
  }

  int RGWDBStore::get_config_key_val(string name, bufferlist *bl)
  {
    //  return svc()->config_key->get(name, true, bl);
    return 0;
  }

  int RGWDBStore::put_system_obj(const rgw_pool& pool, const string& oid,
                                 bufferlist& data, bool exclusive,
                                 RGWObjVersionTracker *objv_tracker, real_time set_mtime,
                                 optional_yield y, map<string, bufferlist> *pattrs)
  {
    return 0;
    /*  auto obj_ctx = svc()->sysobj->init_obj_ctx();
        return rgw_put_system_obj(obj_ctx, pool, oid, data, exclusive, objv_tracker, set_mtime, y, pattrs);*/
  }

  int RGWDBStore::get_system_obj(const DoutPrefixProvider *dpp,
                                 const rgw_pool& pool, const string& key,
                                 bufferlist& bl,
                                 RGWObjVersionTracker *objv_tracker, real_time *pmtime,
                                 optional_yield y, map<string, bufferlist> *pattrs,
                                 rgw_cache_entry_info *cache_info,
                                 boost::optional<obj_version> refresh_version)
  {
    return 0;
    /*  auto obj_ctx = svc()->sysobj->init_obj_ctx();
        return rgw_get_system_obj(obj_ctx, pool, key, bl, objv_tracker, pmtime, y, pattrs, cache_info, refresh_version);*/
  }

  int RGWDBStore::delete_system_obj(const rgw_pool& pool, const string& oid,
                                    RGWObjVersionTracker *objv_tracker, optional_yield y)
  {
    return 0;
    //  return rgw_delete_system_obj(svc()->sysobj, pool, oid, objv_tracker, y);
  }

  int RGWDBStore::meta_list_keys_init(const string& section, const string& marker, void** phandle)
  {
    return 0; //ctl()->meta.mgr->list_keys_init(section, marker, phandle);
  }

  int RGWDBStore::meta_list_keys_next(void* handle, int max, list<string>& keys, bool* truncated)
  {
    return 0; //ctl()->meta.mgr->list_keys_next(handle, max, keys, truncated);
  }

  void RGWDBStore::meta_list_keys_complete(void* handle)
  {
    return; //  ctl()->meta.mgr->list_keys_complete(handle);
  }

  std::string RGWDBStore::meta_get_marker(void* handle)
  {
    return ""; // ctl()->meta.mgr->get_marker(handle);
  }

  int RGWDBStore::meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y)
  {
    return 0; //ctl()->meta.mgr->remove(metadata_key, y, dpp);
  }

} // namespace rgw::sal

extern "C" {

  void *newRGWDBStore(void)
  {
    rgw::sal::RGWDBStore *store = new rgw::sal::RGWDBStore();
    if (store) {
      DBStoreManager *dbsm = new DBStoreManager();

      if (!dbsm ) {
        delete store; store = nullptr;
      }

      DBStore *dbstore = dbsm->getDBStore();
      if (!dbstore ) {
        delete dbsm;
        delete store; store = nullptr;
      }

      store->setDBStoreManager(dbsm);
      store->setDBStore(dbstore);
    }

    return store;
  }

}
