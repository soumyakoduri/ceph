// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>
#include <iostream>
#include <map>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "common/Formatter.h"
#include <common/errno.h>
#include "include/random.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_lc.h"
#include "rgw_zone.h"
#include "rgw_string.h"
#include "rgw_multi.h"
#include "rgw_cr_rados.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"
#include "rgw_coroutine.h"
#include "rgw_rados.h"

#include <boost/asio/yield.hpp>

// this seems safe to use, at least for now--arguably, we should
// prefer header-only fmt, in general
#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include "fmt/format.h"

#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

const char* LC_STATUS[] = {
      "UNINITIAL",
      "PROCESSING",
      "FAILED",
      "COMPLETE"
};

using namespace librados;
using namespace rgw::sal;

bool LCRule::valid() const
{
  if (id.length() > MAX_ID_LEN) {
    return false;
  }
  else if(expiration.empty() && noncur_expiration.empty() && mp_expiration.empty() && !dm_expiration &&
          transitions.empty() && noncur_transitions.empty()) {
    return false;
  }
  else if (!expiration.valid() || !noncur_expiration.valid() || !mp_expiration.valid()) {
    return false;
  }
  if (!transitions.empty()) {
    bool using_days = expiration.has_days();
    bool using_date = expiration.has_date();
    for (const auto& elem : transitions) {
      if (!elem.second.valid()) {
        return false;
      }
      using_days = using_days || elem.second.has_days();
      using_date = using_date || elem.second.has_date();
      if (using_days && using_date) {
        return false;
      }
    }
  }
  for (const auto& elem : noncur_transitions) {
    if (!elem.second.valid()) {
      return false;
    }
  }

  return true;
}

void LCRule::init_simple_days_rule(std::string_view _id, std::string_view _prefix, int num_days)
{
  id = _id;
  prefix = _prefix;
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", num_days);
  expiration.set_days(buf);
  set_enabled(true);
}

void RGWLifecycleConfiguration::add_rule(const LCRule& rule)
{
  auto& id = rule.get_id(); // note that this will return false for groups, but that's ok, we won't search groups
  rule_map.insert(pair<string, LCRule>(id, rule));
}

bool RGWLifecycleConfiguration::_add_rule(const LCRule& rule)
{
  lc_op op(rule.get_id());
  op.status = rule.is_enabled();
  if (rule.get_expiration().has_days()) {
    op.expiration = rule.get_expiration().get_days();
  }
  if (rule.get_expiration().has_date()) {
    op.expiration_date = ceph::from_iso_8601(rule.get_expiration().get_date());
  }
  if (rule.get_noncur_expiration().has_days()) {
    op.noncur_expiration = rule.get_noncur_expiration().get_days();
  }
  if (rule.get_mp_expiration().has_days()) {
    op.mp_expiration = rule.get_mp_expiration().get_days();
  }
  op.dm_expiration = rule.get_dm_expiration();
  for (const auto &elem : rule.get_transitions()) {
    transition_action action;
    if (elem.second.has_days()) {
      action.days = elem.second.get_days();
    } else {
      action.date = ceph::from_iso_8601(elem.second.get_date());
    }
    action.storage_class = rgw_placement_rule::get_canonical_storage_class(elem.first);
    op.transitions.emplace(elem.first, std::move(action));
  }
  for (const auto &elem : rule.get_noncur_transitions()) {
    transition_action action;
    action.days = elem.second.get_days();
    action.date = ceph::from_iso_8601(elem.second.get_date());
    action.storage_class = elem.first;
    op.noncur_transitions.emplace(elem.first, std::move(action));
  }
  std::string prefix;
  if (rule.get_filter().has_prefix()){
    prefix = rule.get_filter().get_prefix();
  } else {
    prefix = rule.get_prefix();
  }

  if (rule.get_filter().has_tags()){
    op.obj_tags = rule.get_filter().get_tags();
  }
  prefix_map.emplace(std::move(prefix), std::move(op));
  return true;
}

int RGWLifecycleConfiguration::check_and_add_rule(const LCRule& rule)
{
  if (!rule.valid()) {
    return -EINVAL;
  }
  auto& id = rule.get_id();
  if (rule_map.find(id) != rule_map.end()) {  //id shouldn't be the same 
    return -EINVAL;
  }
  if (rule.get_filter().has_tags() && (rule.get_dm_expiration() || !rule.get_mp_expiration().empty())) {
    return -ERR_INVALID_REQUEST;
  }
  rule_map.insert(pair<string, LCRule>(id, rule));

  if (!_add_rule(rule)) {
    return -ERR_INVALID_REQUEST;
  }
  return 0;
}

bool RGWLifecycleConfiguration::has_same_action(const lc_op& first, const lc_op& second) {
  if ((first.expiration > 0 || first.expiration_date != boost::none) && 
    (second.expiration > 0 || second.expiration_date != boost::none)) {
    return true;
  } else if (first.noncur_expiration > 0 && second.noncur_expiration > 0) {
    return true;
  } else if (first.mp_expiration > 0 && second.mp_expiration > 0) {
    return true;
  } else if (!first.transitions.empty() && !second.transitions.empty()) {
    for (auto &elem : first.transitions) {
      if (second.transitions.find(elem.first) != second.transitions.end()) {
        return true;
      }
    }
  } else if (!first.noncur_transitions.empty() && !second.noncur_transitions.empty()) {
    for (auto &elem : first.noncur_transitions) {
      if (second.noncur_transitions.find(elem.first) != second.noncur_transitions.end()) {
        return true;
      }
    }
  }
  return false;
}

/* Formerly, this method checked for duplicate rules using an invalid
 * method (prefix uniqueness). */
bool RGWLifecycleConfiguration::valid() 
{
  return true;
}

void *RGWLC::LCWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    if (should_work(start)) {
      ldpp_dout(dpp, 2) << "life cycle: start" << dendl;
      int r = lc->process();
      if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: do life cycle process() returned error r=" << r << dendl;
      }
      ldpp_dout(dpp, 2) << "life cycle: stop" << dendl;
    }
    if (lc->going_down())
      break;

    utime_t end = ceph_clock_now();
    int secs = schedule_next_start_time(start, end);
    utime_t next;
    next.set_from_double(end + secs);

    ldpp_dout(dpp, 5) << "schedule life cycle next start time: " << rgw_to_asctime(next) << dendl;

    std::unique_lock l{lock};
    cond.wait_for(l, std::chrono::seconds(secs));
  } while (!lc->going_down());

  return NULL;
}

void RGWLC::initialize(CephContext *_cct, RGWRadosStore *_store) {
  cct = _cct;
  store = _store;
  max_objs = cct->_conf->rgw_lc_max_objs;
  if (max_objs > HASH_PRIME)
    max_objs = HASH_PRIME;

  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = lc_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);
  }

#define COOKIE_LEN 16
  char cookie_buf[COOKIE_LEN + 1];
  gen_rand_alphanumeric(cct, cookie_buf, sizeof(cookie_buf) - 1);
  cookie = cookie_buf;
}

void RGWLC::finalize()
{
  delete[] obj_names;
}

bool RGWLC::if_already_run_today(time_t& start_date)
{
  struct tm bdt;
  time_t begin_of_day;
  utime_t now = ceph_clock_now();
  localtime_r(&start_date, &bdt);

  if (cct->_conf->rgw_lc_debug_interval > 0) {
    if (now - start_date < cct->_conf->rgw_lc_debug_interval)
      return true;
    else
      return false;
  }

  bdt.tm_hour = 0;
  bdt.tm_min = 0;
  bdt.tm_sec = 0;
  begin_of_day = mktime(&bdt);
  if (now - begin_of_day < 24*60*60)
    return true;
  else
    return false;
}

int RGWLC::bucket_lc_prepare(int index)
{
  map<string, int > entries;

  string marker;

#define MAX_LC_LIST_ENTRIES 100
  do {
    int ret = cls_rgw_lc_list(store->getRados()->lc_pool_ctx, obj_names[index], marker, MAX_LC_LIST_ENTRIES, entries);
    if (ret < 0)
      return ret;
    map<string, int>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      pair<string, int > entry(iter->first, lc_uninitial);
      ret = cls_rgw_lc_set_entry(store->getRados()->lc_pool_ctx, obj_names[index],  entry);
      if (ret < 0) {
        ldpp_dout(this, 0) << "RGWLC::bucket_lc_prepare() failed to set entry on "
            << obj_names[index] << dendl;
        return ret;
      }
    }

    if (!entries.empty()) {
      marker = std::move(entries.rbegin()->first);
    }
  } while (!entries.empty());

  return 0;
}

static bool obj_has_expired(CephContext *cct, ceph::real_time mtime, int days, ceph::real_time *expire_time = nullptr)
{
  double timediff, cmp;
  utime_t base_time;
  if (cct->_conf->rgw_lc_debug_interval <= 0) {
    /* Normal case, run properly */
    cmp = days*24*60*60;
    base_time = ceph_clock_now().round_to_day();
  } else {
    /* We're in debug mode; Treat each rgw_lc_debug_interval seconds as a day */
    cmp = days*cct->_conf->rgw_lc_debug_interval;
    base_time = ceph_clock_now();
  }
  timediff = base_time - ceph::real_clock::to_time_t(mtime);

  if (expire_time) {
    *expire_time = mtime + make_timespan(cmp);
  }
  ldout(cct, 20) << __func__ << "(): mtime=" << mtime << " days=" << days << " base_time=" << base_time << " timediff=" << timediff << " cmp=" << cmp << dendl;

  return (timediff >= cmp);
}

static bool pass_object_lock_check(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, RGWObjectCtx& ctx)
{
  if (!bucket_info.obj_lock_enabled()) {
    return true;
  }
  RGWRados::Object op_target(store, bucket_info, ctx, obj);
  RGWRados::Object::Read read_op(&op_target);
  map<string, bufferlist> attrs;
  read_op.params.attrs = &attrs;
  int ret = read_op.prepare(null_yield);
  if (ret < 0) {
    if (ret == -ENOENT) {
      return true;
    } else {
      return false;
    }
  } else {
    auto iter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
    if (iter != attrs.end()) {
      RGWObjectRetention retention;
      try {
        decode(retention, iter->second);
      } catch (buffer::error& err) {
        ldout(store->ctx(), 0) << "ERROR: failed to decode RGWObjectRetention" << dendl;
        return false;
      }
      if (ceph::real_clock::to_time_t(retention.get_retain_until_date()) > ceph_clock_now()) {
        return false;
      }
    }
    iter = attrs.find(RGW_ATTR_OBJECT_LEGAL_HOLD);
    if (iter != attrs.end()) {
      RGWObjectLegalHold obj_legal_hold;
      try {
        decode(obj_legal_hold, iter->second);
      } catch (buffer::error& err) {
        ldout(store->ctx(), 0) << "ERROR: failed to decode RGWObjectLegalHold" << dendl;
        return false;
      }
      if (obj_legal_hold.is_enabled()) {
        return false;
      }
    }
    return true;
  }
}

int RGWLC::handle_multipart_expiration(
  RGWRados::Bucket *target, const multimap<string, lc_op>& prefix_map)
{
  MultipartMetaFilter mp_filter;
  vector<rgw_bucket_dir_entry> objs;
  RGWMPObj mp_obj;
  bool is_truncated;
  int ret;
  RGWBucketInfo& bucket_info = target->get_bucket_info();
  RGWRados::Bucket::List list_op(target);
  auto delay_ms = cct->_conf.get_val<int64_t>("rgw_lc_thread_delay");
  list_op.params.list_versions = false;
  /* lifecycle processing does not depend on total order, so can
   * take advantage of unorderd listing optimizations--such as
   * operating on one shard at a time */
  list_op.params.allow_unordered = true;
  list_op.params.ns = RGW_OBJ_NS_MULTIPART;
  list_op.params.filter = &mp_filter;
  for (auto prefix_iter = prefix_map.begin(); prefix_iter != prefix_map.end(); ++prefix_iter) {
    if (!prefix_iter->second.status || prefix_iter->second.mp_expiration <= 0) {
      continue;
    }
    list_op.params.prefix = prefix_iter->first;
    do {
      objs.clear();
      list_op.params.marker = list_op.get_next_marker();
      ret = list_op.list_objects(1000, &objs, NULL, &is_truncated, null_yield);
      if (ret < 0) {
          if (ret == (-ENOENT))
            return 0;
          ldpp_dout(this, 0) << "ERROR: store->list_objects():" <<dendl;
          return ret;
      }

      for (auto obj_iter = objs.begin(); obj_iter != objs.end(); ++obj_iter) {
        if (obj_has_expired(cct, obj_iter->meta.mtime, prefix_iter->second.mp_expiration)) {
          rgw_obj_key key(obj_iter->key);
          if (!mp_obj.from_meta(key.name)) {
            continue;
          }
          RGWObjectCtx rctx(store);
          ret = abort_multipart_upload(store, cct, &rctx, bucket_info, mp_obj);
          if (ret < 0 && ret != -ERR_NO_SUCH_UPLOAD) {
            ldpp_dout(this, 0) << "ERROR: abort_multipart_upload failed, ret=" << ret << ", meta:" << obj_iter->key << dendl;
          } else if (ret == -ERR_NO_SUCH_UPLOAD) {
            ldpp_dout(this, 5) << "ERROR: abort_multipart_upload failed, ret=" << ret << ", meta:" << obj_iter->key << dendl;
          }
          if (going_down())
            return 0;
        }
      } /* for objs */
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    } while(is_truncated);
  }
  return 0;
}

static int read_obj_tags(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, RGWObjectCtx& ctx, bufferlist& tags_bl)
{
  RGWRados::Object op_target(store, bucket_info, ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  return read_op.get_attr(RGW_ATTR_TAGS, tags_bl, null_yield);
}

static bool is_valid_op(const lc_op& op)
{
      return (op.status &&
              (op.expiration > 0 
               || op.expiration_date != boost::none
               || op.noncur_expiration > 0
               || op.dm_expiration
               || !op.transitions.empty()
               || !op.noncur_transitions.empty()));
}

static inline bool has_all_tags(const lc_op& rule_action,
				const RGWObjTags& object_tags)
{
  if(! rule_action.obj_tags)
    return false;
  if(object_tags.count() < rule_action.obj_tags->count())
    return false;
  size_t tag_count = 0;
  for (const auto& tag : object_tags.get_tags()) {
    const auto& rule_tags = rule_action.obj_tags->get_tags();
    const auto& iter = rule_tags.find(tag.first);
    if(iter->second == tag.second)
    {
      tag_count++;
    }
  /* all tags in the rule appear in obj tags */
  }
  return tag_count == rule_action.obj_tags->count();
}

class LCObjsLister {
  RGWRadosStore *store;
  RGWBucketInfo& bucket_info;
  RGWRados::Bucket target;
  RGWRados::Bucket::List list_op;
  bool is_truncated{false};
  rgw_obj_key next_marker;
  string prefix;
  vector<rgw_bucket_dir_entry> objs;
  vector<rgw_bucket_dir_entry>::iterator obj_iter;
  rgw_bucket_dir_entry pre_obj;
  int64_t delay_ms;

public:
  LCObjsLister(RGWRadosStore *_store, RGWBucketInfo& _bucket_info) :
      store(_store), bucket_info(_bucket_info),
      target(store->getRados(), bucket_info), list_op(&target) {
    list_op.params.list_versions = bucket_info.versioned();
    list_op.params.allow_unordered = true;
    delay_ms = store->ctx()->_conf.get_val<int64_t>("rgw_lc_thread_delay");
  }

  void set_prefix(const string& p) {
    prefix = p;
    list_op.params.prefix = prefix;
  }

  int init() {
    return fetch();
  }

  int fetch() {
    int ret = list_op.list_objects(1000, &objs, NULL, &is_truncated, null_yield);
    if (ret < 0) {
      return ret;
    }

    obj_iter = objs.begin();

    return 0;
  }

  void delay() {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
  }

  bool get_obj(rgw_bucket_dir_entry *obj) {
    if (obj_iter == objs.end()) {
      delay();
      return false;
    }
    if (is_truncated && (obj_iter + 1)==objs.end()) {
      list_op.params.marker = obj_iter->key;

      int ret = fetch();
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: list_op returned ret=" << ret << dendl;
        return ret;
      } 
      obj_iter = objs.begin();
      if (obj_iter == objs.end()) {
        return false;
      }
      delay();
    }
    *obj = *obj_iter;
    return true;
  }

  rgw_bucket_dir_entry get_prev_obj() {
    return pre_obj;
  }

  void next() {
    pre_obj = *obj_iter;
    ++obj_iter;
  }

  bool next_has_same_name()
  {
    if ((obj_iter + 1) == objs.end()) {
      /* this should have been called after get_obj() was called, so this should
       * only happen if is_truncated is false */
      return false;
    }
    return (obj_iter->key.name.compare((obj_iter + 1)->key.name) == 0);
  }
};


struct op_env {
  lc_op& op;
  RGWRadosStore *store;
  RGWLC *lc;
  RGWBucketInfo& bucket_info;
  LCObjsLister& ol;

  op_env(lc_op& _op, RGWRadosStore *_store, RGWLC *_lc, RGWBucketInfo& _bucket_info,
         LCObjsLister& _ol) : op(_op), store(_store), lc(_lc), bucket_info(_bucket_info), ol(_ol) {}
};

class LCRuleOp;

struct lc_op_ctx {
  CephContext *cct;
  op_env& env;
  rgw_bucket_dir_entry& o;

  RGWRadosStore *store;
  RGWBucketInfo& bucket_info;
  lc_op& op;
  LCObjsLister& ol;

  rgw_obj obj;
  RGWObjectCtx rctx;
  const DoutPrefixProvider *dpp;

  lc_op_ctx(op_env& _env, rgw_bucket_dir_entry& _o, const DoutPrefixProvider *_dpp) : cct(_env.store->ctx()), env(_env), o(_o),
                 store(env.store), bucket_info(env.bucket_info), op(env.op), ol(env.ol),
                 obj(env.bucket_info.bucket, o.key), rctx(env.store), dpp(_dpp) {}
};

static int remove_expired_obj(lc_op_ctx& oc, bool remove_indeed)
{
  auto& store = oc.store;
  auto& bucket_info = oc.bucket_info;
  auto& o = oc.o;
  auto obj_key = o.key;
  auto& meta = o.meta;

  if (!remove_indeed) {
    obj_key.instance.clear();
  } else if (obj_key.instance.empty()) {
    obj_key.instance = "null";
  }

  rgw_obj obj(bucket_info.bucket, obj_key);
  ACLOwner obj_owner;
  obj_owner.set_id(rgw_user {meta.owner});
  obj_owner.set_name(meta.owner_display_name);

  RGWRados::Object del_target(store->getRados(), bucket_info, oc.rctx, obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.versioning_status = bucket_info.versioning_status();
  del_op.params.obj_owner = obj_owner;
  del_op.params.unmod_since = meta.mtime;

  return del_op.delete_obj(null_yield);
}

class LCOpAction {
public:
  virtual ~LCOpAction() {}

  virtual bool check(lc_op_ctx& oc, ceph::real_time *exp_time) {
    return false;
  };

  /* called after check(). Check should tell us whether this action
   * is applicable. If there are multiple actions, we'll end up executing
   * the latest applicable action
   * For example:
   *   one action after 10 days, another after 20, third after 40.
   *   After 10 days, the latest applicable action would be the first one,
   *   after 20 days it will be the second one. After 21 days it will still be the
   *   second one. So check() should return true for the second action at that point,
   *   but should_process() if the action has already been applied. In object removal
   *   it doesn't matter, but in object transition it does.
   */
  virtual bool should_process() {
    return true;
  }

  virtual int process(lc_op_ctx& oc) {
    return 0;
  }
};

class LCOpFilter {
public:
virtual ~LCOpFilter() {}
  virtual bool check(lc_op_ctx& oc) {
    return false;
  }
};

class LCOpRule {
  friend class LCOpAction;

  op_env& env;

  std::vector<unique_ptr<LCOpFilter> > filters;
  std::vector<unique_ptr<LCOpAction> > actions;

public:
  LCOpRule(op_env& _env) : env(_env) {}

  void build();
  int process(rgw_bucket_dir_entry& o, const DoutPrefixProvider *dpp);
};

static int check_tags(lc_op_ctx& oc, bool *skip)
{
  auto& op = oc.op;

  if (op.obj_tags != boost::none) {
    *skip = true;

    bufferlist tags_bl;
    int ret = read_obj_tags(oc.store->getRados(), oc.bucket_info, oc.obj, oc.rctx, tags_bl);
    if (ret < 0) {
      if (ret != -ENODATA) {
        ldout(oc.cct, 5) << "ERROR: read_obj_tags returned r=" << ret << dendl;
      }
      return 0;
    }
    RGWObjTags dest_obj_tags;
    try {
      auto iter = tags_bl.cbegin();
      dest_obj_tags.decode(iter);
    } catch (buffer::error& err) {
      ldout(oc.cct,0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
      return -EIO;
    }

    if (! has_all_tags(op, dest_obj_tags)) {
      ldout(oc.cct, 20) << __func__ << "() skipping obj " << oc.obj << " as tags do not match in rule: " << op.id << dendl;
      return 0;
    }
  }
  *skip = false;
  return 0;
}

class LCOpFilter_Tags : public LCOpFilter {
public:
  bool check(lc_op_ctx& oc) override {
    auto& o = oc.o;

    if (o.is_delete_marker()) {
      return true;
    }

    bool skip;

    int ret = check_tags(oc, &skip);
    if (ret < 0) {
      if (ret == -ENOENT) {
        return false;
      }
      ldout(oc.cct, 0) << "ERROR: check_tags on obj=" << oc.obj << " returned ret=" << ret << dendl;
      return false;
    }

    return !skip;
  };
};

class LCOpAction_CurrentExpiration : public LCOpAction {
public:
  bool check(lc_op_ctx& oc, ceph::real_time *exp_time) override {
    auto& o = oc.o;
    if (!o.is_current()) {
      ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": not current, skipping" << dendl;
      return false;
    }
    if (o.is_delete_marker()) {
      if (oc.ol.next_has_same_name()) {
        return false;
      } else {
        *exp_time = real_clock::now();
        return true;
      }
    }

    auto& mtime = o.meta.mtime;
    bool is_expired;
    auto& op = oc.op;
    if (op.expiration <= 0) {
      if (op.expiration_date == boost::none) {
        ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": no expiration set in rule, skipping" << dendl;
        return false;
      }
      is_expired = ceph_clock_now() >= ceph::real_clock::to_time_t(*op.expiration_date);
      *exp_time = *op.expiration_date;
    } else {
      is_expired = obj_has_expired(oc.cct, mtime, op.expiration, exp_time);
    }

    ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": is_expired=" << (int)is_expired << dendl;
    return is_expired;
  }

  int process(lc_op_ctx& oc) {
    auto& o = oc.o;
    int r;
    if (o.is_delete_marker()) {
      r = remove_expired_obj(oc, true);
    } else {
      r = remove_expired_obj(oc, !oc.bucket_info.versioned());
    }
    if (r < 0) {
      ldout(oc.cct, 0) << "ERROR: remove_expired_obj " << dendl;
      return r;
    }
    ldout(oc.cct, 2) << "DELETED:" << oc.bucket_info.bucket << ":" << o.key << dendl;
    return 0;
  }
};

class LCOpAction_NonCurrentExpiration : public LCOpAction {
public:
  bool check(lc_op_ctx& oc, ceph::real_time *exp_time) override {
    auto& o = oc.o;
    if (o.is_current()) {
      ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": current version, skipping" << dendl;
      return false;
    }

    auto mtime = oc.ol.get_prev_obj().meta.mtime;
    int expiration = oc.op.noncur_expiration;
    bool is_expired = obj_has_expired(oc.cct, mtime, expiration, exp_time);

    ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": is_expired=" << is_expired << dendl;
    return is_expired && pass_object_lock_check(oc.store->getRados(), oc.bucket_info, oc.obj, oc.rctx);
  }

  int process(lc_op_ctx& oc) {
    auto& o = oc.o;
    int r = remove_expired_obj(oc, true);
    if (r < 0) {
      ldout(oc.cct, 0) << "ERROR: remove_expired_obj " << dendl;
      return r;
    }
    ldout(oc.cct, 2) << "DELETED:" << oc.bucket_info.bucket << ":" << o.key << " (non-current expiration)" << dendl;
    return 0;
  }
};

class LCOpAction_DMExpiration : public LCOpAction {
public:
  bool check(lc_op_ctx& oc, ceph::real_time *exp_time) override {
    auto& o = oc.o;
    if (!o.is_delete_marker()) {
      ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": not a delete marker, skipping" << dendl;
      return false;
    }

    if (oc.ol.next_has_same_name()) {
      ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": next is same object, skipping" << dendl;
      return false;
    }

    *exp_time = real_clock::now();

    return true;
  }

  int process(lc_op_ctx& oc) {
    auto& o = oc.o;
    int r = remove_expired_obj(oc, true);
    if (r < 0) {
      ldout(oc.cct, 0) << "ERROR: remove_expired_obj " << dendl;
      return r;
    }
    ldout(oc.cct, 2) << "DELETED:" << oc.bucket_info.bucket << ":" << o.key << " (delete marker expiration)" << dendl;
    return 0;
  }
};

  struct RGWRESTCloudTier {
    std::shared_ptr<RGWRESTConn> conn;
    string bucket_name;
    RGWHTTPManager *http_manager;

    RGWRESTCloudTier() {}
    RGWRESTCloudTier(RGWRESTConn *_conn, string _bucket, RGWHTTPManager *_http)
          : conn(_conn), bucket_name(_bucket), http_manager(_http) {}
  };


struct rgw_lc_multipart_part_info {
  int part_num{0};
  uint64_t ofs{0};
  uint64_t size{0};
  string etag;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(part_num, bl);
    encode(ofs, bl);
    encode(size, bl);
    encode(etag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(part_num, bl);
    decode(ofs, bl);
    decode(size, bl);
    decode(etag, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_lc_multipart_part_info)

struct rgw_lc_obj_properties {
  ceph::real_time mtime;
  string etag;
  uint64_t versioned_epoch{0};

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(mtime, bl);
    encode(etag, bl);
    encode(versioned_epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(mtime, bl);
    decode(etag, bl);
    decode(versioned_epoch, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_lc_obj_properties)



struct rgw_lc_multipart_upload_info {
  string upload_id;
  uint64_t obj_size;
  rgw_lc_obj_properties obj_properties;
  uint32_t part_size{0};
  uint32_t num_parts{0};

  int cur_part{0};
  uint64_t cur_ofs{0};

  std::map<int, rgw_lc_multipart_part_info> parts;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(upload_id, bl);
    encode(obj_size, bl);
    encode(obj_properties, bl);
    encode(part_size, bl);
    encode(num_parts, bl);
    encode(cur_part, bl);
    encode(cur_ofs, bl);
    encode(parts, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(upload_id, bl);
    decode(obj_size, bl);
    decode(obj_properties, bl);
    decode(part_size, bl);
    decode(num_parts, bl);
    decode(cur_part, bl);
    decode(cur_ofs, bl);
    decode(parts, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_lc_multipart_upload_info)


#define DEFAULT_MULTIPART_SYNC_PART_SIZE (5 * 1024 * 1024)
#define MULTIPART_MIN_POSSIBLE_PART_SIZE (5 * 1024 * 1024)

static string get_key_oid(const rgw_obj_key& key)
{
  string oid = key.name;
  if (!key.instance.empty() &&
      !key.have_null_instance()) {
    oid += string(":") + key.instance;
  }
  return oid;
}

static string obj_to_aws_path(const rgw_obj& obj)
{
  string path = obj.bucket.name + "/" + get_key_oid(obj.key);


  return path;
}


static std::set<string> keep_headers = { "CONTENT_TYPE",
                                         "CONTENT_ENCODING",
                                         "CONTENT_DISPOSITION",
                                         "CONTENT_LANGUAGE" };

class RGWLCStreamPutCRF : public RGWStreamWriteHTTPResourceCRF
{
  CephContext *cct;
  RGWHTTPManager *http_manager;
  rgw_lc_obj_properties obj_properties;
  std::shared_ptr<RGWRESTConn> conn;
  rgw_obj dest_obj;
  string etag;
public:
  RGWLCStreamPutCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWHTTPManager *_http_manager,
                               const rgw_lc_obj_properties&  _obj_properties,
                               std::shared_ptr<RGWRESTConn> _conn,
                               rgw_obj& _dest_obj) :
            RGWStreamWriteHTTPResourceCRF(_cct, _env, _caller, _http_manager),
                               cct(_cct), http_manager(_http_manager), obj_properties(_obj_properties), conn(_conn), dest_obj(_dest_obj) {
  }

  int init() override {
    /* init output connection */
    RGWRESTStreamS3PutObj *out_req{nullptr};

    if (multipart.is_multipart) {
      char buf[32];
      snprintf(buf, sizeof(buf), "%d", multipart.part_num);
      rgw_http_param_pair params[] = { { "uploadId", multipart.upload_id.c_str() },
                                       { "partNumber", buf },
                                       { nullptr, nullptr } };
      conn->put_obj_send_init(dest_obj, params, &out_req);
    } else {
      conn->put_obj_send_init(dest_obj, nullptr, &out_req);
    }

    set_req(out_req);

    return RGWStreamWriteHTTPResourceCRF::init();
  }

  static bool keep_attr(const string& h) {
    return (keep_headers.find(h) != keep_headers.end() ||
            boost::algorithm::starts_with(h, "X_AMZ_"));
  }

  static void init_send_attrs(CephContext *cct,
                              const rgw_rest_obj& rest_obj,
                              const rgw_lc_obj_properties& obj_properties,
                              map<string, string> *attrs) {
    auto& new_attrs = *attrs;

    new_attrs.clear();

    for (auto& hi : rest_obj.attrs) {
      if (keep_attr(hi.first)) {
        new_attrs.insert(hi);
      }
    }

    auto acl = rest_obj.acls.get_acl();

    map<int, vector<string> > access_map;

/*    if (target->acls) {
      for (auto& grant : acl.get_grant_map()) {
        auto& orig_grantee = grant.first;
        auto& perm = grant.second;

        string grantee;

        const auto& am = target->acls->acl_mappings;

        auto iter = am.find(orig_grantee);
        if (iter == am.end()) {
          ldout(cct, 20) << "acl_mappings: Could not find " << orig_grantee << " .. ignoring" << dendl;
          continue;
        }

        grantee = iter->second.dest_id;

        string type;

        switch (iter->second.type) {
          case ACL_TYPE_CANON_USER:
            type = "id";
            break;
          case ACL_TYPE_EMAIL_USER:
            type = "emailAddress";
            break;
          case ACL_TYPE_GROUP:
            type = "uri";
            break;
          default:
            continue;
        }

        string tv = type + "=" + grantee;

        int flags = perm.get_permission().get_permissions();
        if ((flags & RGW_PERM_FULL_CONTROL) == RGW_PERM_FULL_CONTROL) {
          access_map[flags].push_back(tv);
          continue;
        }

        for (int i = 1; i <= RGW_PERM_WRITE_ACP; i <<= 1) {
          if (flags & i) {
            access_map[i].push_back(tv);
          }
        }
      }
    } */

    for (auto aiter : access_map) {
      int grant_type = aiter.first;

      string header_str("x-amz-grant-");

      switch (grant_type) {
        case RGW_PERM_READ:
          header_str.append("read");
          break;
        case RGW_PERM_WRITE:
          header_str.append("write");
          break;
        case RGW_PERM_READ_ACP:
          header_str.append("read-acp");
          break;
        case RGW_PERM_WRITE_ACP:
          header_str.append("write-acp");
          break;
        case RGW_PERM_FULL_CONTROL:
          header_str.append("full-control");
          break;
      }

      string s;

      for (auto viter : aiter.second) {
        if (!s.empty()) {
          s.append(", ");
        }
        s.append(viter);
      }

      ldout(cct, 20) << "acl_mappings: set acl: " << header_str << "=" << s << dendl;

      new_attrs[header_str] = s;
    }

    char buf[32];
    snprintf(buf, sizeof(buf), "%llu", (long long)obj_properties.versioned_epoch);
    new_attrs["x-amz-meta-rgwx-versioned-epoch"] = buf;

    utime_t ut(obj_properties.mtime);
    snprintf(buf, sizeof(buf), "%lld.%09lld",
             (long long)ut.sec(),
             (long long)ut.nsec());

    new_attrs["x-amz-meta-rgwx-source-mtime"] = buf;
    new_attrs["x-amz-meta-rgwx-source-etag"] = obj_properties.etag;
    new_attrs["x-amz-meta-rgwx-source-key"] = rest_obj.key.name;
    if (!rest_obj.key.instance.empty()) {
      new_attrs["x-amz-meta-rgwx-source-version-id"] = rest_obj.key.instance;
    }
  }

  void send_ready(const rgw_rest_obj& rest_obj) override {
    RGWRESTStreamS3PutObj *r = static_cast<RGWRESTStreamS3PutObj *>(req);

    map<string, string> new_attrs;
    if (!multipart.is_multipart) {
      init_send_attrs(cct, rest_obj, obj_properties, &new_attrs);
    }

    r->set_send_length(rest_obj.content_len);

    RGWAccessControlPolicy policy;

    r->send_ready(conn->get_key(), new_attrs, policy, false);
  }

  void handle_headers(const map<string, string>& headers) {
    for (auto h : headers) {
      if (h.first == "ETAG") {
        etag = h.second;
      }
    }
  }

  bool get_etag(string *petag) {
    if (etag.empty()) {
      return false;
    }
    *petag = etag;
    return true;
  }
};



  class RGWLCStreamObjToCloudPlainCR : public RGWCoroutine {
    lc_op_ctx& oc;
    RGWRESTCloudTier& tier;

    public:
      RGWLCStreamObjToCloudPlainCR(lc_op_ctx& _oc, RGWRESTCloudTier& _tier)
          : RGWCoroutine(_oc.cct), oc(_oc), tier(_tier) {}

      int operate() override {

        /* Prepare Read from source */
        RGWObjectCtx& obj_ctx = oc.rctx ;
        RGWBucketInfo& bucket_info = oc.bucket_info;
        rgw_obj& obj = oc.obj;
        const real_time&  mtime = oc.o.meta.mtime;
        uint64_t olh_epoch = oc.o.versioned_epoch;
        optional_yield y = null_yield;
        map<string, bufferlist> attrs;
        real_time read_mtime;
        uint64_t obj_size;

        RGWRados::Object op_target(oc.store->getRados(), bucket_info, obj_ctx, obj);
        RGWRados::Object::Read read_op(&op_target);

        read_op.params.attrs = &attrs;
        read_op.params.lastmod = &read_mtime;
        read_op.params.obj_size = &obj_size;

        int ret = read_op.prepare(y);
        if (ret < 0) {
            return set_cr_error(ret);
        }

        if (read_mtime != mtime) {
            /* raced */
            return set_cr_error(-ECANCELED);
        }

        /* Prepare write */
        std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;
        rgw_lc_obj_properties obj_properties;
        obj_properties.mtime = mtime;
        obj_properties.etag = oc.o.meta.etag;
        obj_properties.versioned_epoch = olh_epoch;
       
        rgw_bucket target_bucket;
        target_bucket.name = tier.bucket_name;
        string target_obj_name = obj.key.name; // cross check with aws module
        rgw_obj dest_obj(target_bucket, target_obj_name);

        out_crf.reset(new RGWLCStreamPutCRF((CephContext *)(oc.cct), get_env(), this,
                                     (RGWHTTPManager*)(tier.http_manager),
                                     obj_properties, tier.conn, dest_obj));


        /* actual Read & Write */
        off_t ofs = 0;
        off_t end = obj_size - 1;
        bool need_retry{false};
        bool sent_attrs{false};
        bufferlist bl;
        uint64_t read_len = 0;

        reenter(this) {
        do {
            bl.clear();

            /* TODO: Instead of reading chunks,  check if it can b read
             * in large chunks at once 
             */
            {
            ret = read_op.read(ofs, end, bl, y);
            if (ret < 0) {
              ldout(oc.cct, 0) << "ERROR: fail to read object data, ret = " << ret << dendl;
              return ret;
            }
            read_len = ret;
            }
 
           if (bl.length() == 0) {
              break;
           } 

            if (!sent_attrs) {
                ret = out_crf->init();
                if (ret < 0) {
                    return set_cr_error(ret);
                }
              
               /* Initialize rgw_rest_obj. 
                * Reference: do_decode_rest_obj
                * Check how to copy headers content */ 
                rgw_rest_obj rest_obj;
                rest_obj.init(obj.key);
                rest_obj.content_len = obj_size;

                rest_obj.acls.set_ctx(oc.cct);
                auto aiter = attrs.find(RGW_ATTR_ACL);
                if (aiter != attrs.end()) {
                    bufferlist& bl = aiter->second;
                    auto bliter = bl.cbegin();
                    try {
                        rest_obj.acls.decode(bliter);
                    } catch (buffer::error& err) {
                        ldout(oc.cct, 0) << "ERROR: failed to decode policy off attrs" << dendl;
                        return set_cr_error(-EIO);
                    }
                } else {
                    ldout(oc.cct, 0) << "WARNING: acl attrs not provided" << dendl;
                }
                

                out_crf->send_ready(rest_obj);
                ret = out_crf->send();
                if (ret < 0) {
                    return set_cr_error(ret);
                }
                sent_attrs = true;
            }

            /* TODO: try to do large chunk writes  (eg., 4MB like in 
             * aws_sync module?
             */
            do {
                /* TODO: having yield here doesn't seem to be working..
                 * most likely need_retry always set to true..once that is
                 * fixed yield may work too.
                 */
            //    yield {
                    ldout(oc.cct, 20) << "writing " << bl.length() << " bytes" << dendl;
                    ret = out_crf->write(bl, &need_retry);
           //     }
                    if (ret < 0)  {
                        return set_cr_error(ret);
                    }

                if (retcode < 0) {
                    ldout(oc.cct, 20) << __func__ << ": out_crf->write() retcode=" << retcode << dendl;
                    return set_cr_error(ret);
                }
              } while (need_retry);


            ofs += read_len;
        } while (ofs <= end);

        do {
            /* TODO: Here need_retry is always being set to true sometimes.
             * Debug and address the issue.
             */
          //  yield {
                ret = out_crf->drain_writes(&need_retry);
          //  }
                if (ret < 0) {
                    return set_cr_error(ret);
                }
        } while (need_retry);


        return set_cr_done();
        } //reenter

        return 0;
      }
  };


class RGWLCStreamObjToCloudMultipartPartCR : public RGWCoroutine {
    lc_op_ctx& oc;
    RGWRESTCloudTier& tier;

  string upload_id;

  rgw_lc_multipart_part_info part_info;

  string *petag;

public:
 RGWLCStreamObjToCloudMultipartPartCR(lc_op_ctx& _oc, RGWRESTCloudTier& _tier,
                                const string& _upload_id,
                                const rgw_lc_multipart_part_info& _part_info,
                                string *_petag)
          : RGWCoroutine(_oc.cct), oc(_oc), tier(_tier),
                                        upload_id(_upload_id),
                                        part_info(_part_info),
                                        petag(_petag) {}

  int operate() override {

        reenter(this) {
        /* Prepare Read from source */
        RGWObjectCtx& obj_ctx = oc.rctx ;
        RGWBucketInfo& bucket_info = oc.bucket_info;
        rgw_obj& obj = oc.obj;
        const real_time&  mtime = oc.o.meta.mtime;
        uint64_t olh_epoch = oc.o.versioned_epoch;
        optional_yield y = null_yield;
        map<string, bufferlist> attrs;
        real_time read_mtime;
        uint64_t obj_size;

        RGWRados::Object op_target(oc.store->getRados(), bucket_info, obj_ctx, obj);
        RGWRados::Object::Read read_op(&op_target);

        read_op.params.attrs = &attrs;
        read_op.params.lastmod = &read_mtime;
        read_op.params.obj_size = &obj_size;

        int ret = read_op.prepare(y);
        if (ret < 0) {
            return set_cr_error(ret);
        }

        if (read_mtime != mtime) {
            /* raced */
            return set_cr_error(-ECANCELED);
        }

        /* Prepare write */
        std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;
        rgw_lc_obj_properties obj_properties;
        obj_properties.mtime = mtime;
        obj_properties.etag = oc.o.meta.etag;
        obj_properties.versioned_epoch = olh_epoch;
       
        rgw_bucket target_bucket;
        target_bucket.name = tier.bucket_name;
        string target_obj_name = obj.key.name; // cross check with aws module
        rgw_obj dest_obj(target_bucket, target_obj_name);

        out_crf.reset(new RGWLCStreamPutCRF((CephContext *)(oc.cct), get_env(), this,
                                     (RGWHTTPManager*)(tier.http_manager),
                                     obj_properties, tier.conn, dest_obj));

      out_crf->set_multipart(upload_id, part_info.part_num, part_info.size);

        /* actual Read & Write */
        off_t ofs = part_info.ofs;
        off_t end = part_info.ofs + part_info.size - 1;
        bool need_retry{false};
        bool sent_attrs{false};
        bufferlist bl;
        uint64_t read_len = 0;

        do {
            bl.clear();

            /* TODO: Instead of reading chunks,  check if it can b read
             * in large chunks at once 
             */
            {
            ret = read_op.read(ofs, end, bl, y);
            if (ret < 0) {
              ldout(oc.cct, 0) << "XXXXXXXXX ERROR: fail to read object data, ret = " << ret << dendl;
              return ret;
            }
            read_len = ret;
            }
 
           if (bl.length() == 0) {
              break;
           } 

            if (!sent_attrs) {
                ret = out_crf->init();
                if (ret < 0) {
                    return set_cr_error(ret);
                }
              
               /* Initialize rgw_rest_obj. 
                * Reference: do_decode_rest_obj
                * Check how to copy headers content */ 
                rgw_rest_obj rest_obj;
                rest_obj.init(obj.key);
                //rest_obj.content_len = obj_size;
                rest_obj.content_len = part_info.size;

                rest_obj.acls.set_ctx(oc.cct);
                auto aiter = attrs.find(RGW_ATTR_ACL);
                if (aiter != attrs.end()) {
                    bufferlist& bl = aiter->second;
                    auto bliter = bl.cbegin();
                    try {
                        rest_obj.acls.decode(bliter);
                    } catch (buffer::error& err) {
                        ldout(oc.cct, 0) << "ERROR: failed to decode policy off attrs" << dendl;
                        return set_cr_error(-EIO);
                    }
                } else {
                    ldout(oc.cct, 0) << "WARNING: acl attrs not provided" << dendl;
                }
                

                out_crf->send_ready(rest_obj);
                ret = out_crf->send();
                if (ret < 0) {
                    return set_cr_error(ret);
                }
                    ldout(oc.cct, 0) << " XXXXXXXXX send_ready done "<< dendl;
                sent_attrs = true;
            }

            /* TODO: try to do large chunk writes  (eg., 4MB like in 
             * aws_sync module?
             */
            do {
                /* TODO: having yield here doesn't seem to be working..
                 * most likely need_retry always set to true..once that is
                 * fixed yield may work too.
                 */
    //            yield {
                    ldout(oc.cct, 0) << " XXXXXXXXX writing " << bl.length() << " bytes" << dendl;
                    ret = out_crf->write(bl, &need_retry);
                    if (ret < 0)  {
                        return set_cr_error(ret);
                    }
      //          }

                if (retcode < 0) {
                    ldout(oc.cct, 20) << __func__ << ": XXXXXXX out_crf->write() retcode=" << retcode << dendl;
                    return set_cr_error(ret);
                }
              } while (need_retry);

        ldout(oc.cct, 0) << "XXXXXXXXX ofs ="<<ofs<<" ,read_len = "<<read_len<<", end = "<<end<<dendl;
            ofs += read_len;
        } while (ofs <= end);

                ldout(oc.cct, 0) << "XXXXXXXXX before drain_writes " << dendl;
        do {
            /* TODO: Here need_retry is always being set to true sometimes.
             * Debug and address the issue.
             */
//            yield {
   //             ldout(oc.cct, 0) << "XXXXXXXXX before drain_writes " << dendl;
                ret = out_crf->drain_writes(&need_retry);
                if (ret < 0) {
                    return set_cr_error(ret);
                }
  //          }
        } while (need_retry);

      if (!(static_cast<RGWLCStreamPutCRF *>(out_crf.get()))->get_etag(petag)) {
        ldout(oc.cct, 0) << "XXXXXXXXXX ERROR: failed to get etag from PUT request" << dendl;
        return set_cr_error(-EIO);
      }


        return set_cr_done();
        } //reenter

    return 0;
  }
};

class RGWLCAbortMultipartCR : public RGWCoroutine {
  CephContext *cct;
  RGWHTTPManager *http_manager;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  string upload_id;

public:
  RGWLCAbortMultipartCR(CephContext *_cct,
                        RGWHTTPManager *_http_manager,
                        RGWRESTConn *_dest_conn,
                        const rgw_obj& _dest_obj,
                        const string& _upload_id) : RGWCoroutine(_cct),
                                   cct(_cct), http_manager(_http_manager),
                                                   dest_conn(_dest_conn),
                                                   dest_obj(_dest_obj),
                                                   upload_id(_upload_id) {}

  int operate() override {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };
        bufferlist bl;
        call(new RGWDeleteRESTResourceCR(cct, dest_conn, http_manager,
                                         obj_to_aws_path(dest_obj), params));
      }

      if (retcode < 0) {
        ldout(cct, 0) << "ERROR: failed to abort multipart upload for dest object=" << dest_obj << " (retcode=" << retcode << ")" << dendl;
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }

    return 0;
  }
};

class RGWLCInitMultipartCR : public RGWCoroutine {
  CephContext *cct;
  RGWHTTPManager *http_manager;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  uint64_t obj_size;
  map<string, string> attrs;

  bufferlist out_bl;

  string *upload_id;

  struct InitMultipartResult {
    string bucket;
    string key;
    string upload_id;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("UploadId", upload_id, obj);
    }
  } result;

public:
  RGWLCInitMultipartCR(CephContext *_cct,
                        RGWHTTPManager *_http_manager,
                        RGWRESTConn *_dest_conn,
                        const rgw_obj& _dest_obj,
                        uint64_t _obj_size,
                        const map<string, string>& _attrs,
                        string *_upload_id) : RGWCoroutine(_cct),
                                cct(_cct),
                                http_manager(_http_manager),
                                                   dest_conn(_dest_conn),
                                                   dest_obj(_dest_obj),
                                                   obj_size(_obj_size),
                                                   attrs(_attrs),
                                                   upload_id(_upload_id) {}

  int operate() override {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploads", nullptr }, {nullptr, nullptr} };
        bufferlist bl;
        call(new RGWPostRawRESTResourceCR <bufferlist> (cct, dest_conn, http_manager,
                                                 obj_to_aws_path(dest_obj), params, &attrs, bl, &out_bl));
      }

      if (retcode < 0) {
        ldout(cct, 0) << "ERROR: failed to initialize multipart upload for dest object=" << dest_obj << dendl;
        return set_cr_error(retcode);
      }
      {
        /*
         * If one of the following fails we cannot abort upload, as we cannot
         * extract the upload id. If one of these fail it's very likely that that's
         * the least of our problem.
         */
        RGWXMLDecoder::XMLParser parser;
        if (!parser.init()) {
          ldout(cct, 0) << "ERROR: failed to initialize xml parser for parsing multipart init response from server" << dendl;
          return set_cr_error(-EIO);
        }

        if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(cct, 5) << "ERROR: failed to parse xml: " << str << dendl;
          return set_cr_error(-EIO);
        }

        try {
          RGWXMLDecoder::decode_xml("InitiateMultipartUploadResult", result, &parser, true);
        } catch (RGWXMLDecoder::err& err) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(cct, 5) << "ERROR: unexpected xml: " << str << dendl;
          return set_cr_error(-EIO);
        }
      }

      ldout(cct, 20) << "init multipart result: bucket=" << result.bucket << " key=" << result.key << " upload_id=" << result.upload_id << dendl;

      *upload_id = result.upload_id;

      return set_cr_done();
    }

    return 0;
  }
};

class RGWLCCompleteMultipartCR : public RGWCoroutine {
  CephContext *cct;
  RGWHTTPManager *http_manager;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  bufferlist out_bl;

  string upload_id;

  struct CompleteMultipartReq {
    map<int, rgw_lc_multipart_part_info> parts;

    explicit CompleteMultipartReq(const map<int, rgw_lc_multipart_part_info>& _parts) : parts(_parts) {}

    void dump_xml(Formatter *f) const {
      for (auto p : parts) {
        f->open_object_section("Part");
        encode_xml("PartNumber", p.first, f);
        encode_xml("ETag", p.second.etag, f);
        f->close_section();
      };
    }
  } req_enc;

  struct CompleteMultipartResult {
    string location;
    string bucket;
    string key;
    string etag;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Location", bucket, obj);
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("ETag", etag, obj);
    }
  } result;

public:
  RGWLCCompleteMultipartCR(CephContext *_cct,
                        RGWHTTPManager *_http_manager,
                        RGWRESTConn *_dest_conn,
                        const rgw_obj& _dest_obj,
                        string _upload_id,
                        const map<int, rgw_lc_multipart_part_info>& _parts) : RGWCoroutine(_cct),
                         cct(_cct), http_manager(_http_manager),
                                                   dest_conn(_dest_conn),
                                                   dest_obj(_dest_obj),
                                                   upload_id(_upload_id),
                                                   req_enc(_parts) {}

  int operate() override {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };
        stringstream ss;
        XMLFormatter formatter;

        encode_xml("CompleteMultipartUpload", req_enc, &formatter);

        formatter.flush(ss);

        bufferlist bl;
        bl.append(ss.str());

        call(new RGWPostRawRESTResourceCR <bufferlist> (cct, dest_conn, http_manager,
                                                 obj_to_aws_path(dest_obj), params, nullptr, bl, &out_bl));
      }

      if (retcode < 0) {
        ldout(cct, 0) << "ERROR: failed to initialize multipart upload for dest object=" << dest_obj << dendl;
        return set_cr_error(retcode);
      }
      {
        /*
         * If one of the following fails we cannot abort upload, as we cannot
         * extract the upload id. If one of these fail it's very likely that that's
         * the least of our problem.
         */
        RGWXMLDecoder::XMLParser parser;
        if (!parser.init()) {
          ldout(cct, 0) << "ERROR: failed to initialize xml parser for parsing multipart init response from server" << dendl;
          return set_cr_error(-EIO);
        }

        if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(cct, 5) << "ERROR: failed to parse xml: " << str << dendl;
          return set_cr_error(-EIO);
        }

        try {
          RGWXMLDecoder::decode_xml("CompleteMultipartUploadResult", result, &parser, true);
        } catch (RGWXMLDecoder::err& err) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(cct, 5) << "ERROR: unexpected xml: " << str << dendl;
          return set_cr_error(-EIO);
        }
      }

      ldout(cct, 20) << "complete multipart result: location=" << result.location << " bucket=" << result.bucket << " key=" << result.key << " etag=" << result.etag << dendl;

      return set_cr_done();
    }

    return 0;
  }
};


class RGWLCStreamAbortMultipartUploadCR : public RGWCoroutine {
  lc_op_ctx &oc;
  CephContext *cct;
  RGWHTTPManager *http_manager;
  RGWRESTConn *dest_conn;
  const rgw_obj dest_obj;
  const rgw_raw_obj status_obj;

  string upload_id;

public:

  RGWLCStreamAbortMultipartUploadCR(lc_op_ctx& _oc, CephContext *_cct,
                                RGWHTTPManager *_http_manager,
                                RGWRESTConn *_dest_conn,
                                const rgw_obj& _dest_obj,
                                const rgw_raw_obj& _status_obj,
                                const string& _upload_id) : RGWCoroutine(_cct), oc(_oc), cct(_cct), http_manager(_http_manager),
                                                            dest_conn(_dest_conn),
                                                            dest_obj(_dest_obj),
                                                            status_obj(_status_obj),
                                                            upload_id(_upload_id) {}

  int operate() override {
    reenter(this) {
      yield call(new RGWLCAbortMultipartCR(cct, http_manager, dest_conn, dest_obj, upload_id));
      if (retcode < 0) {
        ldout(oc.cct, 0) << "ERROR: failed to abort multipart upload dest obj=" << dest_obj << " upload_id=" << upload_id << " retcode=" << retcode << dendl;
        /* ignore error, best effort */
      }
#ifdef TODO_STATUS_OBJ
      yield call(new RGWRadosRemoveCR(oc.store, status_obj));
      if (retcode < 0) {
        ldout(oc.cct, 0) << "ERROR: failed to remove sync status obj obj=" << status_obj << " retcode=" << retcode << dendl;
        /* ignore error, best effort */
      }
#endif
      return set_cr_done();
    }

    return 0;
  }
};

class RGWLCStreamObjToCloudMultipartCR : public RGWCoroutine {
    lc_op_ctx& oc;
    RGWRESTCloudTier& tier;
  RGWRESTConn *source_conn;
  rgw_obj src_obj;
  rgw_obj dest_obj;

  uint64_t obj_size;
  string src_etag;
  rgw_rest_obj rest_obj;

  rgw_lc_multipart_upload_info status;

  map<string, string> new_attrs;

  rgw_lc_multipart_part_info *pcur_part_info{nullptr};

  int ret_err{0};

  rgw_raw_obj status_obj;

public:
  RGWLCStreamObjToCloudMultipartCR(lc_op_ctx& _oc, RGWRESTCloudTier& _tier) : RGWCoroutine(_oc.cct), oc(_oc), tier(_tier) {

  }


  int operate() override {
        rgw_lc_obj_properties obj_properties;
        obj_properties.mtime = oc.o.meta.mtime;
        obj_properties.etag = oc.o.meta.etag;
        obj_properties.versioned_epoch = oc.o.versioned_epoch;
        bool init_multipart{false};

        rgw_obj& obj = oc.obj;
        obj_size = oc.o.meta.size;

        rgw_bucket target_bucket;
        target_bucket.name = tier.bucket_name;
        string target_obj_name = obj.key.name; // cross check with aws module
        rgw_obj dest_obj(target_bucket, target_obj_name);

       
    reenter(this) {
#ifdef TODO_STATUS_OBJ
      yield call(new RGWSimpleRadosReadCR<rgw_lc_multipart_upload_info>(oc.async_rados, oc.store->svc()->sysobj,
                                                                 status_obj, &status, false));

      if (retcode < 0 && retcode != -ENOENT) {
        ldout(oc.cct, 0) << "ERROR: failed to read sync status of object " << src_obj << " retcode=" << retcode << dendl;
        return retcode;
      }

      if (retcode >= 0) {
        /* check here that mtime and size did not change */
        if (status.src_properties.mtime != src_properties.mtime || status.obj_size != obj_size ||
            status.src_properties.etag != src_properties.etag) {
          yield call(new RGWLCStreamAbortMultipartUploadCR(oc, oc.cct, tier.http_manager, tier.conn.get(), dest_obj, status_obj, status.upload_id));
          retcode = -ENOENT;
        }
      }

      if (retcode == -ENOENT) {
      }
#endif
      if (!init_multipart) {
               /* Initialize rgw_rest_obj. 
                * Reference: do_decode_rest_obj
                * Check how to copy headers content */ 
                rest_obj.init(obj.key);
                rest_obj.content_len = obj_size;

                rest_obj.acls.set_ctx(oc.cct);
                /*auto aiter = attrs.find(RGW_ATTR_ACL);
                if (aiter != attrs.end()) {
                    bufferlist& bl = aiter->second;
                    auto bliter = bl.cbegin();
                    try {
                        rest_obj.acls.decode(bliter);
                    } catch (buffer::error& err) {
                        ldout(cct, 0) << "ERROR: failed to decode policy off attrs" << dendl;
                        return set_cr_error(-EIO);
                    }
                } else {
                    ldout(cct, 0) << "WARNING: acl attrs not provided" << dendl;
                }*/
                
        RGWLCStreamPutCRF::init_send_attrs(oc.cct, rest_obj, obj_properties, &new_attrs);

        yield call(new RGWLCInitMultipartCR(oc.cct, tier.http_manager, tier.conn.get(), dest_obj, obj_size, std::move(new_attrs), &status.upload_id));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        init_multipart = true;
        status.obj_size = obj_size;
        status.obj_properties = obj_properties;
#define MULTIPART_MAX_PARTS 10000
//        uint64_t min_part_size = obj_size / MULTIPART_MAX_PARTS;
        //status.part_size = std::max(conf.s3.multipart_min_part_size, min_part_size);
        status.part_size = MULTIPART_MIN_POSSIBLE_PART_SIZE;
        status.num_parts = (obj_size + status.part_size - 1) / status.part_size;
        status.cur_part = 1;

        ldout(oc.cct, 0) << "XXXXXXXXX InitMultipart done" << dendl;
      }

      for (; (uint32_t)status.cur_part <= status.num_parts; ++status.cur_part) {
          ldout(oc.cct, 0) << "XXXXXXXXX before calling MultipartpartCR"<<dendl;

          ldout(oc.cct, 0) << "XXXXXXXXX status.cur_part = "<<status.cur_part <<", info.ofs = "<< status.cur_ofs <<", info.size = "<< status.part_size<< ", obj size = " << status.obj_size<<dendl;
        yield {
          rgw_lc_multipart_part_info& cur_part_info = status.parts[status.cur_part];
          cur_part_info.part_num = status.cur_part;
          cur_part_info.ofs = status.cur_ofs;
          cur_part_info.size = std::min((uint64_t)status.part_size, status.obj_size - status.cur_ofs);

          pcur_part_info = &cur_part_info;

          status.cur_ofs += status.part_size;

          call(new RGWLCStreamObjToCloudMultipartPartCR(oc, tier,
                                                             status.upload_id,
                                                             cur_part_info,
                                                             &cur_part_info.etag));
        }

        if (retcode < 0) {
          ldout(oc.cct, 0) << "XXXXXXXXXXXXX ERROR: failed to sync obj=" << oc.obj << ", sync via multipart upload, upload_id=" << status.upload_id << " part number " << status.cur_part << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
          ret_err = retcode;
          yield call(new RGWLCStreamAbortMultipartUploadCR(oc, oc.cct, tier.http_manager, tier.conn.get(), dest_obj, status_obj, status.upload_id));
        ldout(oc.cct, 0) << "XXXXXXXXX AbortMultipart done" << dendl;
          return set_cr_error(ret_err);
        }

#ifdef TODO_STATUS_OBJ
        yield call(new RGWSimpleRadosWriteCR<rgw_lc_multipart_upload_info>(sync_env->async_rados, sync_env->store->svc()->sysobj, status_obj, status));
        if (retcode < 0) {
          ldout(oc.cct, 0) << "XXXXXXX ERROR: failed to store multipart upload state, retcode=" << retcode << dendl;
          /* continue with upload anyway */
        }
#endif
        ldout(oc.cct, 0) << "XXXXXXX sync of object=" << oc.obj << " via multipart upload, finished sending part #" << status.cur_part << " etag=" << pcur_part_info->etag << dendl;
      }

      yield call(new RGWLCCompleteMultipartCR(oc.cct, tier.http_manager, tier.conn.get(), dest_obj, status.upload_id, status.parts));
      if (retcode < 0) {
        ldout(oc.cct, 0) << "XXXXXXXXXX ERROR: failed to complete multipart upload of obj=" << oc.obj << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
        ret_err = retcode;
        yield call(new RGWLCStreamAbortMultipartUploadCR(oc, oc.cct, tier.http_manager, tier.conn.get(), dest_obj, status_obj, status.upload_id));
        return set_cr_error(ret_err);
      }
        ldout(oc.cct, 0) << "XXXXXXXXX CompleteMultipart done" << dendl;

#ifdef TODO_STATUS_OBJ
      /* remove status obj */
      yield call(new RGWRadosRemoveCR(oc.store, status_obj));
      if (retcode < 0) {
        ldout(oc.cct, 0) << "ERROR: failed to abort multipart upload obj=" << oc.obj << " upload_id=" << status.upload_id << " part number " << status.cur_part << " (" << cpp_strerror(-retcode) << ")" << dendl;
        /* ignore error, best effort */
      }
#endif
      return set_cr_done();
    }
    return 0;
  }
};

  class cloudtierCR : public RGWCoroutine {
    lc_op_ctx& oc;
    RGWRESTCloudTier& tier;
    bufferlist out_bl;
    int retcode;
    bool bucket_created = false;
    struct CreateBucketResult {
      string code;

      void decode_xml(XMLObj *obj) {
         RGWXMLDecoder::decode_xml("Code", code, obj);
      }
    } result;

    public:
      cloudtierCR(lc_op_ctx& _oc, RGWRESTCloudTier& _tier)
          : RGWCoroutine(_oc.cct), oc(_oc), tier(_tier) {}

      int operate() override {
        reenter(this) {

          yield {
        // xxx: find if bucket is already created
          ldout(oc.cct,0) << "Cloud_tier: creating bucket " << tier.bucket_name << dendl;
          bufferlist bl;
              call(new RGWPutRawRESTResourceCR <bufferlist> (oc.cct, tier.conn.get(),
                                                  tier.http_manager,
                                                  tier.bucket_name, nullptr, bl, &out_bl));
        }
        if (retcode < 0 ) {
          RGWXMLDecoder::XMLParser parser;
          if (!parser.init()) {
            ldout(oc.cct, 0) << "ERROR: failed to initialize xml parser for parsing create_bucket response from server" << dendl;
            return set_cr_error(retcode);
          }

          if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
            string str(out_bl.c_str(), out_bl.length());
            ldout(oc.cct, 5) << "ERROR: failed to parse xml: " << str << dendl;
            return set_cr_error(retcode);
          }

          try {
            RGWXMLDecoder::decode_xml("Error", result, &parser, true);
          } catch (RGWXMLDecoder::err& err) {
            string str(out_bl.c_str(), out_bl.length());
            ldout(oc.cct, 5) << "ERROR: unexpected xml: " << str << dendl;
            return set_cr_error(retcode);
          }

          if ((result.code != "BucketAlreadyOwnedByYou") &&
                 (result.code != "BucketAlreadyExists")) {
            return set_cr_error(retcode);
          }
        }

        bucket_created = true;

        yield {
            uint64_t size = oc.o.meta.size;
            if (size < DEFAULT_MULTIPART_SYNC_PART_SIZE) {
                call (new RGWLCStreamObjToCloudPlainCR(oc, tier));
            } else {
                call(new RGWLCStreamObjToCloudMultipartCR(oc, tier));

            } 
        }

        if (retcode < 0) {
            return set_cr_error(retcode);
        }

        return set_cr_done();
       } //reenter

        return 0;
      }
  };

class LCOpAction_Transition : public LCOpAction {
  const transition_action& transition;
  bool need_to_process{false};

protected:
  virtual bool check_current_state(bool is_current) = 0;
  virtual ceph::real_time get_effective_mtime(lc_op_ctx& oc) = 0;
public:
  LCOpAction_Transition(const transition_action& _transition) : transition(_transition) {}

  bool check(lc_op_ctx& oc, ceph::real_time *exp_time) override {
    auto& o = oc.o;

    if (o.is_delete_marker()) {
      return false;
    }

    if (!check_current_state(o.is_current())) {
      return false;
    }

    auto mtime = get_effective_mtime(oc);
    bool is_expired;
    if (transition.days <= 0) {
      if (transition.date == boost::none) {
        ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": no transition day/date set in rule, skipping" << dendl;
        return false;
      }
      is_expired = ceph_clock_now() >= ceph::real_clock::to_time_t(*transition.date);
      *exp_time = *transition.date;
    } else {
      is_expired = obj_has_expired(oc.cct, mtime, transition.days, exp_time);
    }

    ldout(oc.cct, 20) << __func__ << "(): key=" << o.key << ": is_expired=" << is_expired << dendl;

    need_to_process = (rgw_placement_rule::get_canonical_storage_class(o.meta.storage_class) != transition.storage_class);

    return is_expired;
  }

  bool should_process() override {
    return need_to_process;
  }

  int transition_obj_to_cloud(lc_op_ctx& oc, RGWZoneGroupPlacementTier &tier) {
    std::shared_ptr<RGWRESTConn> conn;

    /* init */
    string id = "cloudid";

/*    string endpoint="http://localhost:8002";
    string access_key="CNV15CC2ZN44IPB24A5X";
    string secret="W0X3NbPXNW1B7Ru79xuuUI53ftSKieEl2ouuHP8C"; */

/*    string endpoint="http://10.19.54.249:8000";
    string access_key="0555b35654ad1656d804";
    string secret="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="; */

/*    string endpoint="https://10.8.152.14:30633"; 
    string access_key="lwqBSJcbIlNTJAdYqei4";
    string secret="wVI+7joJ/7bx8J56gv48de9Or/CJU/hnyaP8vOp7";*/

    string bucket_name="cloud-bucket"; 

    string endpoint = tier.endpoint;
    RGWAccessKey key = tier.key;
    HostStyle host_style = PathStyle;

    ldpp_dout(oc.dpp, 0) << "Tierrrrr endpoint:" << tier.endpoint << ", key.access_key= " << key.id << ", key.secret= " << key.key<<dendl;
//    key = RGWAccessKey(access_key, secret);

    conn.reset(new S3RESTConn(oc.cct, oc.store->svc()->zone,
                                id, { endpoint }, key, host_style));


    /* http_mngr */
    RGWCoroutinesManager crs(oc.store->ctx(), oc.store->getRados()->get_cr_registry());
    RGWHTTPManager http_manager(oc.store->ctx(), crs.get_completion_mgr());
    int ret = http_manager.start();
    if (ret < 0) {
        ldpp_dout(oc.dpp, 0) << "failed in http_manager.start() ret=" << ret << dendl;
        return ret;
    }

    RGWRESTCloudTier cloudtier;
    cloudtier.conn = conn;
    cloudtier.bucket_name = bucket_name;
    cloudtier.http_manager= &http_manager;

    ret = crs.run(new cloudtierCR(oc, cloudtier));
    http_manager.stop();
         
    if (ret < 0) {
        ldpp_dout(oc.dpp, 0) << "failed in cloudtierCR() ret=" << ret << dendl;
        return ret;
    }

    return 0;
  }

/* find out if the the storage class is remote cloud */
int get_tier_target(const RGWZoneGroup &zonegroup, rgw_placement_rule rule,
        string storage_class, RGWZoneGroupPlacementTier &tier) {
  std::map<std::string, RGWZoneGroupPlacementTarget>::const_iterator titer;
    titer = zonegroup.placement_targets.find(rule.name);
    if (titer == zonegroup.placement_targets.end()) {
      //ldout(cct, 0) << "could not find requested placement id " << rule << " within zonegroup " << dendl;
      return -1;
    }

  if (storage_class.empty()) {
      storage_class = rule.storage_class;
  }

  const auto& target_rule = titer->second;
  std::map<std::string, RGWZoneGroupPlacementTier>::const_iterator ttier;
  ttier = target_rule.tier_targets.find(storage_class);
  if (ttier != target_rule.tier_targets.end()) {
      tier = ttier->second;
  }

  return 0;
}

  int process(lc_op_ctx& oc) {
    auto& o = oc.o;
    int r;
    std::string tier_type = ""; 

    const RGWZoneGroup& zonegroup = oc.store->svc()->zone->get_zonegroup();
/*    r = zonegroup.init(oc.cct, oc.store->svc()->sysobj, false);

    if (r < 0) {
         ldpp_dout(oc.dpp, 0) << "ERROR: failed to init zonegroup(r=" << r << ")"
                           << dendl;
         return r;
    }*/

    RGWZoneGroupPlacementTier tier = {};

    rgw_placement_rule target_placement;
    target_placement.inherit_from(oc.bucket_info.placement_rule);
    target_placement.storage_class = transition.storage_class;

    r = get_tier_target(zonegroup, target_placement, target_placement.storage_class, tier);

    if (tier.tier_type == "cloud") {
    /* XXX: decision to be taken based on lc profile */
       r = transition_obj_to_cloud(oc, tier);
       if (r < 0) {
         ldpp_dout(oc.dpp, 0) << "ERROR: failed to transition obj to cloud (r=" << r << ")"
                           << dendl;
       }
    } else {

    if (!oc.store->svc()->zone->get_zone_params().valid_placement(target_placement)) {
      ldpp_dout(oc.dpp, 0) << "ERROR: non existent dest placement: " << target_placement
                           << " bucket="<< oc.bucket_info.bucket
                           << " rule_id=" << oc.op.id << dendl;
      return -EINVAL;
    }
    }

    r = oc.store->getRados()->transition_obj(oc.rctx, oc.bucket_info, oc.obj,
                                     target_placement, o.meta.mtime, o.versioned_epoch, oc.dpp, null_yield);
    if (r < 0) {
      ldpp_dout(oc.dpp, 0) << "ERROR: failed to transition obj (r=" << r << ")" << dendl;
      return r;
    }
    ldpp_dout(oc.dpp, 2) << "TRANSITIONED:" << oc.bucket_info.bucket << ":" << o.key << " -> " << transition.storage_class << dendl;
    return 0;
  }
};

class LCOpAction_CurrentTransition : public LCOpAction_Transition {
protected:
  bool check_current_state(bool is_current) override {
    return is_current;
  }

  ceph::real_time get_effective_mtime(lc_op_ctx& oc) override {
    return oc.o.meta.mtime;
  }
public:
  LCOpAction_CurrentTransition(const transition_action& _transition) : LCOpAction_Transition(_transition) {}
};

class LCOpAction_NonCurrentTransition : public LCOpAction_Transition {
protected:
  bool check_current_state(bool is_current) override {
    return !is_current;
  }

  ceph::real_time get_effective_mtime(lc_op_ctx& oc) override {
    return oc.ol.get_prev_obj().meta.mtime;
  }
public:
  LCOpAction_NonCurrentTransition(const transition_action& _transition) : LCOpAction_Transition(_transition) {}
};

void LCOpRule::build()
{
  filters.emplace_back(new LCOpFilter_Tags);

  auto& op = env.op;

  if (op.expiration > 0 ||
      op.expiration_date != boost::none) {
    actions.emplace_back(new LCOpAction_CurrentExpiration);
  }

  if (op.dm_expiration) {
    actions.emplace_back(new LCOpAction_DMExpiration);
  }

  if (op.noncur_expiration > 0) {
    actions.emplace_back(new LCOpAction_NonCurrentExpiration);
  }

  for (auto& iter : op.transitions) {
    actions.emplace_back(new LCOpAction_CurrentTransition(iter.second));
  }

  for (auto& iter : op.noncur_transitions) {
    actions.emplace_back(new LCOpAction_NonCurrentTransition(iter.second));
  }
}

int LCOpRule::process(rgw_bucket_dir_entry& o, const DoutPrefixProvider *dpp)
{
  lc_op_ctx ctx(env, o, dpp);

  unique_ptr<LCOpAction> *selected = nullptr;
  real_time exp;

  for (auto& a : actions) {
    real_time action_exp;

    if (a->check(ctx, &action_exp)) {
      if (action_exp > exp) {
        exp = action_exp;
        selected = &a;
      }
    }
  }

  if (selected &&
      (*selected)->should_process()) {

    /*
     * Calling filter checks after action checks because
     * all action checks (as they are implemented now) do
     * not access the objects themselves, but return result
     * from info from bucket index listing. The current tags filter
     * check does access the objects, so we avoid unnecessary rados calls
     * having filters check later in the process.
     */

    bool cont = false;
    for (auto& f : filters) {
      if (f->check(ctx)) {
        cont = true;
        break;
      }
    }

    if (!cont) {
      ldpp_dout(dpp, 20) << __func__ << "(): key=" << o.key << ": no rule match, skipping" << dendl;
      return 0;
    }

    int r = (*selected)->process(ctx);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: remove_expired_obj " << dendl;
      return r;
    }
    ldpp_dout(dpp, 20) << "processed:" << env.bucket_info.bucket << ":" << o.key << dendl;
  }

  return 0;

}

int RGWLC::bucket_lc_process(string& shard_id)
{
  RGWLifecycleConfiguration  config(cct);
  RGWBucketInfo bucket_info;
  map<string, bufferlist> bucket_attrs;
  string no_ns, list_versions;
  vector<rgw_bucket_dir_entry> objs;
  vector<std::string> result;
  boost::split(result, shard_id, boost::is_any_of(":"));
  string bucket_tenant = result[0];
  string bucket_name = result[1];
  string bucket_marker = result[2];
  int ret = store->getRados()->get_bucket_info(store->svc(), bucket_tenant, bucket_name, bucket_info, NULL, null_yield, &bucket_attrs);
  if (ret < 0) {
    ldpp_dout(this, 0) << "LC:get_bucket_info for " << bucket_name << " failed" << dendl;
    return ret;
  }

  if (bucket_info.bucket.marker != bucket_marker) {
    ldpp_dout(this, 1) << "LC: deleting stale entry found for bucket=" << bucket_tenant
                       << ":" << bucket_name << " cur_marker=" << bucket_info.bucket.marker
                       << " orig_marker=" << bucket_marker << dendl;
    return -ENOENT;
  }

  RGWRados::Bucket target(store->getRados(), bucket_info);

  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == bucket_attrs.end())
    return 0;

  bufferlist::const_iterator iter{&aiter->second};
  try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      ldpp_dout(this, 0) << __func__ <<  "() decode life cycle config failed" << dendl;
      return -1;
    }

  multimap<string, lc_op>& prefix_map = config.get_prefix_map();

  ldpp_dout(this, 10) << __func__ <<  "() prefix_map size="
		      << prefix_map.size()
		      << dendl;

  rgw_obj_key pre_marker;
  rgw_obj_key next_marker;
  for(auto prefix_iter = prefix_map.begin(); prefix_iter != prefix_map.end(); ++prefix_iter) {
    auto& op = prefix_iter->second;
    if (!is_valid_op(op)) {
      continue;
    }
    ldpp_dout(this, 20) << __func__ << "(): prefix=" << prefix_iter->first << dendl;
    if (prefix_iter != prefix_map.begin() && 
        (prefix_iter->first.compare(0, prev(prefix_iter)->first.length(), prev(prefix_iter)->first) == 0)) {
      next_marker = pre_marker;
    } else {
      pre_marker = next_marker;
    }

    LCObjsLister ol(store, bucket_info);
    ol.set_prefix(prefix_iter->first);

    ret = ol.init();

    if (ret < 0) {
      if (ret == (-ENOENT))
        return 0;
      ldpp_dout(this, 0) << "ERROR: store->list_objects():" <<dendl;
      return ret;
    }

    op_env oenv(op, store, this, bucket_info, ol);

    LCOpRule orule(oenv);

    orule.build();

    rgw_bucket_dir_entry o;
    for (; ol.get_obj(&o); ol.next()) {
      ldpp_dout(this, 20) << __func__ << "(): key=" << o.key << dendl;
      int ret = orule.process(o, this);
      if (ret < 0) {
        ldpp_dout(this, 20) << "ERROR: orule.process() returned ret="
			    << ret
			    << dendl;
      }

      if (going_down()) {
        return 0;
      }
    }
  }

  ret = handle_multipart_expiration(&target, prefix_map);

  return ret;
}

int RGWLC::bucket_lc_post(int index, int max_lock_sec, pair<string, int >& entry, int& result)
{
  utime_t lock_duration(cct->_conf->rgw_lc_lock_max_time, 0);

  rados::cls::lock::Lock l(lc_index_lock_name);
  l.set_cookie(cookie);
  l.set_duration(lock_duration);

  do {
    int ret = l.lock_exclusive(&store->getRados()->lc_pool_ctx, obj_names[index]);
    if (ret == -EBUSY || ret == -EEXIST) { /* already locked by another lc processor */
      ldpp_dout(this, 0) << "RGWLC::bucket_lc_post() failed to acquire lock on "
          << obj_names[index] << ", sleep 5, try again" << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;
    ldpp_dout(this, 20) << "RGWLC::bucket_lc_post() lock " << obj_names[index] << dendl;
    if (result ==  -ENOENT) {
      ret = cls_rgw_lc_rm_entry(store->getRados()->lc_pool_ctx, obj_names[index],  entry);
      if (ret < 0) {
        ldpp_dout(this, 0) << "RGWLC::bucket_lc_post() failed to remove entry "
            << obj_names[index] << dendl;
      }
      goto clean;
    } else if (result < 0) {
      entry.second = lc_failed;
    } else {
      entry.second = lc_complete;
    }

    ret = cls_rgw_lc_set_entry(store->getRados()->lc_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to set entry on "
          << obj_names[index] << dendl;
    }
clean:
    l.unlock(&store->getRados()->lc_pool_ctx, obj_names[index]);
    ldpp_dout(this, 20) << "RGWLC::bucket_lc_post() unlock " << obj_names[index] << dendl;
    return 0;
  } while (true);
}

int RGWLC::list_lc_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map)
{
  int index = 0;
  progress_map->clear();
  for(; index <max_objs; index++) {
    map<string, int > entries;
    int ret = cls_rgw_lc_list(store->getRados()->lc_pool_ctx, obj_names[index], marker, max_entries, entries);
    if (ret < 0) {
      if (ret == -ENOENT) {
        ldpp_dout(this, 10) << __func__ << "() ignoring unfound lc object="
                             << obj_names[index] << dendl;
        continue;
      } else {
        return ret;
      }
    }
    map<string, int>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      progress_map->insert(*iter);
    }
  }
  return 0;
}

int RGWLC::process()
{
  int max_secs = cct->_conf->rgw_lc_lock_max_time;

  const int start = ceph::util::generate_random_number(0, max_objs - 1);

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process(index, max_secs);
    if (ret < 0)
      return ret;
  }

  return 0;
}

int RGWLC::process(int index, int max_lock_secs)
{
  rados::cls::lock::Lock l(lc_index_lock_name);
  do {
    utime_t now = ceph_clock_now();
    pair<string, int > entry;//string = bucket_name:bucket_id ,int = LC_BUCKET_STATUS
    if (max_lock_secs <= 0)
      return -EAGAIN;

    utime_t time(max_lock_secs, 0);
    l.set_duration(time);

    int ret = l.lock_exclusive(&store->getRados()->lc_pool_ctx, obj_names[index]);
    if (ret == -EBUSY || ret == -EEXIST) { /* already locked by another lc processor */
      ldpp_dout(this, 0) << "RGWLC::process() failed to acquire lock on "
          << obj_names[index] << ", sleep 5, try again" << dendl;
      sleep(5);
      continue;
    }
    if (ret < 0)
      return 0;

    cls_rgw_lc_obj_head head;
    ret = cls_rgw_lc_get_head(store->getRados()->lc_pool_ctx, obj_names[index], head);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to get obj head "
          << obj_names[index] << ", ret=" << ret << dendl;
      goto exit;
    }

    if(!if_already_run_today(head.start_date)) {
      head.start_date = now;
      head.marker.clear();
      ret = bucket_lc_prepare(index);
      if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to update lc object "
          << obj_names[index] << ", ret=" << ret << dendl;
      goto exit;
      }
    }

    ret = cls_rgw_lc_get_next_entry(store->getRados()->lc_pool_ctx, obj_names[index], head.marker, entry);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to get obj entry "
          << obj_names[index] << dendl;
      goto exit;
    }

    if (entry.first.empty())
      goto exit;

    entry.second = lc_processing;
    ret = cls_rgw_lc_set_entry(store->getRados()->lc_pool_ctx, obj_names[index],  entry);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to set obj entry " << obj_names[index]
          << " (" << entry.first << "," << entry.second << ")" << dendl;
      goto exit;
    }

    head.marker = entry.first;
    ret = cls_rgw_lc_put_head(store->getRados()->lc_pool_ctx, obj_names[index],  head);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWLC::process() failed to put head " << obj_names[index] << dendl;
      goto exit;
    }
    l.unlock(&store->getRados()->lc_pool_ctx, obj_names[index]);
    ret = bucket_lc_process(entry.first);
    bucket_lc_post(index, max_lock_secs, entry, ret);
  }while(1);

exit:
    l.unlock(&store->getRados()->lc_pool_ctx, obj_names[index]);
    return 0;
}

void RGWLC::start_processor()
{
  worker = new LCWorker(this, cct, this);
  worker->create("lifecycle_thr");
}

void RGWLC::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}


unsigned RGWLC::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWLC::gen_prefix(std::ostream& out) const
{
  return out << "lifecycle: ";
}

void RGWLC::LCWorker::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}

bool RGWLC::going_down()
{
  return down_flag;
}

bool RGWLC::LCWorker::should_work(utime_t& now)
{
  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_lifecycle_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",&start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);

  if (cct->_conf->rgw_lc_debug_interval > 0) {
	  /* We're debugging, so say we can run */
	  return true;
  } else if ((bdt.tm_hour*60 + bdt.tm_min >= start_hour*60 + start_minute) &&
		     (bdt.tm_hour*60 + bdt.tm_min <= end_hour*60 + end_minute)) {
	  return true;
  } else {
	  return false;
  }

}

int RGWLC::LCWorker::schedule_next_start_time(utime_t &start, utime_t& now)
{
  int secs;

  if (cct->_conf->rgw_lc_debug_interval > 0) {
	secs = start + cct->_conf->rgw_lc_debug_interval - now;
	if (secs < 0)
	  secs = 0;
	return (secs);
  }

  int start_hour;
  int start_minute;
  int end_hour;
  int end_minute;
  string worktime = cct->_conf->rgw_lifecycle_work_time;
  sscanf(worktime.c_str(),"%d:%d-%d:%d",&start_hour, &start_minute, &end_hour, &end_minute);
  struct tm bdt;
  time_t tt = now.sec();
  time_t nt;
  localtime_r(&tt, &bdt);
  bdt.tm_hour = start_hour;
  bdt.tm_min = start_minute;
  bdt.tm_sec = 0;
  nt = mktime(&bdt);
  secs = nt - tt;

  return secs>0 ? secs : secs+24*60*60;
}

void RGWLifecycleConfiguration::generate_test_instances(list<RGWLifecycleConfiguration*>& o)
{
  o.push_back(new RGWLifecycleConfiguration);
}

void get_lc_oid(CephContext *cct, const string& shard_id, string *oid)
{
  int max_objs = (cct->_conf->rgw_lc_max_objs > HASH_PRIME ? HASH_PRIME : cct->_conf->rgw_lc_max_objs);
  int index = ceph_str_hash_linux(shard_id.c_str(), shard_id.size()) % HASH_PRIME % max_objs;
  *oid = lc_oid_prefix;
  char buf[32];
  snprintf(buf, 32, ".%d", index);
  oid->append(buf);
  return;
}



static std::string get_lc_shard_name(const rgw_bucket& bucket){
  return string_join_reserve(':', bucket.tenant, bucket.name, bucket.marker);
}

template<typename F>
static int guard_lc_modify(RGWRadosStore* store, const rgw_bucket& bucket, const string& cookie, const F& f) {
  CephContext *cct = store->ctx();

  string shard_id = get_lc_shard_name(bucket);

  string oid; 
  get_lc_oid(cct, shard_id, &oid);

  pair<string, int> entry(shard_id, lc_uninitial);
  int max_lock_secs = cct->_conf->rgw_lc_lock_max_time;

  rados::cls::lock::Lock l(lc_index_lock_name); 
  utime_t time(max_lock_secs, 0);
  l.set_duration(time);
  l.set_cookie(cookie);

  librados::IoCtx *ctx = store->getRados()->get_lc_pool_ctx();
  int ret;

  do {
    ret = l.lock_exclusive(ctx, oid);
    if (ret == -EBUSY || ret == -EEXIST) {
      ldout(cct, 0) << "RGWLC::RGWPutLC() failed to acquire lock on "
          << oid << ", sleep 5, try again" << dendl;
      sleep(5); // XXX: return retryable error
      continue;
    }
    if (ret < 0) {
      ldout(cct, 0) << "RGWLC::RGWPutLC() failed to acquire lock on "
          << oid << ", ret=" << ret << dendl;
      break;
    }
    ret = f(ctx, oid, entry);
    if (ret < 0) {
      ldout(cct, 0) << "RGWLC::RGWPutLC() failed to set entry on "
          << oid << ", ret=" << ret << dendl;
    }
    break;
  } while(true);
  l.unlock(ctx, oid);
  return ret;
}

int RGWLC::set_bucket_config(RGWBucketInfo& bucket_info,
                         const map<string, bufferlist>& bucket_attrs,
                         RGWLifecycleConfiguration *config)
{
  map<string, bufferlist> attrs = bucket_attrs;
  bufferlist lc_bl;
  config->encode(lc_bl);

  attrs[RGW_ATTR_LC] = std::move(lc_bl);

  int ret = store->ctl()->bucket->set_bucket_instance_attrs(bucket_info, attrs,
							 &bucket_info.objv_tracker,
							 null_yield);
  if (ret < 0)
    return ret;

  rgw_bucket& bucket = bucket_info.bucket;


  ret = guard_lc_modify(store, bucket, cookie, [&](librados::IoCtx *ctx, const string& oid,
                                                   const pair<string, int>& entry) {
    return cls_rgw_lc_set_entry(*ctx, oid, entry);
  });

  return ret;
}

int RGWLC::remove_bucket_config(RGWBucketInfo& bucket_info,
                                const map<string, bufferlist>& bucket_attrs)
{
  map<string, bufferlist> attrs = bucket_attrs;
  attrs.erase(RGW_ATTR_LC);
  int ret = store->ctl()->bucket->set_bucket_instance_attrs(bucket_info, attrs,
							 &bucket_info.objv_tracker,
							 null_yield);

  rgw_bucket& bucket = bucket_info.bucket;

  if (ret < 0) {
    ldout(cct, 0) << "RGWLC::RGWDeleteLC() failed to set attrs on bucket="
        << bucket.name << " returned err=" << ret << dendl;
    return ret;
  }


  ret = guard_lc_modify(store, bucket, cookie, [&](librados::IoCtx *ctx, const string& oid,
                                                   const pair<string, int>& entry) {
    return cls_rgw_lc_rm_entry(*ctx, oid, entry);
  });

  return ret;
}

namespace rgw::lc {

int fix_lc_shard_entry(rgw::sal::RGWRadosStore* store, const RGWBucketInfo& bucket_info,
		       const map<std::string,bufferlist>& battrs)
{
  if (auto aiter = battrs.find(RGW_ATTR_LC);
      aiter == battrs.end()) {
    return 0;    // No entry, nothing to fix
  }

  auto shard_name = get_lc_shard_name(bucket_info.bucket);
  std::string lc_oid;
  get_lc_oid(store->ctx(), shard_name, &lc_oid);

  rgw_lc_entry_t entry;
  // There are multiple cases we need to encounter here
  // 1. entry exists and is already set to marker, happens in plain buckets & newly resharded buckets
  // 2. entry doesn't exist, which usually happens when reshard has happened prior to update and next LC process has already dropped the update
  // 3. entry exists matching the current bucket id which was after a reshard (needs to be updated to the marker)
  // We are not dropping the old marker here as that would be caught by the next LC process update
  auto lc_pool_ctx = store->getRados()->get_lc_pool_ctx();
  int ret = cls_rgw_lc_get_entry(*lc_pool_ctx,
				 lc_oid, shard_name, entry);
  if (ret == 0) {
    ldout(store->ctx(), 5) << "Entry already exists, nothing to do" << dendl;
    return ret; // entry is already existing correctly set to marker
  }
  ldout(store->ctx(), 5) << "cls_rgw_lc_get_entry errored ret code=" << ret << dendl;
  if (ret == -ENOENT) {
    ldout(store->ctx(), 1) << "No entry for bucket=" << bucket_info.bucket.name
			   << " creating " << dendl;
    // TODO: we have too many ppl making cookies like this!
    char cookie_buf[COOKIE_LEN + 1];
    gen_rand_alphanumeric(store->ctx(), cookie_buf, sizeof(cookie_buf) - 1);
    std::string cookie = cookie_buf;

    ret = guard_lc_modify(store, bucket_info.bucket, cookie,
			  [&lc_pool_ctx, &lc_oid](librados::IoCtx *ctx, const string& oid,
					    const pair<string, int>& entry) {
			    return cls_rgw_lc_set_entry(*lc_pool_ctx,
							lc_oid, entry);
			  });

  }

  return ret;
}

std::string s3_expiration_header(
  DoutPrefixProvider* dpp,
  const rgw_obj_key& obj_key,
  const RGWObjTags& obj_tagset,
  const ceph::real_time& mtime,
  const std::map<std::string, buffer::list>& bucket_attrs)
{
  CephContext* cct = dpp->get_cct();
  RGWLifecycleConfiguration config(cct);
  std::string hdr{""};

  const auto& aiter = bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == bucket_attrs.end())
    return hdr;

  bufferlist::const_iterator iter{&aiter->second};
  try {
      config.decode(iter);
  } catch (const buffer::error& e) {
      ldpp_dout(dpp, 0) << __func__
			<<  "() decode life cycle config failed"
			<< dendl;
      return hdr;
  } /* catch */

  /* dump tags at debug level 16 */
  RGWObjTags::tag_map_t obj_tag_map = obj_tagset.get_tags();
  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 16)) {
    for (const auto& elt : obj_tag_map) {
      ldout(cct, 16) << __func__
		     <<  "() key=" << elt.first << " val=" << elt.second
		     << dendl;
    }
  }

  boost::optional<ceph::real_time> expiration_date;
  boost::optional<std::string> rule_id;

  const auto& rule_map = config.get_rule_map();
  for (const auto& ri : rule_map) {
    const auto& rule = ri.second;
    auto& id = rule.get_id();
    auto& prefix = rule.get_prefix();
    auto& filter = rule.get_filter();
    auto& expiration = rule.get_expiration();
    auto& noncur_expiration = rule.get_noncur_expiration();

    ldpp_dout(dpp, 10) << "rule: " << ri.first
		       << " prefix: " << prefix
		       << " expiration: "
		       << " date: " << expiration.get_date()
		       << " days: " << expiration.get_days()
		       << " noncur_expiration: "
		       << " date: " << noncur_expiration.get_date()
		       << " days: " << noncur_expiration.get_days()
		       << dendl;

    /* skip if rule !enabled
     * if rule has prefix, skip iff object !match prefix
     * if rule has tags, skip iff object !match tags
     * note if object is current or non-current, compare accordingly
     * if rule has days, construct date expression and save iff older
     * than last saved
     * if rule has date, convert date expression and save iff older
     * than last saved
     * if the date accum has a value, format it into hdr
     */

    if (! rule.is_enabled())
      continue;

    if(! prefix.empty()) {
      if (! boost::starts_with(obj_key.name, prefix))
	continue;
    }

    if (filter.has_tags()) {
      bool tag_match = false;
      const RGWObjTags& rule_tagset = filter.get_tags();
      for (auto& tag : rule_tagset.get_tags()) {
	/* remember, S3 tags are {key,value} tuples */
	auto ma1 = obj_tag_map.find(tag.first);
	if ( ma1 != obj_tag_map.end()) {
	  if (tag.second == ma1->second) {
	    ldpp_dout(dpp, 10) << "tag match obj_key=" << obj_key
			       << " rule_id=" << id
			       << " tag=" << tag
			       << " (ma=" << *ma1 << ")"
			       << dendl;
	    tag_match = true;
	    break;
	  }
	}
      }
      if (! tag_match)
	continue;
    }

    // compute a uniform expiration date
    boost::optional<ceph::real_time> rule_expiration_date;
    const LCExpiration& rule_expiration =
      (obj_key.instance.empty()) ? expiration : noncur_expiration;

    if (rule_expiration.has_date()) {
      rule_expiration_date =
	boost::optional<ceph::real_time>(
	  ceph::from_iso_8601(rule.get_expiration().get_date()));
      rule_id = boost::optional<std::string>(id);
    } else {
      if (rule_expiration.has_days()) {
	rule_expiration_date =
	  boost::optional<ceph::real_time>(
	    mtime + make_timespan(rule_expiration.get_days()*24*60*60));
	rule_id = boost::optional<std::string>(id);
      }
    }

    // update earliest expiration
    if (rule_expiration_date) {
      if ((! expiration_date) ||
	  (*expiration_date < *rule_expiration_date)) {
      expiration_date =
	boost::optional<ceph::real_time>(rule_expiration_date);
      }
    }
  }

  // cond format header
  if (expiration_date && rule_id) {
    // Fri, 23 Dec 2012 00:00:00 GMT
    char exp_buf[100];
    time_t exp = ceph::real_clock::to_time_t(*expiration_date);
    if (std::strftime(exp_buf, sizeof(exp_buf),
		      "%a, %d %b %Y %T %Z", std::gmtime(&exp))) {
      hdr = fmt::format("expiry-date=\"{0}\", rule-id=\"{1}\"", exp_buf,
			*rule_id);
    } else {
      ldpp_dout(dpp, 0) << __func__ <<
	"() strftime of life cycle expiration header failed"
			<< dendl;
    }
  }

  return hdr;

} /* rgwlc_s3_expiration_header */

} /* namespace rgw::lc */
