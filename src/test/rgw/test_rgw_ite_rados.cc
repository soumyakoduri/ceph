// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * ceph_test_rgw_ite: the RADOS implementation of the ITE SAL contracts,
 * and the workers running over it, against a running cluster (vstart).
 * Each test works in its own pool, so runs never see each other's state.
 */

#include <gtest/gtest.h>
#include <fmt/format.h>

#include "cls/rgw/cls_rgw_client.h"
#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/ceph_json.h"
#include "global/global_init.h"
#include "include/scope_guard.h"
#include "rgw_common.h"
#include "rgw_ite.h"
#include "rgw_ite_admin.h"
#include "rgw_ite_aggregator.h"
#include "rgw_ite_decision.h"
#include "rgw_ite_fft_detector.h"
#include "rgw_ite_mover.h"
#include "rgw_ite_temperature.h"
#include "rgw_ite_types.h"
#include "rgw_zone.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_sal_rados.h"
#include "rgw_sal_rados_ite.h"

#define dout_subsys ceph_subsys_rgw

using namespace rgw::ite;
namespace sal = rgw::sal;

namespace {

sal::Driver* driver = nullptr;
DoutPrefix* dpp = nullptr;
rgw::SiteConfig* site = nullptr;

struct ITERados : public ::testing::Test {
  std::string saved_pool;
  std::map<std::string, std::string> saved;

  void SetUp() override {
    // a private pool per test run keeps reruns independent
    saved_pool = g_conf().get_val<std::string>("rgw_ite_pool");
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    g_conf().set_val_or_die("rgw_ite_pool", fmt::format(
        "ite-test.{}.{}", info->name(), ceph::real_clock::now().time_since_epoch().count()));
  }
  void TearDown() override {
    auto rados = static_cast<sal::RadosStore*>(driver)->getRados()->get_rados_handle();
    rados->pool_delete(g_conf().get_val<std::string>("rgw_ite_pool").c_str());
    g_conf().set_val_or_die("rgw_ite_pool", saved_pool);
    for (const auto& [k, v] : saved) {
      g_conf().set_val_or_die(k, v);
    }
  }

  void set_conf(const std::string& key, const std::string& val) {
    std::string old;
    ASSERT_EQ(0, g_conf().get_val(key, &old));
    saved.try_emplace(key, old);
    g_conf().set_val_or_die(key, val);
  }
};

AccessEvent event(const rgw_bucket& b, const std::string& name, Op op)
{
  AccessEvent e;
  e.bucket = b;
  e.obj = InstanceId(name, "", 5);
  e.timestamp = ceph::real_clock::now();
  e.op = op;
  return e;
}

/* a bucket created for one test, with index entries written the way a
 * completed PUT leaves them but without any data */
struct TestBucket {
  std::unique_ptr<sal::Bucket> bucket;
  std::map<std::string, uint64_t> entries; // name -> newest epoch

  int create(const std::string& prefix, uint32_t shards, bool ite) {
    rgw_bucket b;
    b.name = fmt::format("{}-{}", prefix, ceph::real_clock::now().time_since_epoch().count());
    int r = driver->load_bucket(dpp, b, &bucket, null_yield);
    if (!bucket) {
      return r < 0 ? r : -EIO;
    }
    sal::Bucket::CreateParams params;
    params.owner = rgw_user{"", "ceph-test-rgw-ite"};
    params.zonegroup_id = site->get_zonegroup().get_id();
    params.placement_rule = site->get_zonegroup().default_placement;
    params.zone_placement = rgw::find_zone_placement(
        dpp, site->get_zone_params(), params.placement_rule);
    params.index_shards = shards;
    if (ite) {
      BucketConfig c;
      c.enabled = true;
      encode(c, params.attrs[std::string(ATTR_ITE)]);
    }
    r = bucket->create(dpp, params, null_yield);
    if (r < 0) {
      return r;
    }
    // reload, so the bucket carries its stored info and attributes
    const rgw_bucket key = bucket->get_key();
    return driver->load_bucket(dpp, key, &bucket, null_yield);
  }

  ~TestBucket() {
    if (!bucket) {
      return;
    }
    // the entries have no head objects for the bucket removal to delete,
    // so drop them from the index first; all of it best effort
    for (const auto& [name, epoch] : entries) {
      index_op(CLS_RGW_OP_DEL, name, epoch + 1, 0, ceph::real_clock::now());
    }
    bucket->remove(dpp, false, null_yield);
  }

  int add_entry(const std::string& name, uint64_t epoch, uint64_t size,
                ceph::real_time mtime = ceph::real_clock::now()) {
    int r = index_op(CLS_RGW_OP_ADD, name, epoch, size, mtime);
    if (r == 0) {
      entries[name] = epoch;
    }
    return r;
  }

  int index_op(RGWModifyOp op, const std::string& name, uint64_t epoch, uint64_t size,
               ceph::real_time mtime) {
    auto rados = static_cast<sal::RadosStore*>(driver)->getRados();
    rgw_obj obj(bucket->get_key(), rgw_obj_key(name));
    RGWRados::BucketShard bs(rados);
    int r = bs.init(dpp, bucket->get_info(), obj, null_yield);
    if (r < 0) {
      return r;
    }
    const cls_rgw_obj_key key(rgw_obj_key(name).get_index_key_name());
    const std::string tag = fmt::format("ite-test-{}-{}", name, epoch);
    librados::ObjectWriteOperation prepare;
    cls_rgw_bucket_prepare_op(prepare, op, tag, key, "");
    r = bs.bucket_obj.ioctx.operate(bs.bucket_obj.obj.oid, &prepare);
    if (r < 0) {
      return r;
    }
    // the epoch is the write version a real PUT would carry; it must grow
    rgw_bucket_entry_ver ver;
    ver.pool = bs.bucket_obj.ioctx.get_id();
    ver.epoch = epoch;
    rgw_bucket_dir_entry_meta meta;
    meta.category = RGWObjCategory::Main;
    meta.size = size;
    meta.accounted_size = size;
    meta.mtime = mtime;
    meta.storage_class = "INTELLIGENT_TIER";
    librados::ObjectWriteOperation complete;
    cls_rgw_bucket_complete_op(complete, op, tag, ver, key, meta,
                               nullptr, false, 0, nullptr);
    return bs.bucket_obj.ioctx.operate(bs.bucket_obj.obj.oid, &complete);
  }
};

/* the zone's default placement given an INTELLIGENT_TIER class with a
 * hot and a cold pool, for this process only */
struct TieredPlacement {
  RGWZoneParams& zone;
  const std::string rule;
  std::optional<RGWZoneStorageClass> saved;
  // fixed pools, left behind, so that the gc can still reclaim the copies
  // a move leaves for it
  const rgw_pool hot{"ite-test.hot"};
  const rgw_pool cold{"ite-test.cold"};

  TieredPlacement()
    : zone(const_cast<RGWZoneParams&>(
          static_cast<sal::RadosStore*>(driver)->svc()->zone->get_zone_params())),
      rule(site->get_zonegroup().default_placement.name) {
    auto rados = static_cast<sal::RadosStore*>(driver)->getRados()->get_rados_handle();
    rados->pool_create(hot.name.c_str());
    rados->pool_create(cold.name.c_str());
    auto& classes = zone.placement_pools[rule].storage_classes;
    const RGWZoneStorageClass* sc = nullptr;
    if (classes.find("INTELLIGENT_TIER", &sc)) {
      saved = *sc;
    }
    classes.set_storage_class("INTELLIGENT_TIER", &hot, nullptr, &cold);
  }
  ~TieredPlacement() {
    auto& classes = zone.placement_pools[rule].storage_classes;
    if (saved) {
      classes.get_all()["INTELLIGENT_TIER"] = *saved;
    } else {
      classes.remove_storage_class("INTELLIGENT_TIER");
    }
  }
};

/* write an object whose storage class is picked as a PUT picks it */
int put_object(sal::Bucket* bucket, const std::string& name, const std::string& data,
               ceph::real_time* mtime)
{
  auto obj = bucket->get_object(rgw_obj_key(name));
  rgw_placement_rule rule = bucket->get_placement_rule();
  select_placement(g_ceph_context, driver, bucket->get_attrs(), &rule);
  ACLOwner owner;
  owner.id = bucket->get_owner();
  auto writer = driver->get_atomic_writer(dpp, null_yield, obj.get(), owner, &rule, 0,
                                          "ite-test-" + name);
  int r = writer->prepare(null_yield);
  if (r < 0) {
    return r;
  }
  ceph::buffer::list bl;
  bl.append(data);
  r = writer->process(std::move(bl), 0);
  if (r == 0) {
    r = writer->process({}, data.size()); // flush
  }
  if (r < 0) {
    return r;
  }
  const std::string etag = "ite-test-etag";
  sal::Attrs attrs;
  attrs[RGW_ATTR_ETAG].append(etag);
  const req_context rctx{dpp, null_yield, nullptr};
  return writer->complete(data.size(), etag, mtime, ceph::real_time(), attrs, std::nullopt,
                          ceph::real_time(), nullptr, nullptr, nullptr, nullptr, nullptr,
                          rctx, sal::FLAG_LOG_OP);
}

std::string read_object(sal::Bucket* bucket, const std::string& name, uint64_t size)
{
  auto obj = bucket->get_object(rgw_obj_key(name));
  auto op = obj->get_read_op();
  if (op->prepare(null_yield, dpp) < 0) {
    return {};
  }
  std::string out;
  while (out.size() < size) { // a read stops at the end of a rados object
    ceph::buffer::list bl;
    int r = op->read(out.size(), size - 1, bl, null_yield, dpp);
    if (r <= 0) {
      return {};
    }
    out.append(bl.to_str());
  }
  return out;
}

void delete_object(sal::Bucket* bucket, const std::string& name)
{
  auto obj = bucket->get_object(rgw_obj_key(name));
  auto op = obj->get_delete_op();
  op->params.bucket_owner = bucket->get_owner();
  op->delete_obj(dpp, null_yield, 0);
}

/* the pools an object's bytes are in, as its manifest names them */
std::set<std::string> data_pools(sal::Bucket* bucket, const std::string& name)
{
  auto rados = static_cast<sal::RadosStore*>(driver)->getRados();
  RGWObjectCtx octx(driver);
  RGWObjState* state = nullptr;
  RGWObjManifest* manifest = nullptr;
  const rgw_obj obj(bucket->get_key(), rgw_obj_key(name));
  if (rados->get_obj_state(dpp, &octx, bucket->get_info(), obj, &state, &manifest,
                           false, null_yield) < 0 || !manifest) {
    return {};
  }
  std::set<std::string> pools;
  for (auto i = manifest->obj_begin(dpp); i != manifest->obj_end(dpp); ++i) {
    pools.insert(i.get_location().get_raw_obj(rados).pool.name);
  }
  return pools;
}

std::string count_key(const std::string& name, uint64_t gen, Op op)
{
  CountKey k;
  k.obj = InstanceId(name, "", gen);
  k.op = op;
  k.caller = CallerType::User;
  return k.to_str();
}

} // anonymous namespace

TEST_F(ITERados, WriteBufferAppendFlushListTrim)
{
  auto wb = driver->get_ite_write_buffer();
  ASSERT_TRUE(wb);
  rgw_bucket b("", "wb-bucket", "wb-bucket-id");
  // appends before initialize are refused, never lost silently
  EXPECT_EQ(-EINVAL, wb->append(dpp, event(b, "x", Op::Get), null_yield));
  ASSERT_EQ(0, wb->initialize(dpp, null_yield));
  ASSERT_EQ(32u, wb->num_shards());

  // one object's events all land on its shard
  const uint32_t shard = buffer_shard_of(b, "hot.bin", wb->num_shards());
  for (int i = 0; i < 10; ++i) {
    ASSERT_EQ(0, wb->append(dpp, event(b, "hot.bin", Op::Get), null_yield));
  }
  ASSERT_EQ(0, wb->append(dpp, event(b, "hot.bin", Op::Put), null_yield));

  // nothing is visible until the batch is flushed
  std::vector<AccessEventEntry> entries;
  std::string next;
  bool truncated = true;
  ASSERT_EQ(0, wb->list(dpp, shard, "", 100, entries, &next, &truncated, null_yield));
  EXPECT_TRUE(entries.empty());

  ASSERT_EQ(0, wb->flush(dpp, null_yield));
  ASSERT_EQ(0, wb->list(dpp, shard, "", 100, entries, &next, &truncated, null_yield));
  ASSERT_EQ(11u, entries.size());
  EXPECT_FALSE(truncated);
  EXPECT_EQ(entries.back().marker, next);
  EXPECT_EQ("hot.bin", entries[0].event.obj.name);
  EXPECT_EQ(5u, entries[0].event.obj.gen);
  EXPECT_EQ(b, entries[0].event.bucket);
  EXPECT_EQ(Op::Put, entries.back().event.op);
  for (size_t i = 1; i < entries.size(); ++i) {
    EXPECT_LT(entries[i - 1].marker, entries[i].marker); // cursors sort
  }

  // paging resumes after a cursor
  std::vector<AccessEventEntry> page;
  ASSERT_EQ(0, wb->list(dpp, shard, "", 4, page, &next, &truncated, null_yield));
  EXPECT_EQ(4u, page.size());
  EXPECT_TRUE(truncated);
  ASSERT_EQ(0, wb->list(dpp, shard, next, 100, page, &next, &truncated, null_yield));
  EXPECT_EQ(7u, page.size());

  // trim is inclusive of the cursor
  ASSERT_EQ(0, wb->trim(dpp, shard, entries[5].marker, null_yield));
  ASSERT_EQ(0, wb->list(dpp, shard, "", 100, page, &next, &truncated, null_yield));
  ASSERT_EQ(5u, page.size());
  EXPECT_EQ(entries[6].marker, page[0].marker);
}

TEST_F(ITERados, WriteBufferFlushesInBackground)
{
  auto wb = driver->get_ite_write_buffer();
  ASSERT_EQ(0, wb->initialize(dpp, null_yield));
  rgw_bucket b("", "bg-bucket", "bg-bucket-id");
  ASSERT_EQ(0, wb->append(dpp, event(b, "a", Op::Head), null_yield));
  const uint32_t shard = buffer_shard_of(b, "a", wb->num_shards());

  std::vector<AccessEventEntry> entries;
  std::string next;
  bool truncated = false;
  for (int i = 0; i < 50 && entries.empty(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(0, wb->list(dpp, shard, "", 10, entries, &next, &truncated, null_yield));
  }
  ASSERT_EQ(1u, entries.size());
  EXPECT_EQ(Op::Head, entries[0].event.op);
}

TEST_F(ITERados, WriteBufferDestructorFlushes)
{
  rgw_bucket b("", "d-bucket", "d-bucket-id");
  {
    auto wb = driver->get_ite_write_buffer();
    ASSERT_EQ(0, wb->initialize(dpp, null_yield));
    ASSERT_EQ(0, wb->append(dpp, event(b, "a", Op::Get), null_yield));
  }
  auto wb = driver->get_ite_write_buffer();
  ASSERT_EQ(0, wb->initialize(dpp, null_yield));
  std::vector<AccessEventEntry> entries;
  std::string next;
  bool truncated = false;
  ASSERT_EQ(0, wb->list(dpp, buffer_shard_of(b, "a", wb->num_shards()), "", 10,
                        entries, &next, &truncated, null_yield));
  EXPECT_EQ(1u, entries.size());
}

TEST_F(ITERados, WriteBufferShardLockIsExclusive)
{
  auto wb = driver->get_ite_write_buffer();
  ASSERT_EQ(0, wb->initialize(dpp, null_yield));
  auto a = wb->get_serializer(3, "cookie-a");
  auto b = wb->get_serializer(3, "cookie-b");
  auto other = wb->get_serializer(4, "cookie-b");
  ASSERT_EQ(0, a->try_lock(dpp, std::chrono::seconds(30), null_yield));
  EXPECT_EQ(-EBUSY, b->try_lock(dpp, std::chrono::seconds(30), null_yield));
  EXPECT_EQ(0, other->try_lock(dpp, std::chrono::seconds(30), null_yield));
  EXPECT_EQ(0, a->try_lock(dpp, std::chrono::seconds(60), null_yield)); // renewal
  ASSERT_EQ(0, a->unlock(dpp, null_yield));
  EXPECT_EQ(0, b->try_lock(dpp, std::chrono::seconds(30), null_yield));
  EXPECT_EQ(0, b->unlock(dpp, null_yield));
  EXPECT_EQ(0, other->unlock(dpp, null_yield));
}

TEST_F(ITERados, AggStoreCountsAreGuardedByMarkers)
{
  auto agg = driver->get_ite_agg_store();
  ASSERT_TRUE(agg);
  ASSERT_EQ(0, agg->initialize(dpp, null_yield));
  ASSERT_EQ(8u, agg->num_shards());
  rgw_bucket b("", "agg-bucket", "agg-bucket-id");
  const Window w{Granularity::Day, 20000};
  const std::string a = "a::5:GET:USER";

  std::string m = "x";
  ASSERT_EQ(0, agg->get_marker(dpp, b, w, 3, 7, &m, null_yield));
  EXPECT_EQ("", m); // no object yet
  ASSERT_EQ(0, agg->add_counts(dpp, b, w, 3, 7, "", "c1", {{a, 2}}, null_yield));
  // a replayed batch is refused whole
  EXPECT_EQ(-ECANCELED, agg->add_counts(dpp, b, w, 3, 7, "", "c1", {{a, 2}}, null_yield));
  ASSERT_EQ(0, agg->add_counts(dpp, b, w, 3, 7, "c1", "c2", {{a, 1}}, null_yield));
  ASSERT_EQ(0, agg->get_marker(dpp, b, w, 3, 7, &m, null_yield));
  EXPECT_EQ("c2", m);
  ASSERT_EQ(0, agg->get_marker(dpp, b, w, 3, 8, &m, null_yield));
  EXPECT_EQ("", m); // markers are per buffer shard

  // another aggregate shard of the window, fed by another buffer shard
  ASSERT_EQ(0, agg->add_counts(dpp, b, w, 5, 0, "", "z",
                               {{a, 10}, {"b::0:PUT:USER", 1}}, null_yield));
  std::map<std::string, uint64_t> counts;
  ASSERT_EQ(0, agg->read_counts(dpp, b, w, counts, null_yield));
  EXPECT_EQ(2u, counts.size()); // markers are not counts
  EXPECT_EQ(13u, counts[a]);
  EXPECT_EQ(1u, counts["b::0:PUT:USER"]);

  // another bucket's window is separate
  ASSERT_EQ(0, agg->read_counts(dpp, rgw_bucket("", "other", "other-id"), w,
                                counts, null_yield));
  EXPECT_TRUE(counts.empty());
}

TEST_F(ITERados, AggStoreWindowsListAndRemove)
{
  auto agg = driver->get_ite_agg_store();
  ASSERT_EQ(0, agg->initialize(dpp, null_yield));
  rgw_bucket b("", "win-bucket", "win-bucket-id");
  auto add = [&](Window w, uint32_t shard) {
    ASSERT_EQ(0, agg->add_counts(dpp, b, w, shard, 0, "", "m",
                                 {{"o::1:GET:USER", 1}}, null_yield));
  };
  add(Window{Granularity::Day, 100}, 0);
  add(Window{Granularity::Day, 100}, 6);
  add(Window{Granularity::Day, 101}, 2);
  add(Window{Granularity::Day, 105}, 1);
  add(Window{Granularity::Week, 20}, 0);

  auto list = [&](Granularity g, uint64_t from, uint64_t to) {
    std::vector<Window> out;
    EXPECT_EQ(0, agg->list_windows(dpp, b, Window{g, from}, Window{g, to}, out, null_yield));
    std::vector<uint64_t> idx;
    for (const auto& w : out) {
      EXPECT_EQ(g, w.granularity);
      idx.push_back(w.index);
    }
    return idx;
  };
  EXPECT_EQ((std::vector<uint64_t>{100, 101}), list(Granularity::Day, 100, 104));
  EXPECT_EQ((std::vector<uint64_t>{100, 101, 105}), list(Granularity::Day, 0, UINT64_MAX));
  EXPECT_EQ((std::vector<uint64_t>{20}), list(Granularity::Week, 0, UINT64_MAX));

  // removal drops every shard of the window
  ASSERT_EQ(0, agg->remove_window(dpp, b, Window{Granularity::Day, 100}, null_yield));
  EXPECT_EQ((std::vector<uint64_t>{101, 105}), list(Granularity::Day, 0, UINT64_MAX));
  std::map<std::string, uint64_t> counts;
  ASSERT_EQ(0, agg->read_counts(dpp, b, Window{Granularity::Day, 100}, counts, null_yield));
  EXPECT_TRUE(counts.empty());
  EXPECT_EQ(0, agg->remove_window(dpp, b, Window{Granularity::Day, 100}, null_yield));
  // a window can be written again after removal
  add(Window{Granularity::Day, 100}, 0);
  EXPECT_EQ((std::vector<uint64_t>{100, 101, 105}), list(Granularity::Day, 0, UINT64_MAX));
}

TEST(ITERadosKeys, AggStoreIndexKeys)
{
  using sal::RadosITEAggStore;
  const Window d{Granularity::Day, 20123};
  const Window w{Granularity::Week, 2875};
  auto e = RadosITEAggStore::parse_index_key(RadosITEAggStore::index_key(d, 7));
  ASSERT_TRUE(e);
  EXPECT_EQ(d, e->first);
  EXPECT_EQ(7u, e->second);
  e = RadosITEAggStore::parse_index_key(RadosITEAggStore::index_key(w, 1023));
  ASSERT_TRUE(e);
  EXPECT_EQ(w, e->first);
  EXPECT_EQ(1023u, e->second);
  // keys sort by granularity, window, then shard
  EXPECT_LT(RadosITEAggStore::index_key(Window{Granularity::Day, 9}, 5),
            RadosITEAggStore::index_key(Window{Granularity::Day, 10}, 0));
  EXPECT_LT(RadosITEAggStore::index_key(d, 2), RadosITEAggStore::index_key(d, 10));
  EXPECT_FALSE(RadosITEAggStore::parse_index_key("garbage"));
  EXPECT_FALSE(RadosITEAggStore::parse_index_key("d00000000000000000001x00001"));
  EXPECT_FALSE(RadosITEAggStore::parse_index_key("q00000000000000000001.00001"));
}

TEST_F(ITERados, AggStoreBucketDirectory)
{
  auto agg = driver->get_ite_agg_store();
  ASSERT_EQ(0, agg->initialize(dpp, null_yield));
  std::vector<BucketProgress> in;
  for (int i = 0; i < 5; ++i) {
    BucketProgress p;
    p.bucket = rgw_bucket("", fmt::format("dir-{}", i), fmt::format("dir-{}-id", i));
    p.enabled_at = ceph::real_clock::now();
    p.last_scored = Window{Granularity::Day, 100u + i};
    ASSERT_EQ(0, agg->put_bucket(dpp, p, null_yield));
    in.push_back(p);
  }

  std::vector<BucketProgress> all, page;
  std::string marker, next;
  bool truncated = true;
  while (truncated) {
    ASSERT_EQ(0, agg->list_buckets(dpp, marker, 2, page, &next, &truncated, null_yield));
    EXPECT_LE(page.size(), 2u);
    all.insert(all.end(), page.begin(), page.end());
    marker = next;
  }
  ASSERT_EQ(5u, all.size());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(in[i].bucket, all[i].bucket);
    ASSERT_TRUE(all[i].last_scored);
    EXPECT_EQ(100u + i, all[i].last_scored->index);
  }

  BucketProgress got;
  ASSERT_EQ(0, agg->get_bucket(dpp, in[2].bucket, &got, null_yield));
  EXPECT_EQ(in[2].bucket, got.bucket);
  ASSERT_EQ(0, agg->remove_bucket(dpp, in[2].bucket, null_yield));
  EXPECT_EQ(-ENOENT, agg->get_bucket(dpp, in[2].bucket, &got, null_yield));
  EXPECT_EQ(0, agg->remove_bucket(dpp, in[2].bucket, null_yield));
}

TEST_F(ITERados, AggStoreBucketLockIsExclusive)
{
  auto agg = driver->get_ite_agg_store();
  ASSERT_EQ(0, agg->initialize(dpp, null_yield));
  rgw_bucket b("", "lock-bucket", "lock-bucket-id");
  rgw_bucket o("", "other-bucket", "other-bucket-id");
  auto a = agg->get_bucket_serializer(b, "cookie-a");
  auto c = agg->get_bucket_serializer(b, "cookie-b");
  auto other = agg->get_bucket_serializer(o, "cookie-b");
  ASSERT_EQ(0, a->try_lock(dpp, std::chrono::seconds(30), null_yield));
  EXPECT_EQ(-EBUSY, c->try_lock(dpp, std::chrono::seconds(30), null_yield));
  EXPECT_EQ(0, other->try_lock(dpp, std::chrono::seconds(30), null_yield));
  ASSERT_EQ(0, a->unlock(dpp, null_yield));
  EXPECT_EQ(0, c->try_lock(dpp, std::chrono::seconds(30), null_yield));
  EXPECT_EQ(0, c->unlock(dpp, null_yield));
  EXPECT_EQ(0, other->unlock(dpp, null_yield));
}

TEST_F(ITERados, AggregatorFoldsTheWriteBuffer)
{
  Context ctx;
  ctx.cct = g_ceph_context;
  ctx.driver = driver;
  ctx.cookie = "ceph_test_rgw_ite";
  ctx.buffer = driver->get_ite_write_buffer();
  ctx.agg = driver->get_ite_agg_store();
  ASSERT_EQ(0, ctx.buffer->initialize(dpp, null_yield));
  ASSERT_EQ(0, ctx.agg->initialize(dpp, null_yield));

  rgw_bucket b("", "e2e-bucket", "e2e-bucket-id");
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(0, ctx.buffer->append(dpp, event(b, "a", Op::Get), null_yield));
  }
  ASSERT_EQ(0, ctx.buffer->append(dpp, event(b, "b", Op::Head), null_yield));
  auto repl = event(b, "a", Op::Get);
  repl.caller = CallerType::Replication;
  ASSERT_EQ(0, ctx.buffer->append(dpp, repl, null_yield));
  ASSERT_EQ(0, ctx.buffer->flush(dpp, null_yield));

  Aggregator agg(ctx);
  ASSERT_EQ(0, agg.process(dpp, null_yield));
  EXPECT_EQ(5u, agg.stats().events);
  EXPECT_EQ(4u, agg.stats().counted);
  EXPECT_EQ(1u, agg.stats().not_user);

  const Window today = Window::of(ceph::real_clock::now(), ctx.granularity(), ctx.timescale());
  std::map<std::string, uint64_t> counts;
  ASSERT_EQ(0, ctx.agg->read_counts(dpp, b, today, counts, null_yield));
  CountKey ka;
  ka.obj = InstanceId("a", "", 5);
  CountKey kb;
  kb.obj = InstanceId("b", "", 5);
  kb.op = Op::Head;
  EXPECT_EQ(3u, counts[ka.to_str()]);
  EXPECT_EQ(1u, counts[kb.to_str()]);

  // consumed shards are trimmed, and the bucket is in the directory
  for (uint32_t s = 0; s < ctx.buffer->num_shards(); ++s) {
    std::vector<AccessEventEntry> entries;
    std::string next;
    bool truncated = false;
    ASSERT_EQ(0, ctx.buffer->list(dpp, s, "", 10, entries, &next, &truncated, null_yield));
    EXPECT_TRUE(entries.empty()) << "shard " << s;
  }
  BucketProgress p;
  ASSERT_EQ(0, ctx.agg->get_bucket(dpp, b, &p, null_yield));

  // nothing is counted twice
  Aggregator again(ctx);
  ASSERT_EQ(0, again.process(dpp, null_yield));
  ASSERT_EQ(0, ctx.agg->read_counts(dpp, b, today, counts, null_yield));
  EXPECT_EQ(3u, counts[ka.to_str()]);
}

TEST_F(ITERados, StateStoreReadsAndWritesIndexEntries)
{
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-state", 3, true));
  ASSERT_EQ(0, tb.add_entry("a", 5, 4096));
  ASSERT_EQ(0, tb.add_entry("b", 6, 1 << 20));
  auto state = driver->get_ite_state_store();
  ASSERT_TRUE(state);
  sal::Bucket* b = tb.bucket.get();

  ObjectEntry e;
  ASSERT_EQ(0, state->get_state(dpp, b, rgw_obj_key("a"), &e, null_yield));
  EXPECT_EQ("a", e.key.name);
  EXPECT_EQ(5u, e.gen);
  EXPECT_EQ(4096u, e.size);
  EXPECT_EQ("INTELLIGENT_TIER", e.storage_class);
  EXPECT_TRUE(e.exists);
  EXPECT_TRUE(e.current);
  EXPECT_FALSE(e.delete_marker);
  EXPECT_EQ(5u, e.ite.base_gen); // a fresh instance is stamped
  EXPECT_EQ(0.0, e.ite.temperature);
  EXPECT_EQ(-ENOENT, state->get_state(dpp, b, rgw_obj_key("missing"), &e, null_yield));

  rgw_bucket_ite_state s;
  s.temperature = 0.7;
  s.temp_updated_at = 123;
  s.tier_state = RGWITETierState::Committed; // not in the mask
  ASSERT_EQ(0, state->set_state(dpp, b, rgw_obj_key("a"), s,
                                rgw_bucket_ite_state::MASK_TEMPERATURE, 5, null_yield));
  ASSERT_EQ(0, state->get_state(dpp, b, rgw_obj_key("a"), &e, null_yield));
  EXPECT_DOUBLE_EQ(0.7, e.ite.temperature);
  EXPECT_EQ(123u, e.ite.temp_updated_at);
  EXPECT_EQ(RGWITETierState::Hot, e.ite.tier_state);

  // a stale generation, and a missing object, are outcomes, not errors
  EXPECT_EQ(-ECANCELED, state->set_state(dpp, b, rgw_obj_key("a"), s,
                                         rgw_bucket_ite_state::MASK_TEMPERATURE, 4,
                                         null_yield));
  EXPECT_EQ(-ENOENT, state->set_state(dpp, b, rgw_obj_key("missing"), s,
                                      rgw_bucket_ite_state::MASK_TEMPERATURE, 0,
                                      null_yield));

  // an overwrite starts a new instance, cold
  ASSERT_EQ(0, tb.add_entry("a", 9, 4096));
  ASSERT_EQ(0, state->get_state(dpp, b, rgw_obj_key("a"), &e, null_yield));
  EXPECT_EQ(0.0, e.ite.temperature);
  EXPECT_EQ(9u, e.ite.base_gen);
}

TEST_F(ITERados, StateStoreListsEveryEntryAcrossShards)
{
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-list", 7, true));
  for (int i = 0; i < 25; ++i) {
    ASSERT_EQ(0, tb.add_entry(fmt::format("o{:02}", i), 100 + i, 10));
  }
  ASSERT_EQ(0, tb.add_entry("_under", 99, 10)); // escaped in the index
  auto state = driver->get_ite_state_store();
  std::vector<ObjectEntry> all, page;
  rgw_obj_key marker, next;
  bool truncated = true;
  while (truncated) {
    ASSERT_EQ(0, state->list_state(dpp, tb.bucket.get(), marker, 10, page, &next,
                                   &truncated, null_yield));
    all.insert(all.end(), page.begin(), page.end());
    marker = next;
  }
  ASSERT_EQ(26u, all.size());
  EXPECT_EQ("_under", all[0].key.name); // its index name "__under" sorts first
  EXPECT_EQ(99u, all[0].gen);
  for (int i = 0; i < 25; ++i) {
    EXPECT_EQ(fmt::format("o{:02}", i), all[i + 1].key.name); // in key order
    EXPECT_EQ(100u + i, all[i + 1].gen);
  }

  // the listed key addresses the entry again
  ObjectEntry e;
  ASSERT_EQ(0, state->get_state(dpp, tb.bucket.get(), all[0].key, &e, null_yield));
  EXPECT_EQ("_under", e.key.name);
  rgw_bucket_ite_state s;
  s.temperature = 0.5;
  ASSERT_EQ(0, state->set_state(dpp, tb.bucket.get(), all[0].key, s,
                                rgw_bucket_ite_state::MASK_TEMPERATURE, 99, null_yield));
}

TEST_F(ITERados, TemperatureScorerScoresIndexEntries)
{
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-temp", 3, true));
  ASSERT_EQ(0, tb.add_entry("a", 5, 4096));
  ASSERT_EQ(0, tb.add_entry("b", 5, 4096));
  const rgw_bucket key = tb.bucket->get_key();

  Context ctx;
  ctx.cct = g_ceph_context;
  ctx.driver = driver;
  ctx.cookie = "ceph_test_rgw_ite";
  ctx.agg = driver->get_ite_agg_store();
  ctx.state = driver->get_ite_state_store();
  ASSERT_EQ(0, ctx.agg->initialize(dpp, null_yield));

  const TimeScale ts = ctx.timescale();
  const Window today = Window::of(ceph::real_clock::now(), Granularity::Day, ts);
  const Window yesterday = today.prev();
  const auto noon = today.start(ts) + std::chrono::hours(12);
  BucketProgress p;
  p.bucket = key;
  p.enabled_at = noon - std::chrono::hours(24 * 30);
  ASSERT_EQ(0, ctx.agg->put_bucket(dpp, p, null_yield));
  const uint32_t shards = ctx.agg->num_shards();
  ASSERT_EQ(0, ctx.agg->add_counts(dpp, key, yesterday, agg_shard_of("a", shards), 0,
                                   "", "m", {{count_key("a", 5, Op::Head), 3}}, null_yield));
  ASSERT_EQ(0, ctx.agg->add_counts(dpp, key, yesterday, agg_shard_of("b", shards), 1,
                                   "", "m", {{count_key("b", 0, Op::Put), 1}}, null_yield));

  TemperatureScorer scorer(ctx);
  ASSERT_EQ(0, scorer.process_bucket(dpp, key, noon, null_yield));
  EXPECT_EQ(1u, scorer.stats().buckets);
  EXPECT_EQ(2u, scorer.stats().objects);
  ObjectEntry e;
  ASSERT_EQ(0, ctx.state->get_state(dpp, tb.bucket.get(), rgw_obj_key("a"), &e, null_yield));
  EXPECT_NEAR(0.3, e.ite.temperature, 1e-9);
  EXPECT_EQ(to_epoch_secs(yesterday.end(ts)), e.ite.temp_updated_at);
  ASSERT_EQ(0, ctx.state->get_state(dpp, tb.bucket.get(), rgw_obj_key("b"), &e, null_yield));
  EXPECT_NEAR(0.5, e.ite.temperature, 1e-9);

  // the progress is saved, and a rerun changes nothing
  ASSERT_EQ(0, ctx.agg->get_bucket(dpp, key, &p, null_yield));
  ASSERT_TRUE(p.last_scored);
  EXPECT_EQ(yesterday, *p.last_scored);
  TemperatureScorer again(ctx);
  ASSERT_EQ(0, again.process_bucket(dpp, key, noon, null_yield));
  EXPECT_EQ(0u, again.stats().objects);
  ASSERT_EQ(0, ctx.state->get_state(dpp, tb.bucket.get(), rgw_obj_key("a"), &e, null_yield));
  EXPECT_NEAR(0.3, e.ite.temperature, 1e-9);
}

TEST_F(ITERados, FFTDetectorWritesVerdictsToIndexEntries)
{
  set_conf("rgw_ite_fft_span", "21");
  set_conf("rgw_ite_fft_warmup", "0");
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-fft", 3, true));
  ASSERT_EQ(0, tb.add_entry("weekly", 5, 4096));
  ASSERT_EQ(0, tb.add_entry("burst", 5, 4096));
  const rgw_bucket key = tb.bucket->get_key();

  Context ctx;
  ctx.cct = g_ceph_context;
  ctx.driver = driver;
  ctx.cookie = "ceph_test_rgw_ite";
  ctx.agg = driver->get_ite_agg_store();
  ctx.state = driver->get_ite_state_store();
  ASSERT_EQ(0, ctx.agg->initialize(dpp, null_yield));

  const TimeScale ts = ctx.timescale();
  const Window today = Window::of(ceph::real_clock::now(), Granularity::Day, ts);
  const auto noon = today.start(ts) + std::chrono::hours(12);
  BucketProgress p;
  p.bucket = key;
  p.enabled_at = noon - std::chrono::hours(24 * 30);
  ASSERT_EQ(0, ctx.agg->put_bucket(dpp, p, null_yield));
  const uint32_t shards = ctx.agg->num_shards();
  // each object's counts come from its own buffer shard, so the two never
  // contend for one marker even when they share an aggregated shard
  auto reads = [&](const std::string& name, uint32_t buffer_shard,
                   std::initializer_list<uint64_t> days) {
    for (auto d : days) {
      const Window w{Granularity::Day, today.index - d};
      ASSERT_EQ(0, ctx.agg->add_counts(dpp, key, w, agg_shard_of(name, shards),
                                       buffer_shard, "", "m",
                                       {{count_key(name, 5, Op::Get), 1}}, null_yield));
    }
  };
  reads("weekly", 0, {1, 8, 15});
  reads("burst", 1, {1, 2, 3});

  FFTDetector detector(ctx);
  ASSERT_EQ(0, detector.process_bucket(dpp, key, noon, null_yield));
  EXPECT_EQ(1u, detector.stats().buckets);
  EXPECT_EQ(2u, detector.stats().candidates);
  EXPECT_EQ(1u, detector.stats().periodic);
  ObjectEntry e;
  ASSERT_EQ(0, ctx.state->get_state(dpp, tb.bucket.get(), rgw_obj_key("weekly"), &e,
                                    null_yield));
  EXPECT_TRUE(e.ite.fft_periodic);
  EXPECT_EQ(7u, e.ite.fft_period_days);
  const Window next{Granularity::Day, today.index - 1 + 7};
  EXPECT_EQ(to_epoch_secs(next.start(ts)), e.ite.fft_next_access);
  EXPECT_EQ(0.0, e.ite.temperature); // only the FFT fields are written
  ASSERT_EQ(0, ctx.state->get_state(dpp, tb.bucket.get(), rgw_obj_key("burst"), &e,
                                    null_yield));
  EXPECT_FALSE(e.ite.fft_periodic);

  ASSERT_EQ(0, ctx.agg->get_bucket(dpp, key, &p, null_yield));
  EXPECT_EQ(noon, p.last_fft);
}

TEST_F(ITERados, MoveQueueAppendListAck)
{
  auto mq = driver->get_ite_move_queue();
  ASSERT_TRUE(mq);
  MoveAction a;
  a.bucket = rgw_bucket("", "mq-bucket", "mq-bucket-id");
  a.obj = InstanceId("cold.bin", "", 7);
  a.kind = MoveKind::Commit;
  a.target = Placement::Cold;
  a.zone_id = "zone-a";
  a.queued_at = ceph::real_clock::now();
  EXPECT_EQ(-EINVAL, mq->append(dpp, a, null_yield)); // before initialize
  ASSERT_EQ(0, mq->initialize(dpp, null_yield));
  ASSERT_EQ(32u, mq->num_shards());

  const uint32_t shard = move_shard_of(a, mq->num_shards());
  ASSERT_EQ(0, mq->append(dpp, a, null_yield));
  a.kind = MoveKind::Restore;
  a.target = Placement::Hot;
  ASSERT_EQ(0, mq->append(dpp, a, null_yield)); // durable at once: no flush

  std::vector<MoveActionEntry> entries;
  std::string next;
  bool truncated = true;
  ASSERT_EQ(0, mq->list(dpp, shard, "", 100, entries, &next, &truncated, null_yield));
  ASSERT_EQ(2u, entries.size());
  EXPECT_FALSE(truncated);
  EXPECT_EQ(entries.back().marker, next);
  EXPECT_EQ(MoveKind::Commit, entries[0].action.kind);
  EXPECT_EQ(Placement::Cold, entries[0].action.target);
  EXPECT_EQ("cold.bin", entries[0].action.obj.name);
  EXPECT_EQ(7u, entries[0].action.obj.gen);
  EXPECT_EQ("zone-a", entries[0].action.zone_id);
  EXPECT_EQ(a.bucket, entries[0].action.bucket);
  EXPECT_EQ(MoveKind::Restore, entries[1].action.kind);

  // acknowledging is inclusive of the cursor
  ASSERT_EQ(0, mq->ack(dpp, shard, entries[0].marker, null_yield));
  ASSERT_EQ(0, mq->list(dpp, shard, "", 100, entries, &next, &truncated, null_yield));
  ASSERT_EQ(1u, entries.size());
  EXPECT_EQ(MoveKind::Restore, entries[0].action.kind);

  auto lock = mq->get_serializer(shard, "a");
  auto other = mq->get_serializer(shard, "b");
  ASSERT_EQ(0, lock->try_lock(dpp, std::chrono::seconds(60), null_yield));
  EXPECT_EQ(-EBUSY, other->try_lock(dpp, std::chrono::seconds(60), null_yield));
  EXPECT_EQ(0, lock->unlock(dpp, null_yield));
}

TEST_F(ITERados, DecisionEngineOnIndexEntries)
{
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-decide", 3, true));
  const auto now = ceph::real_clock::now();
  const auto old = now - std::chrono::hours(24 * 40);
  ASSERT_EQ(0, tb.add_entry("cold", 5, 1 << 20, old));
  ASSERT_EQ(0, tb.add_entry("young", 5, 1 << 20, now));
  ASSERT_EQ(0, tb.add_entry("tiny", 5, 10, old));
  const rgw_bucket key = tb.bucket->get_key();

  Context ctx;
  ctx.cct = g_ceph_context;
  ctx.driver = driver;
  ctx.cookie = "ceph_test_rgw_ite";
  ctx.zone_id = driver->get_zone()->get_id();
  ctx.agg = driver->get_ite_agg_store();
  ctx.state = driver->get_ite_state_store();
  ctx.moves = driver->get_ite_move_queue();
  ASSERT_EQ(0, ctx.agg->initialize(dpp, null_yield));
  ASSERT_EQ(0, ctx.moves->initialize(dpp, null_yield));
  BucketProgress p;
  p.bucket = key;
  p.enabled_at = old;
  ASSERT_EQ(0, ctx.agg->put_bucket(dpp, p, null_yield));

  // first pass: the old cold instance is marked, for free
  DecisionEngine first(ctx);
  ASSERT_EQ(0, first.process_bucket(dpp, key, now, null_yield));
  EXPECT_EQ(3u, first.stats().objects);
  EXPECT_EQ(1u, first.stats().pseudo);
  EXPECT_EQ(1u, first.stats().skip);
  ObjectEntry e;
  ASSERT_EQ(0, ctx.state->get_state(dpp, tb.bucket.get(), rgw_obj_key("cold"), &e,
                                    null_yield));
  EXPECT_EQ(RGWITETierState::Pseudo, e.ite.tier_state);
  EXPECT_EQ(to_epoch_secs(now), e.ite.pseudo_since);
  ASSERT_EQ(0, ctx.state->get_state(dpp, tb.bucket.get(), rgw_obj_key("young"), &e,
                                    null_yield));
  EXPECT_EQ(RGWITETierState::Hot, e.ite.tier_state);

  // a grace period later it is queued for commit
  const auto later = now + std::chrono::hours(24 * 8);
  DecisionEngine second(ctx);
  ASSERT_EQ(0, second.process_bucket(dpp, key, later, null_yield));
  EXPECT_EQ(1u, second.stats().commit);
  ASSERT_EQ(0, ctx.state->get_state(dpp, tb.bucket.get(), rgw_obj_key("cold"), &e,
                                    null_yield));
  EXPECT_EQ(RGWITEMovePending::Commit, e.ite.move_pending);
  EXPECT_EQ(RGWITETierState::Pseudo, e.ite.tier_state);

  MoveAction want;
  want.bucket = key;
  want.obj = InstanceId("cold", "", 5);
  std::vector<MoveActionEntry> entries;
  std::string next;
  bool truncated;
  ASSERT_EQ(0, ctx.moves->list(dpp, move_shard_of(want, ctx.moves->num_shards()), "",
                               100, entries, &next, &truncated, null_yield));
  ASSERT_EQ(1u, entries.size());
  EXPECT_EQ(MoveKind::Commit, entries[0].action.kind);
  EXPECT_EQ(key, entries[0].action.bucket);
  EXPECT_EQ(5u, entries[0].action.obj.gen);
  EXPECT_EQ(ctx.zone_id, entries[0].action.zone_id);
}

TEST_F(ITERados, PlacementMoveKeepsTheInstance)
{
  TieredPlacement tiered;
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-placement", 3, true));
  sal::Bucket* bucket = tb.bucket.get();
  // past the head chunk, so the bytes span several rados objects
  std::string data(6 << 20, '\0');
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<char>('a' + i % 26);
  }
  ceph::real_time mtime;
  ASSERT_EQ(0, put_object(bucket, "big", data, &mtime));
  auto cleanup = make_scope_guard([&] { delete_object(bucket, "big"); });

  // an ITE bucket's object takes the class at PUT, in its hot placement
  auto state = driver->get_ite_state_store();
  const rgw_obj_key key("big");
  ObjectEntry e;
  ASSERT_EQ(0, state->get_state(dpp, bucket, key, &e, null_yield));
  EXPECT_EQ("INTELLIGENT_TIER", e.storage_class);
  const uint64_t gen = e.instance().gen;
  Placement where;
  ASSERT_EQ(0, bucket->get_object(key)->ite_get_placement(dpp, &where, null_yield));
  EXPECT_EQ(Placement::Hot, where);
  EXPECT_EQ(std::set<std::string>{tiered.hot.name}, data_pools(bucket, "big"));

  rgw_bucket_ite_state s;
  s.temperature = 0.01;
  s.temp_updated_at = 1234;
  s.tier_state = RGWITETierState::Pseudo;
  s.pseudo_since = 1;
  ASSERT_EQ(0, state->set_state(dpp, bucket, key, s,
                                rgw_bucket_ite_state::MASK_TEMPERATURE |
                                rgw_bucket_ite_state::MASK_TIER, gen, null_yield));

  ASSERT_EQ(0, bucket->get_object(key)->ite_move_placement(dpp, Placement::Cold, gen,
                                                           null_yield));
  // the bytes are in the cold pool and read back the same
  ASSERT_EQ(0, bucket->get_object(key)->ite_get_placement(dpp, &where, null_yield));
  EXPECT_EQ(Placement::Cold, where);
  EXPECT_EQ(std::set<std::string>{tiered.cold.name}, data_pools(bucket, "big"));
  EXPECT_EQ(data, read_object(bucket, "big", data.size()));
  // with the class and mtime the client sees unchanged
  auto obj = bucket->get_object(key);
  ASSERT_EQ(0, obj->get_obj_attrs(null_yield, dpp));
  EXPECT_EQ("INTELLIGENT_TIER", rgw_bl_str(obj->get_attrs()[RGW_ATTR_STORAGE_CLASS]));
  EXPECT_EQ(mtime, obj->get_mtime());
  // and the instance its identity and ITE state through the rewrite
  ASSERT_EQ(0, state->get_state(dpp, bucket, key, &e, null_yield));
  EXPECT_EQ("INTELLIGENT_TIER", e.storage_class);
  EXPECT_EQ(gen, e.instance().gen);
  EXPECT_LT(gen, e.gen); // the head was rewritten
  EXPECT_EQ(mtime, e.mtime);
  EXPECT_DOUBLE_EQ(0.01, e.ite.temperature);
  EXPECT_EQ(1234u, e.ite.temp_updated_at);
  EXPECT_EQ(RGWITETierState::Pseudo, e.ite.tier_state);
  const uint64_t rewritten = e.gen;

  // already there: nothing is rewritten
  ASSERT_EQ(0, bucket->get_object(key)->ite_move_placement(dpp, Placement::Cold, gen,
                                                           null_yield));
  ASSERT_EQ(0, state->get_state(dpp, bucket, key, &e, null_yield));
  EXPECT_EQ(rewritten, e.gen);

  // a metadata update keeps the instance's state too
  ceph::buffer::list meta;
  meta.append("v");
  ASSERT_EQ(0, obj->modify_obj_attrs(RGW_ATTR_META_PREFIX "ite-test", meta, null_yield, dpp));
  ASSERT_EQ(0, state->get_state(dpp, bucket, key, &e, null_yield));
  EXPECT_EQ(gen, e.instance().gen);
  EXPECT_EQ(RGWITETierState::Pseudo, e.ite.tier_state);
  EXPECT_DOUBLE_EQ(0.01, e.ite.temperature);

  // a move names its instance: another generation's is refused
  EXPECT_EQ(-ECANCELED, bucket->get_object(key)->ite_move_placement(
                            dpp, Placement::Hot, gen + 1000, null_yield));

  ASSERT_EQ(0, bucket->get_object(key)->ite_move_placement(dpp, Placement::Hot, gen,
                                                           null_yield));
  ASSERT_EQ(0, bucket->get_object(key)->ite_get_placement(dpp, &where, null_yield));
  EXPECT_EQ(Placement::Hot, where);
  EXPECT_EQ(std::set<std::string>{tiered.hot.name}, data_pools(bucket, "big"));
  EXPECT_EQ(data, read_object(bucket, "big", data.size()));
  ASSERT_EQ(0, state->get_state(dpp, bucket, key, &e, null_yield));
  EXPECT_EQ(gen, e.instance().gen);
  EXPECT_DOUBLE_EQ(0.01, e.ite.temperature);
}

TEST_F(ITERados, MoveNeedsAColdPlacement)
{
  TieredPlacement tiered;
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-plain", 1, false));
  sal::Bucket* bucket = tb.bucket.get();
  ceph::real_time mtime;
  ASSERT_EQ(0, put_object(bucket, "plain", std::string(8192, 'x'), &mtime));
  auto cleanup = make_scope_guard([&] { delete_object(bucket, "plain"); });

  // without ITE on the bucket, the object keeps the class it asked for
  auto state = driver->get_ite_state_store();
  ObjectEntry e;
  ASSERT_EQ(0, state->get_state(dpp, bucket, rgw_obj_key("plain"), &e, null_yield));
  EXPECT_EQ(RGW_STORAGE_CLASS_STANDARD,
            rgw_placement_rule::get_canonical_storage_class(e.storage_class));
  // which has no cold placement to move to
  EXPECT_EQ(-EOPNOTSUPP, bucket->get_object(rgw_obj_key("plain"))->ite_move_placement(
                             dpp, Placement::Cold, e.instance().gen, null_yield));
  EXPECT_EQ(std::string(8192, 'x'), read_object(bucket, "plain", 8192));
}

TEST_F(ITERados, MoverCommitsAndRestores)
{
  TieredPlacement tiered;
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-mover", 3, true));
  sal::Bucket* bucket = tb.bucket.get();
  const std::string data(1 << 20, 'm');
  ceph::real_time mtime;
  ASSERT_EQ(0, put_object(bucket, "obj", data, &mtime));
  auto cleanup = make_scope_guard([&] { delete_object(bucket, "obj"); });

  Context ctx;
  ctx.cct = g_ceph_context;
  ctx.driver = driver;
  ctx.cookie = "ceph_test_rgw_ite";
  ctx.zone_id = driver->get_zone()->get_id();
  ctx.state = driver->get_ite_state_store();
  ctx.moves = driver->get_ite_move_queue();
  ASSERT_EQ(0, ctx.moves->initialize(dpp, null_yield));

  const rgw_obj_key key("obj");
  ObjectEntry e;
  ASSERT_EQ(0, ctx.state->get_state(dpp, bucket, key, &e, null_yield));
  const uint64_t gen = e.instance().gen;

  // as the engine leaves it: PSEUDO past grace, its commit queued and marked
  rgw_bucket_ite_state s;
  s.tier_state = RGWITETierState::Pseudo;
  s.pseudo_since = 1;
  s.move_pending = RGWITEMovePending::Commit;
  ASSERT_EQ(0, ctx.state->set_state(dpp, bucket, key, s,
                                    rgw_bucket_ite_state::MASK_TIER |
                                    rgw_bucket_ite_state::MASK_MOVE, gen, null_yield));
  MoveAction a;
  a.bucket = bucket->get_key();
  a.obj = InstanceId("obj", "", gen);
  a.kind = MoveKind::Commit;
  a.target = Placement::Cold;
  a.zone_id = ctx.zone_id;
  a.queued_at = ceph::real_clock::now() - std::chrono::hours(1);
  ASSERT_EQ(0, ctx.moves->append(dpp, a, null_yield));
  const uint32_t shard = move_shard_of(a, ctx.moves->num_shards());
  {
    Mover mover(ctx);
    ASSERT_EQ(0, mover.process_shard(dpp, shard, ceph::real_clock::now(), null_yield));
    EXPECT_EQ(1u, mover.stats().started);
    EXPECT_EQ(1u, mover.stats().commits);
  }
  EXPECT_EQ(std::set<std::string>{tiered.cold.name}, data_pools(bucket, "obj"));
  ASSERT_EQ(0, ctx.state->get_state(dpp, bucket, key, &e, null_yield));
  EXPECT_EQ(gen, e.instance().gen);
  EXPECT_EQ(RGWITETierState::Committed, e.ite.tier_state);
  EXPECT_EQ(RGWITEMovePending::None, e.ite.move_pending);
  EXPECT_EQ(1u, e.ite.commit_count);

  // promoted on demand
  s = rgw_bucket_ite_state();
  s.move_pending = RGWITEMovePending::Restore;
  ASSERT_EQ(0, ctx.state->set_state(dpp, bucket, key, s, rgw_bucket_ite_state::MASK_MOVE,
                                    gen, null_yield));
  a.kind = MoveKind::Restore;
  a.target = Placement::Hot;
  ASSERT_EQ(0, ctx.moves->append(dpp, a, null_yield));
  {
    Mover mover(ctx);
    ASSERT_EQ(0, mover.process_shard(dpp, shard, ceph::real_clock::now(), null_yield));
    EXPECT_EQ(1u, mover.stats().restores);
  }
  EXPECT_EQ(std::set<std::string>{tiered.hot.name}, data_pools(bucket, "obj"));
  EXPECT_EQ(data, read_object(bucket, "obj", data.size()));
  ASSERT_EQ(0, ctx.state->get_state(dpp, bucket, key, &e, null_yield));
  EXPECT_EQ(RGWITETierState::Hot, e.ite.tier_state);
  EXPECT_EQ(1u, e.ite.commit_count);
  EXPECT_EQ(1u, e.ite.restore_count);
  EXPECT_EQ(RGWITEMovePending::None, e.ite.move_pending);

  // nothing is left on the queue
  std::vector<MoveActionEntry> entries;
  std::string next;
  bool truncated;
  ASSERT_EQ(0, ctx.moves->list(dpp, shard, "", 100, entries, &next, &truncated, null_yield));
  EXPECT_TRUE(entries.empty());
}

TEST_F(ITERados, AdminEnablesABucket)
{
  TieredPlacement tiered;
  TestBucket tb;
  ASSERT_EQ(0, tb.create("ite-admin", 1, false));
  Context ctx;
  ASSERT_EQ(0, ctx.init(dpp, g_ceph_context, driver, null_yield));

  // enabled with a knob: the config is stored with the bucket, and the
  // bucket joins the directory
  BucketConfig knobs;
  knobs.half_life_days = 3;
  const auto now = ceph::real_clock::now();
  JSONFormattable f;
  ASSERT_EQ(0, admin::bucket_enable(dpp, ctx, tb.bucket.get(), knobs, now, &f, null_yield));
  std::unique_ptr<sal::Bucket> loaded;
  ASSERT_EQ(0, driver->load_bucket(dpp, tb.bucket->get_key(), &loaded, null_yield));
  auto config = get_bucket_config(loaded->get_attrs());
  ASSERT_TRUE(config);
  EXPECT_TRUE(config->enabled);
  EXPECT_DOUBLE_EQ(3, *config->half_life_days);
  tb.bucket = std::move(loaded);
  BucketProgress p;
  ASSERT_EQ(0, ctx.agg->get_bucket(dpp, tb.bucket->get_key(), &p, null_yield));
  EXPECT_EQ(now, p.enabled_at);
  // the directory is shared with the other tests' buckets
  bool listed = false;
  std::string marker;
  for (bool more = true; more && !listed;) {
    JSONFormattable g;
    ASSERT_EQ(0, admin::bucket_list(dpp, ctx, marker, 0, &g, null_yield));
    for (const auto& b : g["buckets"].array()) {
      listed = listed || b["bucket"]["name"].val() == tb.bucket->get_name();
    }
    more = g["truncated"].val_bool();
    marker = g["next_marker"].val();
  }
  EXPECT_TRUE(listed);

  // a PUT from now on takes the class, and the inspectors see the object
  ceph::real_time mtime;
  ASSERT_EQ(0, put_object(tb.bucket.get(), "obj", std::string(65536, 'x'), &mtime));
  auto cleanup = make_scope_guard([&] { delete_object(tb.bucket.get(), "obj"); });
  JSONFormattable h;
  ASSERT_EQ(0, admin::object_stat(dpp, ctx, tb.bucket.get(), rgw_obj_key("obj"),
                                  ceph::real_clock::now(), &h, null_yield));
  EXPECT_EQ("INTELLIGENT_TIER", h["storage_class"].val());
  EXPECT_EQ("hot", h["placement"].val());
  EXPECT_EQ("hold", h["decision"]["decision"].val());
  EXPECT_EQ("younger than cold-after", h["decision"]["reason"].val());
  JSONFormattable i;
  ASSERT_EQ(0, admin::bucket_stats(dpp, ctx, tb.bucket.get(), &i, null_yield));
  EXPECT_EQ(1, i["objects"]["hot"]["objects"].val_long_long());
  EXPECT_EQ(65536, i["objects"]["hot"]["bytes"].val_long_long());

  // disabled: the next PUT keeps the class it asks for
  JSONFormattable j;
  ASSERT_EQ(0, admin::bucket_disable(dpp, ctx, tb.bucket.get(), &j, null_yield));
  ASSERT_EQ(0, driver->load_bucket(dpp, tb.bucket->get_key(), &loaded, null_yield));
  config = get_bucket_config(loaded->get_attrs());
  ASSERT_TRUE(config);
  EXPECT_FALSE(config->enabled);
  EXPECT_DOUBLE_EQ(3, *config->half_life_days);
  rgw_placement_rule rule = loaded->get_placement_rule();
  select_placement(g_ceph_context, driver, loaded->get_attrs(), &rule);
  EXPECT_EQ(loaded->get_placement_rule(), rule);
  tb.bucket = std::move(loaded);
  ctx.reset(dpp);
}

int main(int argc, const char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = rgw_global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  DoutPrefix prefix(g_ceph_context, dout_subsys, "ceph_test_rgw_ite: ");
  dpp = &prefix;

  ceph::async::io_context_pool context_pool{cct->_conf->rgw_thread_pool_size};
  DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);
  auto cfgstore = DriverManager::create_config_store(
      dpp, g_conf().get_val<std::string>("rgw_config_store"));
  if (!cfgstore) {
    std::cerr << "unable to initialize the config store" << std::endl;
    return EXIT_FAILURE;
  }
  rgw::SiteConfig site_config;
  site = &site_config;
  if (int r = site_config.load(dpp, null_yield, cfgstore.get()); r < 0) {
    std::cerr << "unable to load the site config: " << cpp_strerror(-r) << std::endl;
    return EXIT_FAILURE;
  }
  // as radosgw-cr-test does: no gc, lc, restore, quota, sync or reshard threads
  driver = DriverManager::get_storage(dpp, g_ceph_context, cfg, context_pool, site_config,
                                      false, false, false, false, false, false,
                                      true, true, true, null_yield,
                                      cfgstore.get(), false);
  if (!driver) {
    std::cerr << "unable to initialize the RADOS driver" << std::endl;
    return EXIT_FAILURE;
  }

  ::testing::InitGoogleTest();
  int r = RUN_ALL_TESTS();
  DriverManager::close_storage(driver);
  return r;
}
