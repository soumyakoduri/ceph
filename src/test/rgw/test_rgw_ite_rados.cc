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
#include "global/global_init.h"
#include "rgw_common.h"
#include "rgw_ite.h"
#include "rgw_ite_aggregator.h"
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
      index_op(CLS_RGW_OP_DEL, name, epoch + 1, 0);
    }
    bucket->remove(dpp, false, null_yield);
  }

  int add_entry(const std::string& name, uint64_t epoch, uint64_t size) {
    int r = index_op(CLS_RGW_OP_ADD, name, epoch, size);
    if (r == 0) {
      entries[name] = epoch;
    }
    return r;
  }

  int index_op(RGWModifyOp op, const std::string& name, uint64_t epoch, uint64_t size) {
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
    meta.mtime = ceph::real_clock::now();
    meta.storage_class = "INTELLIGENT_TIER";
    librados::ObjectWriteOperation complete;
    cls_rgw_bucket_complete_op(complete, op, tag, ver, key, meta,
                               nullptr, false, 0, nullptr);
    return bs.bucket_obj.ioctx.operate(bs.bucket_obj.obj.oid, &complete);
  }
};

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
