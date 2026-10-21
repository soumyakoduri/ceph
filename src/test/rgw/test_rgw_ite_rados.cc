// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * ceph_test_rgw_ite: the RADOS implementation of the ITE SAL contracts,
 * and the workers running over it, against a running cluster (vstart).
 * Each test works in its own pool, so runs never see each other's state.
 */

#include <gtest/gtest.h>
#include <fmt/format.h>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "rgw_common.h"
#include "rgw_ite.h"
#include "rgw_ite_aggregator.h"
#include "rgw_ite_types.h"
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
  rgw::SiteConfig site;
  if (int r = site.load(dpp, null_yield, cfgstore.get()); r < 0) {
    std::cerr << "unable to load the site config: " << cpp_strerror(-r) << std::endl;
    return EXIT_FAILURE;
  }
  // as radosgw-cr-test does: no gc, lc, restore, quota, sync or reshard threads
  driver = DriverManager::get_storage(dpp, g_ceph_context, cfg, context_pool, site,
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
