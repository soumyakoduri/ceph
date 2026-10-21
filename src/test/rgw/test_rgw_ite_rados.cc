// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * ceph_test_rgw_ite: the RADOS implementation of the ITE SAL contracts,
 * against a running cluster (vstart). Each test works in its own pool
 * namespace, so runs never see each other's state.
 */

#include <gtest/gtest.h>
#include <fmt/format.h>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "rgw_common.h"
#include "rgw_ite_types.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_sal_rados.h"

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
