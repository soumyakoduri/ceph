// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* The ITE aggregator over in-memory contracts: counting, trimming,
 * exactly-once replay, lock skipping, paging, registration and expiry. */

#include "rgw_ite_aggregator.h"

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "rgw_ite.h"
#include "ite_fakes.h"

using namespace rgw::ite;
using namespace rgw::ite::test;

namespace {

struct AggregatorTest : public ::testing::Test {
  DoutPrefix dpp{g_ceph_context, ceph_subsys_rgw, "aggregator test: "};
  rgw::ite::Context ctx;
  MemWriteBuffer* buffer = nullptr;
  MemAggStore* agg = nullptr;
  const rgw_bucket b1{"", "b1", "b1-id"};
  const rgw_bucket b2{"tenant", "b2", "b2-id"};
  const ceph::real_time now = ceph::real_clock::now();
  TimeScale ts;
  std::map<std::string, std::string> saved;

  void SetUp() override {
    ctx.cct = g_ceph_context;
    ctx.cookie = "this-gateway";
    contracts(4, 2);
    ts = ctx.timescale();
  }

  void TearDown() override {
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

  void contracts(uint32_t buffer_shards, uint32_t agg_shards) {
    auto wb = std::make_unique<MemWriteBuffer>(buffer_shards);
    buffer = wb.get();
    ctx.buffer = std::move(wb);
    auto as = std::make_unique<MemAggStore>(agg_shards);
    agg = as.get();
    ctx.agg = std::move(as);
  }

  void access(const rgw_bucket& b, const std::string& name, Op op,
              ceph::real_time t, CallerType caller = CallerType::User) {
    AccessEvent e;
    e.bucket = b;
    e.obj = InstanceId(name, "", 7);
    e.timestamp = t;
    e.op = op;
    e.caller = caller;
    ASSERT_EQ(0, buffer->append(&dpp, e, null_yield));
  }

  uint64_t count(const rgw_bucket& b, ceph::real_time t, const std::string& name, Op op) {
    CountKey k;
    k.obj = InstanceId(name, "", 7);
    k.op = op;
    return agg->count(b, Window::of(t, ctx.granularity(), ts), k);
  }

  int process(AggregatorStats* stats = nullptr) {
    Aggregator a(ctx);
    int r = a.process(&dpp, null_yield);
    if (stats) {
      *stats = a.stats();
    }
    return r;
  }
};

} // anonymous namespace

TEST(ITERetention, Arithmetic)
{
  EXPECT_EQ(97u, retention_windows(90, 7, Granularity::Day));
  EXPECT_EQ(91u, retention_windows(90, 7, Granularity::Week));
  EXPECT_EQ(90u, retention_windows(90, 0, Granularity::Week));
  EXPECT_EQ(92u, retention_windows(90, 8, Granularity::Week));
  EXPECT_EQ(0u, oldest_accepted(5, 97));

  // no window is both accepted and expired, and exactly one guard window
  // between them is neither
  const uint64_t cur = 300, keep = 97;
  int neither = 0;
  for (uint64_t w = 0; w <= cur; ++w) {
    const bool accepted = w >= oldest_accepted(cur, keep);
    const bool expired = is_expired(w, cur, keep);
    EXPECT_FALSE(accepted && expired) << w;
    if (!accepted && !expired) {
      ++neither;
      EXPECT_EQ(cur - keep, w);
    }
  }
  EXPECT_EQ(1, neither);
}

TEST_F(AggregatorTest, CountsEveryEventOnceAndTrims)
{
  for (int i = 0; i < 3; ++i) {
    access(b1, "a", Op::Get, now);
  }
  access(b1, "a", Op::Head, now);
  access(b1, "b", Op::Put, now);
  access(b2, "a", Op::Get, now);

  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(3u, count(b1, now, "a", Op::Get));
  EXPECT_EQ(1u, count(b1, now, "a", Op::Head));
  EXPECT_EQ(1u, count(b1, now, "b", Op::Put));
  EXPECT_EQ(1u, count(b2, now, "a", Op::Get));
  EXPECT_EQ(0u, count(b2, now, "b", Op::Put));
  EXPECT_EQ(0u, buffer->size());
  EXPECT_EQ(6u, st.events);
  EXPECT_EQ(6u, st.counted);
  EXPECT_EQ(0u, st.replayed);
  EXPECT_EQ(4u, st.shards);
  EXPECT_TRUE(buffer->locks.held.empty()); // every shard lock released

  // both buckets entered the directory, dated by their first event
  ASSERT_EQ(2u, agg->dir.size());
  EXPECT_EQ(now, agg->dir[b1.get_key()].enabled_at);
  EXPECT_EQ(2u, st.buckets_registered);

  // a second cycle has nothing to do
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(0u, st.events);
  EXPECT_EQ(3u, count(b1, now, "a", Op::Get));
}

TEST_F(AggregatorTest, SplitsByWindowAndShard)
{
  const auto yesterday = now - std::chrono::hours(24);
  access(b1, "a", Op::Get, now);
  access(b1, "a", Op::Get, yesterday);
  access(b1, "a", Op::Get, yesterday);
  ASSERT_EQ(0, process());
  EXPECT_EQ(1u, count(b1, now, "a", Op::Get));
  EXPECT_EQ(2u, count(b1, yesterday, "a", Op::Get));

  // each object is in exactly the aggregate shard it hashes to
  for (const auto& [key, ws] : agg->windows) {
    for (const auto& [k, n] : ws.counts) {
      auto ck = CountKey::parse(k);
      ASSERT_TRUE(ck);
      EXPECT_EQ(agg_shard_of(ck->obj.name, agg->num_shards()), std::get<2>(key));
    }
  }
}

TEST_F(AggregatorTest, WeekWindows)
{
  set_conf("rgw_ite_window_granularity", "week");
  const Window w = Window::of(now, Granularity::Week, ts);
  // two days of the same week land in one window, unless that is the future
  const auto monday = w.start(ts) + std::chrono::hours(1);
  const auto tuesday = monday + std::chrono::hours(24);
  access(b1, "a", Op::Get, monday);
  if (tuesday < now) {
    access(b1, "a", Op::Get, tuesday);
  }
  ASSERT_EQ(0, process());
  EXPECT_EQ(tuesday < now ? 2u : 1u, count(b1, monday, "a", Op::Get));
  for (const auto& [key, ws] : agg->windows) {
    EXPECT_EQ(Granularity::Week, std::get<1>(key).granularity);
  }
}

TEST_F(AggregatorTest, DropsNonUserAndExpiredEvents)
{
  access(b1, "a", Op::Get, now, CallerType::Replication);
  access(b1, "a", Op::Get, now - std::chrono::hours(24 * 100));
  access(b1, "a", Op::Get, now);
  access(b2, "a", Op::Get, now, CallerType::Lifecycle);

  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(1u, count(b1, now, "a", Op::Get));
  EXPECT_EQ(2u, st.not_user);
  EXPECT_EQ(1u, st.too_old);
  EXPECT_EQ(1u, st.counted);
  // dropped events are consumed too
  EXPECT_EQ(0u, buffer->size());
  // a bucket with no counted event is not registered
  EXPECT_EQ(1u, agg->dir.size());
  EXPECT_EQ(0u, agg->dir.count(b2.get_key()));
}

TEST_F(AggregatorTest, ReplayAfterFailedTrimDoesNotDoubleCount)
{
  for (int i = 0; i < 5; ++i) {
    access(b1, "a", Op::Get, now);
  }
  buffer->trim_ret = -EIO;
  ASSERT_EQ(-EIO, process());
  EXPECT_EQ(5u, count(b1, now, "a", Op::Get));
  EXPECT_EQ(5u, buffer->size()); // consumed but not trimmed

  // the next cycle replays the page; the stored markers filter it out
  buffer->trim_ret = 0;
  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(5u, count(b1, now, "a", Op::Get));
  EXPECT_EQ(0u, buffer->size());
  EXPECT_EQ(0u, st.counted);
  EXPECT_EQ(5u, st.replayed);
}

TEST_F(AggregatorTest, PartialApplyFailureIsRetriedExactlyOnce)
{
  // one buffer shard, so both objects are in one page, but on two
  // aggregate shards, so the page applies as two groups
  contracts(1, 64);
  std::string x = "a", y;
  for (int i = 0; y.empty(); ++i) {
    std::string n = fmt::format("o{}", i);
    if (agg_shard_of(n, 64) != agg_shard_of(x, 64)) {
      y = n;
    }
  }
  access(b1, x, Op::Get, now);
  access(b1, y, Op::Get, now);

  int calls = 0;
  agg->before_add = [&](const MemAggStore::Key&, uint32_t) {
    if (++calls == 2) {
      agg->add_ret = -EIO; // the second group fails
    }
  };
  ASSERT_EQ(-EIO, process());
  EXPECT_EQ(1, agg->adds);
  EXPECT_EQ(2u, buffer->size()); // the page is not trimmed

  agg->before_add = nullptr;
  agg->add_ret = 0;
  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(1u, count(b1, now, x, Op::Get));
  EXPECT_EQ(1u, count(b1, now, y, Op::Get));
  EXPECT_EQ(1u, st.counted);
  EXPECT_EQ(1u, st.replayed);
  EXPECT_EQ(0u, buffer->size());
}

TEST_F(AggregatorTest, MarkerMovedElsewhereCountsOnlyTheRest)
{
  contracts(1, 1);
  for (int i = 0; i < 4; ++i) {
    access(b1, "a", Op::Get, now);
  }
  // another gateway, whose lock outlived ours, folds in the first two events
  // just before our apply
  bool once = false;
  agg->before_add = [&](const MemAggStore::Key& key, uint32_t shard) {
    if (once) {
      return;
    }
    once = true;
    auto& ws = agg->windows[key];
    CountKey k;
    k.obj = InstanceId("a", "", 7);
    ws.counts[k.to_str()] += 2;
    ws.markers[shard] = buffer->data[0][1].marker;
  };
  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(4u, count(b1, now, "a", Op::Get));
  EXPECT_EQ(2u, st.counted);
  EXPECT_EQ(2u, st.replayed);
}

TEST_F(AggregatorTest, ShardOwnedElsewhereIsSkipped)
{
  const uint32_t busy = buffer_shard_of(b1, "a", buffer->num_shards());
  std::string other;
  for (int i = 0; other.empty(); ++i) {
    std::string n = fmt::format("o{}", i);
    if (buffer_shard_of(b1, n, buffer->num_shards()) != busy) {
      other = n;
    }
  }
  access(b1, "a", Op::Get, now);
  access(b1, other, Op::Get, now);
  buffer->locks.busy.insert(fmt::format("ite-log.{}", busy));

  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(1u, st.shards_busy);
  EXPECT_EQ(0u, count(b1, now, "a", Op::Get));
  EXPECT_EQ(1u, count(b1, now, other, Op::Get));
  EXPECT_EQ(1u, buffer->data[busy].size());

  buffer->locks.busy.clear();
  ASSERT_EQ(0, process());
  EXPECT_EQ(1u, count(b1, now, "a", Op::Get));
  EXPECT_EQ(1u, count(b1, now, other, Op::Get));
}

TEST_F(AggregatorTest, PagesThroughABacklog)
{
  contracts(1, 2);
  const int n = 2 * Aggregator::page_size + 100;
  for (int i = 0; i < n; ++i) {
    access(b1, fmt::format("o{}", i % 10), Op::Get, now);
  }
  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  uint64_t total = 0;
  for (int i = 0; i < 10; ++i) {
    total += count(b1, now, fmt::format("o{}", i), Op::Get);
  }
  EXPECT_EQ(static_cast<uint64_t>(n), total);
  EXPECT_EQ(3, buffer->trims); // one trim per page
  EXPECT_EQ(0u, buffer->size());
}

TEST_F(AggregatorTest, RegistrationKeepsExistingProgress)
{
  BucketProgress p;
  p.bucket = b1;
  p.enabled_at = now - std::chrono::hours(240);
  p.last_scored = Window{Granularity::Day, 42};
  agg->dir[b1.get_key()] = p;

  access(b1, "a", Op::Get, now);
  access(b2, "a", Op::Get, now - std::chrono::hours(2));
  access(b2, "b", Op::Get, now);
  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(1u, st.buckets_registered);
  EXPECT_EQ(p.enabled_at, agg->dir[b1.get_key()].enabled_at);
  ASSERT_TRUE(agg->dir[b1.get_key()].last_scored);
  EXPECT_EQ(42u, agg->dir[b1.get_key()].last_scored->index);
  ASSERT_EQ(1u, agg->dir.count(b2.get_key()));
}

TEST_F(AggregatorTest, ExpiryKeepsTheRetainedSpan)
{
  set_conf("rgw_ite_fft_span", "10");
  set_conf("rgw_ite_agg_retention_margin", "0");
  const uint64_t cur = Window::of(now, Granularity::Day, ts).index;
  const uint64_t wcur = Window::of(now, Granularity::Week, ts).index;
  auto put = [&](Granularity g, uint64_t index) {
    agg->windows[MemAggStore::Key{b1.get_key(), Window{g, index}, 0}].counts["x::1:GET:USER"] = 1;
  };
  for (uint64_t back : {0, 5, 10, 11, 30}) {
    put(Granularity::Day, cur - back);
  }
  for (uint64_t back : {2, 11}) {
    put(Granularity::Week, wcur - back);
  }
  BucketProgress p;
  p.bucket = b1;
  agg->dir[b1.get_key()] = p;

  Aggregator a(ctx);
  ASSERT_EQ(0, a.expire(&dpp, now, null_yield));
  EXPECT_EQ(3u, a.stats().windows_expired);
  std::set<std::pair<Granularity, uint64_t>> left;
  for (const auto& [key, ws] : agg->windows) {
    left.emplace(std::get<1>(key).granularity, std::get<1>(key).index);
  }
  const std::set<std::pair<Granularity, uint64_t>> expected = {
    {Granularity::Day, cur}, {Granularity::Day, cur - 5}, {Granularity::Day, cur - 10},
    {Granularity::Week, wcur - 2},
  };
  EXPECT_EQ(expected, left);
}

TEST_F(AggregatorTest, ShutdownStopsBetweenShards)
{
  access(b1, "a", Op::Get, now);
  ctx.going_down = true;
  AggregatorStats st;
  ASSERT_EQ(0, process(&st));
  EXPECT_EQ(0u, st.shards);
  EXPECT_EQ(1u, buffer->size());
}
