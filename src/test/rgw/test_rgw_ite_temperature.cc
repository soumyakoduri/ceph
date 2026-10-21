// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* The ITE temperature scorer over in-memory contracts: the decay math,
 * folding closed windows exactly once, attribution to the current
 * instance, catch-up, the sweep, and the per-bucket skips. */

#include "rgw_ite_temperature.h"

#include <chrono>
#include <cmath>
#include <thread>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "rgw_ite.h"
#include "ite_fakes.h"

using namespace rgw::ite;
using namespace rgw::ite::test;

namespace {

constexpr auto DAY = std::chrono::hours(24);

struct TemperatureTest : public ::testing::Test {
  DoutPrefix dpp{g_ceph_context, ceph_subsys_rgw, "temperature test: "};
  Context ctx;
  MemAggStore* agg = nullptr;
  MemStateStore* state = nullptr;
  const rgw_bucket b1{"", "hot", "hot-id"};
  const rgw_bucket b2{"", "disabled", "disabled-id"};
  const rgw_bucket b3{"", "deleted", "deleted-id"};
  TimeScale ts;
  Window today;
  ceph::real_time now; // noon of today, so yesterday has settled
  std::map<std::string, std::string> saved;

  void SetUp() override {
    ctx.cct = g_ceph_context;
    ctx.cookie = "this-gateway";
    auto as = std::make_unique<MemAggStore>(4);
    agg = as.get();
    ctx.agg = std::move(as);
    auto ss = std::make_unique<MemStateStore>();
    state = ss.get();
    ctx.state = std::move(ss);
    ctx.bucket_loader = [this](const DoutPrefixProvider*, const rgw_bucket& b,
                               std::unique_ptr<rgw::sal::Bucket>* out, optional_yield) {
      if (b == b3) {
        return -ENOENT;
      }
      *out = make_bucket(b, b != b2, 7.0);
      return 0;
    };
    ts = ctx.timescale();
    today = Window::of(ceph::real_clock::now(), Granularity::Day, ts);
    now = today.start(ts) + std::chrono::hours(12);
    enable(b1, today.start(ts) - 30 * DAY);
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

  /* a name whose counts land in another aggregated shard than name's */
  std::string in_other_shard(const std::string& name) {
    const uint32_t shards = agg->num_shards();
    for (int i = 0; ; ++i) {
      auto n = fmt::format("o{}", i);
      if (agg_shard_of(n, shards) != agg_shard_of(name, shards)) {
        return n;
      }
    }
  }

  void enable(const rgw_bucket& b, ceph::real_time at) {
    BucketProgress p;
    p.bucket = b;
    p.enabled_at = at;
    agg->dir[b.get_key()] = p;
  }

  Window back(uint64_t days) const { return Window{Granularity::Day, today.index - days}; }

  void count(const rgw_bucket& b, const Window& w, const std::string& name,
             uint64_t gen, Op op, uint64_t n, CallerType caller = CallerType::User) {
    CountKey k;
    k.obj = InstanceId(name, "", gen);
    k.op = op;
    k.caller = caller;
    const uint32_t shard = agg_shard_of(name, agg->num_shards());
    agg->windows[MemAggStore::Key{b.get_key(), w, shard}].counts[k.to_str()] += n;
  }

  const rgw_bucket_ite_state& ite(const std::string& name) {
    auto e = state->find(b1, rgw_obj_key(name));
    EXPECT_TRUE(e);
    return e->ite;
  }

  int score(TemperatureStats* stats = nullptr) {
    TemperatureScorer scorer(ctx);
    int r = scorer.process_bucket(&dpp, b1, now, null_yield);
    if (stats) {
      *stats = scorer.stats();
    }
    return r;
  }

  BucketProgress progress(const rgw_bucket& b) { return agg->dir[b.get_key()]; }
};

} // anonymous namespace

TEST(ITETemperature, DecayMath)
{
  TimeScale ts;
  EXPECT_DOUBLE_EQ(0.5, decay(1.0, 7, 7));
  EXPECT_DOUBLE_EQ(0.25, decay(1.0, 14, 7));
  EXPECT_DOUBLE_EQ(0.8, decay(0.8, 0, 7));
  EXPECT_DOUBLE_EQ(0.8, decay(0.8, -3, 7)); // never raised
  EXPECT_DOUBLE_EQ(0.0, decay(0.0, 3, 7));

  rgw_bucket_ite_state s;
  s.temperature = 0.8;
  s.temp_updated_at = 1000 * 86400;
  const auto t = from_epoch_secs(s.temp_updated_at);
  EXPECT_DOUBLE_EQ(0.4, temperature_at(s, t + 7 * DAY, 7, ts));
  EXPECT_DOUBLE_EQ(0.8, temperature_at(s, t - DAY, 7, ts));
  EXPECT_DOUBLE_EQ(0.6, fold(s, t + 7 * DAY, 0.2, 7, ts));
  EXPECT_DOUBLE_EQ(1.0, fold(s, t, 0.5, 7, ts)); // capped
  EXPECT_DOUBLE_EQ(0.1, fold(rgw_bucket_ite_state(), t, 0.1, 7, ts));
}

TEST_F(TemperatureTest, FoldsTheClosedWindowOnce)
{
  state->put(b1, "a", 5);
  state->put(b1, "b", 5);
  count(b1, back(1), "a", 5, Op::Head, 2); // 0.2
  count(b1, back(1), "b", 0, Op::Put, 1);  // a write: generation unknown, 0.5
  count(b1, today, "a", 5, Op::Get, 9);    // still open, not scored

  TemperatureStats st;
  ASSERT_EQ(0, score(&st));
  EXPECT_DOUBLE_EQ(0.2, ite("a").temperature);
  EXPECT_DOUBLE_EQ(0.5, ite("b").temperature);
  EXPECT_EQ(to_epoch_secs(back(1).end(ts)), ite("a").temp_updated_at);
  EXPECT_EQ(1u, st.windows);
  EXPECT_EQ(2u, st.objects);
  ASSERT_TRUE(progress(b1).last_scored);
  EXPECT_EQ(back(1), *progress(b1).last_scored);

  // a second run finds nothing new to fold
  ASSERT_EQ(0, score(&st));
  EXPECT_EQ(0u, st.windows);
  EXPECT_DOUBLE_EQ(0.2, ite("a").temperature);
}

TEST_F(TemperatureTest, RerunAfterFailureDoesNotDoubleCount)
{
  state->put(b1, "a", 5);
  state->put(b1, "b", 5);
  count(b1, back(1), "a", 5, Op::Head, 2);
  count(b1, back(1), "b", 5, Op::Head, 3);
  int sets = 0;
  state->before_set = [&](const rgw_obj_key&) {
    if (++sets == 2) {
      state->set_ret = -EIO; // dies halfway through the window
    }
  };
  ASSERT_EQ(-EIO, score());
  EXPECT_FALSE(progress(b1).last_scored); // the window is not marked scored

  state->before_set = nullptr;
  state->set_ret = 0;
  TemperatureStats st;
  ASSERT_EQ(0, score(&st));
  EXPECT_EQ(1u, st.already);
  EXPECT_DOUBLE_EQ(0.2, ite("a").temperature);
  EXPECT_DOUBLE_EQ(0.3, ite("b").temperature);
}

TEST_F(TemperatureTest, DecaysTheStoredValueForward)
{
  auto& a = state->put(b1, "a", 5);
  a.ite.temperature = 0.8;
  a.ite.temp_updated_at = to_epoch_secs(back(1).end(ts) - 7 * DAY); // one half-life
  count(b1, back(1), "a", 5, Op::Head, 2);
  ASSERT_EQ(0, score());
  EXPECT_NEAR(0.6, ite("a").temperature, 1e-9);
}

TEST_F(TemperatureTest, CatchesUpMissedWindowsInOrder)
{
  state->put(b1, "a", 5);
  BucketProgress p = progress(b1);
  p.last_scored = back(4);
  agg->dir[b1.get_key()] = p;
  count(b1, back(3), "a", 5, Op::Head, 1);
  count(b1, back(1), "a", 5, Op::Head, 1);

  TemperatureStats st;
  ASSERT_EQ(0, score(&st));
  EXPECT_EQ(2u, st.windows);
  EXPECT_NEAR(0.1 * std::exp2(-2.0 / 7) + 0.1, ite("a").temperature, 1e-9);
  EXPECT_EQ(back(1), *progress(b1).last_scored);
}

TEST_F(TemperatureTest, FirstRunStartsAtEnablement)
{
  enable(b1, back(1).start(ts) + std::chrono::hours(1));
  state->put(b1, "a", 5);
  count(b1, back(3), "a", 5, Op::Head, 5); // before ITE was enabled
  count(b1, back(1), "a", 5, Op::Head, 1);
  ASSERT_EQ(0, score());
  EXPECT_DOUBLE_EQ(0.1, ite("a").temperature);
}

TEST_F(TemperatureTest, OnlyTheCurrentInstanceIsCredited)
{
  state->put(b1, "a", 9); // overwritten at generation 9
  count(b1, back(1), "a", 5, Op::Get, 3);  // reads of the replaced instance
  count(b1, back(1), "a", 9, Op::Head, 1); // a read of this one
  count(b1, back(1), "a", 0, Op::Put, 1);  // the overwrite itself
  TemperatureStats st;
  ASSERT_EQ(0, score(&st));
  EXPECT_DOUBLE_EQ(0.6, ite("a").temperature);
  EXPECT_EQ(1u, st.stale);
}

TEST_F(TemperatureTest, OverwriteDuringScoringIsRefused)
{
  state->put(b1, "a", 5);
  count(b1, back(1), "a", 5, Op::Get, 1);
  state->before_set = [&](const rgw_obj_key& k) { state->overwrite(b1, k.name, 12); };
  TemperatureStats st;
  ASSERT_EQ(0, score(&st));
  EXPECT_EQ(1u, st.stale);
  EXPECT_DOUBLE_EQ(0.0, ite("a").temperature); // the new instance starts cold
  EXPECT_EQ(12u, ite("a").base_gen);
}

TEST_F(TemperatureTest, IgnoresWhatIsNotUserReadPressure)
{
  state->put(b1, "a", 5);
  count(b1, back(1), "a", 5, Op::Get, 5, CallerType::Replication);
  count(b1, back(1), "a", 5, Op::Delete, 1);
  count(b1, back(1), "gone", 5, Op::Get, 1);
  agg->windows[MemAggStore::Key{b1.get_key(), back(1), 0}].counts["garbage"] = 1;
  TemperatureStats st;
  ASSERT_EQ(0, score(&st));
  EXPECT_DOUBLE_EQ(0.0, ite("a").temperature);
  EXPECT_EQ(0, state->sets);
  EXPECT_EQ(1u, st.missing);
  EXPECT_EQ(1u, st.unparsable);
}

TEST_F(TemperatureTest, SweepCoolsUntouchedObjects)
{
  const auto end = back(1).end(ts);
  auto& c = state->put(b1, "cold", 5);
  c.ite.temperature = 0.5;
  c.ite.temp_updated_at = to_epoch_secs(end - 14 * DAY);
  auto& d = state->put(b1, "faint", 5);
  d.ite.temperature = 1e-6;
  d.ite.temp_updated_at = to_epoch_secs(end - 70 * DAY);
  state->put(b1, "never", 5);

  TemperatureStats st;
  ASSERT_EQ(0, score(&st));
  EXPECT_EQ(2u, st.swept);
  EXPECT_DOUBLE_EQ(0.125, ite("cold").temperature);
  // decayed to the end of the last scored window, not to now, so that a
  // window scored later still folds in
  EXPECT_EQ(to_epoch_secs(end), ite("cold").temp_updated_at);
  EXPECT_EQ(0.0, ite("faint").temperature);
  EXPECT_EQ(0u, ite("never").temp_updated_at);
  EXPECT_EQ(now, progress(b1).last_sweep);

  // the sweep is weekly
  ASSERT_EQ(0, score(&st));
  EXPECT_EQ(0u, st.swept);
}

TEST_F(TemperatureTest, SkipsBusyDisabledAndDeletedBuckets)
{
  enable(b2, now - 30 * DAY);
  enable(b3, now - 30 * DAY);
  agg->locks.busy.insert("bucket." + b1.get_key());
  TemperatureScorer scorer(ctx);
  ASSERT_EQ(0, scorer.process(&dpp, null_yield));
  EXPECT_EQ(0u, scorer.stats().buckets);
  EXPECT_EQ(1u, scorer.stats().buckets_busy);
  EXPECT_EQ(2u, scorer.stats().buckets_skipped);
  EXPECT_TRUE(agg->locks.held.empty()); // every lock taken was released
}

TEST_F(TemperatureTest, LosingTheLockStopsTheRun)
{
  set_conf("rgw_ite_lock_max_time", "1"); // renewed every third of a second
  const std::string other = in_other_shard("a");
  state->put(b1, "a", 5);
  state->put(b1, other, 5);
  count(b1, back(1), "a", 5, Op::Head, 1);
  count(b1, back(1), other, 5, Op::Head, 1);
  const std::string lock = "bucket." + b1.get_key();
  state->before_set = [&](const rgw_obj_key&) {
    // another gateway takes the bucket over while the first shard is folded
    agg->locks.held[lock] = "other-gateway";
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
  };
  ASSERT_EQ(0, score());
  EXPECT_EQ(1, state->sets); // the run stopped at the next shard
  EXPECT_FALSE(progress(b1).last_scored);
  EXPECT_EQ("other-gateway", agg->locks.held[lock]); // not released as ours
}

TEST_F(TemperatureTest, ShutdownLeavesNoPartialProgress)
{
  state->put(b1, "a", 5);
  count(b1, back(1), "a", 5, Op::Head, 1);
  ctx.going_down = true;
  ASSERT_EQ(0, score());
  EXPECT_FALSE(progress(b1).last_scored);
  EXPECT_EQ(0, state->sets);
}

TEST_F(TemperatureTest, ClosedWindowsWithoutCountsAdvanceProgress)
{
  // closed windows without counts still advance the progress
  ASSERT_EQ(0, score());
  ASSERT_TRUE(progress(b1).last_scored);
  EXPECT_EQ(back(1), *progress(b1).last_scored);
}
