// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* The ITE decision engine: the state machine one instance at a time, and
 * the daily pass over in-memory contracts that writes the free
 * transitions and queues the byte moves. */

#include "rgw_ite_decision.h"

#include <chrono>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "rgw_ite.h"
#include "ite_fakes.h"

using namespace rgw::ite;
using namespace rgw::ite::test;

namespace {

struct DecideTest : public ::testing::Test {
  DecisionInputs in;

  void SetUp() override {
    in.now = from_epoch_secs(20000ull * 86400);
  }

  ceph::real_time ago(double days) const { return in.now - in.ts.days(days); }

  /* an instance of the given age whose temperature was t as of now */
  ObjectEntry obj(double age, double t,
                  RGWITETierState state = RGWITETierState::Hot) const {
    ObjectEntry e;
    e.key = rgw_obj_key("o");
    e.gen = 5;
    e.size = 1 << 20;
    e.mtime = ago(age);
    e.storage_class = "INTELLIGENT_TIER";
    e.ite.base_gen = 5;
    e.ite.temperature = t;
    e.ite.temp_updated_at = to_epoch_secs(in.now);
    e.ite.tier_state = state;
    return e;
  }
  ObjectEntry pseudo(double age, double t, double since) const {
    auto e = obj(age, t, RGWITETierState::Pseudo);
    e.ite.pseudo_since = to_epoch_secs(ago(since));
    return e;
  }
  /* a periodic verdict predicting an access in `in` days */
  static void predict(ObjectEntry& e, const DecisionInputs& in, double days,
                      double confidence = 0.9) {
    e.ite.fft_periodic = true;
    e.ite.fft_period_days = 30;
    e.ite.fft_confidence = confidence;
    e.ite.fft_next_access = to_epoch_secs(in.now + in.ts.days(days));
  }

  Decision d(const ObjectEntry& e) const { return decide(e, in).decision; }
};

} // anonymous namespace

TEST_F(DecideTest, SkipsWhatITEDoesNotTier)
{
  auto e = obj(40, 0);
  e.delete_marker = true;
  EXPECT_EQ(Decision::Skip, d(e));
  e = obj(40, 0);
  e.exists = false;
  EXPECT_EQ(Decision::Skip, d(e));
  e = obj(40, 0);
  e.storage_class = "STANDARD";
  EXPECT_EQ(Decision::Skip, d(e));
  e = obj(40, 0);
  e.size = 100; // under min_tier_size
  EXPECT_EQ(Decision::Skip, d(e));
}

TEST_F(DecideTest, HotCoolsThroughTheThresholds)
{
  EXPECT_EQ(Decision::Hold, d(obj(40, 0.5)));   // hot
  EXPECT_EQ(Decision::Hold, d(obj(40, 0.2)));   // lukewarm
  EXPECT_EQ(Decision::Pseudo, d(obj(40, 0.05))); // cold
  EXPECT_EQ("cold", decide(obj(40, 0.05), in).reason);
}

TEST_F(DecideTest, ColdAfterIsAResidencyFloor)
{
  const auto v = decide(obj(10, 0), in);
  EXPECT_EQ(Decision::Hold, v.decision);
  EXPECT_EQ("younger than cold-after", v.reason);
  EXPECT_EQ(Decision::Pseudo, d(obj(30, 0)));
}

TEST_F(DecideTest, TemperatureIsDecayedToNow)
{
  auto e = obj(40, 0.4);
  e.ite.temp_updated_at = to_epoch_secs(ago(21)); // three half-lives: 0.05
  EXPECT_EQ(Decision::Pseudo, d(e));
  e.ite.temp_updated_at = to_epoch_secs(ago(7)); // one: 0.2
  EXPECT_EQ(Decision::Hold, d(e));
}

TEST_F(DecideTest, PseudoGraceAndPullBack)
{
  EXPECT_EQ(Decision::Hold, d(pseudo(40, 0, 3)));   // grace is the half-life
  EXPECT_EQ(Decision::Commit, d(pseudo(40, 0, 7)));
  EXPECT_EQ(Decision::Hot, d(pseudo(40, 0.15, 10))); // accessed again
  EXPECT_EQ(Decision::Hot, d(pseudo(10, 0, 10)));    // cold-after was raised
}

TEST_F(DecideTest, CommittedPromotesOnlyWhenHotAgain)
{
  EXPECT_EQ(Decision::Hold, d(obj(90, 0.2, RGWITETierState::Committed)));
  EXPECT_EQ(Decision::Restore, d(obj(90, 0.3, RGWITETierState::Committed)));
  // the floor only guards demotion
  EXPECT_EQ(Decision::Restore, d(obj(5, 0.9, RGWITETierState::Committed)));
}

TEST_F(DecideTest, PredictedAccess)
{
  auto hot = obj(40, 0);
  predict(hot, in, 3);
  EXPECT_EQ(Decision::Hold, d(hot));
  EXPECT_EQ("access predicted", decide(hot, in).reason);

  auto committed = obj(90, 0, RGWITETierState::Committed);
  predict(committed, in, 3);
  EXPECT_EQ(Decision::PreWarm, d(committed));

  auto p = pseudo(40, 0, 10);
  predict(p, in, 3);
  EXPECT_EQ(Decision::Hold, d(p));

  // beyond the lead, in the past, below the confidence, or pre-warm off
  predict(committed, in, 10);
  EXPECT_EQ(Decision::Hold, d(committed));
  predict(committed, in, -1);
  EXPECT_EQ(Decision::Hold, d(committed));
  predict(committed, in, 3, 0.4);
  EXPECT_EQ(Decision::Hold, d(committed));
  predict(committed, in, 3);
  in.knobs.prewarm = false;
  EXPECT_EQ(Decision::Hold, d(committed));
  EXPECT_EQ(Decision::Pseudo, d(hot));
}

TEST_F(DecideTest, PendingMoveHolds)
{
  auto e = pseudo(40, 0, 10);
  e.ite.move_pending = RGWITEMovePending::Commit;
  EXPECT_EQ(Decision::Hold, d(e));
  EXPECT_EQ("move pending", decide(e, in).reason);
}

TEST_F(DecideTest, DemotionDisabled)
{
  in.demotion_enabled = false;
  EXPECT_EQ(Decision::Hold, d(obj(40, 0)));
  EXPECT_EQ(Decision::Hot, d(pseudo(40, 0, 10)));
  EXPECT_EQ(Decision::Restore, d(obj(90, 0.5, RGWITETierState::Committed)));
}

TEST_F(DecideTest, MaxAgeBackstop)
{
  in.max_age_days = 60;
  EXPECT_EQ(Decision::Hold, d(obj(50, 0.9)));
  EXPECT_EQ(Decision::Pseudo, d(obj(70, 0.9)));
  // past max-age a warm PSEUDO is not pulled back, and commits after grace
  EXPECT_EQ(Decision::Hold, d(pseudo(70, 0.9, 3)));
  EXPECT_EQ(Decision::Commit, d(pseudo(70, 0.9, 8)));
}

namespace {

struct EngineTest : public ::testing::Test {
  DoutPrefix dpp{g_ceph_context, ceph_subsys_rgw, "decision test: "};
  rgw::ite::Context ctx;
  MemAggStore* agg = nullptr;
  MemStateStore* state = nullptr;
  MemMoveQueue* moves = nullptr;
  const rgw_bucket b1{"", "tiered", "tiered-id"};
  const rgw_bucket b2{"", "disabled", "disabled-id"};
  TimeScale ts;
  ceph::real_time now = ceph::real_clock::now();

  void SetUp() override {
    ctx.cct = g_ceph_context;
    ctx.cookie = "this-gateway";
    ctx.zone_id = "zone-a";
    auto as = std::make_unique<MemAggStore>(4);
    agg = as.get();
    ctx.agg = std::move(as);
    auto ss = std::make_unique<MemStateStore>();
    state = ss.get();
    ctx.state = std::move(ss);
    auto mq = std::make_unique<MemMoveQueue>(4);
    moves = mq.get();
    ctx.moves = std::move(mq);
    ctx.bucket_loader = [this](const DoutPrefixProvider*, const rgw_bucket& b,
                               std::unique_ptr<rgw::sal::Bucket>* out, optional_yield) {
      *out = make_bucket(b, b != b2);
      return 0;
    };
    ts = ctx.timescale();
    enable(b1);
  }

  void enable(const rgw_bucket& b) {
    BucketProgress p;
    p.bucket = b;
    p.enabled_at = now - ts.days(100);
    agg->dir[b.get_key()] = p;
  }

  /* an instance 40 days old whose temperature was t as of now */
  ObjectEntry& put(const std::string& name, double t,
                   RGWITETierState tier = RGWITETierState::Hot, double pseudo_days = 0) {
    auto& e = state->put(b1, name, 5);
    e.mtime = now - ts.days(40);
    e.ite.temperature = t;
    e.ite.temp_updated_at = to_epoch_secs(now);
    e.ite.tier_state = tier;
    if (tier == RGWITETierState::Pseudo) {
      e.ite.pseudo_since = to_epoch_secs(now - ts.days(pseudo_days));
    }
    return e;
  }

  const rgw_bucket_ite_state& ite(const std::string& name) {
    auto e = state->find(b1, rgw_obj_key(name));
    EXPECT_TRUE(e);
    return e->ite;
  }

  int run(DecisionStats* stats = nullptr, ceph::real_time at = {}) {
    DecisionEngine engine(ctx);
    int r = engine.process_bucket(&dpp, b1, at == ceph::real_time() ? now : at, null_yield);
    if (stats) {
      *stats = engine.stats();
    }
    return r;
  }

  BucketProgress progress(const rgw_bucket& b) { return agg->dir[b.get_key()]; }

  void make_due() {
    auto p = progress(b1);
    p.last_decision = ceph::real_time();
    agg->dir[b1.get_key()] = p;
  }
};

} // anonymous namespace

TEST_F(EngineTest, WritesFreeTransitionsAndQueuesMoves)
{
  put("cold", 0);
  put("back", 0.5, RGWITETierState::Pseudo, 2);
  put("due", 0, RGWITETierState::Pseudo, 10);
  put("hot-again", 0.5, RGWITETierState::Committed);
  auto& soon = put("soon", 0, RGWITETierState::Committed);
  soon.ite.fft_periodic = true;
  soon.ite.fft_confidence = 0.9;
  soon.ite.fft_next_access = to_epoch_secs(now + ts.days(2));
  state->put(b1, "small", 5, 10);

  DecisionStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.buckets);
  EXPECT_EQ(6u, st.objects);
  EXPECT_EQ(1u, st.pseudo);
  EXPECT_EQ(1u, st.hot);
  EXPECT_EQ(1u, st.commit);
  EXPECT_EQ(1u, st.restore);
  EXPECT_EQ(1u, st.prewarm);
  EXPECT_EQ(1u, st.skip);

  EXPECT_EQ(RGWITETierState::Pseudo, ite("cold").tier_state);
  EXPECT_EQ(to_epoch_secs(now), ite("cold").pseudo_since);
  EXPECT_EQ(RGWITETierState::Hot, ite("back").tier_state);
  EXPECT_EQ(0u, ite("back").pseudo_since);
  // the engine only marks a queued move; the mover writes the terminal state
  EXPECT_EQ(RGWITETierState::Pseudo, ite("due").tier_state);
  EXPECT_EQ(RGWITEMovePending::Commit, ite("due").move_pending);
  EXPECT_EQ(RGWITEMovePending::Restore, ite("hot-again").move_pending);
  EXPECT_EQ(RGWITEMovePending::PreWarm, ite("soon").move_pending);

  const auto queued = moves->all();
  ASSERT_EQ(3u, queued.size());
  for (const auto& a : queued) {
    EXPECT_EQ(b1, a.bucket);
    EXPECT_EQ(5u, a.obj.gen);
    EXPECT_EQ("zone-a", a.zone_id);
    EXPECT_EQ(now, a.queued_at);
    if (a.obj.name == "due") {
      EXPECT_EQ(MoveKind::Commit, a.kind);
      EXPECT_EQ(Placement::Cold, a.target);
    } else if (a.obj.name == "hot-again") {
      EXPECT_EQ(MoveKind::Restore, a.kind);
      EXPECT_EQ(Placement::Hot, a.target);
    } else {
      EXPECT_EQ("soon", a.obj.name);
      EXPECT_EQ(MoveKind::PreWarm, a.kind);
      EXPECT_EQ(Placement::Hot, a.target);
    }
  }
  EXPECT_EQ(now, progress(b1).last_decision);
}

TEST_F(EngineTest, PendingMovesAreNotQueuedAgain)
{
  put("due", 0, RGWITETierState::Pseudo, 10);
  ASSERT_EQ(0, run());
  ASSERT_EQ(1u, moves->all().size());
  make_due();
  DecisionStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(0u, st.commit);
  EXPECT_EQ(1u, st.hold);
  EXPECT_EQ(1u, moves->all().size());
}

TEST_F(EngineTest, FailedQueueLeavesNoMarker)
{
  put("due", 0, RGWITETierState::Pseudo, 10);
  moves->append_ret = -EIO;
  ASSERT_EQ(-EIO, run());
  EXPECT_EQ(RGWITEMovePending::None, ite("due").move_pending);
  EXPECT_EQ(ceph::real_time(), progress(b1).last_decision);

  moves->append_ret = 0;
  ASSERT_EQ(0, run());
  EXPECT_EQ(RGWITEMovePending::Commit, ite("due").move_pending);
}

TEST_F(EngineTest, OverwriteBeforeTheWriteIsStale)
{
  put("cold", 0);
  state->before_set = [&](const rgw_obj_key& k) { state->overwrite(b1, k.name, 12); };
  DecisionStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.stale);
  EXPECT_EQ(0u, st.pseudo);
  EXPECT_EQ(RGWITETierState::Hot, ite("cold").tier_state); // the new instance
}

TEST_F(EngineTest, RunsOncePerPeriod)
{
  put("cold", 0);
  ASSERT_EQ(0, run());
  DecisionStats st;
  ASSERT_EQ(0, run(&st, now + std::chrono::hours(1)));
  EXPECT_EQ(1u, st.buckets_not_due);
  EXPECT_EQ(0u, st.objects);
  ASSERT_EQ(0, run(&st, now + ctx.period("rgw_ite_decision_period")));
  EXPECT_EQ(1u, st.buckets);
}

TEST_F(EngineTest, SkipsBusyAndDisabledBuckets)
{
  enable(b2);
  agg->locks.busy.insert("bucket." + b1.get_key());
  DecisionEngine engine(ctx);
  ASSERT_EQ(0, engine.process(&dpp, null_yield));
  EXPECT_EQ(1u, engine.stats().buckets_busy);
  EXPECT_EQ(1u, engine.stats().buckets_skipped);
  EXPECT_EQ(0u, engine.stats().buckets);
  EXPECT_TRUE(agg->locks.held.empty());
}

TEST_F(EngineTest, ShutdownLeavesTheBucketDue)
{
  put("cold", 0);
  ctx.going_down = true;
  ASSERT_EQ(0, run());
  EXPECT_EQ(ceph::real_time(), progress(b1).last_decision);
  EXPECT_EQ(0, state->sets);
}
