// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* The ITE lazy mover over in-memory contracts: the three byte moves, the
 * terminal state written only once the bytes are in place, reconciliation
 * on replay, retries, and the throttle. */

#include "rgw_ite_mover.h"

#include <chrono>
#include <map>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "rgw_ite.h"
#include "ite_fakes.h"

using namespace rgw::ite;
using namespace rgw::ite::test;

namespace {

struct MoverTest : public ::testing::Test {
  DoutPrefix dpp{g_ceph_context, ceph_subsys_rgw, "mover test: "};
  rgw::ite::Context ctx;
  MemStateStore* state = nullptr;
  MemMoveQueue* moves = nullptr;
  std::unique_ptr<MemPlacements> placements;
  const rgw_bucket b1{"", "tiered", "tiered-id"};
  const rgw_bucket b2{"", "disabled", "disabled-id"};
  ceph::real_time now = ceph::real_clock::now();
  std::map<std::string, std::string> saved;

  void SetUp() override {
    ctx.cct = g_ceph_context;
    ctx.cookie = "this-gateway";
    ctx.zone_id = "zone-a";
    auto ss = std::make_unique<MemStateStore>();
    state = ss.get();
    ctx.state = std::move(ss);
    auto mq = std::make_unique<MemMoveQueue>(4);
    moves = mq.get();
    ctx.moves = std::move(mq);
    placements = std::make_unique<MemPlacements>(state);
    ctx.bucket_loader = [this](const DoutPrefixProvider*, const rgw_bucket& b,
                               std::unique_ptr<rgw::sal::Bucket>* out, optional_yield) {
      if (b.name == "deleted") {
        return -ENOENT;
      }
      *out = make_bucket(b, b != b2, {}, placements.get());
      return 0;
    };
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

  MoveAction action(const std::string& name, MoveKind kind, uint64_t gen = 5,
                    const rgw_bucket* b = nullptr) const {
    MoveAction a;
    a.bucket = b ? *b : b1;
    a.obj = InstanceId(name, "", gen);
    a.kind = kind;
    a.target = target_of(kind);
    a.zone_id = "zone-a";
    a.queued_at = now - std::chrono::hours(1); // queued by an earlier pass
    return a;
  }
  void queue(const MoveAction& a) {
    ASSERT_EQ(0, moves->append(&dpp, a, null_yield));
  }

  /* an instance the engine queued a move for: its bytes where they are
   * before the move, its state and mark, and the action on the queue */
  ObjectEntry& pending(const std::string& name, MoveKind kind, const rgw_bucket& b) {
    auto& e = state->put(b, name, 5);
    e.ite.tier_state = kind == MoveKind::Commit ? RGWITETierState::Pseudo
                                                : RGWITETierState::Committed;
    e.ite.pseudo_since = kind == MoveKind::Commit ? to_epoch_secs(now) - 86400 * 8 : 0;
    e.ite.move_pending = move_pending_of(kind);
    placements->set(b, name, kind == MoveKind::Commit ? Placement::Hot : Placement::Cold);
    queue(action(name, kind, 5, &b));
    return e;
  }
  ObjectEntry& pending(const std::string& name, MoveKind kind) {
    return pending(name, kind, b1);
  }

  const rgw_bucket_ite_state& ite(const std::string& name) {
    auto e = state->find(b1, rgw_obj_key(name));
    EXPECT_TRUE(e);
    return e->ite;
  }
  Placement at(const std::string& name) { return placements->at(b1, name); }

  /* one mover cycle over every shard, as of now */
  int run(MoverStats* stats = nullptr) {
    Mover mover(ctx);
    int ret = 0;
    for (uint32_t shard = 0; shard < moves->num_shards(); ++shard) {
      int r = mover.process_shard(&dpp, shard, now, null_yield);
      if (r < 0) {
        ret = r;
      }
    }
    if (stats) {
      *stats = mover.stats();
    }
    return ret;
  }
};

} // anonymous namespace

TEST_F(MoverTest, CommitMovesTheBytesThenTheState)
{
  placements->before_move = [&](const rgw_obj_key&) {
    // nothing claims COMMITTED before the bytes have moved
    EXPECT_EQ(RGWITETierState::Pseudo, ite("cold").tier_state);
  };
  pending("cold", MoveKind::Commit);
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(Placement::Cold, at("cold"));
  EXPECT_EQ(RGWITETierState::Committed, ite("cold").tier_state);
  EXPECT_EQ(0u, ite("cold").pseudo_since);
  EXPECT_EQ(RGWITEMovePending::None, ite("cold").move_pending);
  EXPECT_EQ(1u, ite("cold").commit_count);
  EXPECT_EQ(0u, ite("cold").restore_count);
  EXPECT_EQ(1u, st.started);
  EXPECT_EQ(1u, st.commits);
  EXPECT_EQ(0u, st.reconciled);
  EXPECT_TRUE(moves->all().empty()); // acknowledged
  EXPECT_EQ(1, moves->acks);
}

TEST_F(MoverTest, PreWarmLandsInPseudo)
{
  pending("soon", MoveKind::PreWarm);
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(Placement::Hot, at("soon"));
  EXPECT_EQ(RGWITETierState::Pseudo, ite("soon").tier_state);
  EXPECT_EQ(to_epoch_secs(now), ite("soon").pseudo_since); // the grace clock restarts
  EXPECT_EQ(1u, ite("soon").restore_count);
  EXPECT_EQ(RGWITEMovePending::None, ite("soon").move_pending);
  EXPECT_EQ(1u, st.prewarms);
}

TEST_F(MoverTest, RestoreLandsInHot)
{
  auto& e = pending("hot-again", MoveKind::Restore);
  e.ite.commit_count = 1;
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(Placement::Hot, at("hot-again"));
  EXPECT_EQ(RGWITETierState::Hot, ite("hot-again").tier_state);
  EXPECT_EQ(0u, ite("hot-again").pseudo_since);
  EXPECT_EQ(1u, ite("hot-again").commit_count);
  EXPECT_EQ(1u, ite("hot-again").restore_count);
  EXPECT_EQ(1u, st.restores);
}

TEST_F(MoverTest, BytesAlreadyInPlaceOnlyWriteTheState)
{
  // a crash after the move, before the state was written
  pending("cold", MoveKind::Commit);
  placements->set(b1, "cold", Placement::Cold);
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(0, placements->moves);
  EXPECT_EQ(0u, st.started);
  EXPECT_EQ(1u, st.reconciled);
  EXPECT_EQ(RGWITETierState::Committed, ite("cold").tier_state);
  EXPECT_EQ(1u, ite("cold").commit_count);
}

TEST_F(MoverTest, AnActionQueuedTwiceMovesOnce)
{
  pending("cold", MoveKind::Commit);
  queue(action("cold", MoveKind::Commit));
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1, placements->moves);
  EXPECT_EQ(1u, st.commits);
  EXPECT_EQ(1u, st.superseded);
  EXPECT_EQ(1u, ite("cold").commit_count);
  EXPECT_TRUE(moves->all().empty());
}

TEST_F(MoverTest, AFreshActionWaitsForItsMark)
{
  state->put(b1, "cold", 5).ite.tier_state = RGWITETierState::Pseudo;
  auto a = action("cold", MoveKind::Commit);
  a.queued_at = now; // appended a moment ago; the engine marks it next
  queue(a);
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.requeued);
  EXPECT_EQ(0, placements->moves);
  auto left = moves->all();
  ASSERT_EQ(1u, left.size());
  EXPECT_EQ(0u, left[0].attempts); // not a failure

  state->find(b1, rgw_obj_key("cold"))->ite.move_pending = RGWITEMovePending::Commit;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.commits);
  EXPECT_EQ(Placement::Cold, at("cold"));

  // one never marked is dropped once older than a lock lifetime
  state->put(b1, "unmarked", 5);
  a = action("unmarked", MoveKind::Commit);
  a.queued_at = now - ctx.lock_time();
  queue(a);
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.superseded);
  EXPECT_TRUE(moves->all().empty());
  EXPECT_EQ(Placement::Hot, at("unmarked"));
}

TEST_F(MoverTest, ActionsTheInstanceOutlivedAreDropped)
{
  // replaced since queued
  pending("replaced", MoveKind::Commit);
  state->overwrite(b1, "replaced", 12);
  // deleted, with its bucket or alone
  queue(action("gone", MoveKind::Commit));
  const rgw_bucket deleted{"", "deleted", "deleted-id"};
  queue(action("x", MoveKind::Commit, 5, &deleted));
  // waiting for another move
  pending("other", MoveKind::Restore);
  queue(action("other", MoveKind::Commit));
  // queued by another zone
  pending("foreign", MoveKind::Commit);
  auto a = action("foreign", MoveKind::Commit);
  a.zone_id = "zone-b";
  queue(a);

  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(3u, st.gone);
  EXPECT_EQ(2u, st.superseded);
  EXPECT_EQ(Placement::Hot, at("replaced"));
  EXPECT_EQ(RGWITETierState::Hot, ite("replaced").tier_state); // the new instance
  EXPECT_EQ(RGWITEMovePending::None, ite("replaced").move_pending);
  // the moves the instances wait for still happen
  EXPECT_EQ(Placement::Hot, at("other"));
  EXPECT_EQ(Placement::Cold, at("foreign"));
  EXPECT_EQ(1u, st.restores);
  EXPECT_EQ(1u, st.commits);
  EXPECT_TRUE(moves->all().empty());
}

TEST_F(MoverTest, OverwriteDuringTheMoveIsNotCommitted)
{
  pending("cold", MoveKind::Commit);
  placements->before_move = [&](const rgw_obj_key&) {
    state->overwrite(b1, "cold", 12); // a PUT lands while the bytes are copied
  };
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(0, placements->moves); // refused for the new generation
  EXPECT_EQ(1u, st.requeued);
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.gone); // the retry finds the replacement
  EXPECT_EQ(Placement::Hot, at("cold"));
  EXPECT_EQ(RGWITETierState::Hot, ite("cold").tier_state);
  EXPECT_TRUE(moves->all().empty());
}

TEST_F(MoverTest, FailingMovesAreRetriedThenGivenUp)
{
  pending("cold", MoveKind::Commit);
  placements->move_ret = -EIO;
  MoverStats st;
  for (uint32_t i = 1; i < MOVE_MAX_ATTEMPTS; ++i) {
    ASSERT_EQ(0, run(&st));
    EXPECT_EQ(1u, st.requeued);
    auto left = moves->all();
    ASSERT_EQ(1u, left.size());
    EXPECT_EQ(i, left[0].attempts);
    EXPECT_EQ(RGWITEMovePending::Commit, ite("cold").move_pending);
  }
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.failed);
  EXPECT_TRUE(moves->all().empty());
  // the engine decides again: the mark is gone, the bytes and state stay
  EXPECT_EQ(RGWITEMovePending::None, ite("cold").move_pending);
  EXPECT_EQ(RGWITETierState::Pseudo, ite("cold").tier_state);
  EXPECT_EQ(Placement::Hot, at("cold"));
}

TEST_F(MoverTest, NoColdPlacementIsNotRetried)
{
  pending("cold", MoveKind::Commit);
  placements->move_ret = -EOPNOTSUPP;
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.failed);
  EXPECT_EQ(0u, st.requeued);
  EXPECT_EQ(RGWITEMovePending::None, ite("cold").move_pending);
  EXPECT_EQ(RGWITETierState::Pseudo, ite("cold").tier_state);
}

TEST_F(MoverTest, UnverifiedMoveIsNotCommitted)
{
  pending("cold", MoveKind::Commit);
  placements->stuck = true;
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1, placements->moves);
  EXPECT_EQ(1u, st.requeued);
  EXPECT_EQ(RGWITETierState::Pseudo, ite("cold").tier_state);
  EXPECT_EQ(RGWITEMovePending::Commit, ite("cold").move_pending);
}

TEST_F(MoverTest, FailedStateWriteIsReplayedWithoutMovingAgain)
{
  pending("cold", MoveKind::Commit);
  state->set_ret = -EIO;
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1, placements->moves);
  EXPECT_EQ(1u, st.requeued);
  EXPECT_EQ(RGWITETierState::Pseudo, ite("cold").tier_state);

  state->set_ret = 0;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1, placements->moves);
  EXPECT_EQ(1u, st.reconciled);
  EXPECT_EQ(RGWITETierState::Committed, ite("cold").tier_state);
  EXPECT_EQ(1u, ite("cold").commit_count);
}

TEST_F(MoverTest, FailedRequeueLeavesTheActionQueued)
{
  pending("cold", MoveKind::Commit);
  placements->move_ret = -EIO;
  moves->append_ret = -EIO;
  ASSERT_EQ(-EIO, run());
  auto left = moves->all();
  ASSERT_EQ(1u, left.size());
  EXPECT_EQ(0u, left[0].attempts); // the original, not acknowledged
  EXPECT_EQ(0, moves->acks);
}

TEST_F(MoverTest, DisabledBucketKeepsItsBytes)
{
  pending("cold", MoveKind::Commit, b2);
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.superseded);
  EXPECT_EQ(0, placements->moves);
  auto e = state->find(b2, rgw_obj_key("cold"));
  ASSERT_TRUE(e);
  EXPECT_EQ(RGWITEMovePending::None, e->ite.move_pending);
  EXPECT_EQ(RGWITETierState::Pseudo, e->ite.tier_state);
}

TEST_F(MoverTest, BatchCapBoundsTheMovesOfACycle)
{
  set_conf("rgw_ite_move_batch_cap", "3");
  for (int i = 0; i < 8; ++i) {
    pending(fmt::format("o{}", i), MoveKind::Commit);
  }
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(3u, st.started);
  EXPECT_EQ(3, placements->moves);
  EXPECT_EQ(5u, moves->all().size());
  ASSERT_EQ(0, run(&st));
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(2u, st.started);
  EXPECT_EQ(8, placements->moves);
  EXPECT_TRUE(moves->all().empty());
}

TEST_F(MoverTest, DrainsEveryShardFromAnyStart)
{
  std::set<uint32_t> shards;
  for (int i = 0; shards.size() < 3 && i < 1000; ++i) {
    auto name = fmt::format("o{}", i);
    if (shards.insert(move_shard_of(action(name, MoveKind::Commit), 4)).second) {
      pending(name, MoveKind::Commit);
    }
  }
  ASSERT_EQ(3u, shards.size());
  Mover mover(ctx);
  ASSERT_EQ(0, mover.process(&dpp, null_yield, 3));
  EXPECT_EQ(3u, mover.stats().commits);
  EXPECT_EQ(4u, mover.stats().shards);
  EXPECT_TRUE(moves->all().empty());
}

TEST_F(MoverTest, SkipsShardsOwnedElsewhere)
{
  pending("cold", MoveKind::Commit);
  const uint32_t shard = move_shard_of(action("cold", MoveKind::Commit), 4);
  moves->locks.busy.insert(fmt::format("ite-move.{}", shard));
  MoverStats st;
  ASSERT_EQ(0, run(&st));
  EXPECT_EQ(1u, st.shards_busy);
  EXPECT_EQ(0, placements->moves);
  EXPECT_EQ(1u, moves->all().size());
  EXPECT_TRUE(moves->locks.held.empty());
}

TEST_F(MoverTest, ShutdownLeavesTheQueue)
{
  pending("cold", MoveKind::Commit);
  ctx.going_down = true;
  ASSERT_EQ(0, run());
  EXPECT_EQ(0, placements->moves);
  EXPECT_EQ(1u, moves->all().size());
  EXPECT_EQ(RGWITEMovePending::Commit, ite("cold").move_pending);
}
