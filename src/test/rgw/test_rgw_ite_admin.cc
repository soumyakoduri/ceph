// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* What radosgw-admin's "ite" commands do, over in-memory contracts: the
 * bucket opt-in and opt-out, the read-only inspectors, and a one-shot run
 * of the workers. The output is read back through JSONFormattable. */

#include "rgw_ite_admin.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

#include <fmt/format.h>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "global/global_context.h"
#include "rgw_ite.h"
#include "ite_fakes.h"

using namespace rgw::ite;
using namespace rgw::ite::test;

namespace {

long long num(const JSONFormattable& v)
{
  return v.val_long_long();
}

struct AdminTest : public ::testing::Test {
  DoutPrefix dpp{g_ceph_context, ceph_subsys_rgw, "admin test: "};
  rgw::ite::Context ctx;
  MemWriteBuffer* buffer = nullptr;
  MemAggStore* agg = nullptr;
  MemStateStore* state = nullptr;
  MemMoveQueue* moves = nullptr;
  std::unique_ptr<MemPlacements> placements;
  const rgw_bucket b1{"", "tiered", "tiered-id"};
  const rgw_bucket b2{"", "plain", "plain-id"};
  ceph::real_time now = ceph::real_clock::now();

  void SetUp() override {
    ctx.cct = g_ceph_context;
    ctx.cookie = "this-gateway";
    ctx.zone_id = "zone-a";
    auto wb = std::make_unique<MemWriteBuffer>(4);
    buffer = wb.get();
    ctx.buffer = std::move(wb);
    auto as = std::make_unique<MemAggStore>(2);
    agg = as.get();
    ctx.agg = std::move(as);
    auto ss = std::make_unique<MemStateStore>();
    state = ss.get();
    ctx.state = std::move(ss);
    auto mq = std::make_unique<MemMoveQueue>(4);
    moves = mq.get();
    ctx.moves = std::move(mq);
    placements = std::make_unique<MemPlacements>(state);
    ctx.bucket_loader = [this](const DoutPrefixProvider*, const rgw_bucket& b,
                               std::unique_ptr<rgw::sal::Bucket>* out, optional_yield) {
      *out = make_bucket(b, b == b1, {}, placements.get());
      return 0;
    };
  }

  /* a bucket that has never had an ITE config */
  std::unique_ptr<MemBucket> fresh_bucket(const rgw_bucket& b) {
    RGWBucketInfo info;
    info.bucket = b;
    return std::make_unique<MemBucket>(placements.get(), info);
  }
  std::unique_ptr<rgw::sal::Bucket> bucket(const rgw_bucket& b, bool enabled) {
    return make_bucket(b, enabled, {}, placements.get());
  }
};

} // anonymous namespace

TEST_F(AdminTest, EnableJoinsTheDirectory)
{
  auto bucket = fresh_bucket(b1);
  JSONFormattable f;
  ASSERT_EQ(0, admin::bucket_enable(&dpp, ctx, bucket.get(), std::nullopt, now, &f, null_yield));
  EXPECT_EQ(1, bucket->stores);
  auto config = get_bucket_config(bucket->get_attrs());
  ASSERT_TRUE(config);
  EXPECT_TRUE(config->enabled);
  ASSERT_EQ(1u, agg->dir.size());
  EXPECT_EQ(now, agg->dir.begin()->second.enabled_at);
  EXPECT_TRUE(f["config"]["enabled"].val_bool());
  EXPECT_EQ("tiered", f["bucket"]["name"].val());
  EXPECT_DOUBLE_EQ(7, std::stod(f["knobs"]["half_life_days"].val()));

  // enabled again, with knobs: the knobs change and the record stays
  BucketConfig knobs;
  knobs.half_life_days = 3;
  knobs.cold_after_days = 10;
  JSONFormattable g;
  ASSERT_EQ(0, admin::bucket_enable(&dpp, ctx, bucket.get(), knobs,
                                    now + std::chrono::hours(1), &g, null_yield));
  config = get_bucket_config(bucket->get_attrs());
  ASSERT_TRUE(config);
  EXPECT_TRUE(config->enabled);
  EXPECT_EQ(3, *config->half_life_days);
  EXPECT_EQ(10u, *config->cold_after_days);
  EXPECT_EQ(now, agg->dir.begin()->second.enabled_at);
  EXPECT_DOUBLE_EQ(3, std::stod(g["knobs"]["half_life_days"].val()));
}

TEST_F(AdminTest, DisableKeepsTheKnobsAndTheRecord)
{
  auto bucket = fresh_bucket(b1);
  BucketConfig knobs;
  knobs.half_life_days = 3;
  JSONFormattable f;
  ASSERT_EQ(0, admin::bucket_enable(&dpp, ctx, bucket.get(), knobs, now, &f, null_yield));
  agg->dir.begin()->second.last_decision = now;

  JSONFormattable g;
  ASSERT_EQ(0, admin::bucket_disable(&dpp, ctx, bucket.get(), &g, null_yield));
  auto config = get_bucket_config(bucket->get_attrs());
  ASSERT_TRUE(config);
  EXPECT_FALSE(config->enabled);
  EXPECT_EQ(3, *config->half_life_days);
  EXPECT_FALSE(g["config"]["enabled"].val_bool());
  // the workers skip it, and its history ages out on its own
  ASSERT_EQ(1u, agg->dir.size());

  // disabled twice: nothing to store
  JSONFormattable h;
  ASSERT_EQ(0, admin::bucket_disable(&dpp, ctx, bucket.get(), &h, null_yield));
  EXPECT_EQ(2, bucket->stores);

  // back on after the pause: the knobs return, and the history restarts
  const auto later = now + std::chrono::hours(48);
  JSONFormattable i;
  ASSERT_EQ(0, admin::bucket_enable(&dpp, ctx, bucket.get(), std::nullopt, later, &i,
                                    null_yield));
  config = get_bucket_config(bucket->get_attrs());
  ASSERT_TRUE(config);
  EXPECT_TRUE(config->enabled);
  EXPECT_EQ(3, *config->half_life_days);
  EXPECT_EQ(later, agg->dir.begin()->second.enabled_at);
  EXPECT_EQ(now, agg->dir.begin()->second.last_decision);
}

TEST_F(AdminTest, DisablingANeverEnabledBucketStoresNothing)
{
  auto bucket = fresh_bucket(b2);
  JSONFormattable f;
  ASSERT_EQ(0, admin::bucket_disable(&dpp, ctx, bucket.get(), &f, null_yield));
  EXPECT_EQ(0, bucket->stores);
  EXPECT_FALSE(f["config"]["enabled"].val_bool());
  EXPECT_TRUE(agg->dir.empty());
}

TEST_F(AdminTest, EnableRefusesKnobsThatMakeNoSense)
{
  auto bucket = fresh_bucket(b1);
  BucketConfig knobs;
  knobs.warm_threshold = 0.05; // below the default cool threshold
  JSONFormattable f;
  EXPECT_EQ(-EINVAL, admin::bucket_enable(&dpp, ctx, bucket.get(), knobs, now, &f, null_yield));
  knobs = BucketConfig();
  knobs.half_life_days = 0;
  EXPECT_EQ(-EINVAL, admin::bucket_enable(&dpp, ctx, bucket.get(), knobs, now, &f, null_yield));
  knobs = BucketConfig();
  knobs.min_fft_confidence = 1.5;
  EXPECT_EQ(-EINVAL, admin::bucket_enable(&dpp, ctx, bucket.get(), knobs, now, &f, null_yield));
  EXPECT_EQ(0, bucket->stores);
  EXPECT_TRUE(agg->dir.empty());

  std::string err;
  EXPECT_EQ(0, admin::validate(Knobs(), &err));
}

TEST_F(AdminTest, FailedStoreLeavesTheBucketDisabled)
{
  auto bucket = fresh_bucket(b1);
  bucket->store_ret = -EIO;
  JSONFormattable f;
  EXPECT_EQ(-EIO, admin::bucket_enable(&dpp, ctx, bucket.get(), std::nullopt, now, &f,
                                       null_yield));
  EXPECT_FALSE(get_bucket_config(bucket->get_attrs()));
  // a directory record without the config is skipped like a disabled bucket
  EXPECT_EQ(1u, agg->dir.size());
}

TEST_F(AdminTest, BucketListPages)
{
  for (const char* name : {"a", "b", "c"}) {
    BucketProgress p;
    p.bucket = rgw_bucket{"", name, std::string(name) + "-id"};
    p.enabled_at = now;
    agg->dir[p.bucket.get_key()] = p;
  }
  JSONFormattable f;
  ASSERT_EQ(0, admin::bucket_list(&dpp, ctx, "", 2, &f, null_yield));
  ASSERT_EQ(2u, f["buckets"].array().size());
  EXPECT_EQ("a", f["buckets"][0]["bucket"]["name"].val());
  EXPECT_TRUE(f["truncated"].val_bool());
  const std::string next = f["next_marker"].val();
  EXPECT_FALSE(next.empty());

  JSONFormattable g;
  ASSERT_EQ(0, admin::bucket_list(&dpp, ctx, next, 2, &g, null_yield));
  ASSERT_EQ(1u, g["buckets"].array().size());
  EXPECT_EQ("c", g["buckets"][0]["bucket"]["name"].val());
  EXPECT_FALSE(g["truncated"].val_bool());
  EXPECT_FALSE(g.exists("next_marker"));
}

TEST_F(AdminTest, BucketStatsCountsWhatITETiers)
{
  state->put(b1, "hot1", 5, 1000000);
  state->put(b1, "hot2", 5, 2000000);
  auto& pseudo = state->put(b1, "pseudo", 5, 3000000);
  pseudo.ite.tier_state = RGWITETierState::Pseudo;
  pseudo.ite.move_pending = RGWITEMovePending::Commit;
  auto& cold = state->put(b1, "cold", 5, 4000000);
  cold.ite.tier_state = RGWITETierState::Committed;
  cold.ite.fft_periodic = true;
  state->put(b1, "standard", 5).storage_class = "STANDARD";
  state->put(b1, "tiny", 5, 10);
  state->put(b1, "marker", 5).delete_marker = true;
  auto bucket = this->bucket(b1, true);

  JSONFormattable f;
  ASSERT_EQ(0, admin::bucket_stats(&dpp, ctx, bucket.get(), &f, null_yield));
  EXPECT_TRUE(f["enabled"].val_bool());
  EXPECT_FALSE(f.exists("directory"));
  const auto& o = f["objects"];
  EXPECT_EQ(7, num(o["entries"]));
  EXPECT_EQ(1, num(o["no_data"]));
  EXPECT_EQ(1, num(o["other_class"]));
  EXPECT_EQ(1, num(o["too_small"]));
  EXPECT_EQ(2, num(o["hot"]["objects"]));
  EXPECT_EQ(3000000, num(o["hot"]["bytes"]));
  EXPECT_EQ(1, num(o["pseudo"]["objects"]));
  EXPECT_EQ(1, num(o["committed"]["objects"]));
  EXPECT_EQ(4000000, num(o["committed"]["bytes"]));
  EXPECT_EQ(1, num(o["periodic"]));
  EXPECT_EQ(1, num(o["move_pending"]["commit"]));
  EXPECT_EQ(0, num(o["move_pending"]["restore"]));
}

TEST_F(AdminTest, BucketStatsWalksEveryPage)
{
  for (int i = 0; i < 2500; ++i) {
    state->put(b1, fmt::format("obj{:05}", i), 5);
  }
  agg->dir[b1.get_key()] = BucketProgress{b1, now};
  auto bucket = this->bucket(b1, true);
  JSONFormattable f;
  ASSERT_EQ(0, admin::bucket_stats(&dpp, ctx, bucket.get(), &f, null_yield));
  EXPECT_EQ(2500, num(f["objects"]["entries"]));
  EXPECT_EQ(2500, num(f["objects"]["hot"]["objects"]));
  EXPECT_TRUE(f.exists("directory"));
}

TEST_F(AdminTest, ObjectStatShowsStatePlacementAndDecision)
{
  auto& e = state->put(b1, "obj", 5);
  e.mtime = now - std::chrono::hours(24 * 60);
  e.ite.temperature = 0.8;
  e.ite.temp_updated_at = to_epoch_secs(now) - 7 * 86400; // one half-life ago
  placements->set(b1, "obj", Placement::Hot);
  auto bucket = this->bucket(b1, true);

  JSONFormattable f;
  ASSERT_EQ(0, admin::object_stat(&dpp, ctx, bucket.get(), rgw_obj_key("obj"), now, &f,
                                  null_yield));
  EXPECT_EQ("obj", f["name"].val());
  EXPECT_EQ(5, num(f["instance_gen"]));
  EXPECT_EQ("INTELLIGENT_TIER", f["storage_class"].val());
  EXPECT_EQ("hot", f["placement"].val());
  EXPECT_NEAR(0.8, std::stod(f["ite"]["temperature"].val()), 1e-6);
  EXPECT_NEAR(0.4, std::stod(f["temperature_now"].val()), 1e-3);
  // still above the warm threshold once decayed
  EXPECT_EQ("hold", f["decision"]["decision"].val());
  EXPECT_EQ("hot", f["decision"]["reason"].val());

  // cooled off, it would be marked
  e.ite.temperature = 0.01;
  JSONFormattable g;
  ASSERT_EQ(0, admin::object_stat(&dpp, ctx, bucket.get(), rgw_obj_key("obj"), now, &g,
                                  null_yield));
  EXPECT_EQ("pseudo", g["decision"]["decision"].val());

  // a disabled bucket is not decided
  auto plain = this->bucket(b1, false);
  JSONFormattable h;
  ASSERT_EQ(0, admin::object_stat(&dpp, ctx, plain.get(), rgw_obj_key("obj"), now, &h,
                                  null_yield));
  EXPECT_FALSE(h.exists("decision"));
  EXPECT_EQ("hot", h["placement"].val());

  JSONFormattable i;
  EXPECT_EQ(-ENOENT, admin::object_stat(&dpp, ctx, bucket.get(), rgw_obj_key("missing"),
                                        now, &i, null_yield));
}

TEST_F(AdminTest, CountsListNewestWindowFirst)
{
  const TimeScale ts = ctx.timescale();
  const Window d1{Granularity::Day, 100}, d2{Granularity::Day, 101}, w1{Granularity::Week, 14};
  add_count(*agg, b1, d1, "a", 5, Op::Get, 3);
  add_count(*agg, b1, d2, "a", 5, Op::Get, 1);
  add_count(*agg, b1, d2, "b", 7, Op::Put, 2, CallerType::Lifecycle);
  add_count(*agg, b1, w1, "b", 7, Op::Head, 4);
  // another bucket's counts stay out
  add_count(*agg, b2, d2, "a", 5, Op::Get, 9);

  JSONFormattable f;
  ASSERT_EQ(0, admin::counts_list(&dpp, ctx, b1, "", 0, now, &f, null_yield));
  const auto& windows = f["windows"].array();
  ASSERT_EQ(3u, windows.size());
  std::vector<Window> expected{d1, d2, w1};
  std::sort(expected.begin(), expected.end(), [&ts](const Window& x, const Window& y) {
    return x.start(ts) > y.start(ts);
  });
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(expected[i].stamp(ts), windows[i]["window"].val());
  }
  EXPECT_FALSE(f["truncated"].val_bool());
  auto window = [&](const JSONFormattable& r, const Window& w) -> const JSONFormattable& {
    for (const auto& x : r["windows"].array()) {
      if (x["window"].val() == w.stamp(ts)) {
        return x;
      }
    }
    ADD_FAILURE() << "no window " << w.stamp(ts);
    return r;
  };
  const auto& w = window(f, d2);
  EXPECT_EQ("day", w["granularity"].val());
  ASSERT_EQ(2u, w["counts"].array().size());
  for (const auto& c : w["counts"].array()) {
    if (c["name"].val() == "b") {
      EXPECT_EQ(7, num(c["gen"]));
      EXPECT_EQ(to_string(Op::Put), c["op"].val());
      EXPECT_EQ(to_string(CallerType::Lifecycle), c["caller"].val());
      EXPECT_EQ(2, num(c["count"]));
    } else {
      EXPECT_EQ("a", c["name"].val());
      EXPECT_EQ(1, num(c["count"]));
    }
  }
  EXPECT_EQ("week", window(f, w1)["granularity"].val());

  // one object's counts only
  JSONFormattable g;
  ASSERT_EQ(0, admin::counts_list(&dpp, ctx, b1, "a", 0, now, &g, null_yield));
  for (const auto& x : g["windows"].array()) {
    for (const auto& c : x["counts"].array()) {
      EXPECT_EQ("a", c["name"].val());
    }
  }
  EXPECT_EQ(3, num(window(g, d1)["counts"][0]["count"]));
  EXPECT_TRUE(window(g, w1)["counts"].array().empty());

  // the newest window only
  JSONFormattable h;
  ASSERT_EQ(0, admin::counts_list(&dpp, ctx, b1, "", 1, now, &h, null_yield));
  ASSERT_EQ(1u, h["windows"].array().size());
  EXPECT_EQ(expected[0].stamp(ts), h["windows"][0]["window"].val());
  EXPECT_TRUE(h["truncated"].val_bool());
}

TEST_F(AdminTest, LogStatusCountsWaitingEvents)
{
  auto event = [&](const std::string& name, ceph::real_time t) {
    AccessEvent e;
    e.bucket = b1;
    e.obj = InstanceId(name, "", 5);
    e.timestamp = t;
    e.op = Op::Get;
    return e;
  };
  const auto first = now - std::chrono::minutes(10);
  ASSERT_EQ(0, buffer->append(&dpp, event("a", first), null_yield));
  ASSERT_EQ(0, buffer->append(&dpp, event("a", now), null_yield));
  ASSERT_EQ(0, buffer->append(&dpp, event("a", now), null_yield));
  ASSERT_EQ(0, buffer->append(&dpp, event("b", now), null_yield));
  const uint32_t a_shard = buffer_shard_of(b1, "a", buffer->num_shards());

  JSONFormattable f;
  ASSERT_EQ(0, admin::log_status(&dpp, ctx, 0, &f, null_yield));
  EXPECT_EQ(4, num(f["num_shards"]));
  EXPECT_EQ(4, num(f["events"]));
  EXPECT_FALSE(f["truncated"].val_bool());
  bool seen = false;
  for (const auto& s : f["shards"].array()) {
    EXPECT_GT(num(s["events"]), 0);
    if (num(s["shard"]) == static_cast<long long>(a_shard)) {
      seen = true;
      EXPECT_GE(num(s["events"]), 3);
      EXPECT_FALSE(s["oldest"].val().empty());
    }
  }
  EXPECT_TRUE(seen);

  // counted up to a bound per shard
  JSONFormattable g;
  ASSERT_EQ(0, admin::log_status(&dpp, ctx, 2, &g, null_yield));
  EXPECT_TRUE(g["truncated"].val_bool());
  for (const auto& s : g["shards"].array()) {
    EXPECT_LE(num(s["events"]), 2);
  }

  // nothing waiting, nothing listed
  for (uint32_t shard = 0; shard < buffer->num_shards(); ++shard) {
    buffer->data[shard].clear();
  }
  JSONFormattable h;
  ASSERT_EQ(0, admin::log_status(&dpp, ctx, 0, &h, null_yield));
  EXPECT_EQ(0, num(h["events"]));
  EXPECT_TRUE(h["shards"].array().empty());
}

TEST_F(AdminTest, MoveListShowsQueuedActions)
{
  auto action = [&](const std::string& name, MoveKind kind) {
    MoveAction a;
    a.bucket = b1;
    a.obj = InstanceId(name, "", 5);
    a.kind = kind;
    a.target = target_of(kind);
    a.zone_id = "zone-a";
    a.queued_at = now;
    return a;
  };
  for (const auto& [name, kind] : std::vector<std::pair<std::string, MoveKind>>{
           {"a", MoveKind::Commit}, {"b", MoveKind::Restore}, {"c", MoveKind::PreWarm}}) {
    ASSERT_EQ(0, moves->append(&dpp, action(name, kind), null_yield));
  }
  JSONFormattable f;
  ASSERT_EQ(0, admin::move_list(&dpp, ctx, std::nullopt, 0, &f, null_yield));
  EXPECT_EQ(3, num(f["actions"]));
  size_t listed = 0;
  for (const auto& s : f["shards"].array()) {
    for (const auto& a : s["actions"].array()) {
      ++listed;
      EXPECT_FALSE(a["marker"].val().empty());
      EXPECT_EQ("zone-a", a["zone_id"].val());
    }
  }
  EXPECT_EQ(3u, listed);

  // one shard, listed even when it is empty
  const uint32_t shard = move_shard_of(action("a", MoveKind::Commit), moves->num_shards());
  JSONFormattable g;
  ASSERT_EQ(0, admin::move_list(&dpp, ctx, shard, 0, &g, null_yield));
  ASSERT_EQ(1u, g["shards"].array().size());
  EXPECT_EQ(static_cast<long long>(shard), num(g["shards"][0]["shard"]));
  EXPECT_EQ(moves->data[shard].size(), g["shards"][0]["actions"].array().size());

  JSONFormattable h;
  EXPECT_EQ(-EINVAL, admin::move_list(&dpp, ctx, moves->num_shards(), 0, &h, null_yield));
}

TEST_F(AdminTest, ProcessRunsTheNamedWorkers)
{
  // a commit the engine queued and marked
  auto& e = state->put(b1, "obj", 5);
  e.ite.tier_state = RGWITETierState::Pseudo;
  e.ite.move_pending = RGWITEMovePending::Commit;
  MoveAction a;
  a.bucket = b1;
  a.obj = InstanceId("obj", "", 5);
  a.kind = MoveKind::Commit;
  a.target = Placement::Cold;
  a.zone_id = "zone-a";
  a.queued_at = now - std::chrono::hours(1);
  ASSERT_EQ(0, moves->append(&dpp, a, null_yield));

  JSONFormattable f;
  ASSERT_EQ(0, admin::process(&dpp, ctx, "move", &f, null_yield));
  EXPECT_TRUE(f.exists("move"));
  EXPECT_FALSE(f.exists("agg"));
  EXPECT_EQ(0, num(f["move"]["ret"]));
  EXPECT_EQ(1, num(f["move"]["stats"]["commits"]));
  EXPECT_EQ(Placement::Cold, placements->at(b1, "obj"));
  EXPECT_EQ(RGWITETierState::Committed, state->find(b1, rgw_obj_key("obj"))->ite.tier_state);

  JSONFormattable g;
  ASSERT_EQ(0, admin::process(&dpp, ctx, "all", &g, null_yield));
  for (auto w : admin::WORKERS) {
    EXPECT_TRUE(g.exists(std::string(w))) << w;
    EXPECT_EQ(0, num(g[std::string(w)]["ret"])) << w;
  }

  // a failing worker is reported, and the rest still run
  moves->list_ret = -EIO;
  JSONFormattable h;
  EXPECT_EQ(-EIO, admin::process(&dpp, ctx, "all", &h, null_yield));
  EXPECT_EQ(-EIO, num(h["move"]["ret"]));
  EXPECT_EQ(0, num(h["agg"]["ret"]));

  JSONFormattable i;
  EXPECT_EQ(-EINVAL, admin::process(&dpp, ctx, "bogus", &i, null_yield));
}
