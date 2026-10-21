// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_types.h"

#include <set>
#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "global/global_context.h"

using namespace rgw::ite;

TEST(ITETypes, OpAndCallerNames)
{
  for (auto op : {Op::Get, Op::Put, Op::Head, Op::Delete}) {
    EXPECT_EQ(op, parse_op(to_string(op)));
  }
  for (auto c : {CallerType::User, CallerType::Replication, CallerType::Lifecycle,
                 CallerType::GC, CallerType::Tiering}) {
    EXPECT_EQ(c, parse_caller(to_string(c)));
  }
  EXPECT_FALSE(parse_op("POST"));
  EXPECT_FALSE(parse_caller("user")); // case sensitive
}

TEST(ITETypes, OpWeights)
{
  EXPECT_DOUBLE_EQ(1.0, op_weight(Op::Get));
  EXPECT_DOUBLE_EQ(0.5, op_weight(Op::Put));
  EXPECT_DOUBLE_EQ(0.1, op_weight(Op::Head));
  EXPECT_DOUBLE_EQ(0.0, op_weight(Op::Delete));
}

TEST(ITETypes, CountKeyRoundTrip)
{
  CountKey k{InstanceId("report.pdf", "vABC", 42), Op::Get, CallerType::User};
  EXPECT_EQ("report.pdf:vABC:42:GET:USER", k.to_str());
  EXPECT_EQ(k, CountKey::parse(k.to_str()));

  // object names may contain the separator; null version is empty
  CountKey c{InstanceId("a:b::c:", "", 0), Op::Head, CallerType::Replication};
  auto parsed = CountKey::parse(c.to_str());
  ASSERT_TRUE(parsed);
  EXPECT_EQ("a:b::c:", parsed->obj.name);
  EXPECT_EQ("", parsed->obj.instance);
  EXPECT_EQ(Op::Head, parsed->op);
  EXPECT_EQ(CallerType::Replication, parsed->caller);
}

TEST(ITETypes, CountKeyRejectsGarbage)
{
  EXPECT_FALSE(CountKey::parse(""));
  EXPECT_FALSE(CountKey::parse("name:GET:USER"));
  EXPECT_FALSE(CountKey::parse(":v:1:GET:USER")); // empty name
  EXPECT_FALSE(CountKey::parse("n:v:x1:GET:USER")); // bad gen
  EXPECT_FALSE(CountKey::parse("n:v:1:FETCH:USER"));
  EXPECT_FALSE(CountKey::parse(marker_key(3)));
}

TEST(ITETypes, MarkerKeys)
{
  EXPECT_EQ("__mkr__:7", marker_key(7));
  EXPECT_TRUE(is_marker_key(marker_key(0)));
  EXPECT_TRUE(is_marker_key(marker_key(31)));
  // a count key for an object literally named __mkr__ is not a marker
  CountKey k{InstanceId("__mkr__", "", 3), Op::Get, CallerType::User};
  EXPECT_FALSE(is_marker_key(k.to_str()));
  EXPECT_FALSE(is_marker_key("__mkr__:"));
  EXPECT_FALSE(is_marker_key("obj:GET"));
}

TEST(ITETypes, ShardsAreStableAndInRange)
{
  rgw_bucket b("", "bucket", "id-1");
  std::set<uint32_t> seen;
  for (int i = 0; i < 1000; ++i) {
    const std::string name = "obj-" + std::to_string(i);
    const auto s = buffer_shard_of(b, name, 32);
    EXPECT_LT(s, 32u);
    EXPECT_EQ(s, buffer_shard_of(b, name, 32));
    seen.insert(s);
    EXPECT_LT(agg_shard_of(name, 8), 8u);
  }
  EXPECT_GT(seen.size(), 24u); // spread over most shards
  EXPECT_EQ(0u, buffer_shard_of(b, "x", 1));
  EXPECT_EQ(0u, agg_shard_of("x", 0));
}

TEST(ITETypes, DayWindows)
{
  TimeScale ts;
  // 2026-10-21 13:00:00 UTC
  const auto t = from_epoch_secs(1792587600);
  const auto w = Window::of(t, Granularity::Day, ts);
  EXPECT_EQ("20261021", w.stamp(ts));
  EXPECT_LE(w.start(ts), t);
  EXPECT_GT(w.end(ts), t);
  EXPECT_EQ(w, Window::parse_stamp("20261021", ts));
  EXPECT_EQ("20261020", w.prev().stamp(ts));
  EXPECT_EQ("20261101", Window{Granularity::Day, w.index + 11}.stamp(ts));
}

TEST(ITETypes, WeekWindowsStartOnMonday)
{
  TimeScale ts;
  // Wednesday 2026-10-21 -> week of Monday 2026-10-19
  const auto t = from_epoch_secs(1792587600);
  const auto w = Window::of(t, Granularity::Week, ts);
  EXPECT_EQ("w20261019", w.stamp(ts));
  EXPECT_EQ(w, Window::parse_stamp("w20261019", ts));
  // the Sunday before is in the previous week, the Monday is in this one
  EXPECT_EQ(w.prev(), Window::of(from_epoch_secs(1792368000 - 1), Granularity::Week, ts));
  EXPECT_EQ(w, Window::of(from_epoch_secs(1792368000), Granularity::Week, ts));
  EXPECT_EQ(std::chrono::seconds(7 * 86400), w.length(ts));
}

TEST(ITETypes, DebugTimeScale)
{
  TimeScale ts;
  ts.secs_per_day = 60;
  const auto w = Window::of(from_epoch_secs(6000), Granularity::Day, ts);
  EXPECT_EQ(100u, w.index);
  EXPECT_EQ("x100", w.stamp(ts));
  EXPECT_EQ(w, Window::parse_stamp("x100", ts));
  // a real-time stamp does not parse under a debug scale, and vice versa
  EXPECT_FALSE(Window::parse_stamp("20261021", ts));
  EXPECT_FALSE(Window::parse_stamp("x100", TimeScale{}));
  EXPECT_DOUBLE_EQ(2.0, ts.days_between(from_epoch_secs(0), from_epoch_secs(120)));
  EXPECT_EQ(std::chrono::seconds(90), ts.days(1.5));
}

TEST(ITETypes, KnobsFromConfAndBucketOverrides)
{
  const Knobs d = Knobs::from_conf(g_ceph_context);
  EXPECT_DOUBLE_EQ(7.0, d.half_life_days);
  EXPECT_DOUBLE_EQ(0.3, d.warm_threshold);
  EXPECT_DOUBLE_EQ(0.1, d.cool_threshold);
  EXPECT_EQ(30u, d.cold_after_days);
  EXPECT_TRUE(d.prewarm);
  EXPECT_EQ(7u, d.prewarm_lead_days);
  EXPECT_EQ(4096u, d.min_tier_size);
  EXPECT_DOUBLE_EQ(0.5, d.min_fft_confidence);
  EXPECT_DOUBLE_EQ(d.half_life_days, d.grace_days());

  BucketConfig c;
  c.enabled = true;
  c.cold_after_days = 5;
  c.prewarm = false;
  const Knobs e = c.effective(d);
  EXPECT_EQ(5u, e.cold_after_days);
  EXPECT_FALSE(e.prewarm);
  EXPECT_DOUBLE_EQ(d.half_life_days, e.half_life_days); // not overridden
}

TEST(ITETypes, BucketConfigAttr)
{
  std::map<std::string, ceph::buffer::list> attrs;
  EXPECT_FALSE(get_bucket_config(attrs));

  BucketConfig c;
  c.enabled = true;
  c.half_life_days = 2.5;
  ceph::buffer::list bl;
  encode(c, bl);
  attrs[std::string(ATTR_ITE)] = bl;
  auto got = get_bucket_config(attrs);
  ASSERT_TRUE(got);
  EXPECT_TRUE(got->enabled);
  EXPECT_DOUBLE_EQ(2.5, *got->half_life_days);
  EXPECT_FALSE(got->warm_threshold);

  attrs[std::string(ATTR_ITE)].clear();
  attrs[std::string(ATTR_ITE)].append("junk");
  EXPECT_FALSE(get_bucket_config(attrs));
}

TEST(ITETypes, EventAndActionEncoding)
{
  for (const auto& e : AccessEvent::generate_test_instances()) {
    ceph::buffer::list bl;
    encode(e, bl);
    AccessEvent d;
    auto p = bl.cbegin();
    decode(d, p);
    EXPECT_EQ(e.bucket, d.bucket);
    EXPECT_EQ(e.obj, d.obj);
    EXPECT_EQ(e.timestamp, d.timestamp);
    EXPECT_EQ(e.op, d.op);
    EXPECT_EQ(e.caller, d.caller);
    EXPECT_EQ(e.client_id, d.client_id);
  }
  for (const auto& a : MoveAction::generate_test_instances()) {
    ceph::buffer::list bl;
    encode(a, bl);
    MoveAction d;
    auto p = bl.cbegin();
    decode(d, p);
    EXPECT_EQ(a.bucket, d.bucket);
    EXPECT_EQ(a.obj, d.obj);
    EXPECT_EQ(a.kind, d.kind);
    EXPECT_EQ(a.target, d.target);
    EXPECT_EQ(a.zone_id, d.zone_id);
  }
}

TEST(ITETypes, MoveKinds)
{
  EXPECT_EQ(Placement::Cold, target_of(MoveKind::Commit));
  EXPECT_EQ(Placement::Hot, target_of(MoveKind::PreWarm));
  EXPECT_EQ(Placement::Hot, target_of(MoveKind::Restore));
  EXPECT_EQ(RGWITEMovePending::Commit, move_pending_of(MoveKind::Commit));
  EXPECT_EQ(RGWITEMovePending::PreWarm, move_pending_of(MoveKind::PreWarm));
  EXPECT_EQ(RGWITEMovePending::Restore, move_pending_of(MoveKind::Restore));
}
