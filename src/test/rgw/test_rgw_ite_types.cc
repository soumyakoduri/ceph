// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_types.h"

#include <set>
#include <sstream>
#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "global/global_context.h"
#include "rgw_zone_types.h"

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

TEST(ITETypes, BucketConfigJson)
{
  // as an admin hands a knob set to radosgw-admin ite bucket enable
  BucketConfig c;
  c.enabled = true;
  c.half_life_days = 2.5;
  c.cold_after_days = 10;
  c.prewarm = false;
  c.min_tier_size = 65536;
  JSONFormatter f;
  f.open_object_section("config");
  c.dump(&f);
  f.close_section();
  std::stringstream ss;
  f.flush(ss);
  JSONParser parser;
  ASSERT_TRUE(parser.parse(ss.str().c_str(), ss.str().size()));
  BucketConfig d;
  d.decode_json(&parser);
  EXPECT_TRUE(d.enabled);
  EXPECT_DOUBLE_EQ(2.5, *d.half_life_days);
  EXPECT_EQ(10u, *d.cold_after_days);
  EXPECT_FALSE(*d.prewarm);
  EXPECT_EQ(65536u, *d.min_tier_size);
  // what is not given stays unset, and falls back to the zone defaults
  EXPECT_FALSE(d.warm_threshold);
  EXPECT_FALSE(d.cool_threshold);
  EXPECT_FALSE(d.prewarm_lead_days);
  EXPECT_FALSE(d.min_fft_confidence);

  const std::string partial = R"({"warm_threshold": 0.5})";
  JSONParser p2;
  ASSERT_TRUE(p2.parse(partial.c_str(), partial.size()));
  BucketConfig e;
  e.decode_json(&p2);
  EXPECT_FALSE(e.enabled);
  EXPECT_DOUBLE_EQ(0.5, *e.warm_threshold);
  EXPECT_FALSE(e.half_life_days);
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
    EXPECT_EQ(a.queued_at, d.queued_at);
    EXPECT_EQ(a.attempts, d.attempts);
  }
}

TEST(ITETypes, MoveActionV1Decodes)
{
  // as queued before actions counted their attempts
  const MoveAction a = MoveAction::generate_test_instances().back();
  ceph::buffer::list bl;
  ENCODE_START(1, 1, bl);
  encode(a.bucket, bl);
  encode(a.obj, bl);
  encode(static_cast<uint8_t>(a.kind), bl);
  encode(static_cast<uint8_t>(a.target), bl);
  encode(a.zone_id, bl);
  encode(a.queued_at, bl);
  ENCODE_FINISH(bl);
  MoveAction d;
  d.attempts = 9;
  auto p = bl.cbegin();
  decode(d, p);
  EXPECT_EQ(a.obj, d.obj);
  EXPECT_EQ(a.kind, d.kind);
  EXPECT_EQ(a.queued_at, d.queued_at);
  EXPECT_EQ(0u, d.attempts);
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

TEST(ITETypes, ColdStorageClassNames)
{
  EXPECT_FALSE(rgw_is_cold_storage_class("INTELLIGENT_TIER"));
  EXPECT_TRUE(rgw_is_cold_storage_class("INTELLIGENT_TIER@cold"));
  EXPECT_FALSE(rgw_is_cold_storage_class("@cold"));
  EXPECT_EQ("INTELLIGENT_TIER", rgw_advertised_storage_class("INTELLIGENT_TIER@cold"));
  EXPECT_EQ("STANDARD", rgw_advertised_storage_class("STANDARD"));
  EXPECT_EQ("INTELLIGENT_TIER@cold", rgw_cold_storage_class("INTELLIGENT_TIER"));
  EXPECT_EQ("INTELLIGENT_TIER@cold", rgw_cold_storage_class("INTELLIGENT_TIER@cold"));
  EXPECT_EQ("STANDARD@cold", rgw_cold_storage_class(""));
}

TEST(ITETypes, TwoPlacementStorageClass)
{
  RGWZonePlacementInfo info;
  const rgw_pool standard("std"), hot("hot"), cold("cold");
  const std::string zlib = "zlib";
  info.storage_classes.set_storage_class("STANDARD", &standard, nullptr);
  info.storage_classes.set_storage_class("INTELLIGENT_TIER", &hot, &zlib, &cold);
  EXPECT_EQ(hot, info.get_data_pool("INTELLIGENT_TIER"));
  EXPECT_EQ(cold, info.get_data_pool("INTELLIGENT_TIER@cold"));
  EXPECT_EQ("zlib", info.get_compression_type("INTELLIGENT_TIER@cold"));
  ASSERT_TRUE(info.get_cold_data_pool("INTELLIGENT_TIER"));
  EXPECT_EQ(cold, *info.get_cold_data_pool("INTELLIGENT_TIER@cold"));
  // no client or bucket can name the internal class
  EXPECT_TRUE(info.storage_class_exists("INTELLIGENT_TIER"));
  EXPECT_FALSE(info.storage_class_exists("INTELLIGENT_TIER@cold"));
  // without a cold placement the cold name resolves to no pool, never to
  // the hot one or the standard one
  EXPECT_FALSE(info.get_cold_data_pool("STANDARD"));
  EXPECT_TRUE(info.get_data_pool("STANDARD@cold").empty());
  EXPECT_TRUE(info.get_data_pool("MISSING@cold").empty());
  EXPECT_EQ(standard, info.get_data_pool("MISSING"));
  // an empty pool removes the cold placement and keeps the rest
  const rgw_pool none;
  info.storage_classes.set_storage_class("INTELLIGENT_TIER", nullptr, nullptr, &none);
  EXPECT_FALSE(info.get_cold_data_pool("INTELLIGENT_TIER"));
  EXPECT_EQ(hot, info.get_data_pool("INTELLIGENT_TIER"));
  EXPECT_EQ("zlib", info.get_compression_type("INTELLIGENT_TIER"));
}

TEST(ITETypes, ZoneStorageClassEncoding)
{
  RGWZoneStorageClass c;
  c.data_pool = rgw_pool("hot");
  c.cold_data_pool = rgw_pool("cold");
  ceph::buffer::list bl;
  encode(c, bl);
  RGWZoneStorageClass d;
  auto p = bl.cbegin();
  decode(d, p);
  EXPECT_EQ(c.data_pool, d.data_pool);
  EXPECT_EQ(c.cold_data_pool, d.cold_data_pool);

  // as a zone written before two-placement classes holds it
  ceph::buffer::list v1;
  ENCODE_START(1, 1, v1);
  encode(c.data_pool, v1);
  encode(c.compression_type, v1);
  ENCODE_FINISH(v1);
  p = v1.cbegin();
  decode(d, p);
  EXPECT_EQ(c.data_pool, d.data_pool);
  EXPECT_FALSE(d.cold_data_pool);

  JSONFormatter f;
  f.open_object_section("storage_class");
  c.dump(&f);
  f.close_section();
  std::stringstream ss;
  f.flush(ss);
  JSONParser parser;
  ASSERT_TRUE(parser.parse(ss.str().c_str(), ss.str().size()));
  RGWZoneStorageClass j;
  j.decode_json(&parser);
  EXPECT_EQ(c.data_pool, j.data_pool);
  EXPECT_EQ(c.cold_data_pool, j.cold_data_pool);
}
