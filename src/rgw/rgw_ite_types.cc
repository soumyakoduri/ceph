// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_types.h"

#include <algorithm>
#include <ctime>
#include <fmt/format.h>

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "common/strtol.h"
#include "include/ceph_hash.h"

namespace rgw::ite {

std::string_view to_string(Op op)
{
  switch (op) {
  case Op::Get: return "GET";
  case Op::Put: return "PUT";
  case Op::Head: return "HEAD";
  case Op::Delete: return "DELETE";
  }
  return "UNKNOWN";
}

std::string_view to_string(CallerType c)
{
  switch (c) {
  case CallerType::User: return "USER";
  case CallerType::Replication: return "REPLICATION";
  case CallerType::Lifecycle: return "LIFECYCLE";
  case CallerType::GC: return "GC";
  case CallerType::Tiering: return "TIERING";
  }
  return "UNKNOWN";
}

std::optional<Op> parse_op(std::string_view s)
{
  for (auto op : {Op::Get, Op::Put, Op::Head, Op::Delete}) {
    if (s == to_string(op)) {
      return op;
    }
  }
  return std::nullopt;
}

std::optional<CallerType> parse_caller(std::string_view s)
{
  for (auto c : {CallerType::User, CallerType::Replication,
                 CallerType::Lifecycle, CallerType::GC, CallerType::Tiering}) {
    if (s == to_string(c)) {
      return c;
    }
  }
  return std::nullopt;
}

double op_weight(Op op)
{
  switch (op) {
  case Op::Get: return 1.0;   // read pressure
  case Op::Put: return 0.5;   // alive, but not read pressure
  case Op::Head: return 0.1;  // metadata interest
  case Op::Delete: return 0.0;
  }
  return 0.0;
}

void InstanceId::dump(ceph::Formatter* f) const
{
  encode_json("name", name, f);
  encode_json("instance", instance, f);
  encode_json("gen", gen, f);
}

void AccessEvent::dump(ceph::Formatter* f) const
{
  encode_json("bucket", bucket, f);
  encode_json("obj", obj, f);
  encode_json("timestamp", timestamp, f);
  encode_json("op", to_string(op), f);
  encode_json("caller", to_string(caller), f);
  encode_json("client_id", client_id, f);
}

std::list<AccessEvent> AccessEvent::generate_test_instances()
{
  std::list<AccessEvent> o;
  o.emplace_back();
  auto& e = o.emplace_back();
  e.bucket = rgw_bucket("tenant", "bucket", "bucket-id");
  e.obj = InstanceId("report.pdf", "vABC", 12);
  e.timestamp = from_epoch_secs(1700000000);
  e.op = Op::Head;
  e.caller = CallerType::User;
  e.client_id = "client";
  return o;
}

static uint32_t hash_str(std::string_view s)
{
  return ceph_str_hash_linux(s.data(), s.size());
}

uint32_t buffer_shard_of(const rgw_bucket& bucket, std::string_view obj_name,
                         uint32_t num_shards)
{
  if (num_shards <= 1) {
    return 0;
  }
  std::string s;
  s.reserve(bucket.bucket_id.size() + 1 + obj_name.size());
  s.append(bucket.bucket_id).append(":").append(obj_name);
  return hash_str(s) % num_shards;
}

uint32_t agg_shard_of(std::string_view obj_name, uint32_t num_shards)
{
  if (num_shards <= 1) {
    return 0;
  }
  return hash_str(obj_name) % num_shards;
}

std::string CountKey::to_str() const
{
  return fmt::format("{}:{}:{}:{}:{}", obj.name, obj.instance, obj.gen,
                     to_string(op), to_string(caller));
}

std::optional<CountKey> CountKey::parse(std::string_view s)
{
  // name:instance:gen:op:caller; only the name may contain ':'
  std::string_view fields[4]; // caller, op, gen, instance
  for (int i = 0; i < 4; ++i) {
    auto pos = s.rfind(':');
    if (pos == std::string_view::npos) {
      return std::nullopt;
    }
    fields[i] = s.substr(pos + 1);
    s = s.substr(0, pos);
  }
  if (s.empty()) {
    return std::nullopt;
  }
  auto caller = parse_caller(fields[0]);
  auto op = parse_op(fields[1]);
  if (!caller || !op || fields[2].empty()) {
    return std::nullopt;
  }
  std::string err;
  uint64_t gen = strict_strtoll(fields[2], 10, &err);
  if (!err.empty()) {
    return std::nullopt;
  }
  CountKey k;
  k.obj = InstanceId(std::string(s), std::string(fields[3]), gen);
  k.op = *op;
  k.caller = *caller;
  return k;
}

static constexpr std::string_view MARKER_PREFIX = "__mkr__:";

std::string marker_key(uint32_t buffer_shard)
{
  return fmt::format("{}{}", MARKER_PREFIX, buffer_shard);
}

bool is_marker_key(std::string_view key)
{
  // a count key for an object named "__mkr__" still ends in :op:caller, so
  // only an all-digit remainder is a marker
  if (key.substr(0, MARKER_PREFIX.size()) != MARKER_PREFIX) {
    return false;
  }
  auto rest = key.substr(MARKER_PREFIX.size());
  return !rest.empty() &&
    std::all_of(rest.begin(), rest.end(), [](char c) { return c >= '0' && c <= '9'; });
}

TimeScale TimeScale::from_conf(CephContext* cct)
{
  TimeScale ts;
  const int64_t dbg = cct->_conf->rgw_ite_debug_interval;
  if (dbg > 0) {
    ts.secs_per_day = static_cast<uint64_t>(dbg);
  }
  return ts;
}

double TimeScale::days_between(ceph::real_time from, ceph::real_time to) const
{
  const double secs = std::chrono::duration<double>(to - from).count();
  return secs / static_cast<double>(secs_per_day);
}

ceph::timespan TimeScale::days(double d) const
{
  return std::chrono::duration_cast<ceph::timespan>(
      std::chrono::duration<double>(d * static_cast<double>(secs_per_day)));
}

uint64_t to_epoch_secs(ceph::real_time t)
{
  return static_cast<uint64_t>(ceph::real_clock::to_time_t(t));
}

ceph::real_time from_epoch_secs(uint64_t s)
{
  return ceph::real_clock::from_time_t(static_cast<time_t>(s));
}

/* 1970-01-01 was a Thursday; shifting by 3 days puts week boundaries on
 * Mondays */
static constexpr int64_t WEEK_SHIFT_DAYS = 3;

Window Window::of(ceph::real_time t, Granularity g, const TimeScale& ts)
{
  const uint64_t day = to_epoch_secs(t) / ts.secs_per_day;
  if (g == Granularity::Week) {
    return Window{g, (day + WEEK_SHIFT_DAYS) / 7};
  }
  return Window{g, day};
}

static int64_t first_day(const Window& w)
{
  if (w.granularity == Granularity::Week) {
    return static_cast<int64_t>(w.index) * 7 - WEEK_SHIFT_DAYS;
  }
  return static_cast<int64_t>(w.index);
}

ceph::timespan Window::length(const TimeScale& ts) const
{
  const uint64_t days = (granularity == Granularity::Week ? 7 : 1);
  return std::chrono::seconds(days * ts.secs_per_day);
}

ceph::real_time Window::start(const TimeScale& ts) const
{
  const int64_t day = std::max<int64_t>(first_day(*this), 0);
  return from_epoch_secs(static_cast<uint64_t>(day) * ts.secs_per_day);
}

ceph::real_time Window::end(const TimeScale& ts) const
{
  return start(ts) + length(ts);
}

std::string Window::stamp(const TimeScale& ts) const
{
  const char* prefix = (granularity == Granularity::Week ? "w" : "");
  if (ts.secs_per_day != 86400) {
    return fmt::format("x{}{}", prefix, index);
  }
  const time_t t = std::max<int64_t>(first_day(*this), 0) * 86400;
  struct tm tm;
  gmtime_r(&t, &tm);
  return fmt::format("{}{:04}{:02}{:02}", prefix, tm.tm_year + 1900,
                     tm.tm_mon + 1, tm.tm_mday);
}

std::optional<Window> Window::parse_stamp(std::string_view s, const TimeScale& ts)
{
  Window w;
  bool debug = false;
  if (!s.empty() && s.front() == 'x') {
    debug = true;
    s.remove_prefix(1);
  }
  if (!s.empty() && s.front() == 'w') {
    w.granularity = Granularity::Week;
    s.remove_prefix(1);
  }
  if (s.empty() || !std::all_of(s.begin(), s.end(),
                                [](char c) { return c >= '0' && c <= '9'; })) {
    return std::nullopt;
  }
  if (debug != (ts.secs_per_day != 86400)) {
    return std::nullopt; // stamp from a different time scale
  }
  std::string err;
  const uint64_t v = strict_strtoll(s, 10, &err);
  if (!err.empty()) {
    return std::nullopt;
  }
  if (debug) {
    w.index = v;
    return w;
  }
  if (s.size() != 8) {
    return std::nullopt;
  }
  struct tm tm = {};
  tm.tm_year = static_cast<int>(v / 10000) - 1900;
  tm.tm_mon = static_cast<int>((v / 100) % 100) - 1;
  tm.tm_mday = static_cast<int>(v % 100);
  const int64_t day = timegm(&tm) / 86400;
  if (w.granularity == Granularity::Week) {
    w.index = static_cast<uint64_t>(day + WEEK_SHIFT_DAYS) / 7;
  } else {
    w.index = static_cast<uint64_t>(day);
  }
  return w;
}

std::optional<Granularity> parse_granularity(std::string_view s)
{
  if (s == "day") {
    return Granularity::Day;
  }
  if (s == "week") {
    return Granularity::Week;
  }
  return std::nullopt;
}

Knobs Knobs::from_conf(CephContext* cct)
{
  const auto& conf = cct->_conf;
  Knobs k;
  k.half_life_days = conf->rgw_ite_half_life;
  k.warm_threshold = conf->rgw_ite_warm_threshold;
  k.cool_threshold = conf->rgw_ite_cool_threshold;
  k.cold_after_days = static_cast<uint32_t>(conf->rgw_ite_cold_after);
  k.prewarm = conf->rgw_ite_prewarm;
  k.prewarm_lead_days = static_cast<uint32_t>(conf->rgw_ite_prewarm_lead);
  k.min_tier_size = conf->rgw_ite_min_tier_size;
  k.min_fft_confidence = conf->rgw_ite_min_fft_confidence;
  return k;
}

void Knobs::dump(ceph::Formatter* f) const
{
  f->dump_float("half_life_days", half_life_days);
  f->dump_float("warm_threshold", warm_threshold);
  f->dump_float("cool_threshold", cool_threshold);
  encode_json("cold_after_days", cold_after_days, f);
  encode_json("prewarm", prewarm, f);
  encode_json("prewarm_lead_days", prewarm_lead_days, f);
  encode_json("min_tier_size", min_tier_size, f);
  f->dump_float("min_fft_confidence", min_fft_confidence);
  f->dump_float("grace_days", grace_days());
}

Knobs BucketConfig::effective(const Knobs& defaults) const
{
  Knobs k = defaults;
  if (half_life_days) k.half_life_days = *half_life_days;
  if (warm_threshold) k.warm_threshold = *warm_threshold;
  if (cool_threshold) k.cool_threshold = *cool_threshold;
  if (cold_after_days) k.cold_after_days = *cold_after_days;
  if (prewarm) k.prewarm = *prewarm;
  if (prewarm_lead_days) k.prewarm_lead_days = *prewarm_lead_days;
  if (min_tier_size) k.min_tier_size = *min_tier_size;
  if (min_fft_confidence) k.min_fft_confidence = *min_fft_confidence;
  return k;
}

void BucketConfig::dump(ceph::Formatter* f) const
{
  encode_json("enabled", enabled, f);
  if (half_life_days) f->dump_float("half_life_days", *half_life_days);
  if (warm_threshold) f->dump_float("warm_threshold", *warm_threshold);
  if (cool_threshold) f->dump_float("cool_threshold", *cool_threshold);
  if (cold_after_days) encode_json("cold_after_days", *cold_after_days, f);
  if (prewarm) encode_json("prewarm", *prewarm, f);
  if (prewarm_lead_days) encode_json("prewarm_lead_days", *prewarm_lead_days, f);
  if (min_tier_size) encode_json("min_tier_size", *min_tier_size, f);
  if (min_fft_confidence) f->dump_float("min_fft_confidence", *min_fft_confidence);
}

std::list<BucketConfig> BucketConfig::generate_test_instances()
{
  std::list<BucketConfig> o;
  o.emplace_back();
  auto& c = o.emplace_back();
  c.enabled = true;
  c.half_life_days = 3.5;
  c.cold_after_days = 10;
  c.min_tier_size = 8192;
  return o;
}

std::optional<BucketConfig> get_bucket_config(
    const std::map<std::string, ceph::buffer::list>& attrs)
{
  auto i = attrs.find(std::string(ATTR_ITE));
  if (i == attrs.end()) {
    return std::nullopt;
  }
  BucketConfig c;
  try {
    auto p = i->second.cbegin();
    decode(c, p);
  } catch (const ceph::buffer::error&) {
    return std::nullopt;
  }
  return c;
}

void BucketProgress::dump(ceph::Formatter* f) const
{
  encode_json("bucket", bucket, f);
  encode_json("enabled_at", enabled_at, f);
  if (last_scored) {
    encode_json("last_scored_window", last_scored->index, f);
  }
  encode_json("last_sweep", last_sweep, f);
  encode_json("last_fft", last_fft, f);
  encode_json("last_decision", last_decision, f);
}

std::string_view to_string(Placement p)
{
  switch (p) {
  case Placement::Hot: return "hot";
  case Placement::Cold: return "cold";
  }
  return "unknown";
}

std::string_view to_string(MoveKind k)
{
  switch (k) {
  case MoveKind::Commit: return "commit";
  case MoveKind::PreWarm: return "pre-warm";
  case MoveKind::Restore: return "restore";
  }
  return "unknown";
}

RGWITEMovePending move_pending_of(MoveKind k)
{
  switch (k) {
  case MoveKind::Commit: return RGWITEMovePending::Commit;
  case MoveKind::PreWarm: return RGWITEMovePending::PreWarm;
  case MoveKind::Restore: return RGWITEMovePending::Restore;
  }
  return RGWITEMovePending::None;
}

Placement target_of(MoveKind k)
{
  return k == MoveKind::Commit ? Placement::Cold : Placement::Hot;
}

void MoveAction::dump(ceph::Formatter* f) const
{
  encode_json("bucket", bucket, f);
  encode_json("obj", obj, f);
  encode_json("kind", to_string(kind), f);
  encode_json("target", to_string(target), f);
  encode_json("zone_id", zone_id, f);
  encode_json("queued_at", queued_at, f);
  encode_json("attempts", attempts, f);
}

std::list<MoveAction> MoveAction::generate_test_instances()
{
  std::list<MoveAction> o;
  o.emplace_back();
  auto& a = o.emplace_back();
  a.bucket = rgw_bucket("", "bucket", "bucket-id");
  a.obj = InstanceId("obj", "", 7);
  a.kind = MoveKind::PreWarm;
  a.target = Placement::Hot;
  a.zone_id = "zone";
  a.queued_at = from_epoch_secs(1700000000);
  a.attempts = 2;
  return o;
}

uint32_t move_shard_of(const MoveAction& a, uint32_t num_shards)
{
  return buffer_shard_of(a.bucket, a.obj.name, num_shards);
}

} // namespace rgw::ite
