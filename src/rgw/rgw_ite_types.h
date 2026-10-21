// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): backend-neutral types.
 *
 * Everything here sits above the SAL line: the access event appended to the
 * write buffer (contract one), the count keys of the aggregated store
 * (contract two), the per-bucket knob set, the window arithmetic and the
 * move action carried by the move queue (contract four). The per-object
 * state block itself (contract three) is rgw_bucket_ite_state.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "include/encoding.h"
#include "common/ceph_time.h"
#include "rgw_basic_types.h"
#include "rgw_bucket_types.h"
#include "rgw_obj_types.h"
#include "cls/rgw/cls_rgw_types.h"

class CephContext;
class JSONObj;
namespace ceph { class Formatter; }

namespace rgw::ite {

/* the bucket attribute that holds an ITE bucket's configuration */
inline constexpr std::string_view ATTR_ITE = "user.rgw.ite";

/* suffix of the internal storage class string that addresses the cold
 * placement of a two-placement storage class; never advertised to clients */
inline constexpr std::string_view COLD_PLACEMENT_SUFFIX = "@cold";

enum class Op : uint8_t {
  Get = 0,
  Put = 1,
  Head = 2,
  Delete = 3,
};

/* who initiated an access, by provenance */
enum class CallerType : uint8_t {
  User = 0,
  Replication = 1,
  Lifecycle = 2,
  GC = 3,
  Tiering = 4,
};

std::string_view to_string(Op op);
std::string_view to_string(CallerType c);
std::optional<Op> parse_op(std::string_view s);
std::optional<CallerType> parse_caller(std::string_view s);

/* temperature weight of one access by operation; DELETE does not feed it */
double op_weight(Op op);

/* the stored data instance an access is attributed to: object name, version
 * id (empty for the null version) and generation. In RADOS the generation is
 * the head object's write version, the index entry's ver.epoch. 0 means the
 * generation was not known when the access was recorded. */
struct InstanceId {
  std::string name;
  std::string instance;
  uint64_t gen = 0;

  InstanceId() = default;
  InstanceId(std::string n, std::string i, uint64_t g)
    : name(std::move(n)), instance(std::move(i)), gen(g) {}

  rgw_obj_key key() const { return rgw_obj_key(name, instance); }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(name, bl);
    encode(instance, bl);
    encode(gen, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(name, bl);
    decode(instance, bl);
    decode(gen, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;

  friend bool operator==(const InstanceId&, const InstanceId&) = default;
  friend auto operator<=>(const InstanceId&, const InstanceId&) = default;
};
WRITE_CLASS_ENCODER(InstanceId)

/* the unit contract one appends */
struct AccessEvent {
  rgw_bucket bucket;
  InstanceId obj;
  ceph::real_time timestamp; // UTC
  Op op = Op::Get;
  CallerType caller = CallerType::User;
  std::string client_id; // optional, for later clustering

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    encode(obj, bl);
    encode(timestamp, bl);
    encode(static_cast<uint8_t>(op), bl);
    encode(static_cast<uint8_t>(caller), bl);
    encode(client_id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket, bl);
    decode(obj, bl);
    decode(timestamp, bl);
    uint8_t v;
    decode(v, bl);
    op = static_cast<Op>(v);
    decode(v, bl);
    caller = static_cast<CallerType>(v);
    decode(client_id, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static std::list<AccessEvent> generate_test_instances();
};
WRITE_CLASS_ENCODER(AccessEvent)

/* an event read back from a write-buffer shard, with its cursor */
struct AccessEventEntry {
  std::string marker;
  AccessEvent event;
};

/* the write-buffer shard of an object: hash of bucket and object, so every
 * event for one object lands on the same shard */
uint32_t buffer_shard_of(const rgw_bucket& bucket, std::string_view obj_name,
                         uint32_t num_shards);
/* the aggregate hash shard of an object inside a bucket window */
uint32_t agg_shard_of(std::string_view obj_name, uint32_t num_shards);

/* count key of the aggregated store: instance:operation:caller, where the
 * instance is name:version:generation. Parsed from the right, so object
 * names may contain ':'. */
struct CountKey {
  InstanceId obj;
  Op op = Op::Get;
  CallerType caller = CallerType::User;

  std::string to_str() const;
  static std::optional<CountKey> parse(std::string_view s);

  friend bool operator==(const CountKey&, const CountKey&) = default;
  friend auto operator<=>(const CountKey&, const CountKey&) = default;
};

/* a progress marker key of an aggregated window object */
std::string marker_key(uint32_t buffer_shard);
bool is_marker_key(std::string_view key);

/* ITE time: one "day" is 86400 seconds unless rgw_ite_debug_interval says
 * otherwise, so every day-based quantity can be compressed for testing */
struct TimeScale {
  uint64_t secs_per_day = 86400;

  static TimeScale from_conf(CephContext* cct);
  double days_between(ceph::real_time from, ceph::real_time to) const;
  ceph::timespan days(double d) const;
};

enum class Granularity : uint8_t {
  Day = 0,
  Week = 1,
};

/* an aggregation window: a day or a week, addressed by its index since the
 * epoch (weeks start on Monday) */
struct Window {
  Granularity granularity = Granularity::Day;
  uint64_t index = 0;

  static Window of(ceph::real_time t, Granularity g, const TimeScale& ts);
  ceph::real_time start(const TimeScale& ts) const;
  ceph::real_time end(const TimeScale& ts) const;
  ceph::timespan length(const TimeScale& ts) const;
  Window prev() const { return Window{granularity, index - 1}; }
  Window next() const { return Window{granularity, index + 1}; }
  /* the stamp used in object names: YYYYMMDD of the window's first day, with
   * a 'w' prefix for week windows; with a debug time scale the stamp is the
   * plain window index prefixed with 'x' */
  std::string stamp(const TimeScale& ts) const;
  static std::optional<Window> parse_stamp(std::string_view s, const TimeScale& ts);

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(static_cast<uint8_t>(granularity), bl);
    encode(index, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    uint8_t v;
    decode(v, bl);
    granularity = static_cast<Granularity>(v);
    decode(index, bl);
    DECODE_FINISH(bl);
  }

  friend bool operator==(const Window&, const Window&) = default;
  friend auto operator<=>(const Window&, const Window&) = default;
};
WRITE_CLASS_ENCODER(Window)

std::optional<Granularity> parse_granularity(std::string_view s);

/* the single flat knob set in effect for a bucket in Stage 0. When workload
 * profiles arrive this becomes the built-in default profile. */
struct Knobs {
  double half_life_days = 7;
  double warm_threshold = 0.3;
  double cool_threshold = 0.1;
  uint32_t cold_after_days = 30;
  bool prewarm = true;
  uint32_t prewarm_lead_days = 7;
  uint64_t min_tier_size = 4096;
  double min_fft_confidence = 0.5;

  static Knobs from_conf(CephContext* cct);
  /* grace period a PSEUDO instance must stay cold before it is committed;
   * derived from the half-life so the two cannot be tuned against each
   * other into thrashing */
  double grace_days() const { return half_life_days; }
  void dump(ceph::Formatter* f) const;
};

/* per-bucket ITE configuration, stored in the ATTR_ITE bucket attribute.
 * Unset knobs fall back to the zone defaults. */
struct BucketConfig {
  bool enabled = false;
  std::optional<double> half_life_days;
  std::optional<double> warm_threshold;
  std::optional<double> cool_threshold;
  std::optional<uint32_t> cold_after_days;
  std::optional<bool> prewarm;
  std::optional<uint32_t> prewarm_lead_days;
  std::optional<uint64_t> min_tier_size;
  std::optional<double> min_fft_confidence;

  Knobs effective(const Knobs& defaults) const;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(enabled, bl);
    encode(half_life_days, bl);
    encode(warm_threshold, bl);
    encode(cool_threshold, bl);
    encode(cold_after_days, bl);
    encode(prewarm, bl);
    encode(prewarm_lead_days, bl);
    encode(min_tier_size, bl);
    encode(min_fft_confidence, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(enabled, bl);
    decode(half_life_days, bl);
    decode(warm_threshold, bl);
    decode(cool_threshold, bl);
    decode(cold_after_days, bl);
    decode(prewarm, bl);
    decode(prewarm_lead_days, bl);
    decode(min_tier_size, bl);
    decode(min_fft_confidence, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static std::list<BucketConfig> generate_test_instances();
};
WRITE_CLASS_ENCODER(BucketConfig)

/* decode the bucket's ITE configuration from its attributes; returns
 * nullopt when the bucket has none */
std::optional<BucketConfig> get_bucket_config(
    const std::map<std::string, ceph::buffer::list>& attrs);

/* the bucket directory entry the workers keep for every ITE bucket, with
 * the progress of each periodic worker over that bucket */
struct BucketProgress {
  rgw_bucket bucket;
  ceph::real_time enabled_at;
  std::optional<Window> last_scored; // newest window the temperature scorer folded in
  ceph::real_time last_sweep;
  ceph::real_time last_fft;
  ceph::real_time last_decision;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bucket, bl);
    encode(enabled_at, bl);
    encode(last_scored, bl);
    encode(last_sweep, bl);
    encode(last_fft, bl);
    encode(last_decision, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bucket, bl);
    decode(enabled_at, bl);
    decode(last_scored, bl);
    decode(last_sweep, bl);
    decode(last_fft, bl);
    decode(last_decision, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(BucketProgress)

/* one object's ITE state as contract three enumerates it, together with
 * the index fields the scorers and the decision engine need */
struct ObjectEntry {
  rgw_obj_key key;
  uint64_t gen = 0; // current generation of the stored instance
  uint64_t size = 0;
  ceph::real_time mtime;
  std::string storage_class;
  bool exists = true;
  bool current = true; // false for a non-current version
  bool delete_marker = false;
  rgw_bucket_ite_state ite;

  /* the instance identity accesses are attributed to */
  InstanceId instance() const {
    return InstanceId(key.name, key.instance, ite.base_gen ? ite.base_gen : gen);
  }
};

/* the two physical placements of a two-placement storage class */
enum class Placement : uint8_t {
  Hot = 0,
  Cold = 1,
};
std::string_view to_string(Placement p);

/* the byte-moving actions the decision engine queues for the lazy mover */
enum class MoveKind : uint8_t {
  Commit = 1,  // hot -> cold
  PreWarm = 2, // cold -> hot ahead of a predicted access, lands in PSEUDO
  Restore = 3, // cold -> hot on real demand, lands in HOT
};
std::string_view to_string(MoveKind k);
RGWITEMovePending move_pending_of(MoveKind k);
Placement target_of(MoveKind k);

/* the unit contract four appends */
struct MoveAction {
  rgw_bucket bucket;
  InstanceId obj; // gen is the instance's base generation
  MoveKind kind = MoveKind::Commit;
  Placement target = Placement::Cold;
  std::string zone_id;
  ceph::real_time queued_at;
  uint32_t attempts = 0; // failed tries so far; the mover puts it back after one

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(bucket, bl);
    encode(obj, bl);
    encode(static_cast<uint8_t>(kind), bl);
    encode(static_cast<uint8_t>(target), bl);
    encode(zone_id, bl);
    encode(queued_at, bl);
    encode(attempts, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(bucket, bl);
    decode(obj, bl);
    uint8_t v;
    decode(v, bl);
    kind = static_cast<MoveKind>(v);
    decode(v, bl);
    target = static_cast<Placement>(v);
    decode(zone_id, bl);
    decode(queued_at, bl);
    if (struct_v >= 2) {
      decode(attempts, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  static std::list<MoveAction> generate_test_instances();
};
WRITE_CLASS_ENCODER(MoveAction)

struct MoveActionEntry {
  std::string marker;
  MoveAction action;
};

uint32_t move_shard_of(const MoveAction& a, uint32_t num_shards);

/* epoch seconds helpers for the per-object state block */
uint64_t to_epoch_secs(ceph::real_time t);
ceph::real_time from_epoch_secs(uint64_t s);

} // namespace rgw::ite
