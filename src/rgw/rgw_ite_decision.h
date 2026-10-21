// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): the decision engine.
 *
 * Daily, one walk of each ITE bucket's per-object state (contract three)
 * puts every instance through the state machine:
 *
 *   HOT       bytes in the hot placement, active
 *   PSEUDO    bytes still hot, marked cold for a grace period; also where
 *             a pre-warm lands
 *   COMMITTED bytes in the cold placement
 *
 * The engine writes the two free transitions itself, HOT to PSEUDO and
 * PSEUDO back to HOT, since no byte moves. The three byte-moving ones,
 * commit, pre-warm and restore, it queues on contract four for the lazy
 * mover, and marks the instance move-pending so that it does not queue
 * them again before the mover is done. The mover writes the terminal
 * state once the bytes are in place, so COMMITTED never claims a move
 * that has not happened.
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common/async/yield_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_ite_types.h"
#include "rgw_sal_fwd.h"

namespace rgw::ite {

struct Context;
class BucketLock;

enum class Decision : uint8_t {
  Skip,    // not ITE's to tier: no data, another class, or too small
  Hold,    // stays as it is
  Pseudo,  // HOT -> PSEUDO, free
  Hot,     // PSEUDO -> HOT, free
  Commit,  // queue PSEUDO -> COMMITTED
  PreWarm, // queue COMMITTED -> PSEUDO ahead of a predicted access
  Restore, // queue COMMITTED -> HOT, the instance is hot again
};
std::string_view to_string(Decision d);

/* what decide() needs besides the object */
struct DecisionInputs {
  Knobs knobs;
  ceph::real_time now;
  TimeScale ts;
  std::string storage_class = "INTELLIGENT_TIER"; // the only class ITE tiers
  bool demotion_enabled = true;
  uint32_t max_age_days = 0; // demote past this age regardless; 0 is off
};

struct Verdict {
  Decision decision = Decision::Skip;
  std::string_view reason;
};

/* the zone's inputs for a bucket with these knobs, as of now */
DecisionInputs decision_inputs(const Context& ctx, const Knobs& knobs, ceph::real_time now);

/* the state machine for one instance, as of in.now */
Verdict decide(const ObjectEntry& e, const DecisionInputs& in);

/* whether e has an access predicted within the pre-warm lead that the
 * bucket's knobs let the engine act on */
bool access_predicted(const ObjectEntry& e, const DecisionInputs& in);

struct DecisionStats {
  uint64_t buckets = 0;          // buckets decided
  uint64_t buckets_busy = 0;     // decided by another gateway
  uint64_t buckets_skipped = 0;  // deleted, or ITE disabled
  uint64_t buckets_not_due = 0;  // decided less than a period ago
  uint64_t objects = 0;          // entries examined
  uint64_t skip = 0;
  uint64_t hold = 0;
  uint64_t pseudo = 0;
  uint64_t hot = 0;
  uint64_t commit = 0;
  uint64_t prewarm = 0;
  uint64_t restore = 0;
  uint64_t stale = 0;            // replaced between the listing and the write
  uint64_t missing = 0;          // gone between the listing and the write

  void dump(ceph::Formatter* f) const;
};

class DecisionEngine {
public:
  explicit DecisionEngine(Context& ctx);

  /* decide every bucket in the directory that is due */
  int process(const DoutPrefixProvider* dpp, optional_yield y);
  /* decide one bucket under its lock, as of now, if it is due */
  int process_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                     ceph::real_time now, optional_yield y);

  const DecisionStats& stats() const { return st; }

private:
  Context& ctx;
  DecisionStats st;

  int decide_bucket(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                    const Knobs& knobs, BucketLock& lock, ceph::real_time now,
                    optional_yield y);
  int apply(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
            const ObjectEntry& e, const Verdict& v, ceph::real_time now,
            optional_yield y);
  int queue(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
            const ObjectEntry& e, MoveKind kind, ceph::real_time now,
            optional_yield y);
};

} // namespace rgw::ite
