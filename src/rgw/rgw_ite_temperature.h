// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): the temperature scorer.
 *
 * Each data instance carries one heat number in [0, 1]: the sum over its
 * accesses of the operation weight times an exponential decay with the
 * bucket's half-life, capped at 1. No history is kept; the stored number
 * is decayed forward and the next window's weighted counts are added.
 *
 * The daily run reads, through contract two, only the windows that closed
 * since the bucket was last scored, and writes the temperature through
 * contract three for the objects those windows touched. A window's
 * accesses are folded in as of the window's end, which is also stored as
 * the update time; an object already updated at or past a window's end
 * is not folded again, so a rerun after a crash never double-counts.
 * Readers decay the stored value to the moment they look at it.
 *
 * Untouched objects still cool: a weekly sweep decays every stored
 * temperature to the end of the last scored window.
 */

#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "common/async/yield_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_ite_types.h"
#include "rgw_sal_fwd.h"

namespace rgw::ite {

struct Context;
class BucketLock;

/* temperature t after elapsed_days of decay with the given half-life */
double decay(double t, double elapsed_days, double half_life_days);

/* a stored temperature as of time at: decayed from its update time, and
 * never raised when at precedes it */
double temperature_at(const rgw_bucket_ite_state& s, ceph::real_time at,
                      double half_life_days, const TimeScale& ts);

/* the temperature after folding in heat, the weighted accesses of a
 * window that ended at window_end */
double fold(const rgw_bucket_ite_state& s, ceph::real_time window_end, double heat,
            double half_life_days, const TimeScale& ts);

/* below this a temperature is stored as zero, so cold objects stop being
 * rewritten by every sweep */
inline constexpr double TEMPERATURE_FLOOR = 1e-6;

struct TemperatureStats {
  uint64_t buckets = 0;          // buckets scored
  uint64_t buckets_busy = 0;     // scored by another gateway
  uint64_t buckets_skipped = 0;  // deleted, or ITE disabled
  uint64_t windows = 0;          // windows folded
  uint64_t objects = 0;          // temperatures raised by a window
  uint64_t already = 0;          // window already folded into the object
  uint64_t stale = 0;            // heat of a replaced instance, dropped
  uint64_t missing = 0;          // the object is gone
  uint64_t unparsable = 0;       // count keys that do not parse
  uint64_t swept = 0;            // temperatures decayed by the sweep

  void dump(ceph::Formatter* f) const;
};

class TemperatureScorer {
public:
  explicit TemperatureScorer(Context& ctx);

  /* score every bucket in the directory */
  int process(const DoutPrefixProvider* dpp, optional_yield y);
  /* score one bucket under its lock, as of now */
  int process_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                     ceph::real_time now, optional_yield y);

  const TemperatureStats& stats() const { return st; }

private:
  Context& ctx;
  TemperatureStats st;

  int score(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
            const Knobs& knobs, BucketLock& lock, ceph::real_time now,
            optional_yield y);
  int fold_window(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                  const Window& w, const Knobs& knobs, BucketLock& lock,
                  optional_yield y);
  int fold_object(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                  const rgw_obj_key& key,
                  const std::vector<std::pair<uint64_t, double>>& heat_by_gen,
                  const Window& w, const Knobs& knobs, optional_yield y);
  int sweep(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
            ceph::real_time to, const Knobs& knobs, BucketLock& lock,
            optional_yield y);
};

} // namespace rgw::ite
