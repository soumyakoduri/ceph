// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): the FFT periodicity detector.
 *
 * Weekly, for every ITE bucket past its warm-up, the detector reads the
 * last span of closed windows through contract two, one aggregated shard
 * at a time, and builds in that one pass the access series of every
 * instance the windows mention. Each instance accessed in enough windows
 * is tested for a repeating pattern, and the verdict, the period, the
 * predicted next access and the confidence are written through contract
 * three for the decision engine to pre-warm on.
 *
 * The test is the autocorrelation of the series with its mean removed,
 * computed with the FFT. An access pattern is spiky, a job reading an
 * object once a month, and a spike train spreads its power evenly over
 * the harmonics of its frequency, so the strongest single frequency is
 * often a harmonic. The autocorrelation sums the harmonics back up and
 * peaks at the period itself. Each lag tolerates a jitter of a tenth of
 * itself, so a monthly job that runs between the 28th and the 31st still
 * lines up.
 */

#pragma once

#include <cstdint>
#include <map>
#include <vector>

#include "common/async/yield_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_ite_types.h"
#include "rgw_sal_fwd.h"

namespace rgw::ite {

struct Context;
class BucketLock;

struct Periodicity {
  bool periodic = false;
  uint32_t period = 0;    // in windows; 0 when aperiodic
  double confidence = 0;  // the score of the best candidate period, in [0, 1]
};

/* look for a repeating pattern in a series of per-window access weights,
 * oldest window first. The candidate periods run from 2 windows up to
 * the series length over min_cycles; the best is the highest local peak
 * of the jitter-tolerant autocorrelation. It is trusted when its score
 * reaches min_confidence and the accesses themselves span min_cycles
 * cycles, so that a burst on a few days in a row is not mistaken for a
 * short period. */
Periodicity detect_period(const std::vector<double>& series, uint32_t min_cycles,
                          double min_confidence);

struct FFTStats {
  uint64_t buckets = 0;          // buckets analysed
  uint64_t buckets_busy = 0;     // analysed by another gateway
  uint64_t buckets_skipped = 0;  // deleted, or ITE disabled
  uint64_t buckets_not_due = 0;  // analysed less than a period ago
  uint64_t buckets_warming = 0;  // not enough history yet
  uint64_t windows = 0;          // window shards read
  uint64_t sparse = 0;           // accessed in too few windows to test
  uint64_t candidates = 0;       // instances tested
  uint64_t periodic = 0;         // periodic verdicts written
  uint64_t aperiodic = 0;        // aperiodic verdicts written
  uint64_t unchanged = 0;        // verdict already stored
  uint64_t stale = 0;            // accesses of a replaced instance dropped,
                                 // or replaced while being analysed
  uint64_t missing = 0;          // the object is gone
  uint64_t unparsable = 0;       // count keys that do not parse

  void dump(ceph::Formatter* f) const;
};

class FFTDetector {
public:
  explicit FFTDetector(Context& ctx);

  /* analyse every bucket in the directory that is due */
  int process(const DoutPrefixProvider* dpp, optional_yield y);
  /* analyse one bucket under its lock, as of now, if it is due */
  int process_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                     ceph::real_time now, optional_yield y);

  const FFTStats& stats() const { return st; }

private:
  /* one window's weighted accesses of one generation */
  struct Hit {
    uint32_t offset; // window index within the span
    uint64_t gen;
    double weight;
  };
  /* the span being analysed */
  struct Span {
    Window first;
    uint32_t length = 0;
    std::vector<Window> windows; // those with counts
    uint32_t min_cycles = 0;
  };

  Context& ctx;
  FFTStats st;

  int analyse(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
              const Knobs& knobs, BucketLock& lock, ceph::real_time now,
              optional_yield y);
  int analyse_shard(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                    const Span& span, uint32_t shard, const Knobs& knobs,
                    BucketLock& lock, optional_yield y);
  int analyse_object(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                     const Span& span, const rgw_obj_key& key,
                     const std::vector<Hit>& hits, const Knobs& knobs,
                     optional_yield y);
};

} // namespace rgw::ite
