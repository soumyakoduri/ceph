// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): the aggregator.
 *
 * The hourly worker that folds the raw access events of the write buffer
 * (contract one) into per-window counts in the aggregated store (contract
 * two), then trims what it consumed and expires windows past retention.
 * It is backend-neutral: everything it does goes through the two
 * contracts.
 *
 * Exactly-once counting without a transaction across the two contracts:
 * each window shard object remembers, per buffer shard, the cursor of the
 * last event folded into it, and add_counts() only applies a batch whose
 * expected cursor still matches, advancing it in the same atomic update.
 * The buffer is trimmed only after every count of a page has landed, so a
 * crash between the two replays the page and the stored cursors filter out
 * what was already counted.
 */

#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/async/yield_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_ite_types.h"

namespace rgw::ite {

struct Context;

/* how many windows the aggregated store keeps: the FFT span plus the
 * retention margin, converted to windows of granularity g. There is no
 * separate retention knob, so the store always holds what the FFT reads. */
uint64_t retention_windows(uint64_t fft_span, uint64_t margin_days, Granularity g);

/* the oldest window the aggregator still adds events to while `current` is
 * the open window */
uint64_t oldest_accepted(uint64_t current, uint64_t retention);

/* whether expiry drops a window. Expiry keeps one window more than the
 * aggregator accepts, so it never removes a window the aggregator may
 * still write, even with some clock skew between gateways. */
bool is_expired(uint64_t window, uint64_t current, uint64_t retention);

struct AggregatorStats {
  uint64_t shards = 0;             // buffer shards processed
  uint64_t shards_busy = 0;        // buffer shards owned by another gateway
  uint64_t events = 0;             // events read
  uint64_t counted = 0;            // events added to the counts
  uint64_t replayed = 0;           // events an earlier run already counted
  uint64_t not_user = 0;           // dropped: not USER provenance
  uint64_t too_old = 0;            // dropped: older than retention
  uint64_t buckets_registered = 0; // added to the bucket directory
  uint64_t windows_expired = 0;

  void dump(ceph::Formatter* f) const;
};

class Aggregator {
public:
  /* events read per list() call */
  static constexpr uint32_t page_size = 512;
  /* attempts to apply one group when another gateway moves its marker */
  static constexpr int max_attempts = 5;

  explicit Aggregator(Context& ctx);

  /* one cycle: every buffer shard, then expiry */
  int process(const DoutPrefixProvider* dpp, optional_yield y);
  /* consume one buffer shard under its processing lock; a shard owned by
   * another gateway is skipped */
  int process_shard(const DoutPrefixProvider* dpp, uint32_t shard, optional_yield y);
  /* drop the windows of every directory bucket that are past retention */
  int expire(const DoutPrefixProvider* dpp, ceph::real_time now, optional_yield y);

  const AggregatorStats& stats() const { return st; }

private:
  /* a window shard object: bucket key, window, aggregate shard */
  using GroupKey = std::tuple<std::string, Window, uint32_t>;

  /* the events of one page that land on one window shard object */
  struct Group {
    rgw_bucket bucket;
    Window window;
    uint32_t agg_shard = 0;
    std::vector<std::pair<std::string, std::string>> events; // cursor, count key
  };

  /* one page of a buffer shard, grouped */
  struct Batch {
    std::map<GroupKey, Group> groups;
    // bucket key -> bucket and the earliest event time seen for it
    std::map<std::string, std::pair<rgw_bucket, ceph::real_time>> buckets;
  };

  /* stored markers of the buffer shard being consumed, per window shard */
  using MarkerCache = std::map<GroupKey, std::string>;

  Context& ctx;
  AggregatorStats st;
  std::set<std::string> registered; // buckets known to be in the directory

  uint64_t retention(Granularity g) const;
  int consume_shard(const DoutPrefixProvider* dpp, uint32_t shard,
                    ceph::coarse_mono_time deadline, optional_yield y);
  void group(const std::vector<AccessEventEntry>& entries, Granularity g,
             const TimeScale& ts, uint64_t oldest, Batch& batch);
  int apply(const DoutPrefixProvider* dpp, uint32_t shard,
            const std::string& new_marker, Batch& batch,
            MarkerCache& markers, optional_yield y);
  int apply_group(const DoutPrefixProvider* dpp, uint32_t shard,
                  const std::string& new_marker, const GroupKey& key,
                  const Group& grp, MarkerCache& markers, optional_yield y);
  int ensure_registered(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                        ceph::real_time first_seen, optional_yield y);
  int expire_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                    ceph::real_time now, optional_yield y);
};

} // namespace rgw::ite
