// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_fft_detector.h"

#include <algorithm>
#include <cmath>
#include <set>

#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "rgw_ite.h"
#include "rgw_ite_fft.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ite {

namespace {

/* how far, in windows, the accesses of a period of lag windows may drift
 * and still line up: a tenth of it */
int tolerance(uint32_t lag)
{
  return lag / 10;
}

/* the autocorrelation at lag of the series smoothed over +-tolerance(lag)
 * windows, relative to its value at lag 0. Smoothing with a box of that
 * width weights the raw autocorrelation r over +-2*tolerance lags with a
 * triangle, so it is computed from r directly. */
double score(const std::vector<double>& r, uint32_t lag)
{
  const int tol = tolerance(lag);
  const int n = r.size();
  auto at = [&](int k) {
    k = std::abs(k);
    return k < n ? r[k] : 0.0;
  };
  double num = 0;
  double den = 0;
  for (int j = -2 * tol; j <= 2 * tol; ++j) {
    const double w = 2 * tol + 1 - std::abs(j);
    num += w * at(static_cast<int>(lag) + j);
    den += w * at(j);
  }
  return den > 0 ? num / den : 0;
}

} // anonymous namespace

Periodicity detect_period(const std::vector<double>& series, uint32_t min_cycles,
                          double min_confidence)
{
  Periodicity p;
  const size_t n = series.size();
  if (min_cycles < 2 || n < 2 * size_t(min_cycles)) {
    return p; // not even a period of 2 fits min_cycles times
  }
  const uint32_t max_lag = n / min_cycles;

  size_t first = n;
  size_t last = 0;
  size_t active = 0;
  double sum = 0;
  for (size_t i = 0; i < n; ++i) {
    if (series[i] > 0) {
      first = std::min(first, i);
      last = i;
      ++active;
    }
    sum += series[i];
  }
  if (active < min_cycles) {
    return p;
  }
  // remove the DC component, which would otherwise dominate every lag
  const double mean = sum / n;
  std::vector<double> d(n);
  for (size_t i = 0; i < n; ++i) {
    d[i] = series[i] - mean;
  }
  const auto r = autocorrelation(d);
  if (r[0] <= 0) {
    return p; // a constant series: accessed every window alike
  }

  std::vector<double> s(max_lag + 1);
  for (uint32_t lag = 1; lag <= max_lag; ++lag) {
    s[lag] = score(r, lag);
  }
  // the best local peak: a burst decays from lag 0 without one. The last
  // lag is a peak while the score still rises into it: a period at the end
  // of the range, as a month is in 90 windows, may have its jittered top
  // just past it
  uint32_t best = 0;
  for (uint32_t lag = 2; lag <= max_lag; ++lag) {
    const bool peak = s[lag] > s[lag - 1] && (lag == max_lag || s[lag] >= s[lag + 1]);
    if (peak && (!best || s[lag] > s[best])) {
      best = lag;
    }
  }
  if (!best) {
    return p;
  }
  p.confidence = std::clamp(s[best], 0.0, 1.0);
  const bool spans = last - first + tolerance(best) >= (min_cycles - 1) * size_t(best);
  if (p.confidence >= min_confidence && spans) {
    p.periodic = true;
    p.period = best;
  }
  return p;
}

void FFTStats::dump(ceph::Formatter* f) const
{
  f->dump_unsigned("buckets", buckets);
  f->dump_unsigned("buckets_busy", buckets_busy);
  f->dump_unsigned("buckets_skipped", buckets_skipped);
  f->dump_unsigned("buckets_not_due", buckets_not_due);
  f->dump_unsigned("buckets_warming", buckets_warming);
  f->dump_unsigned("windows", windows);
  f->dump_unsigned("sparse", sparse);
  f->dump_unsigned("candidates", candidates);
  f->dump_unsigned("periodic", periodic);
  f->dump_unsigned("aperiodic", aperiodic);
  f->dump_unsigned("unchanged", unchanged);
  f->dump_unsigned("stale", stale);
  f->dump_unsigned("missing", missing);
  f->dump_unsigned("unparsable", unparsable);
}

FFTDetector::FFTDetector(Context& ctx) : ctx(ctx) {}

int FFTDetector::process(const DoutPrefixProvider* dpp, optional_yield y)
{
  int r = for_each_bucket(dpp, ctx, [&](const BucketProgress& p) {
    return process_bucket(dpp, p.bucket, ceph::real_clock::now(), y);
  }, y);
  ldpp_dout(dpp, 5) << "analysed " << st.buckets << " buckets (" << st.buckets_busy
                    << " busy, " << st.buckets_not_due << " not due, "
                    << st.buckets_warming << " warming up, " << st.buckets_skipped
                    << " skipped): " << st.candidates << " instances, "
                    << st.periodic << " periodic" << dendl;
  return r;
}

int FFTDetector::process_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                                ceph::real_time now, optional_yield y)
{
  BucketRun run;
  int r = run_on_bucket(dpp, ctx, bucket,
      [&](rgw::sal::Bucket* b, const Knobs& knobs, BucketLock& lock) {
        return analyse(dpp, b, knobs, lock, now, y);
      }, &run, y);
  switch (run) {
  case BucketRun::Done: break; // counted by analyse()
  case BucketRun::Busy: ++st.buckets_busy; break;
  case BucketRun::Gone:
  case BucketRun::Disabled: ++st.buckets_skipped; break;
  case BucketRun::Failed: break;
  }
  return r == STOPPED ? 0 : r;
}

int FFTDetector::analyse(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                         const Knobs& knobs, BucketLock& lock, ceph::real_time now,
                         optional_yield y)
{
  const rgw_bucket& b = bucket->get_key();
  // reread under the lock: another gateway may have just analysed it
  BucketProgress p;
  int r = ctx.agg->get_bucket(dpp, b, &p, y);
  if (r < 0) {
    return r;
  }
  if (now - p.last_fft < ctx.period("rgw_ite_fft_period")) {
    ++st.buckets_not_due;
    return 0;
  }

  const auto& conf = ctx.cct->_conf;
  const uint64_t length = conf.get_val<int64_t>("rgw_ite_fft_span");
  uint64_t warmup = conf.get_val<int64_t>("rgw_ite_fft_warmup");
  if (warmup == 0) {
    warmup = length;
  }
  const auto last = ctx.settled_window(now);
  if (!last) {
    ++st.buckets_warming;
    return 0;
  }
  const Granularity g = last->granularity;
  const TimeScale ts = ctx.timescale();
  // the gate: the three-cycle rule cannot be met on shorter history
  const uint64_t enabled = Window::of(p.enabled_at, g, ts).index;
  if (enabled > last->index || last->index - enabled + 1 < warmup) {
    ++st.buckets_warming;
    return 0;
  }
  ++st.buckets;

  Span span;
  span.first = Window{g, last->index + 1 >= length ? last->index + 1 - length : 0};
  span.length = last->index - span.first.index + 1;
  span.min_cycles = conf.get_val<int64_t>("rgw_ite_fft_min_cycles");
  r = ctx.agg->list_windows(dpp, b, span.first, *last, span.windows, y);
  if (r < 0) {
    return r;
  }
  // an instance's counts are all in one shard: build and test a shard's
  // series at a time, so memory is bounded by one shard's instances
  for (uint32_t shard = 0; shard < ctx.agg->num_shards(); ++shard) {
    r = analyse_shard(dpp, bucket, span, shard, knobs, lock, y);
    if (r < 0) {
      return r; // a rerun recomputes everything; nothing is partial
    }
  }
  p.last_fft = now;
  return ctx.agg->put_bucket(dpp, p, y);
}

int FFTDetector::analyse_shard(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                               const Span& span, uint32_t shard, const Knobs& knobs,
                               BucketLock& lock, optional_yield y)
{
  const rgw_bucket& b = bucket->get_key();
  std::map<rgw_obj_key, std::vector<Hit>> hits;
  for (const auto& w : span.windows) {
    if (ctx.going_down || !lock.keep(dpp, y)) {
      return STOPPED;
    }
    std::map<std::string, uint64_t> counts;
    int r = ctx.agg->read_shard_counts(dpp, b, w, shard, counts, y);
    if (r < 0) {
      return r;
    }
    ++st.windows;
    const uint32_t offset = w.index - span.first.index;
    for (const auto& [k, n] : counts) {
      auto key = CountKey::parse(k);
      if (!key) {
        ++st.unparsable;
        continue;
      }
      const double weight = op_weight(key->op);
      if (key->caller != CallerType::User || weight <= 0) {
        continue;
      }
      hits[key->obj.key()].push_back(Hit{offset, key->obj.gen, weight * n});
    }
  }
  for (const auto& [key, h] : hits) {
    if (ctx.going_down || !lock.keep(dpp, y)) {
      return STOPPED;
    }
    int r = analyse_object(dpp, bucket, span, key, h, knobs, y);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

int FFTDetector::analyse_object(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                                const Span& span, const rgw_obj_key& key,
                                const std::vector<Hit>& hits, const Knobs& knobs,
                                optional_yield y)
{
  // most instances are accessed in a window or two: drop them before
  // reading their state
  std::set<uint32_t> windows;
  for (const auto& h : hits) {
    windows.insert(h.offset);
  }
  if (windows.size() < span.min_cycles) {
    ++st.sparse;
    return 0;
  }

  ObjectEntry e;
  int r = ctx.state->get_state(dpp, bucket, key, &e, y);
  if (r == -ENOENT) {
    ++st.missing;
    return 0;
  }
  if (r < 0) {
    return r;
  }
  // the series of the current instance only, as the temperature scorer
  // credits it: older generations belong to an instance it replaced
  const uint64_t base = e.ite.base_gen;
  std::vector<double> series(span.length, 0.0);
  bool dropped = false;
  for (const auto& h : hits) {
    if (h.gen && base && h.gen < base) {
      dropped = true;
      continue;
    }
    series[h.offset] += h.weight;
  }
  if (dropped) {
    ++st.stale;
  }
  const size_t active = std::count_if(series.begin(), series.end(),
                                      [](double v) { return v > 0; });
  if (active < span.min_cycles) {
    ++st.sparse;
    return 0;
  }

  ++st.candidates;
  const Periodicity v = detect_period(series, span.min_cycles, knobs.min_fft_confidence);
  rgw_bucket_ite_state s;
  s.fft_periodic = v.periodic;
  s.fft_confidence = v.confidence;
  if (v.periodic) {
    const TimeScale ts = ctx.timescale();
    const uint32_t days_per_window = span.first.granularity == Granularity::Week ? 7 : 1;
    s.fft_period_days = v.period * days_per_window;
    // the next access is expected one period after the last
    const auto last = std::find_if(series.rbegin(), series.rend(),
                                   [](double w) { return w > 0; });
    const uint64_t last_offset = series.rend() - last - 1;
    const Window next{span.first.granularity, span.first.index + last_offset + v.period};
    s.fft_next_access = to_epoch_secs(next.start(ts));
  }

  const auto& old = e.ite;
  if (old.fft_periodic == s.fft_periodic && old.fft_period_days == s.fft_period_days &&
      old.fft_next_access == s.fft_next_access &&
      std::abs(old.fft_confidence - s.fft_confidence) < 1e-3) {
    ++st.unchanged;
    return 0;
  }
  r = ctx.state->set_state(dpp, bucket, key, s, rgw_bucket_ite_state::MASK_FFT, base, y);
  if (r == -ECANCELED) {
    ++st.stale; // replaced while we analysed it
    return 0;
  }
  if (r == -ENOENT) {
    ++st.missing;
    return 0;
  }
  if (r < 0) {
    return r;
  }
  if (v.periodic) {
    ++st.periodic;
    ldpp_dout(dpp, 20) << bucket->get_key() << "/" << key << " is periodic: every "
                       << s.fft_period_days << " days, next access at "
                       << s.fft_next_access << ", confidence " << v.confidence << dendl;
  } else {
    ++st.aperiodic;
  }
  return 0;
}

} // namespace rgw::ite
