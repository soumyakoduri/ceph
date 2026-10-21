// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_temperature.h"

#include <algorithm>
#include <cmath>
#include <map>

#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "rgw_ite.h"
#include "rgw_ite_aggregator.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ite {

double decay(double t, double elapsed_days, double half_life_days)
{
  if (t <= 0 || elapsed_days <= 0 || half_life_days <= 0) {
    return t;
  }
  return t * std::exp2(-elapsed_days / half_life_days);
}

double temperature_at(const rgw_bucket_ite_state& s, ceph::real_time at,
                      double half_life_days, const TimeScale& ts)
{
  if (s.temperature <= 0) {
    return 0;
  }
  const double days = ts.days_between(from_epoch_secs(s.temp_updated_at), at);
  return decay(s.temperature, days, half_life_days);
}

double fold(const rgw_bucket_ite_state& s, ceph::real_time window_end, double heat,
            double half_life_days, const TimeScale& ts)
{
  const double t = temperature_at(s, window_end, half_life_days, ts) + std::max(0.0, heat);
  return std::min(1.0, t);
}

void TemperatureStats::dump(ceph::Formatter* f) const
{
  f->dump_unsigned("buckets", buckets);
  f->dump_unsigned("buckets_busy", buckets_busy);
  f->dump_unsigned("buckets_skipped", buckets_skipped);
  f->dump_unsigned("windows", windows);
  f->dump_unsigned("objects", objects);
  f->dump_unsigned("already", already);
  f->dump_unsigned("stale", stale);
  f->dump_unsigned("missing", missing);
  f->dump_unsigned("unparsable", unparsable);
  f->dump_unsigned("swept", swept);
}

TemperatureScorer::TemperatureScorer(Context& ctx) : ctx(ctx) {}

int TemperatureScorer::process(const DoutPrefixProvider* dpp, optional_yield y)
{
  int r = for_each_bucket(dpp, ctx, [&](const BucketProgress& p) {
    return process_bucket(dpp, p.bucket, ceph::real_clock::now(), y);
  }, y);
  ldpp_dout(dpp, 5) << "scored " << st.buckets << " buckets (" << st.buckets_busy
                    << " busy, " << st.buckets_skipped << " skipped): "
                    << st.windows << " windows, " << st.objects << " objects, "
                    << st.swept << " swept" << dendl;
  return r;
}

int TemperatureScorer::process_bucket(const DoutPrefixProvider* dpp,
                                      const rgw_bucket& bucket,
                                      ceph::real_time now, optional_yield y)
{
  BucketRun run;
  int r = run_on_bucket(dpp, ctx, bucket,
      [&](rgw::sal::Bucket* b, const Knobs& knobs, BucketLock& lock) {
        return score(dpp, b, knobs, lock, now, y);
      }, &run, y);
  switch (run) {
  case BucketRun::Done: ++st.buckets; break;
  case BucketRun::Busy: ++st.buckets_busy; break;
  case BucketRun::Gone:
  case BucketRun::Disabled: ++st.buckets_skipped; break;
  case BucketRun::Failed: break;
  }
  return r == STOPPED ? 0 : r;
}

int TemperatureScorer::score(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                             const Knobs& knobs, BucketLock& lock,
                             ceph::real_time now, optional_yield y)
{
  const rgw_bucket& b = bucket->get_key();
  // reread under the lock: the progress another gateway saved is the truth
  BucketProgress p;
  int r = ctx.agg->get_bucket(dpp, b, &p, y);
  if (r < 0) {
    return r;
  }

  const TimeScale ts = ctx.timescale();
  const Granularity g = ctx.granularity();
  // a window is scored once the aggregator has surely folded all of it in
  const auto settled = now - 2 * ctx.period("rgw_ite_agg_period");
  const uint64_t open = Window::of(settled, g, ts).index;
  if (open == 0) {
    return 0;
  }
  const uint64_t last = open - 1;

  uint64_t first;
  if (p.last_scored && p.last_scored->granularity == g) {
    first = p.last_scored->index + 1;
  } else if (p.last_scored) {
    // the granularity changed: resume where the old windows ended
    first = Window::of(p.last_scored->end(ts), g, ts).index;
  } else {
    first = Window::of(p.enabled_at, g, ts).index;
  }
  // nothing older than retention can still exist
  first = std::max(first, oldest_accepted(Window::of(now, g, ts).index, ctx.retention(g)));

  if (first <= last) {
    std::vector<Window> windows;
    r = ctx.agg->list_windows(dpp, b, Window{g, first}, Window{g, last}, windows, y);
    if (r < 0) {
      return r;
    }
    for (const auto& w : windows) {
      r = fold_window(dpp, bucket, w, knobs, lock, y);
      if (r < 0) {
        return r; // progress stays at the last whole window
      }
      p.last_scored = w;
      r = ctx.agg->put_bucket(dpp, p, y);
      if (r < 0) {
        return r;
      }
      ++st.windows;
    }
  }
  // windows with no access are scored by having nothing to fold
  if (!p.last_scored || p.last_scored->granularity != g || p.last_scored->index < last) {
    p.last_scored = Window{g, last};
    r = ctx.agg->put_bucket(dpp, p, y);
    if (r < 0) {
      return r;
    }
  }

  if (now - p.last_sweep >= ctx.period("rgw_ite_temperature_sweep_period")) {
    // decay to the end of the last scored window, never past it, so a
    // window scored later is still folded in
    r = sweep(dpp, bucket, p.last_scored->end(ts), knobs, lock, y);
    if (r < 0) {
      return r;
    }
    p.last_sweep = now;
    r = ctx.agg->put_bucket(dpp, p, y);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

int TemperatureScorer::fold_window(const DoutPrefixProvider* dpp,
                                   rgw::sal::Bucket* bucket, const Window& w,
                                   const Knobs& knobs, BucketLock& lock,
                                   optional_yield y)
{
  const rgw_bucket& b = bucket->get_key();
  // an object's counts are all in one shard, so a shard is a unit of work
  for (uint32_t shard = 0; shard < ctx.agg->num_shards(); ++shard) {
    if (ctx.going_down || !lock.keep(dpp, y)) {
      return STOPPED;
    }
    std::map<std::string, uint64_t> counts;
    int r = ctx.agg->read_shard_counts(dpp, b, w, shard, counts, y);
    if (r < 0) {
      return r;
    }
    // weighted heat per object, by the generation each access was served
    std::map<rgw_obj_key, std::vector<std::pair<uint64_t, double>>> heat;
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
      heat[key->obj.key()].emplace_back(key->obj.gen, weight * n);
    }
    for (const auto& [key, by_gen] : heat) {
      if (ctx.going_down) {
        return STOPPED;
      }
      r = fold_object(dpp, bucket, key, by_gen, w, knobs, y);
      if (r < 0) {
        return r;
      }
    }
  }
  return 0;
}

int TemperatureScorer::fold_object(const DoutPrefixProvider* dpp,
                                   rgw::sal::Bucket* bucket, const rgw_obj_key& key,
                                   const std::vector<std::pair<uint64_t, double>>& heat_by_gen,
                                   const Window& w, const Knobs& knobs,
                                   optional_yield y)
{
  ObjectEntry e;
  int r = ctx.state->get_state(dpp, bucket, key, &e, y);
  if (r == -ENOENT) {
    ++st.missing;
    return 0;
  }
  if (r < 0) {
    return r;
  }

  // accesses served from an older generation belong to the instance this
  // one replaced; generation 0 is a write, credited to the current one
  const uint64_t base = e.ite.base_gen;
  double heat = 0;
  for (const auto& [gen, h] : heat_by_gen) {
    if (gen && base && gen < base) {
      ++st.stale;
      continue;
    }
    heat += h;
  }
  if (heat <= 0) {
    return 0;
  }

  const TimeScale ts = ctx.timescale();
  const ceph::real_time end = w.end(ts);
  if (e.ite.temp_updated_at >= to_epoch_secs(end)) {
    ++st.already;
    return 0;
  }
  rgw_bucket_ite_state s;
  s.temperature = fold(e.ite, end, heat, knobs.half_life_days, ts);
  s.temp_updated_at = to_epoch_secs(end);
  r = ctx.state->set_state(dpp, bucket, key, s,
                           rgw_bucket_ite_state::MASK_TEMPERATURE, base, y);
  if (r == -ECANCELED) {
    ++st.stale; // replaced while we scored it
    return 0;
  }
  if (r == -ENOENT) {
    ++st.missing;
    return 0;
  }
  if (r < 0) {
    return r;
  }
  ++st.objects;
  ldpp_dout(dpp, 20) << bucket->get_key() << "/" << key << " temperature "
                     << s.temperature << " at window " << w.stamp(ts) << dendl;
  return 0;
}

int TemperatureScorer::sweep(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                             ceph::real_time to, const Knobs& knobs,
                             BucketLock& lock, optional_yield y)
{
  const TimeScale ts = ctx.timescale();
  const uint64_t at = to_epoch_secs(to);
  rgw_obj_key marker;
  bool truncated = true;
  while (truncated) {
    if (ctx.going_down || !lock.keep(dpp, y)) {
      return STOPPED;
    }
    std::vector<ObjectEntry> entries;
    rgw_obj_key next;
    int r = ctx.state->list_state(dpp, bucket, marker, 1000, entries, &next,
                                  &truncated, y);
    if (r < 0) {
      return r;
    }
    for (const auto& e : entries) {
      if (e.ite.temperature <= 0 || e.ite.temp_updated_at >= at) {
        continue;
      }
      rgw_bucket_ite_state s;
      s.temperature = temperature_at(e.ite, to, knobs.half_life_days, ts);
      if (s.temperature < TEMPERATURE_FLOOR) {
        s.temperature = 0;
      }
      s.temp_updated_at = at;
      r = ctx.state->set_state(dpp, bucket, e.key, s,
                               rgw_bucket_ite_state::MASK_TEMPERATURE,
                               e.ite.base_gen, y);
      if (r == -ECANCELED || r == -ENOENT) {
        continue;
      }
      if (r < 0) {
        return r;
      }
      ++st.swept;
    }
    if (entries.empty()) {
      break;
    }
    marker = next;
  }
  return 0;
}

} // namespace rgw::ite
