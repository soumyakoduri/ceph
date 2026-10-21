// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_aggregator.h"

#include <algorithm>

#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "rgw_ite.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ite {

uint64_t retention_windows(uint64_t fft_span, uint64_t margin_days, Granularity g)
{
  const uint64_t days_per_window = (g == Granularity::Week ? 7 : 1);
  const uint64_t margin = (margin_days + days_per_window - 1) / days_per_window;
  return std::max<uint64_t>(1, fft_span + margin);
}

uint64_t oldest_accepted(uint64_t current, uint64_t retention)
{
  return current >= retention ? current - retention + 1 : 0;
}

bool is_expired(uint64_t window, uint64_t current, uint64_t retention)
{
  return window + retention < current;
}

void AggregatorStats::dump(ceph::Formatter* f) const
{
  f->dump_unsigned("shards", shards);
  f->dump_unsigned("shards_busy", shards_busy);
  f->dump_unsigned("events", events);
  f->dump_unsigned("counted", counted);
  f->dump_unsigned("replayed", replayed);
  f->dump_unsigned("not_user", not_user);
  f->dump_unsigned("too_old", too_old);
  f->dump_unsigned("buckets_registered", buckets_registered);
  f->dump_unsigned("windows_expired", windows_expired);
}

Aggregator::Aggregator(Context& ctx) : ctx(ctx) {}

int Aggregator::process(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = 0;
  for (uint32_t shard = 0; shard < ctx.buffer->num_shards(); ++shard) {
    if (ctx.going_down) {
      return 0;
    }
    int r = process_shard(dpp, shard, y);
    if (r < 0) {
      // the shard is retried next cycle; the others need not wait for it
      ldpp_dout(dpp, 1) << "ERROR: failed to aggregate buffer shard " << shard
                        << ": " << cpp_strerror(-r) << dendl;
      ret = r;
    }
  }
  int r = expire(dpp, ceph::real_clock::now(), y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to expire aggregated windows: "
                      << cpp_strerror(-r) << dendl;
    ret = r;
  }
  ldpp_dout(dpp, 5) << "aggregated " << st.counted << " of " << st.events
                    << " events (" << st.replayed << " replayed, "
                    << st.not_user << " not user, " << st.too_old << " too old) from "
                    << st.shards << " shards, " << st.shards_busy << " busy; "
                    << st.buckets_registered << " buckets registered, "
                    << st.windows_expired << " windows expired" << dendl;
  return ret;
}

int Aggregator::process_shard(const DoutPrefixProvider* dpp, uint32_t shard,
                              optional_yield y)
{
  const auto lock_time = ctx.lock_time();
  auto lock = ctx.buffer->get_serializer(shard, ctx.cookie);
  int r = lock->try_lock(dpp, lock_time, y);
  if (r == -EBUSY || r == -EEXIST) {
    ldpp_dout(dpp, 20) << "buffer shard " << shard
                       << " is being aggregated elsewhere" << dendl;
    ++st.shards_busy;
    return 0;
  }
  if (r < 0) {
    return r;
  }
  // stop well before the lock can expire under us
  const auto deadline = ceph::coarse_mono_clock::now() + lock_time / 2;
  r = consume_shard(dpp, shard, deadline, y);
  ++st.shards;
  int ur = lock->unlock(dpp, y);
  if (ur < 0 && ur != -ENOENT) {
    ldpp_dout(dpp, 1) << "WARNING: failed to unlock buffer shard " << shard
                      << ": " << cpp_strerror(-ur) << dendl;
  }
  return r;
}

void Aggregator::group(const std::vector<AccessEventEntry>& entries, Granularity g,
                       const TimeScale& ts, uint64_t oldest, Batch& batch)
{
  const uint32_t agg_shards = ctx.agg->num_shards();
  for (const auto& e : entries) {
    ++st.events;
    const AccessEvent& ev = e.event;
    if (ev.caller != CallerType::User) {
      ++st.not_user;
      continue;
    }
    const Window w = Window::of(ev.timestamp, g, ts);
    if (w.index < oldest) {
      ++st.too_old;
      continue;
    }
    const std::string bucket_key = ev.bucket.get_key();
    const uint32_t agg_shard = agg_shard_of(ev.obj.name, agg_shards);
    auto& grp = batch.groups[GroupKey{bucket_key, w, agg_shard}];
    if (grp.events.empty()) {
      grp.bucket = ev.bucket;
      grp.window = w;
      grp.agg_shard = agg_shard;
    }
    CountKey key;
    key.obj = ev.obj;
    key.op = ev.op;
    key.caller = ev.caller;
    grp.events.emplace_back(e.marker, key.to_str());

    auto [b, inserted] = batch.buckets.try_emplace(bucket_key, ev.bucket, ev.timestamp);
    if (!inserted && ev.timestamp < b->second.second) {
      b->second.second = ev.timestamp;
    }
  }
}

int Aggregator::consume_shard(const DoutPrefixProvider* dpp, uint32_t shard,
                              ceph::coarse_mono_time deadline, optional_yield y)
{
  const TimeScale ts = ctx.timescale();
  const Granularity g = ctx.granularity();
  const uint64_t current = Window::of(ceph::real_clock::now(), g, ts).index;
  const uint64_t oldest = oldest_accepted(current, ctx.retention(g));

  MarkerCache markers;
  std::string cursor;
  bool truncated = true;
  while (truncated) {
    if (ctx.going_down) {
      break;
    }
    if (ceph::coarse_mono_clock::now() >= deadline) {
      ldpp_dout(dpp, 5) << "buffer shard " << shard
                        << " has more events; resuming next cycle" << dendl;
      break;
    }
    std::vector<AccessEventEntry> entries;
    std::string next;
    int r = ctx.buffer->list(dpp, shard, cursor, page_size, entries, &next,
                             &truncated, y);
    if (r < 0) {
      return r;
    }
    if (next.empty() || next == cursor) {
      break; // nothing after the cursor
    }
    Batch batch;
    group(entries, g, ts, oldest, batch);
    r = apply(dpp, shard, next, batch, markers, y);
    if (r < 0) {
      // nothing of this page is trimmed; the stored markers make the
      // next attempt skip whatever did land
      return r;
    }
    r = ctx.buffer->trim(dpp, shard, next, y);
    if (r < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to trim buffer shard " << shard
                        << " to " << next << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    cursor = std::move(next);
  }
  return 0;
}

int Aggregator::apply(const DoutPrefixProvider* dpp, uint32_t shard,
                      const std::string& new_marker, Batch& batch,
                      MarkerCache& markers, optional_yield y)
{
  // a bucket enters the directory before its first count, so a bucket
  // with counts is always one the workers visit
  for (const auto& [key, b] : batch.buckets) {
    int r = ensure_registered(dpp, b.first, b.second, y);
    if (r < 0) {
      return r;
    }
  }
  for (const auto& [key, grp] : batch.groups) {
    int r = apply_group(dpp, shard, new_marker, key, grp, markers, y);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

int Aggregator::apply_group(const DoutPrefixProvider* dpp, uint32_t shard,
                            const std::string& new_marker, const GroupKey& key,
                            const Group& grp, MarkerCache& markers, optional_yield y)
{
  for (int attempt = 1; ; ++attempt) {
    auto m = markers.find(key);
    if (m == markers.end()) {
      std::string stored;
      int r = ctx.agg->get_marker(dpp, grp.bucket, grp.window, grp.agg_shard,
                                  shard, &stored, y);
      if (r < 0) {
        return r;
      }
      m = markers.emplace(key, std::move(stored)).first;
    }

    // only events after the stored marker are new to this window shard
    std::map<std::string, uint64_t> deltas;
    uint64_t fresh = 0;
    for (const auto& [cursor, count_key] : grp.events) {
      if (cursor > m->second) {
        ++deltas[count_key];
        ++fresh;
      }
    }
    const uint64_t seen = grp.events.size() - fresh;
    if (deltas.empty()) {
      st.replayed += seen;
      return 0;
    }

    int r = ctx.agg->add_counts(dpp, grp.bucket, grp.window, grp.agg_shard, shard,
                                m->second, new_marker, deltas, y);
    if (r == -ECANCELED && attempt < max_attempts) {
      // another gateway moved the marker, as when our lock expired and it
      // took the shard over: reread and count only what is still new
      ldpp_dout(dpp, 10) << "marker of buffer shard " << shard << " moved in "
                         << grp.bucket << " window " << grp.window.index
                         << " shard " << grp.agg_shard << "; retrying" << dendl;
      markers.erase(m);
      continue;
    }
    if (r < 0) {
      return r;
    }
    st.counted += fresh;
    st.replayed += seen;
    m->second = new_marker;
    return 0;
  }
}

int Aggregator::ensure_registered(const DoutPrefixProvider* dpp,
                                  const rgw_bucket& bucket,
                                  ceph::real_time first_seen, optional_yield y)
{
  const std::string key = bucket.get_key();
  if (registered.count(key)) {
    return 0;
  }
  BucketProgress progress;
  int r = ctx.agg->get_bucket(dpp, bucket, &progress, y);
  if (r == -ENOENT) {
    // a bucket enabled in another zone starts recording here when its
    // attribute syncs, so the directory fills itself
    progress = BucketProgress();
    progress.bucket = bucket;
    progress.enabled_at = first_seen;
    r = ctx.agg->put_bucket(dpp, progress, y);
    if (r == 0) {
      ++st.buckets_registered;
      ldpp_dout(dpp, 10) << "registered ITE bucket " << bucket << dendl;
    }
  }
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to register ITE bucket " << bucket
                      << ": " << cpp_strerror(-r) << dendl;
    return r;
  }
  registered.insert(key);
  return 0;
}

int Aggregator::expire(const DoutPrefixProvider* dpp, ceph::real_time now,
                       optional_yield y)
{
  return for_each_bucket(dpp, ctx, [&](const BucketProgress& p) {
    return expire_bucket(dpp, p.bucket, now, y);
  }, y);
}

int Aggregator::expire_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                              ceph::real_time now, optional_yield y)
{
  const TimeScale ts = ctx.timescale();
  // both granularities: windows written before a granularity change age out too
  for (auto g : {Granularity::Day, Granularity::Week}) {
    const uint64_t keep = ctx.retention(g);
    const uint64_t current = Window::of(now, g, ts).index;
    if (current <= keep) {
      continue;
    }
    std::vector<Window> windows;
    int r = ctx.agg->list_windows(dpp, bucket, Window{g, 0},
                                  Window{g, current - keep - 1}, windows, y);
    if (r < 0) {
      return r;
    }
    for (const auto& w : windows) {
      if (!is_expired(w.index, current, keep)) {
        continue;
      }
      r = ctx.agg->remove_window(dpp, bucket, w, y);
      if (r < 0) {
        return r;
      }
      ++st.windows_expired;
      ldpp_dout(dpp, 20) << "expired " << bucket << " window "
                         << w.stamp(ts) << dendl;
    }
  }
  return 0;
}

} // namespace rgw::ite
