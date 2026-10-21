// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_admin.h"

#include <algorithm>
#include <map>
#include <vector>

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "rgw_ite.h"
#include "rgw_ite_aggregator.h"
#include "rgw_ite_decision.h"
#include "rgw_ite_fft_detector.h"
#include "rgw_ite_mover.h"
#include "rgw_ite_temperature.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ite::admin {

namespace {

constexpr uint32_t PAGE = 1000;

std::string_view granularity_name(Granularity g)
{
  return g == Granularity::Week ? "week" : "day";
}

Knobs knobs_of(const Context& ctx, const std::optional<BucketConfig>& config)
{
  return config ? config->effective(ctx.defaults()) : ctx.defaults();
}

int store_config(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                 const BucketConfig& c, optional_yield y)
{
  rgw::sal::Attrs attrs;
  encode(c, attrs[std::string(ATTR_ITE)]);
  int r = bucket->merge_and_store_attrs(dpp, attrs, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to store the ITE config of " << bucket->get_key()
                      << ": " << cpp_strerror(-r) << dendl;
  }
  return r;
}

void dump_entry(const ObjectEntry& e, ceph::Formatter* f)
{
  f->dump_string("name", e.key.name);
  f->dump_string("instance", e.key.instance);
  f->dump_unsigned("gen", e.gen);
  f->dump_unsigned("instance_gen", e.instance().gen);
  f->dump_unsigned("size", e.size);
  encode_json("mtime", e.mtime, f);
  f->dump_string("storage_class", e.storage_class);
  f->dump_bool("exists", e.exists);
  f->dump_bool("current", e.current);
  f->dump_bool("delete_marker", e.delete_marker);
  encode_json("ite", e.ite, f);
}

struct TierCount {
  uint64_t objects = 0;
  uint64_t bytes = 0;

  void dump(ceph::Formatter* f) const {
    f->dump_unsigned("objects", objects);
    f->dump_unsigned("bytes", bytes);
  }
};

/* one cycle of a fresh worker, as the Manager runs it, and what it did */
template <typename Worker>
int run_worker(const DoutPrefixProvider* dpp, Context& ctx, std::string_view name,
               ceph::Formatter* f, optional_yield y)
{
  Worker w(ctx);
  int r = w.process(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: ITE worker " << name << " failed: "
                      << cpp_strerror(-r) << dendl;
  }
  f->open_object_section(name);
  f->dump_int("ret", r);
  encode_json("stats", w.stats(), f);
  f->close_section();
  return r;
}

} // anonymous namespace

int validate(const Knobs& k, std::string* err)
{
  if (!(k.half_life_days > 0)) {
    *err = "half_life_days must be positive";
  } else if (!(k.cool_threshold >= 0)) {
    *err = "cool_threshold must not be negative";
  } else if (!(k.warm_threshold > k.cool_threshold)) {
    *err = "warm_threshold must be above cool_threshold";
  } else if (!(k.min_fft_confidence >= 0 && k.min_fft_confidence <= 1)) {
    *err = "min_fft_confidence must be between 0 and 1";
  } else {
    return 0;
  }
  return -EINVAL;
}

int bucket_enable(const DoutPrefixProvider* dpp, Context& ctx, rgw::sal::Bucket* bucket,
                  const std::optional<BucketConfig>& knobs, ceph::real_time now,
                  ceph::Formatter* f, optional_yield y)
{
  const auto old = get_bucket_config(bucket->get_attrs());
  const bool was_enabled = old && old->enabled;
  BucketConfig c = knobs ? *knobs : old.value_or(BucketConfig{});
  c.enabled = true;
  std::string err;
  int r = validate(c.effective(ctx.defaults()), &err);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: invalid ITE knobs for " << bucket->get_key() << ": "
                      << err << dendl;
    return r;
  }

  // the directory first: a bucket enabled but missing from it would only
  // be found once its first access is aggregated
  BucketProgress p;
  r = ctx.agg->get_bucket(dpp, bucket->get_key(), &p, y);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  if (r == -ENOENT || !was_enabled) {
    if (r == -ENOENT) {
      p = BucketProgress();
      p.bucket = bucket->get_key();
    }
    p.enabled_at = now;
    r = ctx.agg->put_bucket(dpp, p, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to add " << bucket->get_key()
                        << " to the ITE bucket directory: " << cpp_strerror(-r) << dendl;
      return r;
    }
  }
  r = store_config(dpp, bucket, c, y);
  if (r < 0) {
    return r;
  }

  f->open_object_section("result");
  encode_json("bucket", bucket->get_key(), f);
  encode_json("config", c, f);
  encode_json("knobs", c.effective(ctx.defaults()), f);
  encode_json("directory", p, f);
  // allowed, so that the placement can be set up after, but until then
  // the bucket's objects keep the class they ask for and ITE leaves them be
  const std::string& placement = bucket->get_placement_rule().name;
  const rgw_placement_rule rule(
      placement, ctx.cct->_conf.get_val<std::string>("rgw_ite_storage_class"));
  if (ctx.driver && !placement.empty() && !ctx.driver->valid_placement(rule)) {
    f->dump_string("warning", "placement " + placement + " has no storage class " +
                              rule.storage_class + "; objects will not be tiered");
  }
  f->close_section();
  return 0;
}

int bucket_disable(const DoutPrefixProvider* dpp, Context& ctx, rgw::sal::Bucket* bucket,
                   ceph::Formatter* f, optional_yield y)
{
  // the knobs stay for when the bucket is enabled again
  BucketConfig c = get_bucket_config(bucket->get_attrs()).value_or(BucketConfig{});
  if (c.enabled) {
    c.enabled = false;
    int r = store_config(dpp, bucket, c, y);
    if (r < 0) {
      return r;
    }
  }
  f->open_object_section("result");
  encode_json("bucket", bucket->get_key(), f);
  encode_json("config", c, f);
  f->close_section();
  return 0;
}

int bucket_list(const DoutPrefixProvider* dpp, Context& ctx, const std::string& marker,
                uint32_t max, ceph::Formatter* f, optional_yield y)
{
  std::vector<BucketProgress> buckets;
  std::string next;
  bool truncated = false;
  int r = ctx.agg->list_buckets(dpp, marker, max ? max : PAGE, buckets, &next, &truncated, y);
  if (r < 0) {
    return r;
  }
  f->open_object_section("result");
  f->open_array_section("buckets");
  for (const auto& p : buckets) {
    encode_json("bucket", p, f);
  }
  f->close_section();
  f->dump_bool("truncated", truncated);
  if (truncated) {
    f->dump_string("next_marker", next);
  }
  f->close_section();
  return 0;
}

int bucket_stats(const DoutPrefixProvider* dpp, Context& ctx, rgw::sal::Bucket* bucket,
                 ceph::Formatter* f, optional_yield y)
{
  const auto config = get_bucket_config(bucket->get_attrs());
  const Knobs knobs = knobs_of(ctx, config);
  const std::string ite_class = ctx.cct->_conf.get_val<std::string>("rgw_ite_storage_class");
  BucketProgress p;
  int r = ctx.agg->get_bucket(dpp, bucket->get_key(), &p, y);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  const bool in_directory = (r == 0);

  // counted by the rules decide() skips by, so what is left is what ITE tiers
  uint64_t entries = 0, no_data = 0, other_class = 0, too_small = 0, periodic = 0;
  std::map<RGWITETierState, TierCount> tiers;
  std::map<RGWITEMovePending, uint64_t> pending;
  rgw_obj_key marker;
  bool truncated = true;
  while (truncated) {
    std::vector<ObjectEntry> page;
    rgw_obj_key next;
    r = ctx.state->list_state(dpp, bucket, marker, PAGE, page, &next, &truncated, y);
    if (r < 0) {
      return r;
    }
    for (const auto& e : page) {
      ++entries;
      if (!e.exists || e.delete_marker) {
        ++no_data;
      } else if (e.storage_class != ite_class) {
        ++other_class;
      } else if (e.size < knobs.min_tier_size) {
        ++too_small;
      } else {
        auto& t = tiers[e.ite.tier_state];
        ++t.objects;
        t.bytes += e.size;
        if (e.ite.move_pending != RGWITEMovePending::None) {
          ++pending[e.ite.move_pending];
        }
        if (e.ite.fft_periodic) {
          ++periodic;
        }
      }
    }
    if (page.empty() || next == marker) {
      break;
    }
    marker = std::move(next);
  }

  f->open_object_section("result");
  encode_json("bucket", bucket->get_key(), f);
  f->dump_bool("enabled", config && config->enabled);
  if (config) {
    encode_json("config", *config, f);
  }
  encode_json("knobs", knobs, f);
  if (in_directory) {
    encode_json("directory", p, f);
  }
  f->open_object_section("objects");
  f->dump_unsigned("entries", entries);
  f->dump_unsigned("no_data", no_data);
  f->dump_unsigned("other_class", other_class);
  f->dump_unsigned("too_small", too_small);
  for (auto s : {RGWITETierState::Hot, RGWITETierState::Pseudo, RGWITETierState::Committed}) {
    encode_json(std::string(to_string(s)).c_str(), tiers[s], f);
  }
  f->dump_unsigned("periodic", periodic);
  f->open_object_section("move_pending");
  for (auto m : {RGWITEMovePending::Commit, RGWITEMovePending::PreWarm,
                 RGWITEMovePending::Restore}) {
    f->dump_unsigned(std::string(to_string(m)).c_str(), pending[m]);
  }
  f->close_section();
  f->close_section();
  f->close_section();
  return 0;
}

int object_stat(const DoutPrefixProvider* dpp, Context& ctx, rgw::sal::Bucket* bucket,
                const rgw_obj_key& key, ceph::real_time now, ceph::Formatter* f,
                optional_yield y)
{
  ObjectEntry e;
  int r = ctx.state->get_state(dpp, bucket, key, &e, y);
  if (r < 0) {
    return r;
  }
  const auto config = get_bucket_config(bucket->get_attrs());
  const Knobs knobs = knobs_of(ctx, config);

  f->open_object_section("object");
  dump_entry(e, f);
  f->dump_float("temperature_now",
                temperature_at(e.ite, now, knobs.half_life_days, ctx.timescale()));
  Placement where;
  r = bucket->get_object(key)->ite_get_placement(dpp, &where, y);
  if (r == 0) {
    f->dump_string("placement", to_string(where));
  } else {
    f->dump_string("placement_error", cpp_strerror(-r));
  }
  if (config && config->enabled) {
    const Verdict v = decide(e, decision_inputs(ctx, knobs, now));
    f->open_object_section("decision");
    f->dump_string("decision", to_string(v.decision));
    f->dump_string("reason", v.reason);
    f->close_section();
  }
  f->close_section();
  return 0;
}

int counts_list(const DoutPrefixProvider* dpp, Context& ctx, const rgw_bucket& bucket,
                const std::string& object, uint32_t max_windows, ceph::real_time now,
                ceph::Formatter* f, optional_yield y)
{
  const TimeScale ts = ctx.timescale();
  // both granularities: windows written before a granularity change too
  std::vector<Window> windows;
  for (auto g : {Granularity::Day, Granularity::Week}) {
    std::vector<Window> found;
    int r = ctx.agg->list_windows(dpp, bucket, Window{g, 0},
                                  Window{g, Window::of(now, g, ts).index}, found, y);
    if (r < 0) {
      return r;
    }
    windows.insert(windows.end(), found.begin(), found.end());
  }
  std::sort(windows.begin(), windows.end(), [&ts](const Window& a, const Window& b) {
    return a.start(ts) > b.start(ts);
  });
  const uint32_t max = max_windows ? max_windows : PAGE;
  const bool truncated = windows.size() > max;
  if (truncated) {
    windows.resize(max);
  }

  f->open_object_section("result");
  encode_json("bucket", bucket, f);
  f->open_array_section("windows");
  for (const auto& w : windows) {
    // an object's counts in a window are all in its own shard
    std::map<std::string, uint64_t> counts;
    int r = object.empty()
        ? ctx.agg->read_counts(dpp, bucket, w, counts, y)
        : ctx.agg->read_shard_counts(dpp, bucket, w, agg_shard_of(object, ctx.agg->num_shards()),
                                     counts, y);
    if (r < 0) {
      return r;
    }
    f->open_object_section("window");
    f->dump_string("window", w.stamp(ts));
    f->dump_string("granularity", granularity_name(w.granularity));
    encode_json("start", w.start(ts), f);
    encode_json("end", w.end(ts), f);
    f->open_array_section("counts");
    for (const auto& [k, n] : counts) {
      if (is_marker_key(k)) {
        continue;
      }
      const auto ck = CountKey::parse(k);
      if (!ck) {
        ldpp_dout(dpp, 1) << "WARNING: skipping a malformed count key in " << bucket
                          << " window " << w.stamp(ts) << ": " << k << dendl;
        continue;
      }
      if (!object.empty() && ck->obj.name != object) {
        continue;
      }
      f->open_object_section("count");
      f->dump_string("name", ck->obj.name);
      f->dump_string("instance", ck->obj.instance);
      f->dump_unsigned("gen", ck->obj.gen);
      f->dump_string("op", to_string(ck->op));
      f->dump_string("caller", to_string(ck->caller));
      f->dump_unsigned("count", n);
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->dump_bool("truncated", truncated);
  f->close_section();
  return 0;
}

int log_status(const DoutPrefixProvider* dpp, Context& ctx, uint32_t max_per_shard,
               ceph::Formatter* f, optional_yield y)
{
  const uint32_t max = max_per_shard ? max_per_shard : PAGE;
  const uint32_t n = ctx.buffer->num_shards();
  uint64_t total = 0;
  bool any_truncated = false;
  f->open_object_section("result");
  f->dump_unsigned("num_shards", n);
  // only the shards with something waiting
  f->open_array_section("shards");
  for (uint32_t shard = 0; shard < n; ++shard) {
    uint64_t events = 0;
    std::optional<ceph::real_time> oldest;
    std::string cursor;
    bool truncated = true;
    while (truncated && events < max) {
      std::vector<AccessEventEntry> page;
      std::string next;
      const auto want = static_cast<uint32_t>(std::min<uint64_t>(PAGE, max - events));
      int r = ctx.buffer->list(dpp, shard, cursor, want, page, &next, &truncated, y);
      if (r < 0) {
        return r;
      }
      if (!oldest && !page.empty()) {
        oldest = page.front().event.timestamp;
      }
      events += page.size();
      if (next == cursor) {
        break;
      }
      cursor = std::move(next);
    }
    if (!events) {
      continue;
    }
    total += events;
    any_truncated = any_truncated || truncated;
    f->open_object_section("shard");
    f->dump_unsigned("shard", shard);
    f->dump_unsigned("events", events);
    f->dump_bool("truncated", truncated);
    encode_json("oldest", *oldest, f);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("events", total);
  f->dump_bool("truncated", any_truncated);
  f->close_section();
  return 0;
}

int move_list(const DoutPrefixProvider* dpp, Context& ctx, std::optional<uint32_t> shard,
              uint32_t max_per_shard, ceph::Formatter* f, optional_yield y)
{
  const uint32_t n = ctx.moves->num_shards();
  if (shard && *shard >= n) {
    ldpp_dout(dpp, 0) << "ERROR: the move queue has " << n << " shards" << dendl;
    return -EINVAL;
  }
  const uint32_t max = max_per_shard ? max_per_shard : PAGE;
  uint64_t total = 0;
  f->open_object_section("result");
  f->dump_unsigned("num_shards", n);
  f->open_array_section("shards");
  for (uint32_t i = shard.value_or(0); i < (shard ? *shard + 1 : n); ++i) {
    std::vector<MoveActionEntry> entries;
    std::string next;
    bool truncated = false;
    int r = ctx.moves->list(dpp, i, "", max, entries, &next, &truncated, y);
    if (r < 0) {
      return r;
    }
    if (entries.empty() && !shard) {
      continue;
    }
    total += entries.size();
    f->open_object_section("shard");
    f->dump_unsigned("shard", i);
    f->open_array_section("actions");
    for (const auto& e : entries) {
      f->open_object_section("action");
      f->dump_string("marker", e.marker);
      e.action.dump(f);
      f->close_section();
    }
    f->close_section();
    f->dump_bool("truncated", truncated);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("actions", total);
  f->close_section();
  return 0;
}

int process(const DoutPrefixProvider* dpp, Context& ctx, std::string_view worker,
            ceph::Formatter* f, optional_yield y)
{
  const bool all = (worker == "all");
  if (!all && std::find(std::begin(WORKERS), std::end(WORKERS), worker) == std::end(WORKERS)) {
    ldpp_dout(dpp, 0) << "ERROR: unknown ITE worker " << worker << dendl;
    return -EINVAL;
  }
  int ret = 0;
  auto run = [&](std::string_view name, auto cycle) {
    if (all || worker == name) {
      int r = cycle(name);
      if (r < 0) {
        ret = r;
      }
    }
  };
  f->open_object_section("result");
  run("agg", [&](std::string_view name) { return run_worker<Aggregator>(dpp, ctx, name, f, y); });
  run("temp", [&](std::string_view name) {
    return run_worker<TemperatureScorer>(dpp, ctx, name, f, y);
  });
  run("fft", [&](std::string_view name) { return run_worker<FFTDetector>(dpp, ctx, name, f, y); });
  run("decide", [&](std::string_view name) {
    return run_worker<DecisionEngine>(dpp, ctx, name, f, y);
  });
  run("move", [&](std::string_view name) { return run_worker<Mover>(dpp, ctx, name, f, y); });
  f->close_section();
  return ret;
}

} // namespace rgw::ite::admin
