// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_decision.h"

#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "rgw_ite.h"
#include "rgw_ite_temperature.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ite {

std::string_view to_string(Decision d)
{
  switch (d) {
  case Decision::Skip: return "skip";
  case Decision::Hold: return "hold";
  case Decision::Pseudo: return "pseudo";
  case Decision::Hot: return "hot";
  case Decision::Commit: return "commit";
  case Decision::PreWarm: return "prewarm";
  case Decision::Restore: return "restore";
  }
  return "unknown";
}

bool access_predicted(const ObjectEntry& e, const DecisionInputs& in)
{
  const auto& s = e.ite;
  if (!in.knobs.prewarm || !s.fft_periodic || !s.fft_next_access ||
      s.fft_confidence < in.knobs.min_fft_confidence) {
    return false;
  }
  // a prediction in the past is a missed beat, not an access to come
  const double ahead = in.ts.days_between(in.now, from_epoch_secs(s.fft_next_access));
  return ahead >= 0 && ahead <= in.knobs.prewarm_lead_days;
}

DecisionInputs decision_inputs(const Context& ctx, const Knobs& knobs, ceph::real_time now)
{
  const auto& conf = ctx.cct->_conf;
  DecisionInputs in;
  in.knobs = knobs;
  in.now = now;
  in.ts = ctx.timescale();
  in.storage_class = conf.get_val<std::string>("rgw_ite_storage_class");
  in.demotion_enabled = conf.get_val<bool>("rgw_ite_demotion_enabled");
  in.max_age_days = conf.get_val<int64_t>("rgw_ite_max_age");
  return in;
}

Verdict decide(const ObjectEntry& e, const DecisionInputs& in)
{
  if (!e.exists || e.delete_marker) {
    return {Decision::Skip, "no data"};
  }
  if (e.storage_class != in.storage_class) {
    return {Decision::Skip, "not intelligent-tiered"};
  }
  if (e.size < in.knobs.min_tier_size) {
    return {Decision::Skip, "too small"};
  }
  if (e.ite.move_pending != RGWITEMovePending::None) {
    return {Decision::Hold, "move pending"};
  }

  const Knobs& k = in.knobs;
  const double temp = temperature_at(e.ite, in.now, k.half_life_days, in.ts);
  const double age = in.ts.days_between(e.mtime, in.now);
  const bool young = age < k.cold_after_days;
  // the backstop demotes past max-age whatever the scorer says
  const bool too_old = in.max_age_days && age >= in.max_age_days;
  const bool predicted = access_predicted(e, in);

  switch (e.ite.tier_state) {
  case RGWITETierState::Committed:
    // promotion reuses the warm threshold; one stray read does not reach it
    if (temp >= k.warm_threshold) {
      return {Decision::Restore, "hot again"};
    }
    if (predicted) {
      return {Decision::PreWarm, "access predicted"};
    }
    return {Decision::Hold, "cold"};

  case RGWITETierState::Pseudo:
    if (!in.demotion_enabled) {
      return {Decision::Hot, "demotion disabled"};
    }
    if (!too_old) {
      if (young) {
        return {Decision::Hot, "younger than cold-after"};
      }
      if (temp >= k.cool_threshold) {
        return {Decision::Hot, "accessed again"};
      }
      if (predicted) {
        return {Decision::Hold, "access predicted"};
      }
    }
    if (in.ts.days_between(from_epoch_secs(e.ite.pseudo_since), in.now) >= k.grace_days()) {
      return {Decision::Commit, too_old ? "past max-age" : "cold past grace"};
    }
    return {Decision::Hold, "in grace"};

  case RGWITETierState::Hot:
    break;
  }

  if (!in.demotion_enabled) {
    return {Decision::Hold, "demotion disabled"};
  }
  if (too_old) {
    return {Decision::Pseudo, "past max-age"};
  }
  // cold-after is a residency floor, not the trigger
  if (young) {
    return {Decision::Hold, "younger than cold-after"};
  }
  if (temp >= k.warm_threshold) {
    return {Decision::Hold, "hot"};
  }
  if (predicted) {
    return {Decision::Hold, "access predicted"};
  }
  if (temp >= k.cool_threshold) {
    return {Decision::Hold, "lukewarm"};
  }
  return {Decision::Pseudo, "cold"};
}

void DecisionStats::dump(ceph::Formatter* f) const
{
  f->dump_unsigned("buckets", buckets);
  f->dump_unsigned("buckets_busy", buckets_busy);
  f->dump_unsigned("buckets_skipped", buckets_skipped);
  f->dump_unsigned("buckets_not_due", buckets_not_due);
  f->dump_unsigned("objects", objects);
  f->dump_unsigned("skip", skip);
  f->dump_unsigned("hold", hold);
  f->dump_unsigned("pseudo", pseudo);
  f->dump_unsigned("hot", hot);
  f->dump_unsigned("commit", commit);
  f->dump_unsigned("prewarm", prewarm);
  f->dump_unsigned("restore", restore);
  f->dump_unsigned("stale", stale);
  f->dump_unsigned("missing", missing);
}

DecisionEngine::DecisionEngine(Context& ctx) : ctx(ctx) {}

int DecisionEngine::process(const DoutPrefixProvider* dpp, optional_yield y)
{
  int r = for_each_bucket(dpp, ctx, [&](const BucketProgress& p) {
    return process_bucket(dpp, p.bucket, ceph::real_clock::now(), y);
  }, y);
  ldpp_dout(dpp, 5) << "decided " << st.buckets << " buckets (" << st.buckets_busy
                    << " busy, " << st.buckets_not_due << " not due, "
                    << st.buckets_skipped << " skipped): " << st.objects
                    << " objects, " << st.pseudo << " marked, " << st.hot
                    << " back to hot, " << st.commit << " commits, " << st.prewarm
                    << " pre-warms, " << st.restore << " restores queued" << dendl;
  return r;
}

int DecisionEngine::process_bucket(const DoutPrefixProvider* dpp,
                                   const rgw_bucket& bucket,
                                   ceph::real_time now, optional_yield y)
{
  BucketRun run;
  int r = run_on_bucket(dpp, ctx, bucket,
      [&](rgw::sal::Bucket* b, const Knobs& knobs, BucketLock& lock) {
        return decide_bucket(dpp, b, knobs, lock, now, y);
      }, &run, y);
  switch (run) {
  case BucketRun::Done: break; // counted by decide_bucket()
  case BucketRun::Busy: ++st.buckets_busy; break;
  case BucketRun::Gone:
  case BucketRun::Disabled: ++st.buckets_skipped; break;
  case BucketRun::Failed: break;
  }
  return r == STOPPED ? 0 : r;
}

int DecisionEngine::decide_bucket(const DoutPrefixProvider* dpp,
                                  rgw::sal::Bucket* bucket, const Knobs& knobs,
                                  BucketLock& lock, ceph::real_time now,
                                  optional_yield y)
{
  const rgw_bucket& b = bucket->get_key();
  BucketProgress p;
  int r = ctx.agg->get_bucket(dpp, b, &p, y);
  if (r < 0) {
    return r;
  }
  if (now - p.last_decision < ctx.period("rgw_ite_decision_period")) {
    ++st.buckets_not_due;
    return 0;
  }
  ++st.buckets;

  const DecisionInputs in = decision_inputs(ctx, knobs, now);

  rgw_obj_key marker;
  bool truncated = true;
  while (truncated) {
    std::vector<ObjectEntry> entries;
    rgw_obj_key next;
    r = ctx.state->list_state(dpp, bucket, marker, 1000, entries, &next, &truncated, y);
    if (r < 0) {
      return r;
    }
    for (const auto& e : entries) {
      if (ctx.going_down || !lock.keep(dpp, y)) {
        // whatever was applied stands; the rest is decided next time
        return STOPPED;
      }
      ++st.objects;
      const Verdict v = decide(e, in);
      r = apply(dpp, bucket, e, v, now, y);
      if (r < 0) {
        return r;
      }
    }
    if (entries.empty()) {
      break;
    }
    marker = next;
  }
  p.last_decision = now;
  return ctx.agg->put_bucket(dpp, p, y);
}

int DecisionEngine::apply(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                          const ObjectEntry& e, const Verdict& v, ceph::real_time now,
                          optional_yield y)
{
  rgw_bucket_ite_state s;
  switch (v.decision) {
  case Decision::Skip:
    ++st.skip;
    return 0;
  case Decision::Hold:
    ++st.hold;
    return 0;
  case Decision::Pseudo:
    s.tier_state = RGWITETierState::Pseudo;
    s.pseudo_since = to_epoch_secs(now);
    break;
  case Decision::Hot:
    s.tier_state = RGWITETierState::Hot;
    s.pseudo_since = 0;
    break;
  case Decision::Commit:
    return queue(dpp, bucket, e, MoveKind::Commit, now, y);
  case Decision::PreWarm:
    return queue(dpp, bucket, e, MoveKind::PreWarm, now, y);
  case Decision::Restore:
    return queue(dpp, bucket, e, MoveKind::Restore, now, y);
  }

  int r = ctx.state->set_state(dpp, bucket, e.key, s, rgw_bucket_ite_state::MASK_TIER,
                               e.instance().gen, y);
  if (r == -ECANCELED) {
    ++st.stale;
    return 0;
  }
  if (r == -ENOENT) {
    ++st.missing;
    return 0;
  }
  if (r < 0) {
    return r;
  }
  if (v.decision == Decision::Pseudo) {
    ++st.pseudo;
  } else {
    ++st.hot;
  }
  ldpp_dout(dpp, 20) << bucket->get_key() << "/" << e.key << ": "
                     << to_string(v.decision) << " (" << v.reason << ")" << dendl;
  return 0;
}

int DecisionEngine::queue(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                          const ObjectEntry& e, MoveKind kind, ceph::real_time now,
                          optional_yield y)
{
  MoveAction a;
  a.bucket = bucket->get_key();
  a.obj = e.instance();
  a.kind = kind;
  a.target = target_of(kind);
  a.zone_id = ctx.zone_id;
  a.queued_at = now;
  // queue first: a marker without its action would hold the instance
  // forever, while an action queued twice is harmless to the mover
  int r = ctx.moves->append(dpp, a, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to queue " << to_string(kind) << " of "
                      << a.bucket << "/" << e.key << ": " << cpp_strerror(-r) << dendl;
    return r;
  }

  rgw_bucket_ite_state s;
  s.move_pending = move_pending_of(kind);
  r = ctx.state->set_state(dpp, bucket, e.key, s, rgw_bucket_ite_state::MASK_MOVE,
                           a.obj.gen, y);
  if (r == -ECANCELED) {
    ++st.stale; // the action names a replaced instance; the mover drops it
    return 0;
  }
  if (r == -ENOENT) {
    ++st.missing;
    return 0;
  }
  if (r < 0) {
    return r;
  }
  switch (kind) {
  case MoveKind::Commit: ++st.commit; break;
  case MoveKind::PreWarm: ++st.prewarm; break;
  case MoveKind::Restore: ++st.restore; break;
  }
  ldpp_dout(dpp, 20) << a.bucket << "/" << e.key << ": queued " << to_string(kind) << dendl;
  return 0;
}

} // namespace rgw::ite
