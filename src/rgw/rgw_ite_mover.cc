// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_mover.h"

#include <algorithm>

#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "rgw_ite.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ite {

void MoverStats::dump(ceph::Formatter* f) const
{
  f->dump_unsigned("shards", shards);
  f->dump_unsigned("shards_busy", shards_busy);
  f->dump_unsigned("actions", actions);
  f->dump_unsigned("started", started);
  f->dump_unsigned("commits", commits);
  f->dump_unsigned("prewarms", prewarms);
  f->dump_unsigned("restores", restores);
  f->dump_unsigned("reconciled", reconciled);
  f->dump_unsigned("gone", gone);
  f->dump_unsigned("superseded", superseded);
  f->dump_unsigned("requeued", requeued);
  f->dump_unsigned("failed", failed);
}

Mover::Mover(Context& ctx)
  : ctx(ctx),
    cap(ctx.cct->_conf.get_val<int64_t>("rgw_ite_move_batch_cap"))
{}

int Mover::process(const DoutPrefixProvider* dpp, optional_yield y, uint32_t first_shard)
{
  const uint32_t n = ctx.moves->num_shards();
  int ret = 0;
  for (uint32_t i = 0; i < n && st.started < cap && !ctx.going_down; ++i) {
    int r = process_shard(dpp, (first_shard + i) % n, ceph::real_clock::now(), y);
    if (r < 0) {
      ret = r; // the other shards still drain
    }
  }
  ldpp_dout(dpp, 5) << "drained " << st.shards << " move queue shards (" << st.shards_busy
                    << " busy): " << st.actions << " actions, " << st.started
                    << " moves started, " << st.commits << " commits, " << st.prewarms
                    << " pre-warms, " << st.restores << " restores (" << st.reconciled
                    << " reconciled), " << st.gone << " gone, " << st.superseded
                    << " superseded, " << st.requeued << " requeued, " << st.failed
                    << " failed" << dendl;
  return ret;
}

int Mover::process_shard(const DoutPrefixProvider* dpp, uint32_t shard,
                         ceph::real_time now, optional_yield y)
{
  // like the aggregator, one gateway per shard; every action of an
  // instance lands on the same shard, so no instance moves twice at once
  BucketLock lock(ctx.moves->get_serializer(shard, ctx.cookie), ctx.lock_time());
  int r = lock.lock(dpp, y);
  if (r == -EBUSY) {
    ++st.shards_busy;
    return 0;
  }
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to lock move queue shard " << shard << ": "
                      << cpp_strerror(-r) << dendl;
    return r;
  }
  ++st.shards;
  r = drain(dpp, shard, lock, now, y);
  lock.unlock(dpp, y);
  return r == STOPPED ? 0 : r;
}

int Mover::drain(const DoutPrefixProvider* dpp, uint32_t shard, BucketLock& lock,
                 ceph::real_time now, optional_yield y)
{
  // one page a cycle, no longer than the moves left in the batch: what the
  // cycle puts back lands after it, for a later cycle
  const auto max = static_cast<uint32_t>(
      std::max<uint64_t>(1, cap - std::min(cap, st.started)));
  std::vector<MoveActionEntry> entries;
  std::string next;
  bool truncated = false;
  int r = ctx.moves->list(dpp, shard, "", max, entries, &next, &truncated, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to list move queue shard " << shard << ": "
                      << cpp_strerror(-r) << dendl;
    return r;
  }

  std::string done; // the cursor of the last action handled
  for (const auto& e : entries) {
    if (ctx.going_down || !lock.keep(dpp, y)) {
      r = STOPPED;
      break;
    }
    if (st.started >= cap) {
      break;
    }
    ++st.actions;
    r = handle(dpp, e.action, now, y);
    if (r < 0) {
      break; // stays on the queue, for the next cycle
    }
    done = e.marker;
  }
  if (!done.empty()) {
    // every action up to here is done with, or back on the queue
    int ar = ctx.moves->ack(dpp, shard, done, y);
    if (ar < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to trim move queue shard " << shard << ": "
                        << cpp_strerror(-ar) << dendl;
      return ar;
    }
  }
  return r;
}

int Mover::handle(const DoutPrefixProvider* dpp, const MoveAction& a,
                  ceph::real_time now, optional_yield y)
{
  int err = 0;
  switch (perform(dpp, a, now, &err, y)) {
  case Outcome::Done:
    return 0;
  case Outcome::Gone:
    ++st.gone;
    return 0;
  case Outcome::Superseded:
    ++st.superseded;
    return 0;
  case Outcome::NotYet:
    return requeue(dpp, a, false, y);
  case Outcome::Failed:
    break;
  }
  // no cold placement, or an action that makes no sense, will not mend
  const bool permanent = (err == -EOPNOTSUPP || err == -EINVAL);
  if (!permanent && a.attempts + 1 < MOVE_MAX_ATTEMPTS) {
    ldpp_dout(dpp, 5) << to_string(a.kind) << " of " << a.bucket << "/" << a.obj.name
                      << " failed, will retry: " << cpp_strerror(-err) << dendl;
    return requeue(dpp, a, true, y);
  }
  give_up(dpp, a, err, y);
  return 0;
}

Mover::Outcome Mover::perform(const DoutPrefixProvider* dpp, const MoveAction& a,
                              ceph::real_time now, int* err, optional_yield y)
{
  if (a.zone_id != ctx.zone_id) {
    return Outcome::Superseded; // another zone's placement is not ours to change
  }
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = ctx.load_bucket(dpp, a.bucket, &bucket, y);
  if (r == -ENOENT) {
    return Outcome::Gone;
  }
  if (r < 0) {
    *err = r;
    return Outcome::Failed;
  }

  // reconcile against the state first: the queue may carry an action
  // twice, or one its instance has outlived
  rgw_obj_key key(a.obj.name, a.obj.instance);
  ObjectEntry e;
  r = ctx.state->get_state(dpp, bucket.get(), key, &e, y);
  if (r == -ENOENT) {
    return Outcome::Gone;
  }
  if (r < 0) {
    *err = r;
    return Outcome::Failed;
  }
  if (!e.exists || e.delete_marker || e.instance().gen != a.obj.gen) {
    return Outcome::Gone; // its replacement starts out unmarked
  }
  if (e.ite.move_pending != move_pending_of(a.kind)) {
    // the engine marks an instance just after queueing its move, so a
    // fresh action may be seen before its mark
    if (e.ite.move_pending == RGWITEMovePending::None &&
        now - a.queued_at < ctx.lock_time()) {
      return Outcome::NotYet;
    }
    return Outcome::Superseded;
  }
  if (!ctx.bucket_knobs(bucket.get())) {
    // ITE was disabled on the bucket since: the bytes stay, the mark goes
    r = clear_mark(dpp, bucket.get(), a, y);
    if (r < 0 && r != -ECANCELED && r != -ENOENT) {
      *err = r;
      return Outcome::Failed;
    }
    return Outcome::Superseded;
  }

  // a non-current null version is addressed as lifecycle addresses it
  if (key.instance.empty() && bucket->versioned() && !e.current) {
    key.instance = "null";
  }
  Placement where;
  r = bucket->get_object(key)->ite_get_placement(dpp, &where, y);
  if (r < 0) {
    *err = r;
    return Outcome::Failed;
  }
  const bool in_place = (where == a.target);
  if (!in_place) {
    ++st.started;
    // -ECANCELED when the instance changed under the move: the retry
    // finds out whether it was replaced or only had its metadata updated
    r = bucket->get_object(key)->ite_move_placement(dpp, a.target, a.obj.gen, y);
    if (r < 0) {
      *err = r;
      return Outcome::Failed;
    }
    // verify that the bytes are there before the state says so
    r = bucket->get_object(key)->ite_get_placement(dpp, &where, y);
    if (r < 0) {
      *err = r;
      return Outcome::Failed;
    }
    if (where != a.target) {
      ldpp_dout(dpp, 0) << "ERROR: " << a.bucket << "/" << key << " is still in the "
                        << to_string(where) << " placement after its "
                        << to_string(a.kind) << dendl;
      *err = -EIO;
      return Outcome::Failed;
    }
  }

  r = finish(dpp, bucket.get(), e, a, now, y);
  if (r == -ECANCELED || r == -ENOENT) {
    return Outcome::Gone; // replaced or removed during the move
  }
  if (r < 0) {
    *err = r; // the retry finds the bytes in place and only writes the state
    return Outcome::Failed;
  }
  if (in_place) {
    ++st.reconciled;
  }
  switch (a.kind) {
  case MoveKind::Commit: ++st.commits; break;
  case MoveKind::PreWarm: ++st.prewarms; break;
  case MoveKind::Restore: ++st.restores; break;
  }
  ldpp_dout(dpp, 20) << a.bucket << "/" << key << ": " << to_string(a.kind)
                     << (in_place ? " reconciled" : " moved") << " to the "
                     << to_string(a.target) << " placement" << dendl;
  return Outcome::Done;
}

int Mover::finish(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                  const ObjectEntry& e, const MoveAction& a, ceph::real_time now,
                  optional_yield y)
{
  // the terminal state, now that the bytes are where it says
  rgw_bucket_ite_state s;
  s.commit_count = e.ite.commit_count;
  s.restore_count = e.ite.restore_count;
  s.move_pending = RGWITEMovePending::None;
  switch (a.kind) {
  case MoveKind::Commit:
    s.tier_state = RGWITETierState::Committed;
    ++s.commit_count;
    break;
  case MoveKind::PreWarm:
    // the warmth is only predicted: if the access does not come, the grace
    // period commits the instance again
    s.tier_state = RGWITETierState::Pseudo;
    s.pseudo_since = to_epoch_secs(now);
    ++s.restore_count;
    break;
  case MoveKind::Restore:
    s.tier_state = RGWITETierState::Hot;
    ++s.restore_count;
    break;
  }
  return ctx.state->set_state(dpp, bucket, e.key, s,
                              rgw_bucket_ite_state::MASK_TIER |
                              rgw_bucket_ite_state::MASK_COUNTERS |
                              rgw_bucket_ite_state::MASK_MOVE,
                              a.obj.gen, y);
}

int Mover::requeue(const DoutPrefixProvider* dpp, const MoveAction& a, bool attempt,
                   optional_yield y)
{
  MoveAction again = a;
  if (attempt) {
    ++again.attempts;
  }
  int r = ctx.moves->append(dpp, again, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to requeue " << to_string(a.kind) << " of "
                      << a.bucket << "/" << a.obj.name << ": " << cpp_strerror(-r) << dendl;
    return r;
  }
  ++st.requeued;
  return 0;
}

void Mover::give_up(const DoutPrefixProvider* dpp, const MoveAction& a, int err,
                    optional_yield y)
{
  ++st.failed;
  if (err == -EOPNOTSUPP) {
    // a configuration state, met on every commit until a cold pool is set
    ldpp_dout(dpp, 5) << "no cold placement for " << a.bucket << "/" << a.obj.name
                      << ", dropping its " << to_string(a.kind) << dendl;
  } else {
    ldpp_dout(dpp, 0) << "ERROR: giving up " << to_string(a.kind) << " of " << a.bucket
                      << "/" << a.obj.name << " after " << a.attempts + 1
                      << " attempts: " << cpp_strerror(-err) << dendl;
  }
  // the bytes stay where they are; clearing the mark lets the engine
  // decide about the instance again
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = ctx.load_bucket(dpp, a.bucket, &bucket, y);
  if (r == 0) {
    r = clear_mark(dpp, bucket.get(), a, y);
  }
  if (r < 0 && r != -ENOENT && r != -ECANCELED) {
    ldpp_dout(dpp, 0) << "ERROR: failed to clear the pending " << to_string(a.kind)
                      << " of " << a.bucket << "/" << a.obj.name << ", which waits for "
                      << "it until cleared: " << cpp_strerror(-r) << dendl;
  }
}

int Mover::clear_mark(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                      const MoveAction& a, optional_yield y)
{
  // only the mark of this move; another one has its own action queued
  const rgw_obj_key key(a.obj.name, a.obj.instance);
  ObjectEntry e;
  int r = ctx.state->get_state(dpp, bucket, key, &e, y);
  if (r < 0) {
    return r;
  }
  if (e.instance().gen != a.obj.gen || e.ite.move_pending != move_pending_of(a.kind)) {
    return 0;
  }
  rgw_bucket_ite_state s;
  s.move_pending = RGWITEMovePending::None;
  return ctx.state->set_state(dpp, bucket, key, s, rgw_bucket_ite_state::MASK_MOVE,
                              a.obj.gen, y);
}

} // namespace rgw::ite
