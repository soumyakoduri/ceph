// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): the lazy mover.
 *
 * The one component that moves bytes. It drains the move queue (contract
 * four) in throttled batches, off the request path, and carries out the
 * three byte-moving transitions the decision engine queued:
 *
 *   commit    PSEUDO in the hot placement    -> COMMITTED in the cold one
 *   pre-warm  COMMITTED in the cold placement -> PSEUDO in the hot one
 *   restore   COMMITTED in the cold placement -> HOT in the hot one
 *
 * It writes the terminal tier state only once the bytes are in place, so
 * the state never claims a move that has not happened, and it reconciles
 * the physical placement against the state on every action, so a queue
 * replayed after a crash, or an action queued twice, moves nothing twice.
 */

#pragma once

#include <cstdint>

#include "common/async/yield_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_ite_types.h"
#include "rgw_sal_fwd.h"

namespace rgw::ite {

struct Context;
class BucketLock;

/* an action that fails this many times is given up on: its marker is
 * cleared, and the engine decides about the instance again */
inline constexpr uint32_t MOVE_MAX_ATTEMPTS = 5;

struct MoverStats {
  uint64_t shards = 0;      // shards drained
  uint64_t shards_busy = 0; // drained by another gateway
  uint64_t actions = 0;     // actions taken off the queue
  uint64_t started = 0;     // byte moves started
  uint64_t commits = 0;     // instances committed to the cold placement
  uint64_t prewarms = 0;    // instances pre-warmed back to the hot placement
  uint64_t restores = 0;    // instances restored to the hot placement
  uint64_t reconciled = 0;  // of those, the bytes were in place already
  uint64_t gone = 0;        // deleted or replaced since queued
  uint64_t superseded = 0;  // no longer waiting for this move
  uint64_t requeued = 0;    // put back for a later cycle
  uint64_t failed = 0;      // given up on

  void dump(ceph::Formatter* f) const;
};

class Mover {
public:
  explicit Mover(Context& ctx);

  /* drain every shard this gateway can lock, starting at first_shard, up
   * to rgw_ite_move_batch_cap byte moves */
  int process(const DoutPrefixProvider* dpp, optional_yield y, uint32_t first_shard = 0);
  /* drain one shard under its lock, as of now */
  int process_shard(const DoutPrefixProvider* dpp, uint32_t shard,
                    ceph::real_time now, optional_yield y);

  const MoverStats& stats() const { return st; }

private:
  /* what became of one action */
  enum class Outcome {
    Done,       // bytes in place and the terminal state written
    Gone,       // the instance was deleted or replaced
    Superseded, // the instance no longer waits for this move
    NotYet,     // the instance is not marked yet; look again later
    Failed,     // see the error
  };

  Context& ctx;
  MoverStats st;
  uint64_t cap;

  int drain(const DoutPrefixProvider* dpp, uint32_t shard, BucketLock& lock,
            ceph::real_time now, optional_yield y);
  int handle(const DoutPrefixProvider* dpp, const MoveAction& a,
             ceph::real_time now, optional_yield y);
  Outcome perform(const DoutPrefixProvider* dpp, const MoveAction& a,
                  ceph::real_time now, int* err, optional_yield y);
  int finish(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
             const ObjectEntry& e, const MoveAction& a, ceph::real_time now,
             optional_yield y);
  int requeue(const DoutPrefixProvider* dpp, const MoveAction& a, bool attempt,
              optional_yield y);
  void give_up(const DoutPrefixProvider* dpp, const MoveAction& a, int err,
               optional_yield y);
  int clear_mark(const DoutPrefixProvider* dpp, rgw::sal::Bucket* bucket,
                 const MoveAction& a, optional_yield y);
};

} // namespace rgw::ite
