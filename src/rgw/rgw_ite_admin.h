// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): what radosgw-admin's "ite" commands do.
 *
 * Two of them write: the opt-in and opt-out of a bucket, the Stage 0 way
 * into ITE, and a one-shot run of the workers, for tests and for an admin
 * who will not wait for the next period. The rest are read-only
 * inspectors: the bucket directory and its progress, a bucket's objects by
 * tier, one instance's state and placement, the aggregated counts, and
 * what waits in the two queues. All of it goes through the four contracts
 * and the bucket and object SAL, so it works over any ITE-capable driver.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "common/async/yield_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_ite_types.h"
#include "rgw_sal_fwd.h"

namespace rgw::ite {

struct Context;

namespace admin {

/* whether a knob set makes sense once merged with the zone defaults;
 * -EINVAL with the reason when it does not */
int validate(const Knobs& knobs, std::string* err);

/* opt a bucket in: its config is enabled, with knobs replacing its own
 * when given, and it joins the directory. Enabled again after a pause, its
 * access history restarts now, since the pause left no record. Enabling an
 * enabled bucket only changes its knobs. A bucket whose placement lacks
 * the ITE storage class is enabled with a warning, since its objects keep
 * their class until the placement gets one. */
int bucket_enable(const DoutPrefixProvider* dpp, Context& ctx, rgw::sal::Bucket* bucket,
                  const std::optional<BucketConfig>& knobs, ceph::real_time now,
                  ceph::Formatter* f, optional_yield y);
/* opt a bucket out: the workers skip it from their next cycle, new objects
 * keep the class they ask for, the ones it has keep their placement and
 * state, and its access history ages out of the aggregated store */
int bucket_disable(const DoutPrefixProvider* dpp, Context& ctx, rgw::sal::Bucket* bucket,
                   ceph::Formatter* f, optional_yield y);

/* the directory of ITE buckets, with each worker's progress over them */
int bucket_list(const DoutPrefixProvider* dpp, Context& ctx, const std::string& marker,
                uint32_t max, ceph::Formatter* f, optional_yield y);
/* a bucket's config, knobs and progress, and its instances by tier state,
 * counted in one walk of its per-object state */
int bucket_stats(const DoutPrefixProvider* dpp, Context& ctx, rgw::sal::Bucket* bucket,
                 ceph::Formatter* f, optional_yield y);
/* one instance: its state, its temperature decayed to now, where its bytes
 * are, and what the decision engine would make of it now */
int object_stat(const DoutPrefixProvider* dpp, Context& ctx, rgw::sal::Bucket* bucket,
                const rgw_obj_key& key, ceph::real_time now, ceph::Formatter* f,
                optional_yield y);
/* a bucket's aggregated counts, newest window first, up to max_windows of
 * them; of one object's instances when object is not empty */
int counts_list(const DoutPrefixProvider* dpp, Context& ctx, const rgw_bucket& bucket,
                const std::string& object, uint32_t max_windows, ceph::real_time now,
                ceph::Formatter* f, optional_yield y);
/* the access events waiting in the write buffer, counted per shard up to
 * max_per_shard */
int log_status(const DoutPrefixProvider* dpp, Context& ctx, uint32_t max_per_shard,
               ceph::Formatter* f, optional_yield y);
/* the queued byte moves, of one shard or of all, up to max per shard */
int move_list(const DoutPrefixProvider* dpp, Context& ctx, std::optional<uint32_t> shard,
              uint32_t max, ceph::Formatter* f, optional_yield y);

/* the workers, in the order a run of all of them goes */
inline constexpr std::string_view WORKERS[] = {"agg", "temp", "fft", "decide", "move"};
/* run one cycle of a worker, or of every worker in order for "all", and
 * dump what each did; -EINVAL for an unknown worker. A failing worker does
 * not stop the next; the last failure is returned. */
int process(const DoutPrefixProvider* dpp, Context& ctx, std::string_view worker,
            ceph::Formatter* f, optional_yield y);

} // namespace admin
} // namespace rgw::ite
