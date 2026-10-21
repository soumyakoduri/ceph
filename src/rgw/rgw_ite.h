// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): hosting.
 *
 * The workers are written once, against the four SAL contracts, and know
 * nothing about the backend underneath. A Context bundles the driver with
 * its contracts; the Manager owns one, the access-log sink, and a thread
 * per periodic worker, in the style of the lifecycle and restore threads.
 * radosgw-admin builds its own Context to inspect state or to run a single
 * cycle of a worker.
 */

#pragma once

#include <atomic>
#include <cerrno>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/async/yield_context.h"
#include "common/ceph_mutex.h"
#include "common/dout.h"
#include "rgw_ite_sink.h"
#include "rgw_ite_types.h"
#include "rgw_realm_reloader.h"
#include "rgw_sal_fwd.h"

class CephContext;

namespace rgw::sal {
class ITEWriteBuffer;
class ITEAggStore;
class ITEStateStore;
class ITEMoveQueue;
class ITESerializer;
}

namespace rgw::ite {

/* what the workers run against: the driver and its four contracts */
struct Context {
  CephContext* cct = nullptr;
  rgw::sal::Driver* driver = nullptr;
  std::unique_ptr<rgw::sal::ITEWriteBuffer> buffer;
  std::unique_ptr<rgw::sal::ITEAggStore> agg;
  std::unique_ptr<rgw::sal::ITEStateStore> state;
  std::unique_ptr<rgw::sal::ITEMoveQueue> moves;
  std::string zone_id;
  std::string cookie; // lock cookie of this process
  /* set while the hosting gateway shuts down; long cycles check it between
   * units of work so that shutdown never waits for a whole cycle */
  std::atomic<bool> going_down{false};
  /* how the workers load a bucket; the driver unless a test replaces it */
  std::function<int(const DoutPrefixProvider*, const rgw_bucket&,
                    std::unique_ptr<rgw::sal::Bucket>*, optional_yield)> bucket_loader;

  Context();
  ~Context();

  /* fetch and initialize the contracts; -EOPNOTSUPP when the driver does
   * not provide all four, since then it is not ITE-capable */
  int init(const DoutPrefixProvider* dpp, CephContext* cct,
           rgw::sal::Driver* driver, optional_yield y);
  /* drop the contracts, flushing buffered events */
  void reset(const DoutPrefixProvider* dpp);

  TimeScale timescale() const { return TimeScale::from_conf(cct); }
  Granularity granularity() const;
  Knobs defaults() const { return Knobs::from_conf(cct); }
  /* lifetime of a processing lock, shard or bucket */
  ceph::timespan lock_time() const;
  /* a period key's value, scaled by the time scale */
  ceph::timespan period(std::string_view key) const;
  /* windows the aggregated store keeps at granularity g */
  uint64_t retention(Granularity g) const;
  /* the newest window the aggregator has surely folded in completely by
   * now: one closed for two aggregation periods. nullopt before the first. */
  std::optional<Window> settled_window(ceph::real_time now) const;

  int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                  std::unique_ptr<rgw::sal::Bucket>* bucket, optional_yield y);
  /* the knobs in effect for a bucket, or nullopt unless ITE is enabled on it */
  std::optional<Knobs> bucket_knobs(rgw::sal::Bucket* bucket) const;
};

/* a bucket's exclusive lock for one worker run, renewed while it lasts */
class BucketLock {
  std::unique_ptr<rgw::sal::ITESerializer> ser;
  ceph::timespan dur;
  ceph::coarse_mono_time renew_at;
  bool held = false;

public:
  BucketLock(std::unique_ptr<rgw::sal::ITESerializer> ser, ceph::timespan dur);
  ~BucketLock();
  BucketLock(const BucketLock&) = delete;
  BucketLock& operator=(const BucketLock&) = delete;

  /* take the lock; -EBUSY when another gateway holds it */
  int lock(const DoutPrefixProvider* dpp, optional_yield y);
  /* whether the lock is still ours, renewing it once a third of its life
   * has passed. A run that loses it stops: another gateway took over. */
  bool keep(const DoutPrefixProvider* dpp, optional_yield y);
  void unlock(const DoutPrefixProvider* dpp, optional_yield y);
};

/* how a per-bucket run ended */
enum class BucketRun {
  Done,     // the work ran
  Busy,     // another gateway is working on the bucket
  Gone,     // the bucket no longer exists
  Disabled, // ITE is not enabled on the bucket
  Failed,   // an error came before the work could start
};

using BucketWork = std::function<int(rgw::sal::Bucket* bucket, const Knobs& knobs,
                                     BucketLock& lock)>;

/* run work over one bucket under its exclusive lock, once the bucket is
 * loaded and ITE is enabled on it */
int run_on_bucket(const DoutPrefixProvider* dpp, Context& ctx, const rgw_bucket& b,
                  const BucketWork& work, BucketRun* run, optional_yield y);

/* call fn for every bucket in the ITE directory. A bucket whose fn fails
 * does not stop the rest; the last failure is returned. */
int for_each_bucket(const DoutPrefixProvider* dpp, Context& ctx,
                    const std::function<int(const BucketProgress&)>& fn,
                    optional_yield y);

/* -EINTR from a worker step: it stopped early, on shutdown or because its
 * lock moved to another gateway, and left no partial progress behind */
inline constexpr int STOPPED = -EINTR;

/* a thread running one periodic worker cycle after another */
class WorkerThread : public DoutPrefixProvider {
public:
  using Cycle = std::function<int(const DoutPrefixProvider*, optional_yield)>;
  using Period = std::function<ceph::timespan()>;

private:
  CephContext* const cct;
  const std::string name;
  Cycle cycle;
  Period period;

  ceph::mutex lock = ceph::make_mutex("rgw::ite::WorkerThread");
  ceph::condition_variable cond;
  bool stopping = false;
  std::thread thread;

  void run();

public:
  WorkerThread(CephContext* cct, std::string name, Cycle cycle, Period period);
  ~WorkerThread() override;

  void start();
  void stop();

  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;
};

/* the ITE service of one gateway: the sink on the request path and the
 * background workers */
class Manager : public RGWRealmReloader::Pauser, public DoutPrefixProvider {
  CephContext* const cct;
  rgw::sal::Driver* driver;
  Context ctx;
  Sink sink;
  std::vector<std::unique_ptr<WorkerThread>> workers;
  bool running = false;

  void add_worker(std::string name, WorkerThread::Cycle cycle,
                  WorkerThread::Period period);
  void add_worker(std::string name, WorkerThread::Cycle cycle,
                  std::string_view period_key);
  int start_workers();
  void stop_workers();

public:
  Manager(CephContext* cct, rgw::sal::Driver* driver);
  ~Manager() override;

  /* initialize the contracts and start the sink and the workers */
  int start();
  void stop();

  Sink* get_sink() { return &sink; }

  // RGWRealmReloader::Pauser: the driver is replaced on a realm reload
  void pause() override;
  void resume(rgw::sal::Driver* driver) override;

  CephContext* get_cct() const override { return cct; }
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;
};

/* scale a day-based worker period by the ITE time scale, so that a debug
 * interval compresses the whole schedule */
ceph::timespan scaled_period(const TimeScale& ts, uint64_t secs);

} // namespace rgw::ite
