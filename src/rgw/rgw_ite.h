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

#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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
};

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
