// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite.h"

#include <algorithm>

#include "common/Thread.h"
#include "common/errno.h"
#include "common/random_string.h"
#include "rgw_ite_aggregator.h"
#include "rgw_ite_temperature.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ite {

Context::Context() = default;
Context::~Context() = default;

int Context::init(const DoutPrefixProvider* dpp, CephContext* _cct,
                  rgw::sal::Driver* _driver, optional_yield y)
{
  cct = _cct;
  driver = _driver;
  zone_id = driver->get_zone()->get_id();
  cookie = gen_rand_alphanumeric(cct, 16);

  buffer = driver->get_ite_write_buffer();
  agg = driver->get_ite_agg_store();
  state = driver->get_ite_state_store();
  moves = driver->get_ite_move_queue();
  if (!buffer || !agg || !state || !moves) {
    ldpp_dout(dpp, 0) << "ERROR: driver " << driver->get_name()
                      << " does not implement the ITE contracts" << dendl;
    reset(dpp);
    return -EOPNOTSUPP;
  }

  int r = buffer->initialize(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize the ITE write buffer: "
                      << cpp_strerror(-r) << dendl;
    reset(dpp);
    return r;
  }
  r = agg->initialize(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize the ITE aggregated store: "
                      << cpp_strerror(-r) << dendl;
    reset(dpp);
    return r;
  }
  r = moves->initialize(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize the ITE move queue: "
                      << cpp_strerror(-r) << dendl;
    reset(dpp);
    return r;
  }
  return 0;
}

void Context::reset(const DoutPrefixProvider* dpp)
{
  if (buffer) {
    int r = buffer->flush(dpp, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, 1) << "WARNING: failed to flush the ITE write buffer: "
                        << cpp_strerror(-r) << dendl;
    }
  }
  buffer.reset();
  agg.reset();
  state.reset();
  moves.reset();
}

Granularity Context::granularity() const
{
  const auto g = parse_granularity(
      cct->_conf.get_val<std::string>("rgw_ite_window_granularity"));
  return g.value_or(Granularity::Day);
}

ceph::timespan Context::lock_time() const
{
  return std::chrono::seconds(cct->_conf.get_val<int64_t>("rgw_ite_lock_max_time"));
}

ceph::timespan Context::period(std::string_view key) const
{
  return scaled_period(timescale(), cct->_conf.get_val<int64_t>(std::string(key)));
}

uint64_t Context::retention(Granularity g) const
{
  return retention_windows(cct->_conf.get_val<int64_t>("rgw_ite_fft_span"),
                           cct->_conf.get_val<int64_t>("rgw_ite_agg_retention_margin"), g);
}

int Context::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                         std::unique_ptr<rgw::sal::Bucket>* bucket, optional_yield y)
{
  if (bucket_loader) {
    return bucket_loader(dpp, b, bucket, y);
  }
  if (!driver) {
    return -EINVAL;
  }
  return driver->load_bucket(dpp, b, bucket, y);
}

std::optional<Knobs> Context::bucket_knobs(rgw::sal::Bucket* bucket) const
{
  const auto config = get_bucket_config(bucket->get_attrs());
  if (!config || !config->enabled) {
    return std::nullopt;
  }
  return config->effective(defaults());
}

BucketLock::BucketLock(std::unique_ptr<rgw::sal::ITESerializer> ser, ceph::timespan dur)
  : ser(std::move(ser)), dur(dur)
{}

BucketLock::~BucketLock() = default; // a lock still held expires on its own

int BucketLock::lock(const DoutPrefixProvider* dpp, optional_yield y)
{
  int r = ser->try_lock(dpp, dur, y);
  if (r == -EEXIST) {
    r = -EBUSY;
  }
  if (r < 0) {
    return r;
  }
  held = true;
  renew_at = ceph::coarse_mono_clock::now() + dur / 3;
  return 0;
}

bool BucketLock::keep(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (!held) {
    return false;
  }
  const auto now = ceph::coarse_mono_clock::now();
  if (now < renew_at) {
    return true;
  }
  int r = ser->try_lock(dpp, dur, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "WARNING: lost the lock on " << *ser << ": "
                      << cpp_strerror(-r) << dendl;
    held = false;
    return false;
  }
  renew_at = now + dur / 3;
  return true;
}

void BucketLock::unlock(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (!held) {
    return;
  }
  held = false;
  int r = ser->unlock(dpp, y);
  if (r < 0 && r != -ENOENT) {
    ldpp_dout(dpp, 1) << "WARNING: failed to unlock " << *ser << ": "
                      << cpp_strerror(-r) << dendl;
  }
}

int run_on_bucket(const DoutPrefixProvider* dpp, Context& ctx, const rgw_bucket& b,
                  const BucketWork& work, BucketRun* run, optional_yield y)
{
  *run = BucketRun::Failed;
  BucketLock lock(ctx.agg->get_bucket_serializer(b, ctx.cookie), ctx.lock_time());
  int r = lock.lock(dpp, y);
  if (r == -EBUSY) {
    *run = BucketRun::Busy;
    return 0;
  }
  if (r < 0) {
    return r;
  }
  std::unique_ptr<rgw::sal::Bucket> bucket;
  r = ctx.load_bucket(dpp, b, &bucket, y);
  if (r == -ENOENT) {
    *run = BucketRun::Gone;
    r = 0;
  } else if (r == 0) {
    if (auto knobs = ctx.bucket_knobs(bucket.get()); knobs) {
      *run = BucketRun::Done;
      r = work(bucket.get(), *knobs, lock);
    } else {
      *run = BucketRun::Disabled;
    }
  }
  lock.unlock(dpp, y);
  return r;
}

int for_each_bucket(const DoutPrefixProvider* dpp, Context& ctx,
                    const std::function<int(const BucketProgress&)>& fn,
                    optional_yield y)
{
  int ret = 0;
  std::string marker;
  bool truncated = true;
  while (truncated && !ctx.going_down) {
    std::vector<BucketProgress> buckets;
    std::string next;
    int r = ctx.agg->list_buckets(dpp, marker, 100, buckets, &next, &truncated, y);
    if (r < 0) {
      return r;
    }
    for (const auto& p : buckets) {
      if (ctx.going_down) {
        break;
      }
      r = fn(p);
      if (r < 0) {
        ldpp_dout(dpp, 1) << "ERROR: failed on ITE bucket " << p.bucket << ": "
                          << cpp_strerror(-r) << dendl;
        ret = r;
      }
    }
    if (buckets.empty() || next == marker) {
      break;
    }
    marker = std::move(next);
  }
  return ret;
}

ceph::timespan scaled_period(const TimeScale& ts, uint64_t secs)
{
  const uint64_t scaled = secs * ts.secs_per_day / 86400;
  return std::chrono::seconds(std::max<uint64_t>(1, scaled));
}

WorkerThread::WorkerThread(CephContext* cct, std::string name,
                           Cycle cycle, Period period)
  : cct(cct), name(std::move(name)),
    cycle(std::move(cycle)), period(std::move(period))
{}

WorkerThread::~WorkerThread()
{
  stop();
}

unsigned WorkerThread::get_subsys() const
{
  return dout_subsys;
}

std::ostream& WorkerThread::gen_prefix(std::ostream& out) const
{
  return out << "rgw ite " << name << ": ";
}

void WorkerThread::start()
{
  std::lock_guard l{lock};
  stopping = false;
  const std::string tname = "ite_" + name;
  thread = make_named_thread(tname.substr(0, 15), &WorkerThread::run, this);
}

void WorkerThread::stop()
{
  {
    std::lock_guard l{lock};
    stopping = true;
    cond.notify_all();
  }
  if (thread.joinable()) {
    thread.join();
  }
}

void WorkerThread::run()
{
  std::unique_lock l{lock};
  while (!stopping) {
    const auto start = ceph::coarse_mono_clock::now();
    l.unlock();
    int r = cycle(this, null_yield);
    if (r < 0) {
      ldpp_dout(this, 1) << "cycle failed: " << cpp_strerror(-r) << dendl;
    }
    l.lock();
    cond.wait_until(l, start + period(), [this] { return stopping; });
  }
}

Manager::Manager(CephContext* cct, rgw::sal::Driver* driver)
  : cct(cct), driver(driver), sink(cct)
{}

Manager::~Manager()
{
  stop();
}

unsigned Manager::get_subsys() const
{
  return dout_subsys;
}

std::ostream& Manager::gen_prefix(std::ostream& out) const
{
  return out << "rgw ite: ";
}

int Manager::start()
{
  if (running) {
    return 0;
  }
  ctx.going_down = false;
  int r = ctx.init(this, cct, driver, null_yield);
  if (r < 0) {
    return r;
  }
  sink.set_buffer(ctx.buffer.get());
  r = start_workers();
  if (r < 0) {
    sink.set_buffer(nullptr);
    ctx.reset(this);
    return r;
  }
  running = true;
  ldpp_dout(this, 1) << "started with " << workers.size() << " workers" << dendl;
  return 0;
}

void Manager::stop()
{
  if (!running) {
    return;
  }
  sink.set_buffer(nullptr);
  ctx.going_down = true;
  stop_workers();
  ctx.reset(this);
  running = false;
}

void Manager::add_worker(std::string name, WorkerThread::Cycle cycle,
                         std::string_view period_key)
{
  const std::string key(period_key);
  workers.push_back(std::make_unique<WorkerThread>(
      cct, std::move(name), std::move(cycle),
      [this, key] { return ctx.period(key); }));
}

int Manager::start_workers()
{
  // each cycle builds a fresh worker over the shared context, so no state
  // outlives a cycle except what the contracts hold
  add_worker("agg", [this](const DoutPrefixProvider* dpp, optional_yield y) {
    Aggregator agg(ctx);
    return agg.process(dpp, y);
  }, "rgw_ite_agg_period");
  add_worker("temp", [this](const DoutPrefixProvider* dpp, optional_yield y) {
    TemperatureScorer scorer(ctx);
    return scorer.process(dpp, y);
  }, "rgw_ite_temperature_period");

  for (auto& w : workers) {
    w->start();
  }
  return 0;
}

void Manager::stop_workers()
{
  for (auto& w : workers) {
    w->stop();
  }
  workers.clear();
}

void Manager::pause()
{
  stop();
}

void Manager::resume(rgw::sal::Driver* _driver)
{
  driver = _driver;
  int r = start();
  if (r < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to restart after realm reload: "
                       << cpp_strerror(-r) << dendl;
  }
}

} // namespace rgw::ite
