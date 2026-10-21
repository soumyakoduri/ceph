// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_sal_rados_ite.h"

#include <fmt/format.h>

#include "common/Thread.h"
#include "common/async/blocked_completion.h"
#include "common/errno.h"
#include "neorados/cls/fifo.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "services/svc_zone.h"
#include "yield_completion.h"

#define dout_subsys ceph_subsys_rgw

namespace sys = boost::system;

namespace rgw::sal {

static constexpr const char* ITE_LOCK_NAME = "ite_process";

rgw_pool ite_pool(RadosStore* store)
{
  CephContext* cct = store->ctx();
  return rgw_pool(cct->_conf.get_val<std::string>("rgw_ite_pool"),
                  store->svc()->zone->get_zone_params().get_id());
}

int ite_open_pool(const DoutPrefixProvider* dpp, RadosStore* store,
                  librados::IoCtx& ioctx, neorados::IOContext& neo_ioctx,
                  optional_yield y)
{
  const rgw_pool pool = ite_pool(store);
  // holds small omap and fifo objects
  int r = rgw_init_ioctx(dpp, store->getRados()->get_rados_handle(), pool, ioctx,
                         true /* create */, true /* mostly_omap */);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to open ITE pool " << pool << ": "
                      << cpp_strerror(-r) << dendl;
    return r;
  }
  try {
    neo_ioctx = rgw::init_iocontext(dpp, store->get_neorados(), pool,
                                    rgw::maybe_yield(dpp, y));
  } catch (const sys::system_error& e) {
    ldpp_dout(dpp, 0) << "ERROR: failed to open ITE pool " << pool << ": "
                      << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }
  return 0;
}

RadosITESerializer::RadosITESerializer(const librados::IoCtx& _ioctx,
                                       const std::string& _oid,
                                       const std::string& lock_name,
                                       const std::string& cookie)
  : StoreITESerializer(_oid), ioctx(_ioctx), lock(lock_name)
{
  lock.set_cookie(cookie);
}

int RadosITESerializer::try_lock(const DoutPrefixProvider* dpp,
                                 ceph::timespan dur, optional_yield y)
{
  lock.set_duration(dur);
  librados::ObjectWriteOperation op;
  lock.lock_exclusive(&op);
  return rgw_rados_operate(dpp, ioctx, oid, std::move(op), y);
}

int RadosITESerializer::unlock(const DoutPrefixProvider* dpp, optional_yield y)
{
  librados::ObjectWriteOperation op;
  op.assert_exists();
  lock.unlock(&op);
  return rgw_rados_operate(dpp, ioctx, oid, std::move(op), y);
}

RadosITEFifoSet::RadosITEFifoSet(RadosStore* store, std::string prefix,
                                 uint32_t shards)
  : store(store), prefix(std::move(prefix)), shards(std::max<uint32_t>(1, shards))
{}

std::string RadosITEFifoSet::oid(uint32_t shard) const
{
  return fmt::format("{}.{}", prefix, shard);
}

int RadosITEFifoSet::initialize(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (initialized()) {
    return 0;
  }
  int r = ite_open_pool(dpp, store, ioctx, neo_ioctx, y);
  if (r < 0) {
    return r;
  }
  fifos.clear();
  for (uint32_t i = 0; i < shards; ++i) {
    std::unique_ptr<fifo::FIFO> f;
    try {
      f = fifo::FIFO::create(dpp, store->get_neorados(), oid(i), neo_ioctx,
                             rgw::maybe_yield(dpp, y));
    } catch (const sys::system_error& e) {
      ldpp_dout(dpp, 0) << "ERROR: failed to open ITE fifo " << oid(i) << ": "
                        << e.what() << dendl;
      fifos.clear();
      return ceph::from_error_code(e.code());
    }
    if (!f) {
      fifos.clear();
      return -ENOMEM;
    }
    fifos.push_back(std::move(f));
  }
  ldpp_dout(dpp, 20) << "opened " << shards << " ITE fifos " << prefix
                     << ".* in " << ite_pool(store) << dendl;
  return 0;
}

int RadosITEFifoSet::push(const DoutPrefixProvider* dpp, uint32_t shard,
                          std::deque<ceph::buffer::list>&& entries,
                          optional_yield y)
{
  if (shard >= fifos.size()) {
    return -EINVAL;
  }
  try {
    fifos[shard]->push(dpp, std::move(entries), rgw::maybe_yield(dpp, y));
  } catch (const sys::system_error& e) {
    ldpp_dout(dpp, 1) << "ERROR: failed to push to " << oid(shard) << ": "
                      << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }
  return 0;
}

int RadosITEFifoSet::list(const DoutPrefixProvider* dpp, uint32_t shard,
                          const std::string& cursor, uint32_t max,
                          std::vector<fifo::entry>& entries, bool* truncated,
                          optional_yield y)
{
  entries.clear();
  *truncated = false;
  if (shard >= fifos.size()) {
    return -EINVAL;
  }
  const uint32_t n = std::clamp<uint32_t>(max, 1, fifo::FIFO::max_list_entries);
  std::vector<fifo::entry> buf{n};
  try {
    auto [got, more] = fifos[shard]->list(dpp, cursor, buf,
                                          rgw::maybe_yield(dpp, y));
    entries.assign(std::make_move_iterator(got.begin()),
                   std::make_move_iterator(got.end()));
    *truncated = !more.empty();
  } catch (const sys::system_error& e) {
    if (e.code() == sys::errc::no_such_file_or_directory) {
      return 0;
    }
    ldpp_dout(dpp, 1) << "ERROR: failed to list " << oid(shard) << ": "
                      << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }
  return 0;
}

int RadosITEFifoSet::trim(const DoutPrefixProvider* dpp, uint32_t shard,
                          const std::string& cursor, optional_yield y)
{
  if (shard >= fifos.size()) {
    return -EINVAL;
  }
  try {
    fifos[shard]->trim(dpp, cursor, false /* inclusive */,
                       rgw::maybe_yield(dpp, y));
  } catch (const sys::system_error& e) {
    ldpp_dout(dpp, 1) << "ERROR: failed to trim " << oid(shard) << " to "
                      << cursor << ": " << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }
  return 0;
}

std::unique_ptr<ITESerializer>
RadosITEFifoSet::get_serializer(uint32_t shard, const std::string& cookie)
{
  return std::make_unique<RadosITESerializer>(ioctx, oid(shard), ITE_LOCK_NAME,
                                              cookie);
}

RadosITEWriteBuffer::RadosITEWriteBuffer(RadosStore* store)
  : cct(store->ctx()),
    fifos(store, "ite-log",
          cct->_conf.get_val<int64_t>("rgw_ite_log_num_shards"))
{
  pending.resize(fifos.num_shards());
}

RadosITEWriteBuffer::~RadosITEWriteBuffer()
{
  {
    std::lock_guard l{lock};
    stopping = true;
    cond.notify_all();
  }
  if (flusher.joinable()) {
    flusher.join();
  }
  DoutPrefix dp(cct, dout_subsys, "rgw ite write buffer: ");
  flush_pending(&dp, null_yield);
}

int RadosITEWriteBuffer::initialize(const DoutPrefixProvider* dpp, optional_yield y)
{
  return fifos.initialize(dpp, y);
}

int RadosITEWriteBuffer::append(const DoutPrefixProvider* dpp,
                                const rgw::ite::AccessEvent& event,
                                optional_yield y)
{
  if (!fifos.initialized()) {
    return -EINVAL;
  }
  const uint32_t shard = rgw::ite::buffer_shard_of(event.bucket, event.obj.name,
                                                   fifos.num_shards());
  ceph::buffer::list bl;
  encode(event, bl);

  std::lock_guard l{lock};
  if (stopping) {
    return -ESHUTDOWN;
  }
  if (num_pending >= max_pending) {
    return -ENOSPC;
  }
  pending[shard].push_back(std::move(bl));
  ++num_pending;
  if (!flusher.joinable()) {
    flusher = make_named_thread("ite_log_flush", &RadosITEWriteBuffer::flush_loop,
                                this);
  }
  if (num_pending >= flush_batch) {
    cond.notify_one();
  }
  return 0;
}

void RadosITEWriteBuffer::flush_loop()
{
  DoutPrefix dp(cct, dout_subsys, "rgw ite write buffer: ");
  std::unique_lock l{lock};
  while (!stopping) {
    cond.wait_for(l, flush_interval,
                  [this] { return stopping || num_pending >= flush_batch; });
    if (stopping) {
      break;
    }
    l.unlock();
    flush_pending(&dp, null_yield);
    l.lock();
  }
}

int RadosITEWriteBuffer::flush_pending(const DoutPrefixProvider* dpp,
                                       optional_yield y)
{
  std::lock_guard fl{flush_lock};
  std::vector<std::deque<ceph::buffer::list>> batch(fifos.num_shards());
  {
    std::lock_guard l{lock};
    if (!num_pending) {
      return 0;
    }
    batch.swap(pending);
    num_pending = 0;
  }
  int ret = 0;
  for (uint32_t shard = 0; shard < batch.size(); ++shard) {
    if (batch[shard].empty()) {
      continue;
    }
    const auto n = batch[shard].size();
    int r = fifos.push(dpp, shard, std::move(batch[shard]), y);
    if (r < 0) {
      // fire-and-forget: a lost batch only under-counts access
      ldpp_dout(dpp, 0) << "ERROR: dropped " << n << " access events for "
                        << fifos.oid(shard) << ": " << cpp_strerror(-r) << dendl;
      ret = r;
    }
  }
  return ret;
}

int RadosITEWriteBuffer::flush(const DoutPrefixProvider* dpp, optional_yield y)
{
  return flush_pending(dpp, y);
}

int RadosITEWriteBuffer::list(const DoutPrefixProvider* dpp, uint32_t shard,
                              const std::string& cursor, uint32_t max,
                              std::vector<rgw::ite::AccessEventEntry>& entries,
                              std::string* next_cursor, bool* truncated,
                              optional_yield y)
{
  std::vector<fifo::entry> raw;
  bool more = false;
  int r = fifos.list(dpp, shard, cursor, max, raw, &more, y);
  if (r < 0) {
    return r;
  }
  entries.clear();
  entries.reserve(raw.size());
  std::string last = cursor;
  for (auto& f : raw) {
    last = f.marker;
    rgw::ite::AccessEventEntry e;
    e.marker = f.marker;
    try {
      auto p = f.data.cbegin();
      decode(e.event, p);
    } catch (const ceph::buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: skipping undecodable access event "
                        << f.marker << " in " << fifos.oid(shard) << dendl;
      continue;
    }
    entries.push_back(std::move(e));
  }
  if (next_cursor) {
    *next_cursor = std::move(last);
  }
  if (truncated) {
    *truncated = more;
  }
  return 0;
}

int RadosITEWriteBuffer::trim(const DoutPrefixProvider* dpp, uint32_t shard,
                              const std::string& cursor, optional_yield y)
{
  return fifos.trim(dpp, shard, cursor, y);
}

std::unique_ptr<ITESerializer>
RadosITEWriteBuffer::get_serializer(uint32_t shard, const std::string& cookie)
{
  return fifos.get_serializer(shard, cookie);
}

std::unique_ptr<ITEWriteBuffer> RadosStore::get_ite_write_buffer()
{
  return std::make_unique<RadosITEWriteBuffer>(this);
}

} // namespace rgw::sal
