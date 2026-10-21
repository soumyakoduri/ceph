// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_sal_rados_ite.h"

#include <fmt/format.h>

#include "common/Thread.h"
#include "common/async/blocked_completion.h"
#include "common/errno.h"
#include "cls/rgw/cls_rgw_client.h"
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
    fifos[shard]->trim(dpp, cursor, false /* exclusive: trims cursor too */,
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

static constexpr const char* ITE_BUCKET_LOCK_NAME = "ite_bucket";
static constexpr uint32_t ITE_OMAP_PAGE = 1000;
// a bound on the per-process memory of index entries already written
static constexpr size_t ITE_MAX_INDEXED = 64 * 1024;

RadosITEAggStore::RadosITEAggStore(RadosStore* store)
  : store(store), cct(store->ctx()),
    shards(std::max<int64_t>(1, cct->_conf.get_val<int64_t>("rgw_ite_agg_num_shards")))
{}

int RadosITEAggStore::initialize(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (initialized) {
    return 0;
  }
  int r = ite_open_pool(dpp, store, ioctx, neo_ioctx, y);
  if (r < 0) {
    return r;
  }
  initialized = true;
  return 0;
}

std::string RadosITEAggStore::index_oid(const rgw_bucket& bucket)
{
  return fmt::format("ite-agg-idx.{}", bucket.get_key());
}

static char index_granularity(rgw::ite::Granularity g)
{
  return g == rgw::ite::Granularity::Week ? 'w' : 'd';
}

/* sorts just before every index key of window w */
static std::string index_window_prefix(const rgw::ite::Window& w)
{
  return fmt::format("{}{:020}", index_granularity(w.granularity), w.index);
}

std::string RadosITEAggStore::index_key(const rgw::ite::Window& w, uint32_t shard)
{
  // sorts by granularity, then window, then shard
  return fmt::format("{}.{:05}", index_window_prefix(w), shard);
}

std::optional<std::pair<rgw::ite::Window, uint32_t>>
RadosITEAggStore::parse_index_key(const std::string& key)
{
  // <g><20 digits>.<5 digits>
  if (key.size() != 27 || key[21] != '.' ||
      (key[0] != 'd' && key[0] != 'w')) {
    return std::nullopt;
  }
  auto digit = [](char c) { return c >= '0' && c <= '9'; };
  uint64_t index = 0;
  uint32_t shard = 0;
  for (size_t i = 1; i < 21; ++i) {
    if (!digit(key[i])) {
      return std::nullopt;
    }
    index = index * 10 + (key[i] - '0');
  }
  for (size_t i = 22; i < 27; ++i) {
    if (!digit(key[i])) {
      return std::nullopt;
    }
    shard = shard * 10 + (key[i] - '0');
  }
  rgw::ite::Window w;
  w.granularity = key[0] == 'w' ? rgw::ite::Granularity::Week
                                : rgw::ite::Granularity::Day;
  w.index = index;
  return std::make_pair(w, shard);
}

std::string RadosITEAggStore::window_oid(const rgw_bucket& bucket,
                                         const rgw::ite::Window& w,
                                         uint32_t shard) const
{
  return fmt::format("ite-agg.{}.{}.{}", bucket.get_key(),
                     w.stamp(rgw::ite::TimeScale::from_conf(cct)), shard);
}

int RadosITEAggStore::index_window(const DoutPrefixProvider* dpp,
                                   const rgw_bucket& bucket,
                                   const rgw::ite::Window& w, uint32_t shard,
                                   optional_yield y)
{
  const std::string oid = index_oid(bucket);
  const std::string key = index_key(w, shard);
  const std::string cache_key = oid + "/" + key;
  {
    std::lock_guard l{lock};
    if (indexed.count(cache_key)) {
      return 0;
    }
  }
  std::map<std::string, ceph::buffer::list> kv;
  kv[key].append(w.stamp(rgw::ite::TimeScale::from_conf(cct)));
  librados::ObjectWriteOperation op;
  op.omap_set(kv);
  int r = rgw_rados_operate(dpp, ioctx, oid, std::move(op), y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to index " << window_oid(bucket, w, shard)
                      << ": " << cpp_strerror(-r) << dendl;
    return r;
  }
  std::lock_guard l{lock};
  if (indexed.size() >= ITE_MAX_INDEXED) {
    indexed.clear(); // rewriting an index entry is harmless
  }
  indexed.insert(cache_key);
  return 0;
}

int RadosITEAggStore::read_index(const DoutPrefixProvider* dpp,
                                 const rgw_bucket& bucket,
                                 const std::string& start_after,
                                 const std::string& prefix,
                                 std::vector<std::pair<rgw::ite::Window, uint32_t>>& out,
                                 optional_yield y)
{
  out.clear();
  const std::string oid = index_oid(bucket);
  std::string after = start_after;
  bool more = true;
  while (more) {
    std::map<std::string, ceph::buffer::list> vals;
    int rval = 0;
    librados::ObjectReadOperation op;
    op.omap_get_vals2(after, prefix, ITE_OMAP_PAGE, &vals, &more, &rval);
    int r = rgw_rados_operate(dpp, ioctx, oid, std::move(op), nullptr, y);
    if (r == -ENOENT) {
      return 0;
    }
    if (r < 0) {
      return r;
    }
    for (const auto& [key, bl] : vals) {
      if (auto e = parse_index_key(key); e) {
        out.push_back(*e);
      }
    }
    if (vals.empty()) {
      break;
    }
    after = vals.rbegin()->first;
  }
  return 0;
}

int RadosITEAggStore::get_marker(const DoutPrefixProvider* dpp,
                                 const rgw_bucket& bucket,
                                 const rgw::ite::Window& window,
                                 uint32_t agg_shard, uint32_t buffer_shard,
                                 std::string* marker, optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  marker->clear();
  const std::string key = rgw::ite::marker_key(buffer_shard);
  std::map<std::string, ceph::buffer::list> vals;
  int rval = 0;
  librados::ObjectReadOperation op;
  op.omap_get_vals_by_keys({key}, &vals, &rval);
  int r = rgw_rados_operate(dpp, ioctx, window_oid(bucket, window, agg_shard),
                            std::move(op), nullptr, y);
  if (r == -ENOENT) {
    return 0;
  }
  if (r < 0) {
    return r;
  }
  if (auto i = vals.find(key); i != vals.end()) {
    *marker = i->second.to_str();
  }
  return 0;
}

int RadosITEAggStore::add_counts(const DoutPrefixProvider* dpp,
                                 const rgw_bucket& bucket,
                                 const rgw::ite::Window& window,
                                 uint32_t agg_shard, uint32_t buffer_shard,
                                 const std::string& expected_marker,
                                 const std::string& new_marker,
                                 const std::map<std::string, uint64_t>& deltas,
                                 optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  // indexed first: an object with counts is always one expiry can find
  int r = index_window(dpp, bucket, window, agg_shard, y);
  if (r < 0) {
    return r;
  }
  librados::ObjectWriteOperation op;
  cls_rgw_ite_agg_add(op, rgw::ite::marker_key(buffer_shard), expected_marker,
                      new_marker, deltas);
  r = rgw_rados_operate(dpp, ioctx, window_oid(bucket, window, agg_shard),
                        std::move(op), y);
  if (r < 0 && r != -ECANCELED) {
    ldpp_dout(dpp, 1) << "ERROR: failed to add counts to "
                      << window_oid(bucket, window, agg_shard) << ": "
                      << cpp_strerror(-r) << dendl;
  }
  return r;
}

int RadosITEAggStore::read_counts(const DoutPrefixProvider* dpp,
                                  const rgw_bucket& bucket,
                                  const rgw::ite::Window& window,
                                  std::map<std::string, uint64_t>& counts,
                                  optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  counts.clear();
  const std::string prefix = index_window_prefix(window) + ".";
  std::vector<std::pair<rgw::ite::Window, uint32_t>> idx;
  int r = read_index(dpp, bucket, "", prefix, idx, y);
  if (r < 0) {
    return r;
  }
  for (const auto& [w, shard] : idx) {
    const std::string oid = window_oid(bucket, w, shard);
    std::string after;
    bool more = true;
    while (more) {
      std::map<std::string, ceph::buffer::list> vals;
      int rval = 0;
      librados::ObjectReadOperation op;
      op.omap_get_vals2(after, ITE_OMAP_PAGE, &vals, &more, &rval);
      r = rgw_rados_operate(dpp, ioctx, oid, std::move(op), nullptr, y);
      if (r == -ENOENT) {
        break;
      }
      if (r < 0) {
        return r;
      }
      for (const auto& [key, bl] : vals) {
        if (rgw::ite::is_marker_key(key)) {
          continue;
        }
        uint64_t n = 0;
        try {
          auto p = bl.cbegin();
          decode(n, p);
        } catch (const ceph::buffer::error& e) {
          ldpp_dout(dpp, 0) << "ERROR: corrupt count " << key << " in " << oid << dendl;
          continue;
        }
        counts[key] += n;
      }
      if (vals.empty()) {
        break;
      }
      after = vals.rbegin()->first;
    }
  }
  return 0;
}

int RadosITEAggStore::list_windows(const DoutPrefixProvider* dpp,
                                   const rgw_bucket& bucket,
                                   const rgw::ite::Window& from,
                                   const rgw::ite::Window& to,
                                   std::vector<rgw::ite::Window>& windows,
                                   optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  windows.clear();
  // from the first key of window `from`, within its granularity
  std::vector<std::pair<rgw::ite::Window, uint32_t>> idx;
  int r = read_index(dpp, bucket, index_window_prefix(from),
                     std::string(1, index_granularity(from.granularity)), idx, y);
  if (r < 0) {
    return r;
  }
  for (const auto& [w, shard] : idx) {
    if (w.index > to.index) {
      break;
    }
    if (windows.empty() || windows.back() != w) {
      windows.push_back(w); // the index is sorted by window
    }
  }
  return 0;
}

int RadosITEAggStore::remove_window(const DoutPrefixProvider* dpp,
                                    const rgw_bucket& bucket,
                                    const rgw::ite::Window& window,
                                    optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  std::vector<std::pair<rgw::ite::Window, uint32_t>> idx;
  int r = read_index(dpp, bucket, "", index_window_prefix(window) + ".", idx, y);
  if (r < 0) {
    return r;
  }
  std::set<std::string> keys;
  for (const auto& [w, shard] : idx) {
    // the objects first, so an interrupted removal is found again
    librados::ObjectWriteOperation op;
    op.remove();
    r = rgw_rados_operate(dpp, ioctx, window_oid(bucket, w, shard), std::move(op), y);
    if (r < 0 && r != -ENOENT) {
      ldpp_dout(dpp, 1) << "ERROR: failed to remove " << window_oid(bucket, w, shard)
                        << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    keys.insert(index_key(w, shard));
  }
  if (keys.empty()) {
    return 0;
  }
  librados::ObjectWriteOperation op;
  op.omap_rm_keys(keys);
  r = rgw_rados_operate(dpp, ioctx, index_oid(bucket), std::move(op), y);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  std::lock_guard l{lock};
  for (const auto& key : keys) {
    indexed.erase(index_oid(bucket) + "/" + key);
  }
  return 0;
}

int RadosITEAggStore::put_bucket(const DoutPrefixProvider* dpp,
                                 const rgw::ite::BucketProgress& progress,
                                 optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  std::map<std::string, ceph::buffer::list> kv;
  encode(progress, kv[progress.bucket.get_key()]);
  librados::ObjectWriteOperation op;
  op.omap_set(kv);
  return rgw_rados_operate(dpp, ioctx, dir_oid, std::move(op), y);
}

int RadosITEAggStore::get_bucket(const DoutPrefixProvider* dpp,
                                 const rgw_bucket& bucket,
                                 rgw::ite::BucketProgress* progress,
                                 optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  const std::string key = bucket.get_key();
  std::map<std::string, ceph::buffer::list> vals;
  int rval = 0;
  librados::ObjectReadOperation op;
  op.omap_get_vals_by_keys({key}, &vals, &rval);
  int r = rgw_rados_operate(dpp, ioctx, dir_oid, std::move(op), nullptr, y);
  if (r < 0) {
    return r;
  }
  auto i = vals.find(key);
  if (i == vals.end()) {
    return -ENOENT;
  }
  try {
    auto p = i->second.cbegin();
    decode(*progress, p);
  } catch (const ceph::buffer::error& e) {
    ldpp_dout(dpp, 0) << "ERROR: corrupt ITE directory entry " << key << dendl;
    return -EIO;
  }
  return 0;
}

int RadosITEAggStore::remove_bucket(const DoutPrefixProvider* dpp,
                                    const rgw_bucket& bucket, optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  librados::ObjectWriteOperation op;
  op.omap_rm_keys({bucket.get_key()});
  int r = rgw_rados_operate(dpp, ioctx, dir_oid, std::move(op), y);
  return r == -ENOENT ? 0 : r;
}

int RadosITEAggStore::list_buckets(const DoutPrefixProvider* dpp,
                                   const std::string& marker, uint32_t max,
                                   std::vector<rgw::ite::BucketProgress>& buckets,
                                   std::string* next_marker, bool* truncated,
                                   optional_yield y)
{
  if (!initialized) {
    return -EINVAL;
  }
  buckets.clear();
  *truncated = false;
  std::map<std::string, ceph::buffer::list> vals;
  int rval = 0;
  librados::ObjectReadOperation op;
  op.omap_get_vals2(marker, std::max<uint32_t>(1, max), &vals, truncated, &rval);
  int r = rgw_rados_operate(dpp, ioctx, dir_oid, std::move(op), nullptr, y);
  if (r == -ENOENT) {
    *truncated = false;
    next_marker->clear();
    return 0;
  }
  if (r < 0) {
    return r;
  }
  for (const auto& [key, bl] : vals) {
    rgw::ite::BucketProgress p;
    try {
      auto i = bl.cbegin();
      decode(p, i);
    } catch (const ceph::buffer::error& e) {
      ldpp_dout(dpp, 0) << "ERROR: skipping corrupt ITE directory entry " << key << dendl;
      continue;
    }
    buckets.push_back(std::move(p));
  }
  *next_marker = vals.empty() ? marker : vals.rbegin()->first;
  return 0;
}

std::unique_ptr<ITESerializer>
RadosITEAggStore::get_bucket_serializer(const rgw_bucket& bucket,
                                        const std::string& cookie)
{
  return std::make_unique<RadosITESerializer>(ioctx, index_oid(bucket),
                                              ITE_BUCKET_LOCK_NAME, cookie);
}

std::unique_ptr<ITEWriteBuffer> RadosStore::get_ite_write_buffer()
{
  return std::make_unique<RadosITEWriteBuffer>(this);
}

std::unique_ptr<ITEAggStore> RadosStore::get_ite_agg_store()
{
  return std::make_unique<RadosITEAggStore>(this);
}

} // namespace rgw::sal
