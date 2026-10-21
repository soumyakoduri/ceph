// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): the RADOS implementation of the four
 * SAL contracts.
 *
 * Everything lives in the rgw_ite_pool pool (.rgw.ite), in a namespace named
 * by the zone id, so ITE state is per zone even when zones share a cluster:
 *
 *   contract one   write buffer   cls_fifo objects ite-log.0 .. ite-log.N-1
 *   contract two   agg store      ite-agg.<bucket>.<stamp>.<shard> omap
 *                                 ite-agg-idx.<bucket> window index omap
 *                                 ite-buckets, the ITE bucket directory
 *   contract three state          rgw_bucket_ite_state on the index entry
 *   contract four  move queue     cls_fifo objects ite-move.0 .. ite-move.N-1
 */

#pragma once

#include <deque>
#include <optional>
#include <set>
#include <thread>

#include "rgw_sal_rados.h"

namespace rgw::sal {

/* the zone's ITE pool and namespace */
rgw_pool ite_pool(RadosStore* store);
int ite_open_pool(const DoutPrefixProvider* dpp, RadosStore* store,
                  librados::IoCtx& ioctx, neorados::IOContext& neo_ioctx,
                  optional_yield y);

/* an exclusive cls_lock on one ITE object */
class RadosITESerializer : public StoreITESerializer {
  librados::IoCtx ioctx;
  ::rados::cls::lock::Lock lock;

public:
  RadosITESerializer(const librados::IoCtx& ioctx, const std::string& oid,
                     const std::string& lock_name, const std::string& cookie);

  int try_lock(const DoutPrefixProvider* dpp, ceph::timespan dur,
               optional_yield y) override;
  int unlock(const DoutPrefixProvider* dpp, optional_yield y) override;
};

/* a fixed set of cls_fifo shards <prefix>.0 .. <prefix>.N-1 in the ITE
 * pool. Contracts one and four are each one of these, mirroring the
 * Restore queue. */
class RadosITEFifoSet {
  RadosStore* const store;
  const std::string prefix;
  const uint32_t shards;
  librados::IoCtx ioctx;
  neorados::IOContext neo_ioctx;
  std::vector<std::unique_ptr<fifo::FIFO>> fifos;

public:
  RadosITEFifoSet(RadosStore* store, std::string prefix, uint32_t shards);

  int initialize(const DoutPrefixProvider* dpp, optional_yield y);
  bool initialized() const { return fifos.size() == shards; }
  uint32_t num_shards() const { return shards; }
  std::string oid(uint32_t shard) const;

  int push(const DoutPrefixProvider* dpp, uint32_t shard,
           std::deque<ceph::buffer::list>&& entries, optional_yield y);
  /* entries after cursor; *truncated if more follow */
  int list(const DoutPrefixProvider* dpp, uint32_t shard,
           const std::string& cursor, uint32_t max,
           std::vector<fifo::entry>& entries, bool* truncated,
           optional_yield y);
  /* trim up to and including cursor */
  int trim(const DoutPrefixProvider* dpp, uint32_t shard,
           const std::string& cursor, optional_yield y);
  std::unique_ptr<ITESerializer> get_serializer(uint32_t shard,
                                                const std::string& cookie);
};

/* contract one. append() only queues the event in memory; a flusher thread
 * pushes each shard's batch to its FIFO with one operation, so recording an
 * access costs the request no I/O. The queue is bounded: when the cluster
 * cannot keep up, events are refused rather than letting memory grow. */
class RadosITEWriteBuffer : public StoreITEWriteBuffer {
  CephContext* const cct;
  RadosITEFifoSet fifos;

  ceph::mutex lock = ceph::make_mutex("RadosITEWriteBuffer::lock");
  ceph::condition_variable cond;
  std::vector<std::deque<ceph::buffer::list>> pending; // per shard
  uint64_t num_pending = 0;
  bool stopping = false;
  std::thread flusher;
  // one flush at a time keeps each shard's events in order
  ceph::mutex flush_lock = ceph::make_mutex("RadosITEWriteBuffer::flush_lock");

  void flush_loop();
  int flush_pending(const DoutPrefixProvider* dpp, optional_yield y);

public:
  static constexpr uint64_t max_pending = 128 * 1024;
  static constexpr uint64_t flush_batch = 4096;
  static constexpr auto flush_interval = std::chrono::seconds(1);

  explicit RadosITEWriteBuffer(RadosStore* store);
  ~RadosITEWriteBuffer() override;

  int initialize(const DoutPrefixProvider* dpp, optional_yield y) override;
  uint32_t num_shards() const override { return fifos.num_shards(); }
  int append(const DoutPrefixProvider* dpp, const rgw::ite::AccessEvent& event,
             optional_yield y) override;
  int flush(const DoutPrefixProvider* dpp, optional_yield y) override;
  int list(const DoutPrefixProvider* dpp, uint32_t shard,
           const std::string& cursor, uint32_t max,
           std::vector<rgw::ite::AccessEventEntry>& entries,
           std::string* next_cursor, bool* truncated,
           optional_yield y) override;
  int trim(const DoutPrefixProvider* dpp, uint32_t shard,
           const std::string& cursor, optional_yield y) override;
  std::unique_ptr<ITESerializer> get_serializer(uint32_t shard,
                                                const std::string& cookie) override;
};

/* contract two. Each bucket window shard is one object,
 * ite-agg.<bucket>.<stamp>.<shard>, whose omap holds the window's count keys
 * and the progress marker of every write-buffer shard that contributed. The
 * cls_rgw method ite_agg_add applies a batch and advances its marker in one
 * atomic step, so a replay cannot double-count. ite-agg-idx.<bucket> indexes
 * the window shard objects that exist, written before the first count lands
 * in one, so expiry never misses an object. ite-buckets is the directory of
 * ITE buckets with each worker's progress. */
class RadosITEAggStore : public StoreITEAggStore {
  RadosStore* const store;
  CephContext* const cct;
  const uint32_t shards;
  librados::IoCtx ioctx;
  neorados::IOContext neo_ioctx;
  bool initialized = false;

  // index entries this process already wrote
  ceph::mutex lock = ceph::make_mutex("RadosITEAggStore");
  std::set<std::string> indexed;

  std::string window_oid(const rgw_bucket& bucket, const rgw::ite::Window& w,
                         uint32_t shard) const;
  int index_window(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                   const rgw::ite::Window& w, uint32_t shard, optional_yield y);
  /* add the counts held by one window shard object to counts */
  int read_window_object(const DoutPrefixProvider* dpp, const std::string& oid,
                         std::map<std::string, uint64_t>& counts, optional_yield y);
  /* the (window, shard) objects recorded for a bucket whose index keys
   * follow start_after and begin with prefix, in key order */
  int read_index(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                 const std::string& start_after, const std::string& prefix,
                 std::vector<std::pair<rgw::ite::Window, uint32_t>>& out,
                 optional_yield y);

public:
  static constexpr const char* dir_oid = "ite-buckets";
  static std::string index_oid(const rgw_bucket& bucket);
  static std::string index_key(const rgw::ite::Window& w, uint32_t shard);
  static std::optional<std::pair<rgw::ite::Window, uint32_t>>
  parse_index_key(const std::string& key);

  explicit RadosITEAggStore(RadosStore* store);

  int initialize(const DoutPrefixProvider* dpp, optional_yield y) override;
  uint32_t num_shards() const override { return shards; }
  int get_marker(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                 const rgw::ite::Window& window, uint32_t agg_shard,
                 uint32_t buffer_shard, std::string* marker,
                 optional_yield y) override;
  int add_counts(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                 const rgw::ite::Window& window, uint32_t agg_shard,
                 uint32_t buffer_shard, const std::string& expected_marker,
                 const std::string& new_marker,
                 const std::map<std::string, uint64_t>& deltas,
                 optional_yield y) override;
  int read_counts(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                  const rgw::ite::Window& window,
                  std::map<std::string, uint64_t>& counts,
                  optional_yield y) override;
  int read_shard_counts(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                        const rgw::ite::Window& window, uint32_t agg_shard,
                        std::map<std::string, uint64_t>& counts,
                        optional_yield y) override;
  int list_windows(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                   const rgw::ite::Window& from, const rgw::ite::Window& to,
                   std::vector<rgw::ite::Window>& windows,
                   optional_yield y) override;
  int remove_window(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                    const rgw::ite::Window& window, optional_yield y) override;
  int put_bucket(const DoutPrefixProvider* dpp,
                 const rgw::ite::BucketProgress& progress,
                 optional_yield y) override;
  int get_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                 rgw::ite::BucketProgress* progress, optional_yield y) override;
  int remove_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& bucket,
                    optional_yield y) override;
  int list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
                   uint32_t max, std::vector<rgw::ite::BucketProgress>& buckets,
                   std::string* next_marker, bool* truncated,
                   optional_yield y) override;
  std::unique_ptr<ITESerializer> get_bucket_serializer(
      const rgw_bucket& bucket, const std::string& cookie) override;
};

/* contract three. The ITE block lives on the object's bucket index entry
 * (decision D1). get and set address one entry through cls_rgw, the same
 * entry for both, including a null version behind a version marker; list
 * is an ordered bucket listing with every version, one sequential pass. */
class RadosITEStateStore : public StoreITEStateStore {
  RadosStore* const store;

public:
  explicit RadosITEStateStore(RadosStore* store) : store(store) {}

  static rgw::ite::ObjectEntry to_object_entry(const rgw_bucket_dir_entry& e);

  int get_state(const DoutPrefixProvider* dpp, Bucket* bucket,
                const rgw_obj_key& key, rgw::ite::ObjectEntry* entry,
                optional_yield y) override;
  int set_state(const DoutPrefixProvider* dpp, Bucket* bucket,
                const rgw_obj_key& key, const rgw_bucket_ite_state& state,
                uint32_t mask, uint64_t expected_gen, optional_yield y) override;
  int list_state(const DoutPrefixProvider* dpp, Bucket* bucket,
                 const rgw_obj_key& marker, uint32_t max,
                 std::vector<rgw::ite::ObjectEntry>& entries,
                 rgw_obj_key* next_marker, bool* truncated,
                 optional_yield y) override;
};

} // namespace rgw::sal
