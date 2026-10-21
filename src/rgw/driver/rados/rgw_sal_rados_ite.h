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

} // namespace rgw::sal
