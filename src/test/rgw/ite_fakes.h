// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * In-memory implementations of the ITE SAL contracts for the worker unit
 * tests. They follow the contract semantics the RADOS backend implements:
 * sortable cursors, inclusive trims, exclusive per-cookie locks and the
 * marker check-and-set of add_counts(). Each has hooks to inject failures.
 */

#pragma once

#include <functional>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include <fmt/format.h>

#include "rgw_ite_types.h"
#include "rgw_sal_store.h"

namespace rgw::ite::test {

/* exclusive locks by name, shared by every serializer of one fake */
struct MemLocks {
  std::map<std::string, std::string> held; // lock name -> cookie
  std::set<std::string> busy; // held by "another gateway"
  int taken = 0;
};

class MemSerializer : public rgw::sal::StoreITESerializer {
  MemLocks* locks;
  std::string cookie;

public:
  MemSerializer(MemLocks* locks, std::string name, std::string cookie)
    : StoreITESerializer(std::move(name)), locks(locks), cookie(std::move(cookie)) {}

  int try_lock(const DoutPrefixProvider*, ceph::timespan, optional_yield) override {
    if (locks->busy.count(oid)) {
      return -EBUSY;
    }
    auto [i, inserted] = locks->held.try_emplace(oid, cookie);
    if (!inserted) {
      return i->second == cookie ? -EEXIST : -EBUSY; // as cls_lock
    }
    ++locks->taken;
    return 0;
  }
  int unlock(const DoutPrefixProvider*, optional_yield) override {
    auto i = locks->held.find(oid);
    if (i == locks->held.end() || i->second != cookie) {
      return -ENOENT;
    }
    locks->held.erase(i);
    return 0;
  }
};

inline std::string mem_cursor(uint64_t pos)
{
  return fmt::format("{:020}", pos);
}

/* contract one */
struct MemWriteBuffer : public rgw::sal::StoreITEWriteBuffer {
  uint32_t shards;
  std::vector<std::vector<AccessEventEntry>> data;
  uint64_t pos = 0;
  MemLocks locks;
  int list_ret = 0;
  int trim_ret = 0;
  int trims = 0;

  explicit MemWriteBuffer(uint32_t shards = 4) : shards(shards), data(shards) {}

  int initialize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  uint32_t num_shards() const override { return shards; }
  int append(const DoutPrefixProvider*, const AccessEvent& e, optional_yield) override {
    data[buffer_shard_of(e.bucket, e.obj.name, shards)].push_back({mem_cursor(++pos), e});
    return 0;
  }
  int flush(const DoutPrefixProvider*, optional_yield) override { return 0; }
  int list(const DoutPrefixProvider*, uint32_t shard, const std::string& cursor,
           uint32_t max, std::vector<AccessEventEntry>& entries,
           std::string* next_cursor, bool* truncated, optional_yield) override {
    if (list_ret < 0) {
      return list_ret;
    }
    entries.clear();
    *next_cursor = cursor;
    *truncated = false;
    for (const auto& e : data.at(shard)) {
      if (e.marker <= cursor) {
        continue;
      }
      if (entries.size() == max) {
        *truncated = true;
        break;
      }
      entries.push_back(e);
      *next_cursor = e.marker;
    }
    return 0;
  }
  int trim(const DoutPrefixProvider*, uint32_t shard, const std::string& cursor,
           optional_yield) override {
    if (trim_ret < 0) {
      return trim_ret;
    }
    ++trims;
    auto& d = data.at(shard);
    std::erase_if(d, [&](const AccessEventEntry& e) { return e.marker <= cursor; });
    return 0;
  }
  std::unique_ptr<rgw::sal::ITESerializer> get_serializer(uint32_t shard,
                                                          const std::string& cookie) override {
    return std::make_unique<MemSerializer>(&locks, fmt::format("ite-log.{}", shard), cookie);
  }

  size_t size() const {
    size_t n = 0;
    for (const auto& d : data) {
      n += d.size();
    }
    return n;
  }
};

/* contract two */
struct MemAggStore : public rgw::sal::StoreITEAggStore {
  struct WindowShard {
    std::map<std::string, uint64_t> counts;
    std::map<uint32_t, std::string> markers; // buffer shard -> cursor
  };
  using Key = std::tuple<std::string, Window, uint32_t>; // bucket, window, shard

  uint32_t shards;
  std::map<Key, WindowShard> windows;
  std::map<std::string, BucketProgress> dir;
  MemLocks locks;
  int add_ret = 0;
  int adds = 0;
  std::function<void(const Key&, uint32_t buffer_shard)> before_add;

  explicit MemAggStore(uint32_t shards = 2) : shards(shards) {}

  int initialize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  uint32_t num_shards() const override { return shards; }
  int get_marker(const DoutPrefixProvider*, const rgw_bucket& b, const Window& w,
                 uint32_t agg_shard, uint32_t buffer_shard, std::string* marker,
                 optional_yield) override {
    marker->clear();
    auto i = windows.find(Key{b.get_key(), w, agg_shard});
    if (i != windows.end()) {
      if (auto m = i->second.markers.find(buffer_shard); m != i->second.markers.end()) {
        *marker = m->second;
      }
    }
    return 0;
  }
  int add_counts(const DoutPrefixProvider*, const rgw_bucket& b, const Window& w,
                 uint32_t agg_shard, uint32_t buffer_shard,
                 const std::string& expected, const std::string& next,
                 const std::map<std::string, uint64_t>& deltas,
                 optional_yield) override {
    const Key key{b.get_key(), w, agg_shard};
    if (before_add) {
      before_add(key, buffer_shard);
    }
    if (add_ret < 0) {
      return add_ret;
    }
    std::string stored;
    if (auto i = windows.find(key); i != windows.end()) {
      if (auto m = i->second.markers.find(buffer_shard); m != i->second.markers.end()) {
        stored = m->second;
      }
    }
    if (stored != expected) {
      return -ECANCELED; // and, like the cls method, creates nothing
    }
    auto& ws = windows[key];
    for (const auto& [k, n] : deltas) {
      ws.counts[k] += n;
    }
    ws.markers[buffer_shard] = next;
    ++adds;
    return 0;
  }
  int read_counts(const DoutPrefixProvider*, const rgw_bucket& b, const Window& w,
                  std::map<std::string, uint64_t>& counts, optional_yield) override {
    counts.clear();
    for (const auto& [key, ws] : windows) {
      if (std::get<0>(key) == b.get_key() && std::get<1>(key) == w) {
        for (const auto& [k, n] : ws.counts) {
          counts[k] += n;
        }
      }
    }
    return 0;
  }
  int list_windows(const DoutPrefixProvider*, const rgw_bucket& b, const Window& from,
                   const Window& to, std::vector<Window>& out, optional_yield) override {
    out.clear();
    for (const auto& [key, ws] : windows) {
      const Window& w = std::get<1>(key);
      if (std::get<0>(key) == b.get_key() && w.granularity == from.granularity &&
          w.index >= from.index && w.index <= to.index &&
          (out.empty() || out.back() != w)) {
        out.push_back(w);
      }
    }
    return 0;
  }
  int remove_window(const DoutPrefixProvider*, const rgw_bucket& b, const Window& w,
                    optional_yield) override {
    std::erase_if(windows, [&](const auto& kv) {
      return std::get<0>(kv.first) == b.get_key() && std::get<1>(kv.first) == w;
    });
    return 0;
  }
  int put_bucket(const DoutPrefixProvider*, const BucketProgress& p, optional_yield) override {
    dir[p.bucket.get_key()] = p;
    return 0;
  }
  int get_bucket(const DoutPrefixProvider*, const rgw_bucket& b, BucketProgress* p,
                 optional_yield) override {
    auto i = dir.find(b.get_key());
    if (i == dir.end()) {
      return -ENOENT;
    }
    *p = i->second;
    return 0;
  }
  int remove_bucket(const DoutPrefixProvider*, const rgw_bucket& b, optional_yield) override {
    dir.erase(b.get_key());
    return 0;
  }
  int list_buckets(const DoutPrefixProvider*, const std::string& marker, uint32_t max,
                   std::vector<BucketProgress>& out, std::string* next, bool* truncated,
                   optional_yield) override {
    out.clear();
    *next = marker;
    *truncated = false;
    for (auto i = dir.upper_bound(marker); i != dir.end(); ++i) {
      if (out.size() == max) {
        *truncated = true;
        break;
      }
      out.push_back(i->second);
      *next = i->first;
    }
    return 0;
  }
  std::unique_ptr<rgw::sal::ITESerializer> get_bucket_serializer(
      const rgw_bucket& b, const std::string& cookie) override {
    return std::make_unique<MemSerializer>(&locks, "bucket." + b.get_key(), cookie);
  }

  /* the merged count of one key in a bucket window */
  uint64_t count(const rgw_bucket& b, const Window& w, const CountKey& k) const {
    uint64_t n = 0;
    for (const auto& [key, ws] : windows) {
      if (std::get<0>(key) == b.get_key() && std::get<1>(key) == w) {
        if (auto i = ws.counts.find(k.to_str()); i != ws.counts.end()) {
          n += i->second;
        }
      }
    }
    return n;
  }
};

} // namespace rgw::ite::test
