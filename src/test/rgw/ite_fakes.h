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
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include <fmt/format.h>

#include "rgw_ite_types.h"
#include "rgw_sal_rados.h"
#include "rgw_sal_store.h"

namespace rgw::ite::test {

/* exclusive locks by name, shared by every serializer of one fake */
struct MemLocks {
  std::map<std::string, std::string> held; // lock name -> cookie
  std::set<std::string> busy; // held by "another gateway"
  int taken = 0;
  int renewed = 0;
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
      if (i->second != cookie) {
        return -EBUSY;
      }
      ++locks->renewed; // the holder renews
      return 0;
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
  int read_shard_counts(const DoutPrefixProvider*, const rgw_bucket& b, const Window& w,
                        uint32_t agg_shard, std::map<std::string, uint64_t>& counts,
                        optional_yield) override {
    counts.clear();
    if (auto i = windows.find(Key{b.get_key(), w, agg_shard}); i != windows.end()) {
      counts = i->second.counts;
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

/* contract four */
struct MemMoveQueue : public rgw::sal::StoreITEMoveQueue {
  uint32_t shards;
  std::vector<std::vector<MoveActionEntry>> data;
  uint64_t pos = 0;
  MemLocks locks;
  int append_ret = 0;
  int list_ret = 0;
  int ack_ret = 0;
  int acks = 0;

  explicit MemMoveQueue(uint32_t shards = 4) : shards(shards), data(shards) {}

  int initialize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  uint32_t num_shards() const override { return shards; }
  int append(const DoutPrefixProvider*, const MoveAction& a, optional_yield) override {
    if (append_ret < 0) {
      return append_ret;
    }
    data[move_shard_of(a, shards)].push_back({mem_cursor(++pos), a});
    return 0;
  }
  int list(const DoutPrefixProvider*, uint32_t shard, const std::string& cursor,
           uint32_t max, std::vector<MoveActionEntry>& entries,
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
  int ack(const DoutPrefixProvider*, uint32_t shard, const std::string& cursor,
          optional_yield) override {
    if (ack_ret < 0) {
      return ack_ret;
    }
    ++acks;
    std::erase_if(data.at(shard), [&](const MoveActionEntry& e) { return e.marker <= cursor; });
    return 0;
  }
  std::unique_ptr<rgw::sal::ITESerializer> get_serializer(uint32_t shard,
                                                          const std::string& cookie) override {
    return std::make_unique<MemSerializer>(&locks, fmt::format("ite-move.{}", shard), cookie);
  }

  /* every queued action, in shard then queue order */
  std::vector<MoveAction> all() const {
    std::vector<MoveAction> out;
    for (const auto& d : data) {
      for (const auto& e : d) {
        out.push_back(e.action);
      }
    }
    return out;
  }
};

/* add n accesses of one instance to a window, in the instance's shard */
inline void add_count(MemAggStore& agg, const rgw_bucket& b, const Window& w,
                      const std::string& name, uint64_t gen, Op op, uint64_t n,
                      CallerType caller = CallerType::User)
{
  CountKey k;
  k.obj = InstanceId(name, "", gen);
  k.op = op;
  k.caller = caller;
  const uint32_t shard = agg_shard_of(name, agg.num_shards());
  agg.windows[MemAggStore::Key{b.get_key(), w, shard}].counts[k.to_str()] += n;
}

/* contract three, with the cls setter's rules: masks, the generation
 * check, and base_gen stamped on first write */
struct MemStateStore : public rgw::sal::StoreITEStateStore {
  // bucket key -> object key -> entry
  std::map<std::string, std::map<rgw_obj_key, ObjectEntry>> objects;
  int set_ret = 0;
  int sets = 0;
  int gets = 0;
  std::function<void(const rgw_obj_key&)> before_set;

  ObjectEntry& put(const rgw_bucket& b, const std::string& name, uint64_t gen,
                   uint64_t size = 1 << 20, std::string instance = "") {
    ObjectEntry e;
    e.key = rgw_obj_key(name, instance);
    e.gen = gen;
    e.size = size;
    e.mtime = ceph::real_clock::now();
    e.storage_class = "INTELLIGENT_TIER";
    e.ite.base_gen = gen; // a fresh instance, as complete_op stamps it
    return objects[b.get_key()][e.key] = e;
  }
  ObjectEntry* find(const rgw_bucket& b, const rgw_obj_key& k) {
    auto i = objects.find(b.get_key());
    if (i == objects.end()) {
      return nullptr;
    }
    auto j = i->second.find(k);
    return j == i->second.end() ? nullptr : &j->second;
  }

  int get_state(const DoutPrefixProvider*, rgw::sal::Bucket* bucket,
                const rgw_obj_key& key, ObjectEntry* entry, optional_yield) override {
    ++gets;
    auto e = find(bucket->get_key(), key);
    if (!e || !e->exists) {
      return -ENOENT;
    }
    *entry = *e;
    return 0;
  }
  int set_state(const DoutPrefixProvider*, rgw::sal::Bucket* bucket,
                const rgw_obj_key& key, const rgw_bucket_ite_state& state,
                uint32_t mask, uint64_t expected_gen, optional_yield) override {
    if (before_set) {
      before_set(key);
    }
    if (set_ret < 0) {
      return set_ret;
    }
    auto e = find(bucket->get_key(), key);
    if (!e || !e->exists) {
      return -ENOENT;
    }
    if (expected_gen && e->ite.base_gen && e->ite.base_gen != expected_gen) {
      return -ECANCELED;
    }
    e->ite.apply(state, mask);
    if (!e->ite.base_gen) {
      e->ite.base_gen = expected_gen ? expected_gen : e->gen;
    }
    ++sets;
    return 0;
  }
  int list_state(const DoutPrefixProvider*, rgw::sal::Bucket* bucket,
                 const rgw_obj_key& marker, uint32_t max,
                 std::vector<ObjectEntry>& entries, rgw_obj_key* next,
                 bool* truncated, optional_yield) override {
    entries.clear();
    *next = marker;
    *truncated = false;
    auto& m = objects[bucket->get_key().get_key()];
    auto i = marker.empty() ? m.begin() : m.upper_bound(marker);
    for (; i != m.end(); ++i) {
      if (entries.size() == max) {
        *truncated = true;
        break;
      }
      entries.push_back(i->second);
      *next = i->first;
    }
    return 0;
  }

  /* an overwrite: a new instance at generation gen, starting cold */
  void overwrite(const rgw_bucket& b, const std::string& name, uint64_t gen) {
    auto e = find(b, rgw_obj_key(name));
    ceph_assert(e);
    e->gen = gen;
    e->mtime = ceph::real_clock::now();
    e->ite = rgw_bucket_ite_state();
    e->ite.base_gen = gen;
  }
};

/* where each instance's bytes are, behind the object calls of the mover.
 * Moves are checked against the state store's generations, as a backend
 * checks them against its index. */
struct MemPlacements {
  MemStateStore* state = nullptr;
  // bucket key -> object key -> placement; hot unless moved
  std::map<std::string, std::map<rgw_obj_key, Placement>> where;
  int get_ret = 0;
  int move_ret = 0;
  int moves = 0;
  bool stuck = false; // moves report success but leave the bytes in place
  std::function<void(const rgw_obj_key&)> before_move;

  explicit MemPlacements(MemStateStore* state) : state(state) {}

  Placement at(const rgw_bucket& b, const std::string& name) {
    auto& m = where[b.get_key()];
    auto i = m.find(rgw_obj_key(name));
    return i == m.end() ? Placement::Hot : i->second;
  }
  void set(const rgw_bucket& b, const std::string& name, Placement p) {
    where[b.get_key()][rgw_obj_key(name)] = p;
  }
};

class MemObject : public rgw::sal::RadosObject {
  MemPlacements* p;

  /* the index addresses the null version with an empty instance */
  rgw_obj_key index_key() const {
    rgw_obj_key k = get_key();
    if (k.have_null_instance()) {
      k.instance.clear();
    }
    return k;
  }

public:
  MemObject(MemPlacements* p, const rgw_obj_key& k, rgw::sal::Bucket* b)
    : RadosObject(nullptr, k, b), p(p) {}

  int ite_get_placement(const DoutPrefixProvider*, Placement* placement,
                        optional_yield) override {
    if (p->get_ret < 0) {
      return p->get_ret;
    }
    auto e = p->state->find(get_bucket()->get_key(), index_key());
    if (!e || !e->exists) {
      return -ENOENT;
    }
    auto& m = p->where[get_bucket()->get_key().get_key()];
    auto i = m.find(index_key());
    *placement = i == m.end() ? Placement::Hot : i->second;
    return 0;
  }
  int ite_move_placement(const DoutPrefixProvider*, Placement target,
                         uint64_t expected_gen, optional_yield) override {
    if (p->before_move) {
      p->before_move(get_key());
    }
    if (p->move_ret < 0) {
      return p->move_ret;
    }
    auto e = p->state->find(get_bucket()->get_key(), index_key());
    if (!e || !e->exists) {
      return -ENOENT;
    }
    if (expected_gen && e->instance().gen != expected_gen) {
      return -ECANCELED;
    }
    ++p->moves;
    if (!p->stuck) {
      p->where[get_bucket()->get_key().get_key()][index_key()] = target;
    }
    return 0;
  }
};

/* a bucket that stores its attributes in memory, and whose objects, given
 * placements, keep theirs in a MemPlacements */
class MemBucket : public rgw::sal::RadosBucket {
  MemPlacements* p;

public:
  int store_ret = 0;
  int stores = 0;

  MemBucket(MemPlacements* p, const RGWBucketInfo& info)
    : RadosBucket(nullptr, info), p(p) {}

  std::unique_ptr<rgw::sal::Object> get_object(const rgw_obj_key& k) override {
    if (!p) {
      return RadosBucket::get_object(k);
    }
    return std::make_unique<MemObject>(p, k, this);
  }
  int merge_and_store_attrs(const DoutPrefixProvider*, rgw::sal::Attrs& new_attrs,
                            optional_yield) override {
    if (store_ret < 0) {
      return store_ret;
    }
    for (const auto& [k, v] : new_attrs) {
      get_attrs()[k] = v;
    }
    ++stores;
    return 0;
  }
};

/* a bucket carrying info and attributes, for the bucket loader; with
 * placements, one whose objects move between them */
inline std::unique_ptr<rgw::sal::Bucket> make_bucket(const rgw_bucket& b, bool enabled,
                                                     std::optional<double> half_life = {},
                                                     MemPlacements* placements = nullptr)
{
  RGWBucketInfo info;
  info.bucket = b;
  auto bucket = std::make_unique<MemBucket>(placements, info);
  BucketConfig c;
  c.enabled = enabled;
  c.half_life_days = half_life;
  ceph::buffer::list bl;
  encode(c, bl);
  rgw::sal::Attrs attrs;
  attrs[std::string(ATTR_ITE)] = bl;
  bucket->set_attrs(attrs);
  return bucket;
}

} // namespace rgw::ite::test
