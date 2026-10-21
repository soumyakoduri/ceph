// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* The ITE SAL contracts: the filter layer forwards every operation to the
 * driver underneath and wraps serializers. Fake backends record what they
 * were asked. */

#include "rgw_sal_filter.h"

#include <gtest/gtest.h>

using namespace rgw::sal;
using namespace rgw::ite;

namespace {

struct FakeSerializer : public StoreITESerializer {
  int* locks;
  FakeSerializer(std::string oid, int* l) : StoreITESerializer(std::move(oid)), locks(l) {}
  int try_lock(const DoutPrefixProvider*, ceph::timespan, optional_yield) override {
    ++*locks;
    return 0;
  }
  int unlock(const DoutPrefixProvider*, optional_yield) override {
    --*locks;
    return 0;
  }
};

struct FakeWriteBuffer : public StoreITEWriteBuffer {
  std::vector<AccessEvent> appended;
  std::string trimmed;
  int locks = 0;
  int flushes = 0;

  int initialize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  uint32_t num_shards() const override { return 4; }
  int append(const DoutPrefixProvider*, const AccessEvent& e, optional_yield) override {
    appended.push_back(e);
    return 0;
  }
  int flush(const DoutPrefixProvider*, optional_yield) override {
    ++flushes;
    return 0;
  }
  int list(const DoutPrefixProvider*, uint32_t shard, const std::string& cursor,
           uint32_t max, std::vector<AccessEventEntry>& entries,
           std::string* next, bool* truncated, optional_yield) override {
    entries.clear();
    for (const auto& e : appended) {
      entries.push_back({cursor + "+" + std::to_string(shard), e});
    }
    *next = cursor + "+end";
    *truncated = false;
    return 0;
  }
  int trim(const DoutPrefixProvider*, uint32_t, const std::string& cursor,
           optional_yield) override {
    trimmed = cursor;
    return 0;
  }
  std::unique_ptr<ITESerializer> get_serializer(uint32_t shard,
                                                const std::string&) override {
    return std::make_unique<FakeSerializer>("log." + std::to_string(shard), &locks);
  }
};

struct FakeAggStore : public StoreITEAggStore {
  std::map<std::string, uint64_t> counts;
  std::string marker;
  std::vector<BucketProgress> dir;
  int locks = 0;

  int initialize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  uint32_t num_shards() const override { return 8; }
  int get_marker(const DoutPrefixProvider*, const rgw_bucket&, const Window&,
                 uint32_t, uint32_t, std::string* m, optional_yield) override {
    *m = marker;
    return 0;
  }
  int add_counts(const DoutPrefixProvider*, const rgw_bucket&, const Window&,
                 uint32_t, uint32_t, const std::string& expected,
                 const std::string& next, const std::map<std::string, uint64_t>& deltas,
                 optional_yield) override {
    if (expected != marker) {
      return -ECANCELED;
    }
    for (const auto& [k, v] : deltas) {
      counts[k] += v;
    }
    marker = next;
    return 0;
  }
  int read_counts(const DoutPrefixProvider*, const rgw_bucket&, const Window&,
                  std::map<std::string, uint64_t>& out, optional_yield) override {
    out = counts;
    return 0;
  }
  int read_shard_counts(const DoutPrefixProvider*, const rgw_bucket&, const Window&,
                        uint32_t shard, std::map<std::string, uint64_t>& out,
                        optional_yield) override {
    out = counts;
    out["shard"] = shard;
    return 0;
  }
  int list_windows(const DoutPrefixProvider*, const rgw_bucket&, const Window& from,
                   const Window& to, std::vector<Window>& windows,
                   optional_yield) override {
    windows = {from, to};
    return 0;
  }
  int remove_window(const DoutPrefixProvider*, const rgw_bucket&, const Window&,
                    optional_yield) override {
    counts.clear();
    return 0;
  }
  int put_bucket(const DoutPrefixProvider*, const BucketProgress& p,
                 optional_yield) override {
    dir.push_back(p);
    return 0;
  }
  int get_bucket(const DoutPrefixProvider*, const rgw_bucket&, BucketProgress* p,
                 optional_yield) override {
    if (dir.empty()) {
      return -ENOENT;
    }
    *p = dir.back();
    return 0;
  }
  int remove_bucket(const DoutPrefixProvider*, const rgw_bucket&, optional_yield) override {
    dir.clear();
    return 0;
  }
  int list_buckets(const DoutPrefixProvider*, const std::string&, uint32_t,
                   std::vector<BucketProgress>& out, std::string* next,
                   bool* truncated, optional_yield) override {
    out = dir;
    next->clear();
    *truncated = false;
    return 0;
  }
  std::unique_ptr<ITESerializer> get_bucket_serializer(const rgw_bucket& b,
                                                       const std::string&) override {
    return std::make_unique<FakeSerializer>("bucket." + b.name, &locks);
  }
};

struct FakeStateStore : public StoreITEStateStore {
  Bucket* seen_bucket = reinterpret_cast<Bucket*>(0x1);
  uint32_t last_mask = 0;
  uint64_t last_gen = 0;

  int get_state(const DoutPrefixProvider*, Bucket* b, const rgw_obj_key& key,
                ObjectEntry* e, optional_yield) override {
    seen_bucket = b;
    e->key = key;
    return 0;
  }
  int set_state(const DoutPrefixProvider*, Bucket* b, const rgw_obj_key&,
                const rgw_bucket_ite_state&, uint32_t mask, uint64_t gen,
                optional_yield) override {
    seen_bucket = b;
    last_mask = mask;
    last_gen = gen;
    return 0;
  }
  int list_state(const DoutPrefixProvider*, Bucket* b, const rgw_obj_key& marker,
                 uint32_t, std::vector<ObjectEntry>& entries, rgw_obj_key* next,
                 bool* truncated, optional_yield) override {
    seen_bucket = b;
    entries.clear();
    *next = marker;
    *truncated = false;
    return 0;
  }
};

struct FakeMoveQueue : public StoreITEMoveQueue {
  std::vector<MoveAction> actions;
  std::string acked;
  int locks = 0;

  int initialize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  uint32_t num_shards() const override { return 2; }
  int append(const DoutPrefixProvider*, const MoveAction& a, optional_yield) override {
    actions.push_back(a);
    return 0;
  }
  int list(const DoutPrefixProvider*, uint32_t, const std::string&, uint32_t,
           std::vector<MoveActionEntry>& entries, std::string* next,
           bool* truncated, optional_yield) override {
    entries.clear();
    for (const auto& a : actions) {
      entries.push_back({"m", a});
    }
    *next = "m";
    *truncated = false;
    return 0;
  }
  int ack(const DoutPrefixProvider*, uint32_t, const std::string& cursor,
          optional_yield) override {
    acked = cursor;
    return 0;
  }
  std::unique_ptr<ITESerializer> get_serializer(uint32_t shard,
                                                const std::string&) override {
    return std::make_unique<FakeSerializer>("move." + std::to_string(shard), &locks);
  }
};

} // anonymous namespace

TEST(ITESal, FilterWriteBufferForwards)
{
  auto fake = std::make_unique<FakeWriteBuffer>();
  auto* raw = fake.get();
  FilterITEWriteBuffer f(std::move(fake));

  EXPECT_EQ(0, f.initialize(nullptr, null_yield));
  EXPECT_EQ(4u, f.num_shards());
  AccessEvent e;
  e.obj = InstanceId("o", "", 1);
  EXPECT_EQ(0, f.append(nullptr, e, null_yield));
  ASSERT_EQ(1u, raw->appended.size());

  EXPECT_EQ(0, f.flush(nullptr, null_yield));
  EXPECT_EQ(1, raw->flushes);

  std::vector<AccessEventEntry> entries;
  std::string next;
  bool truncated = true;
  EXPECT_EQ(0, f.list(nullptr, 3, "c", 10, entries, &next, &truncated, null_yield));
  ASSERT_EQ(1u, entries.size());
  EXPECT_EQ("c+3", entries[0].marker);
  EXPECT_EQ("c+end", next);
  EXPECT_FALSE(truncated);

  EXPECT_EQ(0, f.trim(nullptr, 3, "c+3", null_yield));
  EXPECT_EQ("c+3", raw->trimmed);

  auto s = f.get_serializer(2, "cookie");
  ASSERT_TRUE(s);
  EXPECT_EQ(0, s->try_lock(nullptr, std::chrono::seconds(1), null_yield));
  EXPECT_EQ(1, raw->locks);
  EXPECT_EQ(0, s->unlock(nullptr, null_yield));
  EXPECT_EQ(0, raw->locks);
  std::ostringstream os;
  s->print(os);
  EXPECT_EQ("log.2", os.str());
}

TEST(ITESal, FilterAggStoreForwardsAndKeepsCAS)
{
  auto fake = std::make_unique<FakeAggStore>();
  auto* raw = fake.get();
  FilterITEAggStore f(std::move(fake));
  rgw_bucket b("", "b", "id");
  Window w{Granularity::Day, 100};

  EXPECT_EQ(8u, f.num_shards());
  EXPECT_EQ(0, f.add_counts(nullptr, b, w, 0, 1, "", "m1", {{"k", 2}}, null_yield));
  // stale expected marker is refused through the filter too
  EXPECT_EQ(-ECANCELED, f.add_counts(nullptr, b, w, 0, 1, "", "m2", {{"k", 2}}, null_yield));
  std::string m;
  EXPECT_EQ(0, f.get_marker(nullptr, b, w, 0, 1, &m, null_yield));
  EXPECT_EQ("m1", m);
  std::map<std::string, uint64_t> counts;
  EXPECT_EQ(0, f.read_counts(nullptr, b, w, counts, null_yield));
  EXPECT_EQ(2u, counts["k"]);
  EXPECT_EQ(0, f.read_shard_counts(nullptr, b, w, 6, counts, null_yield));
  EXPECT_EQ(2u, counts["k"]);
  EXPECT_EQ(6u, counts["shard"]);

  std::vector<Window> windows;
  EXPECT_EQ(0, f.list_windows(nullptr, b, w, w.next(), windows, null_yield));
  EXPECT_EQ(2u, windows.size());
  EXPECT_EQ(0, f.remove_window(nullptr, b, w, null_yield));
  EXPECT_TRUE(raw->counts.empty());

  BucketProgress p;
  p.bucket = b;
  EXPECT_EQ(0, f.put_bucket(nullptr, p, null_yield));
  BucketProgress got;
  EXPECT_EQ(0, f.get_bucket(nullptr, b, &got, null_yield));
  EXPECT_EQ(b, got.bucket);
  std::vector<BucketProgress> dir;
  std::string next;
  bool truncated = true;
  EXPECT_EQ(0, f.list_buckets(nullptr, "", 10, dir, &next, &truncated, null_yield));
  EXPECT_EQ(1u, dir.size());
  EXPECT_EQ(0, f.remove_bucket(nullptr, b, null_yield));
  EXPECT_EQ(-ENOENT, f.get_bucket(nullptr, b, &got, null_yield));

  auto s = f.get_bucket_serializer(b, "c");
  EXPECT_EQ(0, s->try_lock(nullptr, std::chrono::seconds(1), null_yield));
  EXPECT_EQ(1, raw->locks);
  std::ostringstream os;
  s->print(os);
  EXPECT_EQ("bucket.b", os.str());
}

TEST(ITESal, FilterStateStoreForwardsMaskAndGeneration)
{
  auto fake = std::make_unique<FakeStateStore>();
  auto* raw = fake.get();
  FilterITEStateStore f(std::move(fake));

  rgw_bucket_ite_state st;
  EXPECT_EQ(0, f.set_state(nullptr, nullptr, rgw_obj_key("o"), st,
                           rgw_bucket_ite_state::MASK_TIER, 9, null_yield));
  EXPECT_EQ(nullptr, raw->seen_bucket); // nextBucket(nullptr) stays nullptr
  EXPECT_EQ(rgw_bucket_ite_state::MASK_TIER, raw->last_mask);
  EXPECT_EQ(9u, raw->last_gen);

  ObjectEntry e;
  EXPECT_EQ(0, f.get_state(nullptr, nullptr, rgw_obj_key("o", "v1"), &e, null_yield));
  EXPECT_EQ("v1", e.key.instance);

  std::vector<ObjectEntry> entries;
  rgw_obj_key next;
  bool truncated = true;
  EXPECT_EQ(0, f.list_state(nullptr, nullptr, rgw_obj_key("m"), 10, entries,
                            &next, &truncated, null_yield));
  EXPECT_EQ("m", next.name);
  EXPECT_FALSE(truncated);
}

TEST(ITESal, FilterMoveQueueForwards)
{
  auto fake = std::make_unique<FakeMoveQueue>();
  auto* raw = fake.get();
  FilterITEMoveQueue f(std::move(fake));

  EXPECT_EQ(2u, f.num_shards());
  MoveAction a;
  a.kind = MoveKind::Restore;
  a.target = Placement::Hot;
  EXPECT_EQ(0, f.append(nullptr, a, null_yield));
  std::vector<MoveActionEntry> entries;
  std::string next;
  bool truncated = true;
  EXPECT_EQ(0, f.list(nullptr, 0, "", 10, entries, &next, &truncated, null_yield));
  EXPECT_EQ("m", next);
  ASSERT_EQ(1u, entries.size());
  EXPECT_EQ(MoveKind::Restore, entries[0].action.kind);
  EXPECT_EQ(0, f.ack(nullptr, 0, "m", null_yield));
  EXPECT_EQ("m", raw->acked);

  auto s = f.get_serializer(1, "c");
  EXPECT_EQ(0, s->try_lock(nullptr, std::chrono::seconds(1), null_yield));
  EXPECT_EQ(1, raw->locks);
  std::ostringstream os;
  s->print(os);
  EXPECT_EQ("move.1", os.str());
}

TEST(ITESal, ObjectEntryInstanceUsesBaseGeneration)
{
  ObjectEntry e;
  e.key = rgw_obj_key("o", "v");
  e.gen = 12; // head rewritten by a tier move
  e.ite.base_gen = 7; // instance began here
  EXPECT_EQ(InstanceId("o", "v", 7), e.instance());
  e.ite.base_gen = 0; // entry written before ITE
  EXPECT_EQ(InstanceId("o", "v", 12), e.instance());
}
