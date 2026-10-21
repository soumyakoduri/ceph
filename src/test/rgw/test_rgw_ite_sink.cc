// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/* The ITE access-log sink: which requests become access events, and what
 * the events carry. A fake write buffer records the appends. */

#include "rgw_ite_sink.h"

#include <gtest/gtest.h>

#include "global/global_context.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_sal_rados.h"

using namespace rgw::ite;
namespace sal = rgw::sal;

namespace {

struct FakeWriteBuffer : public sal::StoreITEWriteBuffer {
  std::vector<AccessEvent> events;
  int ret = 0;

  int initialize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  uint32_t num_shards() const override { return 1; }
  int append(const DoutPrefixProvider*, const AccessEvent& e, optional_yield) override {
    if (ret < 0) {
      return ret;
    }
    events.push_back(e);
    return 0;
  }
  int list(const DoutPrefixProvider*, uint32_t, const std::string&, uint32_t,
           std::vector<AccessEventEntry>&, std::string*, bool*,
           optional_yield) override { return 0; }
  int trim(const DoutPrefixProvider*, uint32_t, const std::string&,
           optional_yield) override { return 0; }
  std::unique_ptr<sal::ITESerializer> get_serializer(uint32_t,
                                                     const std::string&) override {
    return nullptr;
  }
};

struct FakeOp : public RGWOp {
  RGWOpType type;
  explicit FakeOp(RGWOpType t, int ret = 0) : type(t) { op_ret = ret; }
  int verify_permission(optional_yield) override { return 0; }
  void execute(optional_yield) override {}
  const char* name() const override { return "fake"; }
  RGWOpType get_type() override { return type; }
};

/* an object whose head is at a known generation */
struct GenObject : public sal::RadosObject {
  uint64_t gen;
  GenObject(const rgw_obj_key& k, sal::Bucket* b, uint64_t g)
    : sal::RadosObject(nullptr, k, b), gen(g) {}
  uint64_t get_ite_generation() const override { return gen; }
};

sal::Attrs ite_attrs(bool enabled)
{
  BucketConfig c;
  c.enabled = enabled;
  ceph::buffer::list bl;
  encode(c, bl);
  sal::Attrs attrs;
  attrs[std::string(ATTR_ITE)] = bl;
  return attrs;
}

struct SinkTest : public ::testing::Test {
  RGWProcessEnv pe;
  RGWEnv env;
  req_state s{g_ceph_context, pe, &env, 0};
  FakeWriteBuffer buffer;
  Sink sink{g_ceph_context};

  void SetUp() override {
    RGWBucketInfo info;
    info.bucket = rgw_bucket("tenant", "bucket", "bucket-id");
    s.bucket.reset(new sal::RadosBucket(nullptr, info));
    s.bucket->set_attrs(ite_attrs(true));
    s.object = std::make_unique<GenObject>(rgw_obj_key("photo.jpg", "v1"),
                                           s.bucket.get(), 42);
    s.op = OP_GET;
    s.err.http_ret = 200;
    sink.set_buffer(&buffer);
  }
};

} // anonymous namespace

TEST(ITESink, OpMapping)
{
  EXPECT_EQ(Op::Get, op_of(RGW_OP_GET_OBJ, false));
  EXPECT_EQ(Op::Head, op_of(RGW_OP_GET_OBJ, true)); // S3 HEAD is a GET op
  EXPECT_EQ(Op::Head, op_of(RGW_OP_STAT_OBJ, false));
  EXPECT_EQ(Op::Put, op_of(RGW_OP_PUT_OBJ, false));
  EXPECT_EQ(Op::Put, op_of(RGW_OP_POST_OBJ, false));
  EXPECT_EQ(Op::Put, op_of(RGW_OP_COPY_OBJ, false));
  EXPECT_EQ(Op::Put, op_of(RGW_OP_COMPLETE_MULTIPART, false));
  EXPECT_EQ(Op::Delete, op_of(RGW_OP_DELETE_OBJ, false));
  EXPECT_FALSE(op_of(RGW_OP_LIST_BUCKET, false));
  EXPECT_FALSE(op_of(RGW_OP_DELETE_MULTI_OBJ, false));
  EXPECT_FALSE(op_of(RGW_OP_PUT_OBJ_TAGGING, false));
}

TEST(ITESink, Provenance)
{
  EXPECT_EQ(CallerType::User, caller_of(false));
  EXPECT_EQ(CallerType::Replication, caller_of(true));
}

TEST(ITESink, Helpers)
{
  EXPECT_TRUE(bucket_enabled(ite_attrs(true)));
  EXPECT_FALSE(bucket_enabled(ite_attrs(false)));
  EXPECT_FALSE(bucket_enabled({}));
  EXPECT_EQ("", index_instance("null"));
  EXPECT_EQ("", index_instance(""));
  EXPECT_EQ("v1", index_instance("v1"));
  EXPECT_TRUE(http_success(200));
  EXPECT_TRUE(http_success(206));
  EXPECT_FALSE(http_success(304));
  EXPECT_FALSE(http_success(404));
}

TEST_F(SinkTest, GetRecordsServedGeneration)
{
  FakeOp op(RGW_OP_GET_OBJ);
  sink.log(&s, &op);
  ASSERT_EQ(1u, buffer.events.size());
  const auto& e = buffer.events[0];
  EXPECT_EQ(s.bucket->get_key(), e.bucket);
  EXPECT_EQ(InstanceId("photo.jpg", "v1", 42), e.obj);
  EXPECT_EQ(Op::Get, e.op);
  EXPECT_EQ(CallerType::User, e.caller);
  EXPECT_FALSE(ceph::real_clock::is_zero(e.timestamp));
  EXPECT_EQ(1u, sink.num_appended());
}

TEST_F(SinkTest, HeadAndWrites)
{
  s.op = OP_HEAD;
  FakeOp get(RGW_OP_GET_OBJ);
  sink.log(&s, &get);
  s.op = OP_PUT;
  FakeOp put(RGW_OP_PUT_OBJ);
  sink.log(&s, &put);
  s.op = OP_DELETE;
  FakeOp del(RGW_OP_DELETE_OBJ);
  sink.log(&s, &del);
  ASSERT_EQ(3u, buffer.events.size());
  EXPECT_EQ(Op::Head, buffer.events[0].op);
  EXPECT_EQ(42u, buffer.events[0].obj.gen);
  // a write's new generation is not known to the request
  EXPECT_EQ(Op::Put, buffer.events[1].op);
  EXPECT_EQ(0u, buffer.events[1].obj.gen);
  EXPECT_EQ(Op::Delete, buffer.events[2].op);
  EXPECT_EQ(0u, buffer.events[2].obj.gen);
}

TEST_F(SinkTest, NullVersionIsEmptyInstance)
{
  s.object = std::make_unique<GenObject>(rgw_obj_key("o", "null"),
                                         s.bucket.get(), 7);
  FakeOp op(RGW_OP_GET_OBJ);
  sink.log(&s, &op);
  ASSERT_EQ(1u, buffer.events.size());
  EXPECT_EQ("", buffer.events[0].obj.instance);
}

TEST_F(SinkTest, ReplicationIsDropped)
{
  s.system_request = true;
  FakeOp op(RGW_OP_GET_OBJ);
  sink.log(&s, &op);
  EXPECT_TRUE(buffer.events.empty());
  EXPECT_EQ(1u, sink.num_dropped());
}

TEST_F(SinkTest, Filters)
{
  FakeOp get(RGW_OP_GET_OBJ);

  s.err.http_ret = 404;
  sink.log(&s, &get);
  s.err.http_ret = 304; // not modified: the client already had the data
  sink.log(&s, &get);
  s.err.http_ret = 200;

  FakeOp failed(RGW_OP_GET_OBJ, -EIO);
  sink.log(&s, &failed);

  FakeOp list(RGW_OP_LIST_BUCKET);
  sink.log(&s, &list);

  sink.log(&s, nullptr);

  s.bucket->set_attrs(ite_attrs(false));
  sink.log(&s, &get);
  s.bucket->set_attrs({});
  sink.log(&s, &get);
  s.bucket->set_attrs(ite_attrs(true));

  s.object.reset();
  sink.log(&s, &get);

  EXPECT_TRUE(buffer.events.empty());
}

TEST_F(SinkTest, DetachedAndRefused)
{
  FakeOp op(RGW_OP_GET_OBJ);
  buffer.ret = -ENOSPC;
  sink.log(&s, &op);
  EXPECT_EQ(1u, sink.num_failed());
  EXPECT_EQ(0u, sink.num_appended());

  buffer.ret = 0;
  sink.set_buffer(nullptr);
  sink.log(&s, &op);
  EXPECT_TRUE(buffer.events.empty());
}
