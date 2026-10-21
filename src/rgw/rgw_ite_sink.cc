// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_ite_sink.h"

#include <mutex>
#include <shared_mutex>

#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::ite {

std::optional<Op> op_of(RGWOpType type, bool http_head)
{
  switch (type) {
  case RGW_OP_GET_OBJ:
    return http_head ? Op::Head : Op::Get;
  case RGW_OP_STAT_OBJ:
    return Op::Head;
  case RGW_OP_PUT_OBJ:
  case RGW_OP_POST_OBJ:
  case RGW_OP_COPY_OBJ:
  case RGW_OP_COMPLETE_MULTIPART:
    return Op::Put;
  case RGW_OP_DELETE_OBJ:
    return Op::Delete;
  default:
    return std::nullopt;
  }
}

CallerType caller_of(bool system_request)
{
  return system_request ? CallerType::Replication : CallerType::User;
}

bool bucket_enabled(const std::map<std::string, ceph::buffer::list>& attrs)
{
  const auto conf = get_bucket_config(attrs);
  return conf && conf->enabled;
}

std::string index_instance(const std::string& instance)
{
  return instance == "null" ? std::string() : instance;
}

void Sink::set_buffer(rgw::sal::ITEWriteBuffer* b)
{
  std::unique_lock l{lock};
  buffer = b;
}

void Sink::log(req_state* s, RGWOp* op)
{
  if (!op || !s) {
    return;
  }
  const auto ite_op = op_of(op->get_type(), s->op == OP_HEAD);
  if (!ite_op || !http_success(s->err.http_ret) || op->get_ret() < 0) {
    return;
  }
  if (rgw::sal::Bucket::empty(s->bucket.get()) ||
      rgw::sal::Object::empty(s->object.get()) ||
      s->object->get_name().empty()) {
    return;
  }
  // bucket info and attrs come from the bucket instance cache, so this
  // check costs no I/O
  if (!bucket_enabled(s->bucket->get_attrs())) {
    return;
  }

  AccessEvent e;
  e.caller = caller_of(s->system_request);
  if (e.caller != CallerType::User) {
    // Stage 0 records real demand only
    ++dropped;
    return;
  }
  e.bucket = s->bucket->get_key();
  e.obj.name = s->object->get_name();
  e.obj.instance = index_instance(s->object->get_instance());
  // a read knows the generation it served; a write created a new instance
  // whose generation the request does not see, and 0 attributes the event
  // to whatever instance is current when it is scored
  if (*ite_op == Op::Get || *ite_op == Op::Head) {
    e.obj.gen = s->object->get_ite_generation();
  }
  e.timestamp = ceph::real_clock::now();
  e.op = *ite_op;
  if (s->user) {
    e.client_id = s->user->get_id().to_str();
  }

  std::shared_lock l{lock};
  if (!buffer) {
    return;
  }
  int r = buffer->append(s, e, s->yield);
  if (r < 0) {
    if (++failed % 1000 == 1) {
      ldpp_dout(s, 1) << "WARNING: ITE write buffer refused an access event: "
                      << cpp_strerror(-r) << " (" << failed << " so far)" << dendl;
    }
    return;
  }
  ++appended;
}

} // namespace rgw::ite
