// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Intelligent Tiering Engine (ITE): the access-log sink.
 *
 * Every finished frontend request passes through Sink::log(). For a
 * successful object GET, PUT, HEAD or DELETE on an ITE-enabled bucket it
 * builds one AccessEvent and appends it through contract one, the write
 * buffer. That append is the only ITE work on the request path.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <optional>
#include <string>

#include "common/ceph_mutex.h"
#include "include/buffer.h"
#include "rgw_ite_types.h"
#include "rgw_op_type.h"

class CephContext;
class RGWOp;
struct req_state;

namespace rgw::sal {
class ITEWriteBuffer;
}

namespace rgw::ite {

/* the ITE operation a request records, if any. An S3 HEAD of an object is
 * served by the GET operation, so the HTTP method tells the two apart. */
std::optional<Op> op_of(RGWOpType type, bool http_head);

/* caller provenance of a frontend request: the multisite system user marks
 * replication; any other identity, anonymous included, is a user */
CallerType caller_of(bool system_request);

/* whether a bucket's attributes enable ITE on it */
bool bucket_enabled(const std::map<std::string, ceph::buffer::list>& attrs);

/* the version id an index entry carries: the null version is stored with
 * an empty instance */
std::string index_instance(const std::string& instance);

/* only successful requests are access */
inline bool http_success(int http_ret) {
  return http_ret >= 200 && http_ret < 300;
}

class Sink {
  CephContext* const cct;
  ceph::shared_mutex lock = ceph::make_shared_mutex("rgw::ite::Sink");
  rgw::sal::ITEWriteBuffer* buffer = nullptr;

  std::atomic<uint64_t> appended{0};
  std::atomic<uint64_t> dropped{0}; // not USER provenance
  std::atomic<uint64_t> failed{0};  // the write buffer refused it

public:
  explicit Sink(CephContext* cct) : cct(cct) {}

  /* attach the write buffer events go to, or detach it with nullptr; a
   * detach waits for appends in flight */
  void set_buffer(rgw::sal::ITEWriteBuffer* b);

  /* record a finished request; never fails or delays it */
  void log(req_state* s, RGWOp* op);

  uint64_t num_appended() const { return appended; }
  uint64_t num_dropped() const { return dropped; }
  uint64_t num_failed() const { return failed; }
};

} // namespace rgw::ite
