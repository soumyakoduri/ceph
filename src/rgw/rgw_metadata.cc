// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/intrusive_ptr.hpp>
#include "common/ceph_json.h"
#include "common/errno.h"
#include "rgw_metadata.h"
#include "rgw_coroutine.h"
#include "cls/version/cls_version_types.h"

#include "rgw_zone.h"
#include "rgw_tools.h"
#include "rgw_mdlog.h"
#include "rgw_sal.h"
#include "rgw_sync.h"

#include "rgw_cr_rados.h"

#include "services/svc_zone.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_cls.h"

#include "include/ceph_assert.h"

#include "cls_fifo_legacy.h" 
#include "cls/fifo/cls_fifo_types.h"

#include "common/async/librados_completion.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

namespace bs = boost::system;

const std::string RGWMetadataLogHistory::oid = "meta.history";

void LogStatusDump::dump(Formatter *f) const {
  string s;
  switch (status) {
    case MDLOG_STATUS_WRITE:
      s = "write";
      break;
    case MDLOG_STATUS_SETATTRS:
      s = "set_attrs";
      break;
    case MDLOG_STATUS_REMOVE:
      s = "remove";
      break;
    case MDLOG_STATUS_COMPLETE:
      s = "complete";
      break;
    case MDLOG_STATUS_ABORT:
      s = "abort";
      break;
    default:
      s = "unknown";
      break;
  }
  encode_json("status", s, f);
}

void RGWMetadataLogData::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(read_version, bl);
  encode(write_version, bl);
  uint32_t s = (uint32_t)status;
  encode(s, bl);
  ENCODE_FINISH(bl);
}

void RGWMetadataLogData::decode(bufferlist::const_iterator& bl) {
   DECODE_START(1, bl);
   decode(read_version, bl);
   decode(write_version, bl);
   uint32_t s;
   decode(s, bl);
   status = (RGWMDLogStatus)s;
   DECODE_FINISH(bl);
}

void RGWMetadataLogData::dump(Formatter *f) const {
  encode_json("read_version", read_version, f);
  encode_json("write_version", write_version, f);
  encode_json("status", LogStatusDump(status), f);
}

void decode_json_obj(RGWMDLogStatus& status, JSONObj *obj) {
  string s;
  JSONDecoder::decode_json("status", s, obj);
  if (s == "complete") {
    status = MDLOG_STATUS_COMPLETE;
  } else if (s == "write") {
    status = MDLOG_STATUS_WRITE;
  } else if (s == "remove") {
    status = MDLOG_STATUS_REMOVE;
  } else if (s == "set_attrs") {
    status = MDLOG_STATUS_SETATTRS;
  } else if (s == "abort") {
    status = MDLOG_STATUS_ABORT;
  } else {
    status = MDLOG_STATUS_UNKNOWN;
  }
}

void RGWMetadataLogData::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("read_version", read_version, obj);
  JSONDecoder::decode_json("write_version", write_version, obj);
  JSONDecoder::decode_json("status", status, obj);
}


RGWMetadataLogTrimCR::RGWMetadataLogTrimCR(CephContext *cct,
					     RGWMetadataLog *_mdlog,
                                             const int _shard_id,
                                             const std::string& _marker,
					     bool _exclusive,
					     std::string *_last_trim_marker)
  : RGWSimpleCoroutine(cct), mdlog(_mdlog), shard_id(_shard_id),
    marker(_marker), exclusive(_exclusive), last_trim_marker(_last_trim_marker)
{
  set_description() << "MetadataLog trim oid=" <<  shard_id
      << " marker=" << marker;
}

int RGWMetadataLogTrimCR::send_request()
{
  set_status() << "sending request";

  cn = stack->create_completion_notifier();

  return mdlog->trim(shard_id, marker, cn->completion(), exclusive);
}

int RGWMetadataLogTrimCR::request_complete()
{
  int r = cn->completion()->get_return_value();

  set_status() << "request complete; ret=" << r;

  if (r != -ENODATA) {
    return r;
  }
  // nothing left to trim, update last_trim_marker
  if (last_trim_marker && *last_trim_marker < marker &&
	 marker != mdlog->max_marker()) {
    *last_trim_marker = marker;
    r = 0;
  }
  return r;
}

class RGWMetadataLogOmap final : public RGWMetadataLogBE {
  RGWSI_Cls& cls;
  std::vector<std::string> oids;
public:
  RGWMetadataLogOmap(CephContext* cct, std::string prefix, RGWSI_Cls& cls)
    : RGWMetadataLogBE(cct, prefix), cls(cls) {
    auto num_shards = cct->_conf->rgw_md_log_max_shards;
    oids.reserve(num_shards);
    for (auto i = 0; i < num_shards; ++i) {
      oids.push_back(RGWMetadataLogBE::get_shard_oid(i));
    }
  }
  ~RGWMetadataLogOmap() override = default;
  static int exists(CephContext* cct, RGWSI_Cls& cls, std::string prefix,
		    bool* exists, bool* has_entries) {
    auto num_shards = cct->_conf->rgw_md_log_max_shards;
    std::string out_marker;
    bool truncated = false;
    std::list<cls_log_entry> log_entries;
    const cls_log_header empty_info;
    *exists = false;
    *has_entries = false;
    for (auto i = 0; i < num_shards; ++i) {
      auto oid = fmt::format("{}{}", prefix, i);
      cls_log_header info;
      auto r = cls.timelog.info(oid, &info, null_yield);
      if (r < 0 && r != -ENOENT) {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": failed to get info " << oid << ": " << cpp_strerror(-r)
		   << dendl;
	return r;
      } else if ((r == -ENOENT) || (info == empty_info)) {
	continue;
      }
      *exists = true;
      r = cls.timelog.list(oid, {}, {}, 100, log_entries, "", &out_marker,
			   &truncated, null_yield);
      if (r < 0) {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": failed to list " << oid << ": " << cpp_strerror(-r)
		   << dendl;
	return r;
      } else if (!log_entries.empty()) {
	*has_entries = true;
	break; // No reason to continue, once we have both existence
	       // AND non-emptiness
      }
    }
    return 0;
  }
  int push(int index, std::vector<cls_log_entry>& items, librados::AioCompletion *&completion) override {
    std::list<cls_log_entry> lentries(std::move_iterator(items.begin()),
				      std::move_iterator(items.end()));

    auto r = cls.timelog.add(oids[index], lentries,
			     completion, false, null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to push to " << oids[index] << cpp_strerror(-r)
		 << dendl;
    }
    return r;
  }
  int push(int index, ceph::real_time now,
	   const std::string& section,
	   const std::string& key,
	   ceph::buffer::list& bl) override {
    auto r = cls.timelog.add(oids[index], now, section, key, bl, null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to push to " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int list(int index, int max_entries,
	   std::vector<cls_log_entry>& e,
	   std::string_view marker,
	   std::string* out_marker, bool* truncated) override {
    std::list<cls_log_entry> lentries;
    auto r = cls.timelog.list(oids[index], {}, {},
			      max_entries, lentries,
			      string(marker),
			      out_marker, truncated, null_yield);
    e.clear();
    std::move(lentries.begin(), lentries.end(), std::back_inserter(e));

    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to list " << oids[index]
		 << cpp_strerror(-r) << dendl;
      return r;
    }
    return 0;
  }
  int get_info(int index, RGWMetadataLogInfo *info) override {
    cls_log_header header;
    auto r = cls.timelog.info(oids[index], &header, null_yield);
    if (r == -ENOENT) r = 0;
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to get info from " << oids[index]
		 << cpp_strerror(-r) << dendl;
    } else {
      info->marker = header.max_marker;
      info->last_update = header.max_time.to_real_time();
    }
    return r;
  }
  int get_info_async(int index, RGWMetadataLogInfoCompletion *completion) override {
    return cls.timelog.info_async(completion->get_io_obj(), oids[index],
                                       &completion->get_header(),
                                       completion->get_completion());
  }
  int trim(int index, std::string_view marker, bool exclusive) override {
    string m = string(marker);
    int r = 0;

    if (!exclusive) {
      r = cls.timelog.trim(oids[index], {}, {},
			      {}, m, nullptr, null_yield);
    } else {
      // Extract timestamp from marker .. needed only for OMAP
      ceph::real_time stable;
      int sec, usec;
      char keyext[256];

      int ret = sscanf(m.c_str(), "1_%d.%d_%255s", &sec, &usec, keyext);

      if (ret < 0) {
        ldout(cct, 0) << "ERROR: scanning marker ret = " << ret << dendl;
        return -1;
      } else {
        stable = utime_t(sec, usec).to_real_time();
        // can only trim -up to- master's first timestamp, so subtract a second.
        // (this is why we use timestamps instead of markers for the peers)
        stable -= std::chrono::seconds(1);
      }

      r = cls.timelog.trim(oids[index], {}, stable,
  			      {}, {}, nullptr,
  			      null_yield);
    }

    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to trim " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int trim(int index, std::string_view marker,
	   librados::AioCompletion* c, bool exclusive) override {
    string m = string(marker);

    int r = 0;

    if (!exclusive) {
      r = cls.timelog.trim(oids[index], {}, {},
			      {}, m, c, null_yield);
    } else {
      // Extract timestamp from marker .. needed only for OMAP
      ceph::real_time stable;
      int sec, usec;
      char keyext[256];

      int ret = sscanf(m.c_str(), "1_%d.%d_%255s", &sec, &usec, keyext);

      if (ret < 0) {
        ldout(cct, 0) << "ERROR: scanning marker ret = " << ret << dendl;
        return -1;
      } else {
        stable = utime_t(sec, usec).to_real_time();
        // can only trim -up to- master's first timestamp, so subtract a second.
        // (this is why we use timestamps instead of markers for the peers)
        stable -= std::chrono::seconds(1);
      }

      r = cls.timelog.trim(oids[index], {}, stable,
			    {}, {}, c, null_yield);
    }

    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to get trim " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  std::string_view max_marker() override {
    return "99999999"sv;
  }
};

class RGWMetadataLogFIFO final : public RGWMetadataLogBE {
  std::vector<std::unique_ptr<rgw::cls::fifo::FIFO>> fifos;
public:
  RGWMetadataLogFIFO(CephContext* cct, std::string prefix,
		     librados::Rados* rados, const rgw_pool& log_pool)
    : RGWMetadataLogBE(cct, prefix) {
    librados::IoCtx ioctx;
    auto shards = cct->_conf->rgw_md_log_max_shards;
    auto r = rgw_init_ioctx(rados, log_pool.name, ioctx,
			    true, false);
    if (r < 0) {
      throw bs::system_error(ceph::to_error_code(r));
    }
    fifos.resize(shards);
    for (auto i = 0; i < shards; ++i) {
      r = rgw::cls::fifo::FIFO::create(ioctx, get_shard_oid(i),
				       &fifos[i], null_yield);
      if (r < 0) {
	throw bs::system_error(ceph::to_error_code(r));
      }
    }
    ceph_assert(fifos.size() == unsigned(shards));
    ceph_assert(std::none_of(fifos.cbegin(), fifos.cend(),
			     [](const auto& p) {
			       return p == nullptr;
			     }));
  }
  ~RGWMetadataLogFIFO() override = default;
  static int exists(CephContext* cct, std::string prefix, librados::Rados* rados,
		    const rgw_pool& log_pool, bool* exists, bool* has_entries) {
    auto num_shards = cct->_conf->rgw_md_log_max_shards;
    librados::IoCtx ioctx;
    auto r = rgw_init_ioctx(rados, log_pool.name, ioctx,
			    false, false);
    if (r < 0) {
      if (r == -ENOENT) {
	return 0;
      } else {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": rgw_init_ioctx failed: " << log_pool.name
		   << ": " << cpp_strerror(-r) << dendl;
	return r;
      }
    }
    *exists = false;
    *has_entries = false;
    for (auto i = 0; i < num_shards; ++i) {
      std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
      auto oid = fmt::format("{}{}", prefix, i);
      std::vector<rgw::cls::fifo::list_entry> log_entries;
      bool more = false;
      auto r = rgw::cls::fifo::FIFO::open(ioctx, oid,
					  &fifo, null_yield);
      if (r == -ENOENT || r == -ENODATA) {
	continue;
      } else if (r < 0) {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": unable to open FIFO: " << log_pool << "/" << oid
		   << ": " << cpp_strerror(-r) << dendl;
	return r;
      }
      *exists = true;
      r = fifo->list(1, nullopt, &log_entries, &more,
		     null_yield);
      if (r < 0) {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": unable to list entries: " << log_pool << "/" << oid
		   << ": " << cpp_strerror(-r) << dendl;
      } else if (!log_entries.empty()) {
	*has_entries = true;
	break;
      }
    }
    return 0;
  }
  int push(int index, std::vector<cls_log_entry>& items, librados::AioCompletion *&completion) override {
    std::vector<ceph::buffer::list> c;
    for (const auto& bs : items) {

      bufferlist bl;
      encode(bs, bl);

      c.push_back(std::move(bl));
    }

    auto r = fifos[index]->push(c, completion);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to push to FIFO: " << get_shard_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int push(int index, ceph::real_time now,
	   const std::string& section,
	   const std::string& key,
	   ceph::buffer::list& bl) override {
    auto r = fifos[index]->push(std::move(bl), null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to push to FIFO: " << get_shard_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int list(int index, int max_entries,
	   std::vector<cls_log_entry>& e,
	   std::string_view marker,
	   std::string* out_marker, bool* truncated) override {
    std::vector<rgw::cls::fifo::list_entry> log_entries;
    bool more = false;
    auto r = fifos[index]->list(max_entries, marker,
		   		&log_entries, &more,
				null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to list FIFO: " << get_shard_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    for (const auto& entry : log_entries) {
      bufferlist& bl((bufferlist &)entry);
      cls_log_entry log_entry;
      try {
        decode(log_entry, bl);
      } catch (const buffer::error& err) {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": failed to decode metadata changes log entry: "
		   << err.what() << dendl;
	return -EIO;
      }
      e.push_back(std::move(log_entry));
    }
    if (truncated)
      *truncated = more;
    if (out_marker && !log_entries.empty()) {
      *out_marker = log_entries.back().marker;
    }
    return 0;
  }

  int get_info(int index, RGWMetadataLogInfo *info) override {
    auto& fifo = fifos[index];
    auto r = fifo->read_meta(null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to get FIFO metadata: " << get_shard_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    auto m = fifo->meta();
    auto p = m.head_part_num;
    if (p < 0) {
      info->marker = rgw::cls::fifo::marker{}.to_string();
      info->last_update = ceph::real_clock::zero();
      return 0;
    }
    rgw::cls::fifo::part_info h;
    r = fifo->get_part_info(p, &h, null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to get part info: " << get_shard_oid(index) << "/" << p
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    info->marker = rgw::cls::fifo::marker{p, h.last_ofs}.to_string();
    info->last_update = h.max_time;
    return 0;
  }

  int get_info_async(int index, RGWMetadataLogInfoCompletion *completion) override {
    auto& fifo = fifos[index];
    rados::cls::fifo::info info;
    rgw::cls::fifo::part_info h;
//    cls_log_header& header(completion->get_header());
    librados::AioCompletion* c = completion->get_completion();
    auto r = fifo->get_head_info(&info, &h, c);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to get head info: " << get_shard_oid(index) << "/"
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
//    header.max_marker = rgw::cls::fifo::marker{p, h.last_ofs}.to_string();
//    header.max_time = utime_t(h.max_time);
  }

  int trim(int index, std::string_view marker, bool exclusive) override {
    auto r = fifos[index]->trim(marker, exclusive, null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to trim FIFO: " << get_shard_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int trim(int index, std::string_view marker,
	   librados::AioCompletion* c, bool exclusive) override {
    int r = 0;
    if (marker == rgw::cls::fifo::marker(0, 0).to_string()) {
      auto pc = c->pc;
      pc->get();
      pc->lock.lock();
      pc->rval = 0;
      pc->complete = true;
      pc->lock.unlock();
      auto cb_complete = pc->callback_complete;
      auto cb_complete_arg = pc->callback_complete_arg;
      if (cb_complete)
	cb_complete(pc, cb_complete_arg);

      auto cb_safe = pc->callback_safe;
      auto cb_safe_arg = pc->callback_safe_arg;
      if (cb_safe)
	cb_safe(pc, cb_safe_arg);

      pc->lock.lock();
      pc->callback_complete = NULL;
      pc->callback_safe = NULL;
      pc->cond.notify_all();
      pc->put_unlock();
    } else {
      r = fifos[index]->trim(marker, exclusive, c);
      if (r < 0) {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": unable to trim FIFO: " << get_shard_oid(index)
		   << ": " << cpp_strerror(-r) << dendl;
      }
    }
    return r;
  }
  std::string_view max_marker() override {
    static const std::string mm =
      rgw::cls::fifo::marker::max().to_string();
    return std::string_view(mm);
  }
};

RGWMetadataLogBE::RGWMetadataLogBE(CephContext* const cct, std::string prefix)
    : cct(cct), prefix(prefix) {}
RGWMetadataLogBE::~RGWMetadataLogBE() {}

int RGWMetadataLogBE::remove(CephContext* cct, std::string prefix,
	       		     librados::Rados* rados, const rgw_pool& log_pool)
{
  auto num_shards = cct->_conf->rgw_md_log_max_shards;
  librados::IoCtx ioctx;
  auto r = rgw_init_ioctx(rados, log_pool.name, ioctx,
			  false, false);
  if (r < 0) {
    if (r == -ENOENT) {
      return 0;
    } else {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": rgw_init_ioctx failed: " << log_pool.name
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
  }
  for (auto i = 0; i < num_shards; ++i) {
    std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
    auto oid = fmt::format("{}{}", prefix, i);
    librados::ObjectWriteOperation op;
    op.remove();
    auto r = rgw_rados_operate(ioctx, oid, &op, null_yield);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": remove failed: " << log_pool.name << "/" << oid
		 << ": " << cpp_strerror(-r) << dendl;
    }
  }
  return 0;
}

int RGWMetadataLog::init(librados::Rados* lr)
{
  auto backing = cct->_conf.get_val<std::string>("rgw_md_log_backing");
  ceph_assert(backing == "auto" || backing == "fifo" || backing == "omap");

  auto log_pool = svc.zone->get_zone_params().log_pool;
  bool omapexists = false, omaphasentries = false;
  auto r = RGWMetadataLogOmap::exists(cct, *svc.cls, prefix, &omapexists, &omaphasentries);
  if (r < 0) {
    lderr(cct) << __PRETTY_FUNCTION__
	       << ": Error when checking for existing Omap datalog backend: "
	       << cpp_strerror(-r) << dendl;
  }
  bool fifoexists = false, fifohasentries = false;
  r = RGWMetadataLogFIFO::exists(cct, prefix, lr, log_pool, &fifoexists, &fifohasentries);
  if (r < 0) {
    lderr(cct) << __PRETTY_FUNCTION__
	       << ": Error when checking for existing FIFO datalog backend: "
	       << cpp_strerror(-r) << dendl;
  }

  bool has_entries = omaphasentries || fifohasentries;
  bool remove = false;

  if (omapexists && fifoexists) {
    if (has_entries) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": Both Omap and FIFO backends exist, cannot continue."
		 << dendl;
      return -EINVAL;
    }
    ldout(cct, 0)
      << __PRETTY_FUNCTION__
      << ": Both Omap and FIFO backends exist, but are empty. Will remove."
      << dendl;
    remove = true;
  }
  if (backing == "omap" && fifoexists) {
    if (has_entries) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": Omap requested, but FIFO backend exists, cannot continue."
		 << dendl;
      return -EINVAL;
    }
    ldout(cct, 0) << __PRETTY_FUNCTION__
		  << ": Omap requested, FIFO exists, but is empty. Deleting."
		  << dendl;
    remove = true;
  }
  if ((backing == "fifo") && omapexists) {
    if (has_entries) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": FIFO requested, but Omap backend exists, cannot continue."
		 << dendl;
      return -EINVAL;
    }
    ldout(cct, 0) << __PRETTY_FUNCTION__
		  << ": FIFO requested, Omap exists, but is empty. Deleting."
		  << dendl;
    remove = true;
  }

  if (remove) {
    r = RGWMetadataLogBE::remove(cct, prefix, lr, log_pool);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": remove failed, cannot continue."
		 << dendl;
      return r;
    }
    omapexists = false;
    fifoexists = false;
  }

  try {
    if (backing == "omap" || (backing == "auto" && omapexists)) {
      be = std::make_unique<RGWMetadataLogOmap>(cct, prefix, *svc.cls);
    } else if (backing != "omap") {
      be = std::make_unique<RGWMetadataLogFIFO>(cct, prefix, lr, log_pool);
    }
  } catch (bs::system_error& e) {
    lderr(cct) << __PRETTY_FUNCTION__
	       << ": Error when starting backend: "
	       << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }

  ceph_assert(be);
  return 0;
}

int RGWMetadataLog::add_entry(const string& hash_key, const string& section,
			      const string& key, bufferlist& bl) {
  if (!svc.zone->need_to_log_metadata())
    return 0;

  auto [oid, shard_id] = rgw_shard_name(prefix,
					cct->_conf->rgw_md_log_max_shards,
					hash_key);
  mark_modified(shard_id);
  real_time now = real_clock::now();
  return be->push(shard_id, now, section, key, (ceph::buffer::list &)bl);
}

int RGWMetadataLog::get_shard_id(std::string_view hash_key)
{
  auto [oid, shard_id] = rgw_shard_name(prefix,
					cct->_conf->rgw_md_log_max_shards,
					hash_key);
  return shard_id;
}

std::string_view RGWMetadataLog::max_marker() const {
  return be->max_marker();
}

int RGWMetadataLog::store_entries_in_shard(std::vector<cls_log_entry>& entries, int shard_id, librados::AioCompletion *completion)
{
  mark_modified(shard_id);
  return be->push(shard_id, entries, completion);
}

int RGWMetadataLog::list_entries(int shard,
				 int max_entries,
				 std::string_view marker,
				 std::vector<cls_log_entry>& entries,
				 string *last_marker,
				 bool *truncated) {

  if (!max_entries) {
    *truncated = false;
    return 0;
  }

  auto oid = get_shard_oid(shard);
  std::string next_marker;
  int ret = be->list(shard, max_entries, entries, marker, &next_marker, truncated);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  if (last_marker) {
    *last_marker = next_marker;
  }

  if (ret == -ENOENT)
    *truncated = false;

  return 0;
}

int RGWMetadataLog::get_info(int shard_id, RGWMetadataLogInfo *info)
{
  return be->get_info(shard_id, info);
}

static void _mdlog_info_completion(librados::completion_t cb, void *arg)
{
  auto infoc = static_cast<RGWMetadataLogInfoCompletion *>(arg);
  infoc->finish(cb);
  infoc->put(); // drop the ref from get_info_async()
}

RGWMetadataLogInfoCompletion::RGWMetadataLogInfoCompletion(info_callback_t cb)
  : completion(librados::Rados::aio_create_completion((void *)this,
                                                      _mdlog_info_completion)),
    callback(cb)
{
}

RGWMetadataLogInfoCompletion::~RGWMetadataLogInfoCompletion()
{
  completion->release();
}

int RGWMetadataLog::get_info_async(int shard_id, RGWMetadataLogInfoCompletion *completion)
{
  completion->get(); // hold a ref until the completion fires
  return be->get_info_async(shard_id, completion);
}

int RGWMetadataLog::trim(int shard_id, std::string_view marker, bool exclusive)
{
  return be->trim(shard_id, marker, exclusive);
}

int RGWMetadataLog::trim(int shard_id, std::string_view marker,
	                 librados::AioCompletion* c, bool exclusive)
{
  return be->trim(shard_id, marker, c, exclusive);
}

int RGWMetadataLog::lock_exclusive(int shard_id, timespan duration, string& zone_id, string& owner_id) {
  auto oid = get_shard_oid(shard_id);

  return svc.cls->lock.lock_exclusive(svc.zone->get_zone_params().log_pool, oid, duration, zone_id, owner_id);
}

int RGWMetadataLog::unlock(int shard_id, string& zone_id, string& owner_id) {
  auto oid = get_shard_oid(shard_id);

  return svc.cls->lock.unlock(svc.zone->get_zone_params().log_pool, oid, zone_id, owner_id);
}

void RGWMetadataLog::mark_modified(int shard_id)
{
  std::shared_lock l(lock);
  if (modified_shards.find(shard_id) != modified_shards.end()) {
    l.unlock();
    return;
  }
  l.unlock();

  std::unique_lock wl{lock};
  modified_shards.insert(shard_id);
}

bc::flat_set<int> RGWMetadataLog::read_clear_modified()
{
  std::unique_lock wl{lock};
  auto m = std::move(modified_shards);
  modified_shards.clear();
  return m;
}


/// purge all log shards for the given mdlog
class PurgeLogShardsCR : public RGWShardCollectCR {
  rgw::sal::RGWRadosStore *const store;
  rgw_raw_obj obj;
  std::size_t i = 0;
  std::vector<std::string> oids;

  static constexpr int max_concurrent = 16;

 public:
  PurgeLogShardsCR(rgw::sal::RGWRadosStore *store, const rgw_pool& pool,
		   std::vector<std::string>&& oids)
    : RGWShardCollectCR(store->ctx(), max_concurrent),
      store(store), obj(pool, ""), oids(std::move(oids)) {}

  bool spawn_next() override {
    if (i == oids.size()) {
      return false;
    }
    obj.oid = oids[i++];
    spawn(new RGWRadosRemoveCR(store, obj), false);
    return true;
  }
};

RGWCoroutine* RGWMetadataLog::purge_cr()
{
  std::vector<std::string> oids;
  for (auto i = 0; i < cct->_conf->rgw_md_log_max_shards; ++i)
    oids.push_back(get_shard_oid(i));

  return new PurgeLogShardsCR(store, svc.zone->get_zone_params().log_pool,
			      std::move(oids));
}

RGWCoroutine* RGWMetadataLog::master_trim_cr(int shard_id, const std::string& marker,
	       				      std::string* last_trim)
{
  return new RGWMetadataLogTrimCR(cct, this, shard_id, marker, false, last_trim);
}

RGWCoroutine* RGWMetadataLog::peer_trim_cr(int shard_id, std::string marker)
{
  return new RGWMetadataLogTrimCR(cct, this, shard_id, marker, true, NULL);
}

obj_version& RGWMetadataObject::get_version()
{
  return objv;
}

class RGWMetadataTopHandler : public RGWMetadataHandler {
  struct iter_data {
    set<string> sections;
    set<string>::iterator iter;
  };

  struct Svc {
    RGWSI_Meta *meta{nullptr};
  } svc;

  RGWMetadataManager *mgr;

public:
  RGWMetadataTopHandler(RGWSI_Meta *meta_svc,
                        RGWMetadataManager *_mgr) : mgr(_mgr) {
    base_init(meta_svc->ctx());
    svc.meta = meta_svc;
  }

  string get_type() override { return string(); }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv,
				  const ceph::real_time& mtime) override {
    return new RGWMetadataObject;
  }

  int get(string& entry, RGWMetadataObject **obj, optional_yield y) override {
    return -ENOTSUP;
  }

  int put(string& entry, RGWMetadataObject *obj,
	  RGWObjVersionTracker& objv_tracker,
	  optional_yield y, RGWMDLogSyncType type) override {
    return -ENOTSUP;
  }

  int remove(string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y) override {
    return -ENOTSUP;
  }

  int mutate(const string& entry,
             const ceph::real_time& mtime,
             RGWObjVersionTracker *objv_tracker,
             optional_yield y,
             RGWMDLogStatus op_type,
             std::function<int()> f) override {
    return -ENOTSUP;
  }

  int list_keys_init(const string& marker, void **phandle) override {
    iter_data *data = new iter_data;
    list<string> sections;
    mgr->get_sections(sections);
    for (auto& s : sections) {
      data->sections.insert(s);
    }
    data->iter = data->sections.lower_bound(marker);

    *phandle = data;

    return 0;
  }
  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) override  {
    iter_data *data = static_cast<iter_data *>(handle);
    for (int i = 0; i < max && data->iter != data->sections.end(); ++i, ++(data->iter)) {
      keys.push_back(*data->iter);
    }

    *truncated = (data->iter != data->sections.end());

    return 0;
  }
  void list_keys_complete(void *handle) override {
    iter_data *data = static_cast<iter_data *>(handle);

    delete data;
  }

  virtual string get_marker(void *handle) override {
    iter_data *data = static_cast<iter_data *>(handle);

    if (data->iter != data->sections.end()) {
      return *(data->iter);
    }

    return string();
  }
};

RGWMetadataManager::RGWMetadataManager(RGWSI_Meta *_meta_svc)
  : cct(_meta_svc->ctx()), meta_svc(_meta_svc)
{
  md_top_handler.reset(new RGWMetadataTopHandler(meta_svc, this));
}

RGWMetadataManager::~RGWMetadataManager()
{
}

int RGWMetadataHandler::attach(RGWMetadataManager *manager)
{
  return manager->register_handler(this);
}

RGWMetadataHandler_GenericMetaBE::Put::Put(RGWMetadataHandler_GenericMetaBE *_handler,
					   RGWSI_MetaBackend_Handler::Op *_op,
					   string& _entry, RGWMetadataObject *_obj,
					   RGWObjVersionTracker& _objv_tracker,
					   optional_yield _y,
					   RGWMDLogSyncType _type):
  handler(_handler), op(_op),
  entry(_entry), obj(_obj),
  objv_tracker(_objv_tracker),
  apply_type(_type),
  y(_y)
{
}

RGWMetadataHandlerPut_SObj::RGWMetadataHandlerPut_SObj(RGWMetadataHandler_GenericMetaBE *handler, RGWSI_MetaBackend_Handler::Op *op,
                                                       string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker,
						       optional_yield y,
                                                       RGWMDLogSyncType type) : Put(handler, op, entry, obj, objv_tracker, y, type) {
}

RGWMetadataHandlerPut_SObj::~RGWMetadataHandlerPut_SObj() {
}

int RGWMetadataHandlerPut_SObj::put_pre()
{
  int ret = get(&old_obj);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  exists = (ret != -ENOENT);

  oo.reset(old_obj);

  auto old_ver = (!old_obj ? obj_version() : old_obj->get_version());
  auto old_mtime = (!old_obj ? ceph::real_time() : old_obj->get_mtime());

  // are we actually going to perform this put, or is it too old?
  if (!handler->check_versions(exists, old_ver, old_mtime,
                               objv_tracker.write_version, obj->get_mtime(),
                               apply_type)) {
    return STATUS_NO_APPLY;
  }

  objv_tracker.read_version = old_ver; /* maintain the obj version we just read */

  return 0;
}

int RGWMetadataHandlerPut_SObj::put()
{
  int ret = put_check();
  if (ret != 0) {
    return ret;
  }

  return put_checked();
}

int RGWMetadataHandlerPut_SObj::put_checked()
{
  RGWSI_MBSObj_PutParams params(obj->get_pattrs(), obj->get_mtime());

  encode_obj(&params.bl);

  int ret = op->put(entry, params, &objv_tracker, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWMetadataHandler_GenericMetaBE::do_put_operate(Put *put_op)
{
  int r = put_op->put_pre();
  if (r != 0) { /* r can also be STATUS_NO_APPLY */
    return r;
  }

  r = put_op->put();
  if (r != 0) {
    return r;
  }

  r = put_op->put_post();
  if (r != 0) {  /* e.g., -error or STATUS_APPLIED */
    return r;
  }

  return 0;
}

int RGWMetadataHandler_GenericMetaBE::get(string& entry, RGWMetadataObject **obj, optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_get(op, entry, obj, y);
  });
}

int RGWMetadataHandler_GenericMetaBE::put(string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker, optional_yield y, RGWMDLogSyncType type)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_put(op, entry, obj, objv_tracker, y, type);
  });
}

int RGWMetadataHandler_GenericMetaBE::remove(string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_remove(op, entry, objv_tracker, y);
  });
}

int RGWMetadataHandler_GenericMetaBE::mutate(const string& entry,
                                             const ceph::real_time& mtime,
                                             RGWObjVersionTracker *objv_tracker,
                                             optional_yield y,
                                             RGWMDLogStatus op_type,
                                             std::function<int()> f)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    RGWSI_MetaBackend::MutateParams params(mtime, op_type);
    return op->mutate(entry,
                      params,
                      objv_tracker,
		      y,
                      f);
  });
}

int RGWMetadataHandler_GenericMetaBE::get_shard_id(const string& entry)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return op->get_shard_id(entry);
  });
}

int RGWMetadataHandler_GenericMetaBE::list_keys_init(const string& marker, void **phandle)
{
  auto op = std::make_unique<RGWSI_MetaBackend_Handler::Op_ManagedCtx>(be_handler);

  int ret = op->list_init(marker);
  if (ret < 0) {
    return ret;
  }

  *phandle = (void *)op.release();

  return 0;
}

int RGWMetadataHandler_GenericMetaBE::list_keys_next(void *handle, int max, list<string>& keys, bool *truncated)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);

  int ret = op->list_next(max, &keys, truncated);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  if (ret == -ENOENT) {
    if (truncated) {
      *truncated = false;
    }
    return 0;
  }

  return 0;
}

void RGWMetadataHandler_GenericMetaBE::list_keys_complete(void *handle)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);
  delete op;
}

string RGWMetadataHandler_GenericMetaBE::get_marker(void *handle)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);
  string marker;
  int r = op->list_get_marker(&marker);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): list_get_marker() returned: r=" << r << dendl;
    /* not much else to do */
  }

  return marker;
}

int RGWMetadataManager::register_handler(RGWMetadataHandler *handler)
{
  string type = handler->get_type();

  if (handlers.find(type) != handlers.end())
    return -EEXIST;

  handlers[type] = handler;

  return 0;
}

RGWMetadataHandler *RGWMetadataManager::get_handler(const string& type)
{
  map<string, RGWMetadataHandler *>::iterator iter = handlers.find(type);
  if (iter == handlers.end())
    return NULL;

  return iter->second;
}

void RGWMetadataManager::parse_metadata_key(const string& metadata_key, string& type, string& entry)
{
  auto pos = metadata_key.find(':');
  if (pos == string::npos) {
    type = metadata_key;
  } else {
    type = metadata_key.substr(0, pos);
    entry = metadata_key.substr(pos + 1);
  }
}

int RGWMetadataManager::find_handler(const string& metadata_key, RGWMetadataHandler **handler, string& entry)
{
  string type;

  parse_metadata_key(metadata_key, type, entry);

  if (type.empty()) {
    *handler = md_top_handler.get();
    return 0;
  }

  auto iter = handlers.find(type);
  if (iter == handlers.end())
    return -ENOENT;

  *handler = iter->second;

  return 0;

}

int RGWMetadataManager::get(string& metadata_key, Formatter *f, optional_yield y)
{
  RGWMetadataHandler *handler;
  string entry;
  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  RGWMetadataObject *obj;

  ret = handler->get(entry, &obj, y);
  if (ret < 0) {
    return ret;
  }

  f->open_object_section("metadata_info");
  encode_json("key", metadata_key, f);
  encode_json("ver", obj->get_version(), f);
  real_time mtime = obj->get_mtime();
  if (!real_clock::is_zero(mtime)) {
    utime_t ut(mtime);
    encode_json("mtime", ut, f);
  }
  encode_json("data", *obj, f);
  f->close_section();

  delete obj;

  return 0;
}

int RGWMetadataManager::put(string& metadata_key, bufferlist& bl,
			    optional_yield y,
                            RGWMDLogSyncType sync_type,
                            obj_version *existing_version)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  JSONParser parser;
  if (!parser.parse(bl.c_str(), bl.length())) {
    return -EINVAL;
  }

  RGWObjVersionTracker objv_tracker;

  obj_version *objv = &objv_tracker.write_version;

  utime_t mtime;

  try {
    JSONDecoder::decode_json("key", metadata_key, &parser);
    JSONDecoder::decode_json("ver", *objv, &parser);
    JSONDecoder::decode_json("mtime", mtime, &parser);
  } catch (JSONDecoder::err& e) {
    return -EINVAL;
  }

  JSONObj *jo = parser.find_obj("data");
  if (!jo) {
    return -EINVAL;
  }

  RGWMetadataObject *obj = handler->get_meta_obj(jo, *objv, mtime.to_real_time());
  if (!obj) {
    return -EINVAL;
  }

  ret = handler->put(entry, obj, objv_tracker, y, sync_type);
  if (existing_version) {
    *existing_version = objv_tracker.read_version;
  }

  delete obj;

  return ret;
}

int RGWMetadataManager::remove(string& metadata_key, optional_yield y)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  RGWMetadataObject *obj;
  ret = handler->get(entry, &obj, y);
  if (ret < 0) {
    return ret;
  }
  RGWObjVersionTracker objv_tracker;
  objv_tracker.read_version = obj->get_version();
  delete obj;

  return handler->remove(entry, objv_tracker, y);
}

int RGWMetadataManager::mutate(const string& metadata_key,
                               const ceph::real_time& mtime,
                               RGWObjVersionTracker *objv_tracker,
			       optional_yield y,
                               RGWMDLogStatus op_type,
                               std::function<int()> f)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  return handler->mutate(entry, mtime, objv_tracker, y, op_type, f);
}

int RGWMetadataManager::get_shard_id(const string& section, const string& entry)
{
  RGWMetadataHandler *handler = get_handler(section);
  if (!handler) {
    return -EINVAL;
  }

  return handler->get_shard_id(entry);
}

struct list_keys_handle {
  void *handle;
  RGWMetadataHandler *handler;
};

int RGWMetadataManager::list_keys_init(const string& section, void **handle)
{
  return list_keys_init(section, string(), handle);
}

int RGWMetadataManager::list_keys_init(const string& section,
                                       const string& marker, void **handle)
{
  string entry;
  RGWMetadataHandler *handler;

  int ret;

  ret = find_handler(section, &handler, entry);
  if (ret < 0) {
    return -ENOENT;
  }

  list_keys_handle *h = new list_keys_handle;
  h->handler = handler;
  ret = handler->list_keys_init(marker, &h->handle);
  if (ret < 0) {
    delete h;
    return ret;
  }

  *handle = (void *)h;

  return 0;
}

int RGWMetadataManager::list_keys_next(void *handle, int max, list<string>& keys, bool *truncated)
{
  list_keys_handle *h = static_cast<list_keys_handle *>(handle);

  RGWMetadataHandler *handler = h->handler;

  return handler->list_keys_next(h->handle, max, keys, truncated);
}

void RGWMetadataManager::list_keys_complete(void *handle)
{
  list_keys_handle *h = static_cast<list_keys_handle *>(handle);

  RGWMetadataHandler *handler = h->handler;

  handler->list_keys_complete(h->handle);
  delete h;
}

string RGWMetadataManager::get_marker(void *handle)
{
  list_keys_handle *h = static_cast<list_keys_handle *>(handle);

  return h->handler->get_marker(h->handle);
}

void RGWMetadataManager::dump_log_entry(const cls_log_entry& entry, Formatter *f)
{
  f->open_object_section("entry");
  f->dump_string("id", entry.id);
  f->dump_string("section", entry.section);
  f->dump_string("name", entry.name);
  entry.timestamp.gmtime_nsec(f->dump_stream("timestamp"));

  try {
    RGWMetadataLogData log_data;
    auto iter = entry.data.cbegin();
    decode(log_data, iter);

    encode_json("data", log_data, f);
  } catch (ceph::buffer::error& err) {
    lderr(cct) << "failed to decode log entry: " << entry.section << ":" << entry.name<< " ts=" << entry.timestamp << dendl;
  }
  f->close_section();
}

void RGWMetadataManager::get_sections(list<string>& sections)
{
  for (auto iter = handlers.begin(); iter != handlers.end(); ++iter) {
    sections.push_back(iter->first);
  }
}
