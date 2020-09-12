// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <cstdint>
#include <numeric>
#include <optional>
#include <string_view>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/rados/librados.hpp"

#include "include/buffer.h"

#include "common/async/yield_context.h"
#include "common/random_string.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

#include "librados/AioCompletionImpl.h"

#include "rgw_tools.h"

#include "cls_fifo_legacy.h"

namespace rgw::cls::fifo {
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;

using ceph::from_error_code;

inline constexpr auto MAX_RACE_RETRIES = 10;

void create_meta(lr::ObjectWriteOperation* op,
		 std::string_view id,
		 std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive,
		 std::uint64_t max_part_size,
		 std::uint64_t max_entry_size)
{
  fifo::op::create_meta cm;

  cm.id = id;
  cm.version = objv;
  cm.oid_prefix = oid_prefix;
  cm.max_part_size = max_part_size;
  cm.max_entry_size = max_entry_size;
  cm.exclusive = exclusive;

  cb::list in;
  encode(cm, in);
  op->exec(fifo::op::CLASS, fifo::op::CREATE_META, in);
}

int get_meta(lr::IoCtx& ioctx, const std::string& oid,
	     std::optional<fifo::objv> objv, fifo::info* info,
	     std::uint32_t* part_header_size,
	     std::uint32_t* part_entry_overhead,
	     optional_yield y)
{
  lr::ObjectReadOperation op;
  fifo::op::get_meta gm;
  gm.version = objv;
  cb::list in;
  encode(gm, in);
  cb::list bl;

  op.exec(fifo::op::CLASS, fifo::op::GET_META, in,
	  &bl, nullptr);
  auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::get_meta_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (info) *info = std::move(reply.info);
      if (part_header_size) *part_header_size = reply.part_header_size;
      if (part_entry_overhead)
	*part_entry_overhead = reply.part_entry_overhead;
    } catch (const cb::error& err) {
      r = from_error_code(err.code());
    }
  return r;
};

namespace {
void update_meta(lr::ObjectWriteOperation* op, const fifo::objv& objv,
		 const fifo::update& update)
{
  fifo::op::update_meta um;

  um.version = objv;
  um.tail_part_num = update.tail_part_num();
  um.head_part_num = update.head_part_num();
  um.min_push_part_num = update.min_push_part_num();
  um.max_push_part_num = update.max_push_part_num();
  um.journal_entries_add = std::move(update).journal_entries_add();
  um.journal_entries_rm = std::move(update).journal_entries_rm();

  cb::list in;
  encode(um, in);
  op->exec(fifo::op::CLASS, fifo::op::UPDATE_META, in);
}

void part_init(lr::ObjectWriteOperation* op, std::string_view tag,
	       fifo::data_params params)
{
  fifo::op::init_part ip;

  ip.tag = tag;
  ip.params = params;

  cb::list in;
  encode(ip, in);
  op->exec(fifo::op::CLASS, fifo::op::INIT_PART, in);
}

int push_part(lr::IoCtx& ioctx, const std::string& oid, std::string_view tag,
	      std::deque<cb::list> data_bufs, optional_yield y)
{
  lr::ObjectWriteOperation op;
  fifo::op::push_part pp;

  pp.tag = tag;
  pp.data_bufs = data_bufs;
  pp.total_len = 0;

  for (const auto& bl : data_bufs)
    pp.total_len += bl.length();

  cb::list in;
  encode(pp, in);
  auto retval = 0;
  op.exec(fifo::op::CLASS, fifo::op::PUSH_PART, in, nullptr, &retval);
  auto r = rgw_rados_operate(ioctx, oid, &op, y, lr::OPERATION_RETURNVEC);
  return r < 0 ? r : retval;
}

int push_part(lr::IoCtx& ioctx, const std::string& oid, std::string_view tag,
	      std::deque<cb::list> data_bufs, lr::AioCompletion* c)
{
  lr::ObjectWriteOperation op;
  fifo::op::push_part pp;

  pp.tag = tag;
  pp.data_bufs = data_bufs;
  pp.total_len = 0;

  for (const auto& bl : data_bufs)
    pp.total_len += bl.length();

  cb::list in;
  encode(pp, in);
  op.exec(fifo::op::CLASS, fifo::op::PUSH_PART, in);
  return ioctx.aio_operate(oid, c, &op, lr::OPERATION_RETURNVEC);
}

void trim_part(lr::ObjectWriteOperation* op,
	       std::optional<std::string_view> tag,
	       std::uint64_t ofs, bool exclusive)
{
  fifo::op::trim_part tp;

  tp.tag = tag;
  tp.ofs = ofs;
  tp.exclusive = exclusive;

  cb::list in;
  encode(tp, in);
  op->exec(fifo::op::CLASS, fifo::op::TRIM_PART, in);
}

int list_part(lr::IoCtx& ioctx, const std::string& oid,
	      std::optional<std::string_view> tag, std::uint64_t ofs,
	      std::uint64_t max_entries,
	      std::vector<fifo::part_list_entry>* entries,
	      bool* more, bool* full_part, std::string* ptag,
	      optional_yield y)
{
  lr::ObjectReadOperation op;
  fifo::op::list_part lp;

  lp.tag = tag;
  lp.ofs = ofs;
  lp.max_entries = max_entries;

  cb::list in;
  encode(lp, in);
  cb::list bl;
  op.exec(fifo::op::CLASS, fifo::op::LIST_PART, in, &bl, nullptr);
  auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::list_part_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (entries) *entries = std::move(reply.entries);
      if (more) *more = reply.more;
      if (full_part) *full_part = reply.full_part;
      if (ptag) *ptag = reply.tag;
    } catch (const cb::error& err) {
      r = from_error_code(err.code());
    }
  return r;
}

struct list_entry_completion : public lr::ObjectOperationCompletion {
  int* r_out;
  std::vector<fifo::part_list_entry>* entries;
  bool* more;
  bool* full_part;
  std::string* ptag;
  list_entry_completion(int* r_out, std::vector<fifo::part_list_entry>* entries,
			bool* more, bool* full_part, std::string* ptag)
    : r_out(r_out), entries(entries), more(more), full_part(full_part),
      ptag(ptag) {}
  virtual ~list_entry_completion() = default;
  void handle_completion(int r, bufferlist& bl) override {
    if (r >= 0) try {
	fifo::op::list_part_reply reply;
	auto iter = bl.cbegin();
	decode(reply, iter);
	if (entries) *entries = std::move(reply.entries);
	if (more) *more = reply.more;
	if (full_part) *full_part = reply.full_part;
	if (ptag) *ptag = reply.tag;
      } catch (const cb::error& err) {
	r = from_error_code(err.code());
      }
    if (r_out) *r_out = r;
  }
};

lr::ObjectReadOperation list_part(std::optional<std::string_view> tag,
				  std::uint64_t ofs,
				  std::uint64_t max_entries,
				  int* r_out,
				  std::vector<fifo::part_list_entry>* entries,
				  bool* more, bool* full_part,
				  std::string* ptag)
{
  lr::ObjectReadOperation op;
  fifo::op::list_part lp;

  lp.tag = tag;
  lp.ofs = ofs;
  lp.max_entries = max_entries;

  cb::list in;
  encode(lp, in);
  op.exec(fifo::op::CLASS, fifo::op::LIST_PART, in,
	  new list_entry_completion(r_out, entries, more, full_part, ptag));
  return op;
}


int get_part_info(lr::IoCtx& ioctx, const std::string& oid,
		  fifo::part_header* header, optional_yield y)
{
  lr::ObjectReadOperation op;
  fifo::op::get_part_info gpi;

  cb::list in;
  cb::list bl;
  encode(gpi, in);
  op.exec(fifo::op::CLASS, fifo::op::GET_PART_INFO, in, &bl, nullptr);
  auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::get_part_info_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (header) *header = std::move(reply.header);
    } catch (const cb::error& err) {
      r = from_error_code(err.code());
    }
  return r;
}

struct partinfo_completion : public lr::ObjectOperationCompletion {
  int* rp;
  fifo::part_header* h;
  partinfo_completion(int* rp, fifo::part_header* h) :
    rp(rp), h(h) {
  }
  virtual ~partinfo_completion() = default;
  void handle_completion(int r, bufferlist& bl) override {
    if (r >= 0) try {
	fifo::op::get_part_info_reply reply;
	auto iter = bl.cbegin();
	decode(reply, iter);
	if (h) *h = std::move(reply.header);
      } catch (const cb::error& err) {
	r = from_error_code(err.code());
      }
    if (rp) {
      *rp = r;
    }
  }
};

lr::ObjectReadOperation get_part_info(fifo::part_header* header, int* r = 0)
{
  lr::ObjectReadOperation op;
  fifo::op::get_part_info gpi;

  cb::list in;
  cb::list bl;
  encode(gpi, in);
  op.exec(fifo::op::CLASS, fifo::op::GET_PART_INFO, in,
	  new partinfo_completion(r, header));
  return op;
}
}

template<typename T>
struct Completion {
private:
  lr::AioCompletion* _cur = nullptr;
  lr::AioCompletion* _super;
public:

  lr::AioCompletion* cur() const {
    return _cur;
  }
  lr::AioCompletion* super() const {
    return _super;
  }

  Completion(lr::AioCompletion* super) : _super(super) {
    super->pc->get();
  }
  void set_cur(lr::callback_t cbt) {
    _cur = lr::Rados::aio_create_completion(static_cast<void*>(this), cbt);
  }
  void clear_cur() {
    _cur->release();
    _cur = nullptr;
  }
  void complete(int r) {
    auto c = _super->pc;
    _super = nullptr;
    c->lock.lock();
    c->rval = r;
    c->complete = true;
    c->lock.unlock();

    auto cb_complete = c->callback_complete;
    auto cb_complete_arg = c->callback_complete_arg;
    if (cb_complete)
      cb_complete(c, cb_complete_arg);

    auto cb_safe = c->callback_safe;
    auto cb_safe_arg = c->callback_safe_arg;
    if (cb_safe)
      cb_safe(c, cb_safe_arg);

    c->lock.lock();
    c->callback_complete = nullptr;
    c->callback_safe = nullptr;
    c->cond.notify_all();
    c->put_unlock();

    delete static_cast<T*>(this);
  }

  void abandon() {
    _super->pc->put();
    if (_cur)
      _cur->release();
    _super = nullptr;
    _cur = nullptr;
    delete static_cast<T*>(this);
  }

  static std::pair<T*, int> cb_init(void* arg) {
    auto t = static_cast<T*>(arg);
    auto r = t->_cur->get_return_value();
    t->_cur->release();
    t->_cur = nullptr;
    return {t, r};
  }
};

std::optional<marker> FIFO::to_marker(std::string_view s)
{
  marker m;
  if (s.empty()) {
    m.num = info.tail_part_num;
    m.ofs = 0;
    return m;
  }

  auto pos = s.find(':');
  if (pos == string::npos) {
    return std::nullopt;
  }

  auto num = s.substr(0, pos);
  auto ofs = s.substr(pos + 1);

  auto n = ceph::parse<decltype(m.num)>(num);
  if (!n) {
    return std::nullopt;
  }
  m.num = *n;
  auto o = ceph::parse<decltype(m.ofs)>(ofs);
  if (!o) {
    return std::nullopt;
  }
  m.ofs = *o;
  return m;
}

std::string FIFO::generate_tag() const
{
  static constexpr auto HEADER_TAG_SIZE = 16;
  return gen_rand_alphanumeric_plain(static_cast<CephContext*>(ioctx.cct()),
				     HEADER_TAG_SIZE);
}


int FIFO::apply_update(fifo::info* info,
		       const fifo::objv& objv,
		       const fifo::update& update)
{
  std::unique_lock l(m);
  auto err = info->apply_update(update);
  if (objv != info->version) {
    return -ECANCELED;
  }
  if (err) {
    return -ECANCELED;
  }

  ++info->version.ver;

  return {};
}

int FIFO::_update_meta(const fifo::update& update,
		       fifo::objv version, bool* pcanceled,
		       optional_yield y)
{
  lr::ObjectWriteOperation op;
  bool canceled = false;
  update_meta(&op, info.version, update);
  auto r = rgw_rados_operate(ioctx, oid, &op, y);
  if (r >= 0 || r == -ECANCELED) {
    canceled = (r == -ECANCELED);
    if (!canceled) {
      r = apply_update(&info, version, update);
      if (r < 0) canceled = true;
    }
    if (canceled) {
      r = read_meta(y);
      canceled = r < 0 ? false : true;
    }
  }
  if (pcanceled) *pcanceled = canceled;
  return r;
}

struct Updater : public Completion<Updater> {
  FIFO* fifo;
  fifo::update update;
  fifo::objv version;
  bool* pcanceled = nullptr;

  Updater(FIFO* fifo, lr::AioCompletion* super,
	  const fifo::update& update, fifo::objv version,
	  bool* pcanceled)
    : Completion(super), fifo(fifo), update(update), version(version),
      pcanceled(pcanceled) {}

  void static cb_update(lr::completion_t, void* arg) {
    auto [u, r] = cb_init(arg);
    if (r < 0 && r != -ECANCELED) {
      u->complete(r);
      return;
    }
    bool canceled = (r == -ECANCELED);
    if (!canceled) {
      int r = u->fifo->apply_update(&u->fifo->info, u->version,
				    u->update);
      if (r < 0)
	canceled = true;
    }
    if (!canceled) {
      if (u->pcanceled)
	*u->pcanceled = false;
      u->complete(0);
    } else {
      u->set_cur(&Updater::cb_reread);
      auto r = u->fifo->read_meta(u->cur());
      if (r < 0) {
	u->clear_cur();
	u->complete(r);
      }
    }
  }

  void static cb_reread(lr::completion_t, void* arg) {
    auto [u, r] = cb_init(arg);
    if (r < 0 && u->pcanceled) {
      *u->pcanceled = false;
    } else if (r >= 0 && u->pcanceled) {
      *u->pcanceled = true;
    }
    u->complete(r);
  }
};

int FIFO::_update_meta(const fifo::update& update,
		       fifo::objv version, bool* pcanceled,
		       lr::AioCompletion* c)
{
  lr::ObjectWriteOperation op;
  update_meta(&op, info.version, update);
  auto updater = new Updater(this, c, update, version, pcanceled);
  updater->set_cur(&Updater::cb_update);
  auto r = ioctx.aio_operate(oid, updater->cur(), &op);
  if (r < 0) {
    updater->abandon();
  }
  return r;
}

int FIFO::create_part(int64_t part_num, std::string_view tag, optional_yield y)
{
  lr::ObjectWriteOperation op;
  op.create(false); /* We don't need exclusivity, part_init ensures
		       we're creating from the  same journal entry. */
  std::unique_lock l(m);
  part_init(&op, tag, info.params);
  auto oid = info.part_oid(part_num);
  l.unlock();
  return rgw_rados_operate(ioctx, oid, &op, y);
}

int FIFO::remove_part(int64_t part_num, std::string_view tag, optional_yield y)
{
  lr::ObjectWriteOperation op;
  op.remove();
  std::unique_lock l(m);
  auto oid = info.part_oid(part_num);
  l.unlock();
  return rgw_rados_operate(ioctx, oid, &op, y);
}

int FIFO::process_journal(optional_yield y)
{
  std::vector<fifo::journal_entry> processed;

  std::unique_lock l(m);
  auto tmpjournal = info.journal;
  auto new_tail = info.tail_part_num;
  auto new_head = info.head_part_num;
  auto new_max = info.max_push_part_num;
  l.unlock();

  int r;
  for (auto& [n, entry] : tmpjournal) {
    switch (entry.op) {
    case fifo::journal_entry::Op::create:
      r = create_part(entry.part_num, entry.part_tag, y);
      if (entry.part_num > new_max) {
	new_max = entry.part_num;
      }
      break;
    case fifo::journal_entry::Op::set_head:
      if (entry.part_num > new_head) {
	new_head = entry.part_num;
      }
      break;
    case fifo::journal_entry::Op::remove:
      r = remove_part(entry.part_num, entry.part_tag, y);
      if (r == -ENOENT) r = 0;
      if (entry.part_num >= new_tail) {
	new_tail = entry.part_num + 1;
      }
      break;
    default:
      return -EIO;
    }

    if (r < 0) {
      return -EIO;
    }

    processed.push_back(std::move(entry));
  }

  // Postprocess

  bool canceled = true;

  for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
    std::optional<int64_t> tail_part_num;
    std::optional<int64_t> head_part_num;
    std::optional<int64_t> max_part_num;

    std::unique_lock l(m);
    auto objv = info.version;
    if (new_tail > tail_part_num) tail_part_num = new_tail;
    if (new_head > info.head_part_num) head_part_num = new_head;
    if (new_max > info.max_push_part_num) max_part_num = new_max;
    l.unlock();

    if (processed.empty() &&
	!tail_part_num &&
	!max_part_num) {
      /* nothing to update anymore */
      canceled = false;
      break;
    }
    _update_meta(fifo::update()
		 .tail_part_num(tail_part_num)
		 .head_part_num(head_part_num)
		 .max_push_part_num(max_part_num)
		 .journal_entries_rm(processed),
		 objv, &canceled, y);
    if (r < 0) break;

    if (canceled) {
      std::vector<fifo::journal_entry> new_processed;
      std::unique_lock l(m);
      for (auto& e : processed) {
	auto jiter = info.journal.find(e.part_num);
	/* journal entry was already processed */
	if (jiter == info.journal.end() ||
	    !(jiter->second == e)) {
	  continue;
	}
	new_processed.push_back(e);
      }
      processed = std::move(new_processed);
    }
  }
  if (canceled)
    r = -ECANCELED;
  return r;
}

int FIFO::_prepare_new_part(bool is_head, optional_yield y)
{
  std::unique_lock l(m);
  std::vector jentries = { info.next_journal_entry(generate_tag()) };
  std::int64_t new_head_part_num = info.head_part_num;
  auto version = info.version;

  if (is_head) {
    auto new_head_jentry = jentries.front();
    new_head_jentry.op = fifo::journal_entry::Op::set_head;
    new_head_part_num = jentries.front().part_num;
    jentries.push_back(std::move(new_head_jentry));
  }
  l.unlock();

  int r = 0;
  bool canceled = true;
  for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
    canceled = false;
    r = _update_meta(fifo::update{}.journal_entries_add(jentries),
			 version, &canceled, y);
    if (r >= 0 && canceled) {
      std::unique_lock l(m);
      auto found = (info.journal.find(jentries.front().part_num) !=
		    info.journal.end());
      if ((info.max_push_part_num >= jentries.front().part_num &&
	   info.head_part_num >= new_head_part_num)) {
	// We don't even need to process the journal.
	return 0;
      }
      if (found) {
	// Journaled, but not processed.
	canceled = false;
      }
      l.unlock();
    }
  }
  if (canceled)
    r = -ECANCELED;
  if (r >= 0)
    r = process_journal(y);
  return r;
}

int FIFO::_prepare_new_head(optional_yield y) {
  std::unique_lock l(m);
  std::int64_t new_head_num = info.head_part_num + 1;
  auto max_push_part_num = info.max_push_part_num;
  auto version = info.version;
  l.unlock();

  int r = 0;
  if (max_push_part_num < new_head_num) {
    r = _prepare_new_part(true, y);
    if (r >= 0) {
      std::unique_lock l(m);
      if (info.max_push_part_num < new_head_num) {
	r = -EIO;
      }
      l.unlock();
    }
  } else {
    bool canceled = true;
    for (auto i = 0; canceled && i < MAX_RACE_RETRIES; ++i) {
      _update_meta(fifo::update{}.head_part_num(new_head_num),
		   version, &canceled, y);
      if (r < 0)
	break;
      std::unique_lock l(m);
      auto head_part_num = info.head_part_num;
      version = info.version;
      l.unlock();
      if (canceled && (head_part_num >= new_head_num)) {
	// Race, but someone did it for us
	canceled = false;
      }
    }
    if (canceled)
      r = -ECANCELED;
  }
  return 0;
}

struct NewPartPreparer : public Completion<NewPartPreparer> {
  FIFO* f;
  std::vector<fifo::journal_entry> jentries;
  int i;
  std::int64_t new_head_part_num;
  bool canceled = false;

  static void callback(lr::completion_t, void* arg) {
    auto [n, r] = cb_init(arg);
    if (r < 0) {
      n->complete(r);
      return;
    }

    if (n->canceled) {
      std::unique_lock l(n->f->m);
      auto iter = n->f->info.journal.find(n->jentries.front().part_num);
      auto max_push_part_num = n->f->info.max_push_part_num;
      auto head_part_num = n->f->info.head_part_num;
      auto version = n->f->info.version;
      auto found = (iter != n->f->info.journal.end());
      l.unlock();
      if ((max_push_part_num >= n->jentries.front().part_num &&
	   head_part_num >= n->new_head_part_num)) {
	/* raced, but new part was already written */
	n->complete(0);
	return;
      }
      if (n->i >= MAX_RACE_RETRIES) {
	n->complete(-ECANCELED);
	return;
      }
      if (!found) {
	n->set_cur(&NewPartPreparer::callback);
	++n->i;
	n->f->_update_meta(fifo::update{}
			   .journal_entries_add(n->jentries),
                           version, &n->canceled, n->cur());
	return;
      }
      // Fall through. We still need to process the journal.
    }
    n->f->process_journal(n->super());
    n->abandon();
    return;
  }

  NewPartPreparer(FIFO* f, lr::AioCompletion* super,
		  std::vector<fifo::journal_entry> jentries,
		  int i, std::int64_t new_head_part_num)
    : Completion(super), f(f), jentries(std::move(jentries)),
      i(i), new_head_part_num(new_head_part_num) {}
};

int FIFO::_prepare_new_part(bool is_head,
			    lr::AioCompletion* c)
{
  std::unique_lock l(m);
  std::vector jentries = { info.next_journal_entry(generate_tag()) };
  std::int64_t new_head_part_num = info.head_part_num;
  auto version = info.version;

  if (is_head) {
    auto new_head_jentry = jentries.front();
    new_head_jentry.op = fifo::journal_entry::Op::set_head;
    new_head_part_num = jentries.front().part_num;
    jentries.push_back(std::move(new_head_jentry));
  }
  l.unlock();

  auto n = new NewPartPreparer(this, c, jentries, 0, new_head_part_num);
  n->set_cur(&NewPartPreparer::callback);
  auto r = _update_meta(fifo::update{}.journal_entries_add(jentries), version,
			&n->canceled, n->cur());
  if (r < 0) {
    n->abandon();
  }
  return r;
}

struct NewHeadPreparer : public Completion<NewHeadPreparer> {
  FIFO* f;
  int i = 0;
  std::int64_t new_head_num;
  bool canceled;

  void handle(int r, bool canceled) {
    std::unique_lock l(f->m);
    auto head_part_num = f->info.head_part_num;
    auto version = f->info.version;
    l.unlock();

    if (r < 0) {
      complete(r);
      return;
    }
    if (canceled) {
      if (i >= MAX_RACE_RETRIES) {
	complete(-ECANCELED);
	return;
      }

      // Raced, but there's still work to do!
      if (head_part_num < new_head_num) {
	set_cur(&NewHeadPreparer::callback);
	++i;
	f->_update_meta(fifo::update{}.head_part_num(new_head_num),
			version, &this->canceled, cur());
	return;
      }
    }
    // Either we succeeded, or we were raced by someone who did it for us.
    complete(0);
    return;
  }

  static void callback(lr::completion_t, void* arg) {
    auto [n, r] = cb_init(arg);
    auto canceled = n->canceled;
    n->canceled = false;
    n->handle(r, canceled);
  }

  static void cb_newpart(lr::completion_t, void* arg) {
    auto [n, r] = cb_init(arg);
    if (r < 0) {
      n->complete(r);
      return;
    }
    std::unique_lock l(n->f->m);
    if (n->f->info.max_push_part_num < n->new_head_num) {
      l.unlock();
      n->complete(-EIO);
    } else {
      l.unlock();
      n->complete(0);
    }
  }

  NewHeadPreparer(FIFO* f, lr::AioCompletion* super,
		  std::int64_t new_head_num)
    : Completion(super), f(f), new_head_num(new_head_num) {}
};

int FIFO::_prepare_new_head(lr::AioCompletion* c)
{
  std::unique_lock l(m);
  int64_t new_head_num = info.head_part_num + 1;
  auto max_push_part_num = info.max_push_part_num;
  auto version = info.version;
  l.unlock();

  auto n = new NewHeadPreparer(this, c, new_head_num);
  int r = 0;
  if (max_push_part_num < new_head_num) {
    n->set_cur(&NewHeadPreparer::cb_newpart);
    r = _prepare_new_part(true, n->cur());
  } else {
    n->set_cur(&NewHeadPreparer::callback);
    r = _update_meta(fifo::update{}.head_part_num(new_head_num), version,
		     &n->canceled, n->cur());
  }
  if (r < 0) {
    n->abandon();
  }
  return r;
}

int FIFO::push_entries(const std::deque<cb::list>& data_bufs,
		       optional_yield y)
{
  std::unique_lock l(m);
  auto head_part_num = info.head_part_num;
  auto tag = info.head_tag;
  const auto part_oid = info.part_oid(head_part_num);
  l.unlock();

  return push_part(ioctx, part_oid, tag, data_bufs, y);
}

int FIFO::push_entries(const std::deque<cb::list>& data_bufs,
		       lr::AioCompletion* c)
{
  std::unique_lock l(m);
  auto head_part_num = info.head_part_num;
  auto tag = info.head_tag;
  const auto part_oid = info.part_oid(head_part_num);
  l.unlock();

  return push_part(ioctx, part_oid, tag, data_bufs, c);
}

int FIFO::trim_part(int64_t part_num, uint64_t ofs,
		    std::optional<std::string_view> tag,
		    bool exclusive,
		    optional_yield y)
{
  lr::ObjectWriteOperation op;
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  rgw::cls::fifo::trim_part(&op, tag, ofs, exclusive);
  return rgw_rados_operate(ioctx, part_oid, &op, y);
}

int FIFO::trim_part(int64_t part_num, uint64_t ofs,
		    std::optional<std::string_view> tag,
		    bool exclusive,
		    lr::AioCompletion* c)
{
  lr::ObjectWriteOperation op;
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  rgw::cls::fifo::trim_part(&op, tag, ofs, exclusive);
  return ioctx.aio_operate(part_oid, c, &op);
}

int FIFO::open(lr::IoCtx ioctx, std::string oid, std::unique_ptr<FIFO>* fifo,
	       optional_yield y, std::optional<fifo::objv> objv)
{
  fifo::info info;
  std::uint32_t size;
  std::uint32_t over;
  int r = get_meta(ioctx, std::move(oid), objv, &info, &size, &over, y);
  if (r >= 0) {
    std::unique_ptr<FIFO> f(new FIFO(std::move(ioctx), oid));
    f->info = info;
    f->part_header_size = size;
    f->part_entry_overhead = over;
    // If there are journal entries, process them, in case
    // someone crashed mid-transaction.
    if (!info.journal.empty()) {
      r = f->process_journal(y);
      if (r >= 0 && fifo)
	*fifo = std::move(f);
    } else {
      *fifo = std::move(f);
    }
  }
  return r;
}

int FIFO::create(lr::IoCtx ioctx, std::string oid, std::unique_ptr<FIFO>* fifo,
		 optional_yield y, std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive, std::uint64_t max_part_size,
		 std::uint64_t max_entry_size)
{
  lr::ObjectWriteOperation op;
  create_meta(&op, oid, objv, oid_prefix, exclusive, max_part_size,
	      max_entry_size);
  int r = rgw_rados_operate(ioctx, oid, &op, y);
  if (r >= 0) {
    r = open(std::move(ioctx), std::move(oid), fifo, y, objv);
  }
  return r;
}

int FIFO::read_meta(optional_yield y) {
  fifo::info _info;
  std::uint32_t _phs;
  std::uint32_t _peo;

  auto r = get_meta(ioctx, oid, nullopt, &_info, &_phs, &_peo, y);
  if (r >= 0) {
    std::unique_lock l(m);
    // We have a newer version already!
    if (_info.version.same_or_later(this->info.version)) {
      info = std::move(_info);
      part_header_size = _phs;
      part_entry_overhead = _peo;
    }
  }
  return r;
}

struct Reader : public Completion<Reader> {
  FIFO* fifo;
  cb::list bl;
  Reader(FIFO* fifo, lr::AioCompletion* super)
    : Completion(super), fifo(fifo) {
    set_cur(&Reader::cb);
  }

  static void cb(lr::completion_t, void* arg) {
    auto [reader, r] = cb_init(arg);
    if (r >= 0) try {
	fifo::op::get_meta_reply reply;
	auto iter = reader->bl.cbegin();
	decode(reply, iter);
	std::unique_lock l(reader->fifo->m);
	if (reply.info.version.same_or_later(reader->fifo->info.version)) {
	  reader->fifo->info = std::move(reply.info);
	  reader->fifo->part_header_size = reply.part_header_size;
	  reader->fifo->part_entry_overhead = reply.part_entry_overhead;
	}
      } catch (const cb::error& err) {
	r = from_error_code(err.code());
      }
    reader->complete(r);
  }
};


int FIFO::read_meta(lr::AioCompletion* c)
{
  lr::ObjectReadOperation op;
  fifo::op::get_meta gm;
  cb::list in;
  encode(gm, in);
  auto reader = new Reader(this, c);
  auto r = ioctx.aio_exec(oid, reader->cur(), fifo::op::CLASS,
			  fifo::op::GET_META, in, &reader->bl);
  if (r < 0) {
    reader->abandon();
  }
  return r;
}


const fifo::info& FIFO::meta() const {
  return info;
}

std::pair<std::uint32_t, std::uint32_t> FIFO::get_part_layout_info() const {
  return {part_header_size, part_entry_overhead};
}

int FIFO::push(const cb::list& bl, optional_yield y) {
  return push(std::vector{ bl }, y);
}

int FIFO::push(const std::vector<cb::list>& data_bufs, optional_yield y)
{
  std::unique_lock l(m);
  auto max_entry_size = info.params.max_entry_size;
  auto need_new_head = info.need_new_head();
  l.unlock();
  if (data_bufs.empty()) {
    return 0;
  }

  // Validate sizes
  for (const auto& bl : data_bufs)
    if (bl.length() > max_entry_size)
      return -E2BIG;

  int r = 0;
  if (need_new_head) {
    r = _prepare_new_head(y);
    if (r < 0)
      return r;
  }

  std::deque<cb::list> remaining(data_bufs.begin(), data_bufs.end());
  std::deque<cb::list> batch;

  uint64_t batch_len = 0;
  auto retries = 0;
  bool canceled = true;
  while ((!remaining.empty() || !batch.empty()) &&
	 (retries <= MAX_RACE_RETRIES)) {
    std::unique_lock l(m);
    auto max_part_size = info.params.max_part_size;
    auto overhead = part_entry_overhead;
    l.unlock();

    while (!remaining.empty() &&
	   (remaining.front().length() + batch_len <= max_part_size)) {
      /* We can send entries with data_len up to max_entry_size,
	 however, we want to also account the overhead when
	 dealing with multiple entries. Previous check doesn't
	 account for overhead on purpose. */
      batch_len += remaining.front().length() + overhead;
      batch.push_back(std::move(remaining.front()));
      remaining.pop_front();
    }

    auto r = push_entries(batch, y);
    if (r >= 0) {
      // Made forward progress!
      canceled = false;
      retries = 0;
      batch_len = 0;
      if (static_cast<unsigned>(r) == batch.size()) {
	batch.clear();
      } else  {
	batch.erase(batch.begin(), batch.begin() + r);
	for (const auto& b : batch) {
	  batch_len +=  b.length() + part_entry_overhead;
	}
      }
    } else if (r == -ERANGE) {
      canceled = true;
      ++retries;
      r = _prepare_new_head(y);
    }
    if (r < 0)
      break;
  }
  if (canceled && r == 0)
    r = -ECANCELED;
  return r;
}

int FIFO::push(const cb::list& bl, lr::AioCompletion* c) {
  return push(std::vector{ bl }, c);
}

struct Pusher : public Completion<Pusher> {
  FIFO* f;
  std::deque<cb::list> remaining;
  std::deque<cb::list> batch;
  int i = 0;

  void prep_then_push(const unsigned successes) {
    std::unique_lock l(f->m);
    auto max_part_size = f->info.params.max_part_size;
    auto part_entry_overhead = f->part_entry_overhead;
    l.unlock();

    uint64_t batch_len = 0;
    if (successes > 0) {
      if (successes == batch.size()) {
	batch.clear();
      } else  {
	batch.erase(batch.begin(), batch.begin() + successes);
	for (const auto& b : batch) {
	  batch_len +=  b.length() + part_entry_overhead;
	}
      }
    }

    if (batch.empty() && remaining.empty()) {
      complete(0);
      return;
    }

    while (!remaining.empty() &&
	   (remaining.front().length() + batch_len <= max_part_size)) {

      /* We can send entries with data_len up to max_entry_size,
	 however, we want to also account the overhead when
	 dealing with multiple entries. Previous check doesn't
	 account for overhead on purpose. */
      batch_len += remaining.front().length() + part_entry_overhead;
      batch.push_back(std::move(remaining.front()));
      remaining.pop_front();
    }
    push();
  }

  void push() {
    set_cur(&push_callback);
    auto r = f->push_entries(batch, cur());
    if (r < 0) {
      clear_cur();
      complete(r);
    }
  }

  void new_head() {
    set_cur(&head_callback);
    auto r = f->_prepare_new_head(cur());
    if (r < 0) {
      clear_cur();
      complete(r);
    }
  }

  // Called with response to push_entries
  static void push_callback(lr::completion_t, void* arg) {
    auto [p, r] = cb_init(arg);
    if (r == -ERANGE) {
      p->new_head();
      return;
    }
    if (r < 0) {
      p->complete(r);
    }
    p->i = 0; // We've made forward progress, so reset the race counter!
    p->prep_then_push(r);
  }

  // Called with response to prepare_new_head
  static void head_callback(lr::completion_t, void* arg) {
    auto [p, r] = cb_init(arg);
    if (r == -ECANCELED) {
      if (p->i == MAX_RACE_RETRIES) {
	p->complete(-ECANCELED);
	return;
      }
      ++p->i;
    } else if (r) {
      p->complete(r);
      return;
    }

    if (p->batch.empty()) {
      p->prep_then_push(0);
      return;
    } else {
      p->push();
      return;
    }
  }

  Pusher(FIFO* f, std::deque<cb::list>&& remaining,
	 std::deque<cb::list> batch,
	 lr::AioCompletion* super)
    : Completion(super), f(f), remaining(std::move(remaining)),
      batch(std::move(batch)) {}
};

int FIFO::push(const std::vector<cb::list>& data_bufs,
		lr::AioCompletion* c)
{
  std::unique_lock l(m);
  auto max_entry_size = info.params.max_entry_size;
  auto need_new_head = info.need_new_head();
  l.unlock();
  // Validate sizes
  for (const auto& bl : data_bufs) {
    if (bl.length() > max_entry_size) {
      return -E2BIG;
    }
  }

  auto p = new Pusher(this, {data_bufs.begin(), data_bufs.end()}, {}, c);
  if (data_bufs.empty() ) {
    // If we return 0, the caller will still expect the completion to
    // be completed.
    p->complete(0);
    return 0;
  }

  if (need_new_head) {
    p->new_head();
  } else {
    p->prep_then_push(0);
  }
  return 0;
}

int FIFO::list(int max_entries,
	       std::optional<std::string_view> markstr,
	       std::vector<list_entry>* presult, bool* pmore,
	       optional_yield y)
{
  std::unique_lock l(m);
  std::int64_t part_num = info.tail_part_num;
  l.unlock();
  std::uint64_t ofs = 0;
  if (markstr) {
    auto marker = to_marker(*markstr);
    if (!marker) return -EINVAL;
    part_num = marker->num;
    ofs = marker->ofs;
  }

  std::vector<list_entry> result;
  result.reserve(max_entries);
  bool more = false;

  std::vector<fifo::part_list_entry> entries;
  int r = 0;
  while (max_entries > 0) {
    bool part_more = false;
    bool part_full = false;

    std::unique_lock l(m);
    auto part_oid = info.part_oid(part_num);
    l.unlock();

    list_part(ioctx, part_oid, {}, ofs, max_entries, &entries,
	      &part_more, &part_full, nullptr, y);
    if (r == -ENOENT) {
      r = read_meta(y);
      if (r < 0) return r;
      if (part_num < info.tail_part_num) {
	/* raced with trim? restart */
	max_entries += result.size();
	result.clear();
	std::unique_lock l(m);
	part_num = info.tail_part_num;
	l.unlock();
	ofs = 0;
	continue;
      }
      /* assuming part was not written yet, so end of data */
      more = false;
      r = 0;
      break;
    }
    if (r < 0) {
      break;
    }
    more = part_full || part_more;
    for (auto& entry : entries) {
      list_entry e;
      e.data = std::move(entry.data);
      e.marker = marker{part_num, entry.ofs}.to_string();
      e.mtime = entry.mtime;
      result.push_back(std::move(e));
      --max_entries;
      if (max_entries == 0)
	break;
    }
    entries.clear();
    if (max_entries > 0 &&
	part_more) {
    }

    if (!part_full) {
      /* head part is not full, so we can assume we're done. */
      break;
    }
    if (!part_more) {
      ++part_num;
      ofs = 0;
    }
  }
  if (r >= 0) {
    if (presult) *presult = std::move(result);
    if (pmore) *pmore =  more;
  }
  return r;
}

int FIFO::trim(std::string_view markstr, bool exclusive, optional_yield y)
{
  auto marker = to_marker(markstr);
  if (!marker) {
    return -EINVAL;
  }
  auto part_num = marker->num;
  auto ofs = marker->ofs;
  std::unique_lock l(m);
  auto pn = info.tail_part_num;
  l.unlock();

  int r;
  while (pn < part_num) {
    std::unique_lock l(m);
    auto max_part_size = info.params.max_part_size;
    l.unlock();
    r = trim_part(pn, max_part_size, std::nullopt, false, y);
    if (r < 0 && r == -ENOENT) {
      return r;
    }
    ++pn;
  }
  r = trim_part(part_num, ofs, std::nullopt, exclusive, y);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  l.lock();
  auto tail_part_num = info.tail_part_num;
  auto objv = info.version;
  l.unlock();
  bool canceled = tail_part_num < part_num;
  int retries = 0;
  while ((tail_part_num < part_num) &&
	 canceled &&
	 (retries <= MAX_RACE_RETRIES)) {
    r = _update_meta(fifo::update{}.tail_part_num(part_num), objv, &canceled,
		     y);
    if (canceled) {
      l.lock();
      tail_part_num = info.tail_part_num;
      objv = info.version;
      l.unlock();
      ++retries;
    }
  }
  if (canceled) {
    r = -EIO;
  }
  return r;
}

struct Trimmer : public Completion<Trimmer> {
  FIFO* fifo;
  std::int64_t part_num;
  std::uint64_t ofs;
  std::int64_t pn;
  bool exclusive;
  bool canceled = false;
  int retries = 0;

  Trimmer(FIFO* fifo, std::int64_t part_num, std::uint64_t ofs, std::int64_t pn,
	  bool exclusive, lr::AioCompletion* super)
    : Completion(super), fifo(fifo), part_num(part_num), ofs(ofs), pn(pn),
      exclusive(exclusive) {}

  static void cb(lr::completion_t, void* arg) {
    auto [t, r] = cb_init(arg);
    if (r == -ENOENT) r = 0;
    if (r < 0) {
      t->complete(r);
      return;
    } 
    t->retries = 0;
    if (t->pn < t->part_num) {
      std::unique_lock l(t->fifo->m);
      const auto max_part_size = t->fifo->info.params.max_part_size;
      l.unlock();
      t->set_cur(&Trimmer::cb);
      r = t->fifo->trim_part(t->pn++, max_part_size, std::nullopt,
			     false, t->cur());
      if (r < 0) {
	t->clear_cur();
	t->complete(r);
      }
    } else {
      std::unique_lock l(t->fifo->m);
      const auto tail_part_num = t->fifo->info.tail_part_num;
      l.unlock();
      t->set_cur(&Trimmer::cb_update);
      t->canceled = tail_part_num < t->part_num;
      r = t->fifo->trim_part(t->part_num, t->ofs, std::nullopt, t->exclusive,
			     t->cur());
      if (r < 0) {
	t->clear_cur();
	t->complete(r);
      }
    }
  }
  static void cb_update(lr::completion_t, void* arg) {
    auto [t, r] = cb_init(arg);
    std::unique_lock l(t->fifo->m);
    auto tail_part_num = t->fifo->info.tail_part_num;
    auto objv = t->fifo->info.version;
    l.unlock();
    if ((tail_part_num < t->part_num) &&
	t->canceled) {
      if (t->retries > MAX_RACE_RETRIES) {
	t->complete(-EIO);
      } else {
	t->set_cur(&Trimmer::cb_update);
	++t->retries;
	auto r = t->fifo->_update_meta(fifo::update{}
				       .tail_part_num(t->part_num),
			               objv, &t->canceled,
                                       t->cur());
	if (r < 0) {
	  t->clear_cur();
	  t->complete(r);
	}
      }
    } else {
      t->complete(0);
    }
  }
};

int FIFO::trim(std::string_view markstr, bool exclusive, lr::AioCompletion* c) {
  auto marker = to_marker(markstr);
  if (!marker) {
    return -EINVAL;
  }
  std::unique_lock l(m);
  const auto max_part_size = info.params.max_part_size;
  const auto pn = info.tail_part_num;
  const auto part_oid = info.part_oid(pn);
  l.unlock();
  auto trimmer = new Trimmer(this, marker->num, marker->ofs, pn, exclusive, c);
  ++trimmer->pn;
  auto ofs = marker->ofs;
  if (pn < marker->num) {
    ofs = max_part_size;
    trimmer->set_cur(&Trimmer::cb);
  } else {
    trimmer->set_cur(&Trimmer::cb_update);
  }
  auto r = trimmer->fifo->trim_part(pn, ofs, std::nullopt, exclusive,
				    trimmer->cur());
  if (r < 0) {
    trimmer->abandon();
  }
  return r;
}

int FIFO::get_part_info(int64_t part_num,
			fifo::part_header* header,
			optional_yield y)
{
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  return rgw::cls::fifo::get_part_info(ioctx, part_oid, header, y);
}

int FIFO::get_part_info(int64_t part_num,
			fifo::part_header* header,
			lr::AioCompletion* c)
{
  std::unique_lock l(m);
  const auto part_oid = info.part_oid(part_num);
  l.unlock();
  auto op = rgw::cls::fifo::get_part_info(header);
  return ioctx.aio_operate(part_oid, c, &op, nullptr);
}

struct InfoGetter : Completion<InfoGetter> {
  FIFO* fifo;
  fifo::info* info;
  fifo::part_header* header;

  InfoGetter(FIFO* fifo, fifo::info* info, fifo::part_header* header,
	     lr::AioCompletion* super)
    : Completion(super), fifo(fifo), info(info), header(header) {
    set_cur(&InfoGetter::cb);
  }
  static void cb(lr::completion_t, void* arg) {
    auto [ig, r] = cb_init(arg);
    if (r < 0) {
      ig->complete(r);
      return;
    }

    auto m = ig->fifo->meta();
    if (ig->info) {
      *ig->info = m;
    }
    auto p = m.head_part_num;
    if (p < 0 && ig->header) {
      *ig->header = fifo::part_header{};
      ig->complete(r);
      return;
    }
    auto op = rgw::cls::fifo::get_part_info(ig->header);
    ig->fifo->ioctx.aio_operate(ig->fifo->info.part_oid(p), ig->super(), &op,
				nullptr);
    ig->abandon();
  }
};

int FIFO::get_head_info(fifo::info* info,
			fifo::part_header* header,
			lr::AioCompletion* c)
{
  auto ig = new InfoGetter(this, info, header, c);
  auto r = read_meta(ig->cur());
  if (r < 0) {
    ig->abandon();
  }
  return r;
}


struct JournalProcessor : public Completion<JournalProcessor> {
private:
  FIFO* const fifo;

  std::vector<fifo::journal_entry> processed;
  std::multimap<std::int64_t, fifo::journal_entry> journal;
  std::multimap<std::int64_t, fifo::journal_entry>::iterator iter;
  std::int64_t new_tail;
  std::int64_t new_head;
  std::int64_t new_max;
  int race_retries = 0;
  bool first_pp = true;
  bool canceled = false;

  void create_part(int64_t part_num, std::string_view tag) {
    set_cur(&JournalProcessor::je_cb);
    lr::ObjectWriteOperation op;
    op.create(false); /* We don't need exclusivity, part_init ensures
			 we're creating from the  same journal entry. */
    std::unique_lock l(fifo->m);
    part_init(&op, tag, fifo->info.params);
    auto oid = fifo->info.part_oid(part_num);
    l.unlock();
    auto r = fifo->ioctx.aio_operate(oid, cur(), &op);
    if (r < 0) {
      clear_cur();
      complete(r);
    }
    return;
  }

  void remove_part(int64_t part_num, std::string_view tag) {
    set_cur(&JournalProcessor::je_cb);
    lr::ObjectWriteOperation op;
    op.remove();
    std::unique_lock l(fifo->m);
    auto oid = fifo->info.part_oid(part_num);
    l.unlock();
    auto r = fifo->ioctx.aio_operate(oid, cur(), &op);
    if (r < 0) {
      clear_cur();
      complete(r);
    }
    return;
  }

  static void je_cb(lr::completion_t, void* arg) {
    auto [j, r] = cb_init(arg);
    j->finish_je(r, j->iter->second);
  }

  void finish_je(int r, const fifo::journal_entry& entry) {
    if (entry.op == fifo::journal_entry::Op::remove && r == -ENOENT)
      r = 0;

    if (r) {
      complete(-EIO);
      return;
    } else {
      switch (entry.op) {
      case fifo::journal_entry::Op::unknown:
      case fifo::journal_entry::Op::set_head:
	// Can't happen. Filtered out in process.
	complete(-EIO);
	return;

      case fifo::journal_entry::Op::create:
	if (entry.part_num > new_max) {
	  new_max = entry.part_num;
	}
	break;
      case fifo::journal_entry::Op::remove:
	if (entry.part_num >= new_tail) {
	  new_tail = entry.part_num + 1;
	}
	break;
      }
      processed.push_back(entry);
    }
    ++iter;
    process();
  }

  void postprocess() {
    if (processed.empty()) {
      complete(0);
      return;
    }
    pp_run(0, false);
  }

public:

  JournalProcessor(FIFO* fifo, lr::AioCompletion* super)
    : Completion(super), fifo(fifo) {
    std::unique_lock l(fifo->m);
    journal = fifo->info.journal;
    iter = journal.begin();
    new_tail = fifo->info.tail_part_num;
    new_head = fifo->info.head_part_num;
    new_max = fifo->info.max_push_part_num;
  }

  static void pp_cb(lr::completion_t, void* arg) {
    auto [j, r] = cb_init(arg);
    auto canceled = j->canceled;
    j->canceled = false;
    j->pp_run(r, canceled);
  }

  void pp_run(int r, bool canceled) {
    std::optional<int64_t> tail_part_num;
    std::optional<int64_t> head_part_num;
    std::optional<int64_t> max_part_num;

    if (!first_pp && r == 0 && !canceled) {
      complete(0);
      return;
    }

    first_pp = false;

    if (canceled) {
      if (race_retries >= MAX_RACE_RETRIES) {
	complete(-ECANCELED);
	return;
      }

      ++race_retries;

      std::vector<fifo::journal_entry> new_processed;
      std::unique_lock l(fifo->m);
      for (auto& e : processed) {
	auto jiter = fifo->info.journal.find(e.part_num);
	/* journal entry was already processed */
	if (jiter == fifo->info.journal.end() ||
	    !(jiter->second == e)) {
	  continue;
	}
	new_processed.push_back(e);
      }
      processed = std::move(new_processed);
    }

    std::unique_lock l(fifo->m);
    auto objv = fifo->info.version;
    if (new_tail > fifo->info.tail_part_num) {
      tail_part_num = new_tail;
    }

    if (new_head > fifo->info.head_part_num) {
      head_part_num = new_head;
    }

    if (new_max > fifo->info.max_push_part_num) {
      max_part_num = new_max;
    }
    l.unlock();

    if (processed.empty() &&
	!tail_part_num &&
	!max_part_num) {
      /* nothing to update anymore */
      complete(0);
      return;
    }
    set_cur(&JournalProcessor::pp_cb);
    r = fifo->_update_meta(fifo::update{}
		           .tail_part_num(tail_part_num)
		           .head_part_num(head_part_num)
		           .max_push_part_num(max_part_num)
			   .journal_entries_rm(processed),
                            objv, &this->canceled, cur());
    if (r < 0) {
      clear_cur();
      complete(r);
      return;
    }
    return;
  }

  JournalProcessor(const JournalProcessor&) = delete;
  JournalProcessor& operator =(const JournalProcessor&) = delete;
  JournalProcessor(JournalProcessor&&) = delete;
  JournalProcessor& operator =(JournalProcessor&&) = delete;

  void process() {
    while (iter != journal.end()) {
      const auto entry = iter->second;
      switch (entry.op) {
      case fifo::journal_entry::Op::create:
	create_part(entry.part_num, entry.part_tag);
	return;
      case fifo::journal_entry::Op::set_head:
	if (entry.part_num > new_head) {
	  new_head = entry.part_num;
	}
	processed.push_back(entry);
	++iter;
	continue;
      case fifo::journal_entry::Op::remove:
	remove_part(entry.part_num, entry.part_tag);
	return;
      default:
	complete(-EIO);
	return;
      }
    }
    postprocess();
    return;
  }

};

void FIFO::process_journal(lr::AioCompletion* c) {
  auto p = new JournalProcessor(this, c);
  p->process();
}

struct Lister : Completion<Lister> {
  FIFO* f;
  std::vector<list_entry> result;
  bool more = false;
  std::int64_t part_num;
  std::uint64_t ofs;
  int max_entries;
  int r_out = 0;
  std::vector<fifo::part_list_entry> entries;
  bool part_more = false;
  bool part_full = false;
  std::vector<list_entry>* entries_out;
  bool* more_out;

  void complete(int r) {
    if (r >= 0) {
      if (more_out) *more_out = more;
      if (entries_out) *entries_out = std::move(result);
    }
    Completion::complete(r);
  }

public:
  Lister(FIFO* f, std::int64_t part_num, std::uint64_t ofs, int max_entries,
	 std::vector<list_entry>* entries_out, bool* more_out,
	 lr::AioCompletion* super)
    : Completion(super), f(f), part_num(part_num), ofs(ofs), max_entries(max_entries),
	entries_out(entries_out), more_out(more_out) {
    result.reserve(max_entries);
  }

  Lister(const Lister&) = delete;
  Lister& operator =(const Lister&) = delete;
  Lister(Lister&&) = delete;
  Lister& operator =(Lister&&) = delete;

  void list() {
    if (max_entries > 0) {
      part_more = false;
      part_full = false;
      entries.clear();

      std::unique_lock l(f->m);
      auto part_oid = f->info.part_oid(part_num);
      l.unlock();

      set_cur(&Lister::list_cb);
      auto op = list_part({}, ofs, max_entries, &r_out,
			  &entries, &part_more, &part_full,
			  nullptr);
      f->ioctx.aio_operate(part_oid, cur(), &op, nullptr);
    } else {
      complete(0);
    }
  }

  static void read_cb(lr::completion_t, void* arg) {
    auto [l, r] = cb_init(arg);
    if (r >= 0) r = l->r_out;
    l->r_out = 0;
    l->read_rep(r);
  }

  void read_rep(int r) {
    if (r < 0) {
      complete(r);
      return;
    }

    if (part_num < f->info.tail_part_num) {
      /* raced with trim? restart */
      max_entries += result.size();
      result.clear();
      part_num = f->info.tail_part_num;
      ofs = 0;
      list();
    }
    /* assuming part was not written yet, so end of data */
    more = false;
    complete(0);
    return;
  }

  static void list_cb(lr::completion_t, void* arg) {
    auto [l, r] = cb_init(arg);
    if (r >= 0) r = l->r_out;
    l->r_out = 0;
    l->list_rep(r);
  }

  void list_rep(int r) {
    auto part_oid = f->info.part_oid(part_num);
    if (r == -ENOENT) {
      set_cur(&Lister::read_cb);
      f->read_meta(cur());
    }
    if (r < 0) {
      clear_cur();
      complete(r);
      return;
    }

    more = part_full || part_more;
    for (auto& entry : entries) {
      list_entry e;
      e.data = std::move(entry.data);
      e.marker = marker{part_num, entry.ofs}.to_string();
      e.mtime = entry.mtime;
      result.push_back(std::move(e));
    }
    max_entries -= entries.size();
    entries.clear();
    if (max_entries > 0 && part_more) {
      list();
      return;
    }

    if (!part_full) { /* head part is not full */
      complete(0);
      return;
    }
    ++part_num;
    ofs = 0;
    list();
  }
};

int FIFO::list(int max_entries,
	       std::optional<std::string_view> markstr,
	       std::vector<list_entry>* out,
	       bool* more, lr::AioCompletion* c) {
  std::unique_lock l(m);
  std::int64_t part_num = info.tail_part_num;
  l.unlock();
  std::uint64_t ofs = 0;

  if (markstr) {
    auto marker = to_marker(*markstr);
    if (!marker) {
      return -EINVAL;
    }
    part_num = marker->num;
    ofs = marker->ofs;
  }

  auto ls = new Lister(this, part_num, ofs, max_entries, out, more, c);
  ls->list();
  return 0;
}
}
