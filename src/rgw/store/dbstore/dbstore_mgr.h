// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
#include "common/ceph_context.h"
#include "common/dbstore.h"
#include "sqlite/sqliteDB.h"
#include <boost/lockfree/queue.hpp>

using namespace std;
using namespace rgw::store;
using DB = rgw::store::DB;

/* XXX: Should be a dbstore config option */
const static string default_tenant = "default_ns";
#define MAX_QUEUE_DEFAULT 20

using namespace std;

typedef boost::lockfree::queue<DB*, boost::lockfree::fixed_sized<true>> DBStoreQueue;

class DBStoreManager {
private:
  map<string, DB*> DBStoreHandles;
  DB *default_db = NULL;
  CephContext *cct;
  static DBStoreQueue DBStoreConns;
  std::mutex mutex;

  // used in the dtor for dbstore conn cleanup
  static std::atomic<uint64_t> max_conn; // XXX: make it configurable
  static std::atomic<uint64_t> total_conn; // total connections created so far
  static void delete_conn(const DB* db) {
    delete db;
  }
public:
  DBStoreManager(CephContext *_cct): DBStoreHandles() {
    cct = _cct;
	default_db = createDB(default_tenant);
  };
  DBStoreManager(string logfile, int loglevel): DBStoreHandles() {
    /* No ceph context. Create one with log args provided */
    vector<const char*> args;
    cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                      CODE_ENVIRONMENT_DAEMON, CINIT_FLAG_NO_MON_CONFIG, 1)->get();
    cct->_log->set_log_file(logfile);
    cct->_log->reopen_log_file();
    cct->_conf->subsys.set_log_level(dout_subsys, loglevel);
  };
  ~DBStoreManager() {
     destroyAllHandles();
     DBStoreManager::DBStoreConns.consume_all(delete_conn);};

  /* XXX: TBD based on testing
   * 1)  Lock to protect DBStoreHandles map.
   * 2) Refcount of each DBStore to protect from
   * being deleted while using it.
   */
  DB* getDefaultDB () { return default_db; };
  std::shared_ptr<DB> getDB() {
    DB* db;
    std::string tenant = default_tenant + "_" + to_string(max_conn);
    if (DBStoreManager::DBStoreConns.empty()) {
      db = createDB(default_tenant);
      total_conn++;
      ldout(cct, 0) << "In getDB() tenant(" << tenant << "), total count is :"<< total_conn << " , newly created db:" << db << dendl;
    } else {
      DBStoreManager::DBStoreConns.pop(std::ref(db));
    //  ldout(cct, 0) << "In getDB() tenant(" << tenant << "), outstanding count is :"<< total_conn << " ,db:" << db << dendl;
    } 

    std::shared_ptr<DB> sh(db, [](DB* p){ DBStoreManager::DBStoreConns.push(p); });
    return sh;
  }
  DB* getDB (string tenant, bool create);
  DB* createDB (string tenant);
  void deleteDB (string tenant);
  void deleteDB (DB* db);
  void destroyAllHandles();
};
