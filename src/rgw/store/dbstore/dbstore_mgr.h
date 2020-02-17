// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <dbstore.h>
#include <sqliteDB.h>

class DBstoreManager {
private:
  map<string, DBstore*> DBstoreHandles;

public:
  DBstoreManager(): DBstoreHandles() {}
  ~DBstoreManager() { destroyAllHandles(); };

  /* XXX: TBD based on testing
   * 1)  Lock to protect DBstoreHandles map.
   * 2) Refcount of each DBstore to protect from
   * being deleted while using it.
   */
  DBstore* getDBstore (string tenant, bool create);
  DBstore* createDBstore (string tenant);
  void deleteDBstore (string tenant);
  void deleteDBstore (DBstore* db);
  void destroyAllHandles();
};
