// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "dbstore_mgr.h"

/* Given a tenant, find and return the DBstore handle.
 * If not found and 'create' set to true, create one
 * and return
 */
DBstore* DBstoreManager::getDBstore (string tenant, bool create)
{
    map<string, DBstore*>::iterator iter;
    DBstore *dbs = nullptr;
    pair<map<string, DBstore*>::iterator,bool> ret;

    if (tenant.empty())
        return NULL;

    if (DBstoreHandles.empty())
        goto not_found;

    iter = DBstoreHandles.find(tenant);

    if (iter != DBstoreHandles.end())
        return iter->second;

not_found:
    if (!create)
        return NULL;

    dbs = createDBstore(tenant);

    return dbs;
}

/* Create DBstore instance */
DBstore* DBstoreManager::createDBstore(string tenant) {
    DBstore *dbs = nullptr;
    pair<map<string, DBstore*>::iterator,bool> ret;

    /* Create the handle */
#ifdef SQLITE_ENABLED
    dbs = new SQLiteDB(tenant);
#else
    dbs = new DBstore(tenant);
#endif

    /* API is DBstore::Initialize(string logfile, int loglevel);
     * If none provided, by default write in to dbstore.log file
     * created in current working directory with loglevel L_EVENT.
     * XXX: need to align these logs to ceph location
     */
    if (dbs->Initialize("", -1) < 0) {
         cout<<"^^^^^^^^^^^^DB initialization failed for tenant("<<tenant<<")^^^^^^^^^^^^^^^^^^^^^^^^^^ \n";

        delete dbs;
        return NULL;
    }

    /* XXX: Do we need lock to protect this map?
     */
    ret = DBstoreHandles.insert(pair<string, DBstore*>(tenant, dbs));

    /*
     * Its safe to check for already existing entry (just
     * incase other thread raced and created the entry)
     */
    if (ret.second == false) {
        /* Entry already created by another thread */
        delete dbs;

        dbs = ret.first->second;
    }

    return dbs;
}

void DBstoreManager::deleteDBstore(string tenant) {
    map<string, DBstore*>::iterator iter;
    DBstore *dbs = nullptr;

    if (tenant.empty() || DBstoreHandles.empty())
        return;

    /* XXX: Check if we need to perform this operation under a lock */
    iter = DBstoreHandles.find(tenant);

    if (iter == DBstoreHandles.end())
        return;

    dbs = iter->second;

    DBstoreHandles.erase(iter);
    dbs->Destroy();
    delete dbs;

    return;
}

void DBstoreManager::deleteDBstore(DBstore *dbs) {
    if (!dbs)
        return;

    (void)deleteDBstore(dbs->getTenant());
}


void DBstoreManager::destroyAllHandles(){
    map<string, DBstore*>::iterator iter;
    DBstore *dbs = nullptr;

    if (DBstoreHandles.empty())
        return;

    for (iter = DBstoreHandles.begin(); iter != DBstoreHandles.end();
            ++iter) {
        dbs = iter->second;
        dbs->Destroy();
        delete dbs;
    }

    DBstoreHandles.clear();

    return;
}


