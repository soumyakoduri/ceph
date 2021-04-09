// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "dbstore.h"

using namespace std;

map<string, class ObjectOp*> DBStore::objectmap = {};

map<string, class ObjectOp*> DBStore::getObjectMap() {
	return DBStore::objectmap;
}

/* Custom Logging initialization */
ofstream fileout;
ostream *dbout;
int LogLevel = L_FULLDEBUG;
string LogFile = "dbstore.log";

static void LogInit(string logfile, int loglevel) {
        if (loglevel >= L_ERR && loglevel <= L_FULLDEBUG)
                LogLevel = loglevel;

        if (!logfile.empty()) {
                LogFile = logfile;
        }

        fileout.open(LogFile);
	dbout = &fileout;

        return;
}

static void LogDestroy() {
        if(dbout && (dbout != &cout))
                fileout.close();
        return;
}

int DBStore::Initialize(string logfile, int loglevel)
{
	int ret = -1;

	LogInit(logfile, loglevel);

	db = openDB();

	if (!db) {
		dbout(L_ERR)<<"Failed to open database \n";
		return ret;
	}

	ret = LockInit();

	if (ret) {
        	dbout(L_ERR)<<"Error: mutex is NULL \n";
                closeDB();
		db = NULL;
                return ret;
        }

	ret = InitializeDBOps();

	if (ret) {
        	dbout(L_ERR)<<"InitializeDBOps failed \n";
		LockDestroy();
                closeDB();
		db = NULL;
                return ret;
        }

	dbout(L_FULLDEBUG)<< "DBStore successfully initialized - name:" \
			<< db_name << "\n";

	return ret;
}

int DBStore::Destroy()
{
	if (!db)
		return 0;

	closeDB();

	LockDestroy();

	FreeDBOps();

	dbout(L_FULLDEBUG)<<"DBStore successfully destroyed - name:" \
			<<db_name<<"\n";

	LogDestroy();
	
	return 0;
}

int DBStore::LockInit() {
	int ret;
	
	ret = pthread_mutex_init(&mutex, NULL);

	if (ret)
		dbout(L_ERR)<<"pthread_mutex_init failed \n";

	return ret;
}

int DBStore::LockDestroy() {
	int ret;
	
	ret = pthread_mutex_destroy(&mutex);

	if (ret)
		dbout(L_ERR)<<"pthread_mutex_destroy failed \n";

	return ret;
}

int DBStore::Lock() {
	int ret;

	ret = pthread_mutex_lock(&mutex);

	if (ret)
		dbout(L_ERR)<<"pthread_mutex_lock failed \n";

	return ret;
}

int DBStore::Unlock() {
	int ret;

	ret = pthread_mutex_unlock(&mutex);

	if (ret)
		dbout(L_ERR)<<"pthread_mutex_unlock failed \n";

	return ret;
}

DBOp * DBStore::getDBOp(string Op, struct DBOpParams *params)
{
	if (!Op.compare("InsertUser"))
		return dbops.InsertUser;
	if (!Op.compare("RemoveUser"))
		return dbops.RemoveUser;
	if (!Op.compare("GetUser"))
		return dbops.GetUser;
	if (!Op.compare("InsertBucket"))
		return dbops.InsertBucket;
	if (!Op.compare("RemoveBucket"))
		return dbops.RemoveBucket;
	if (!Op.compare("GetBucket"))
		return dbops.GetBucket;

	/* Object Operations */
	map<string, class ObjectOp*>::iterator iter;
	class ObjectOp* Ob;

	iter = DBStore::objectmap.find(params->op.bucket.ent.bucket.name);

	if (iter == DBStore::objectmap.end()) {
		dbout(L_EVENT)<<"No objectmap found for bucket: " \
			     <<params->op.bucket.ent.bucket.name<<"\n";
		/* not found */
		return NULL;
	}

	Ob = iter->second;

	if (!Op.compare("InsertObject"))
		return Ob->InsertObject;
	if (!Op.compare("RemoveObject"))
		return Ob->RemoveObject;
	if (!Op.compare("ListObject"))
		return Ob->ListObject;
	if (!Op.compare("PutObjectData"))
		return Ob->PutObjectData;
	if (!Op.compare("GetObjectData"))
		return Ob->GetObjectData;
	if (!Op.compare("DeleteObjectData"))
		return Ob->DeleteObjectData;

	return NULL;
}

int DBStore::objectmapInsert(string bucket, void *ptr)
{
	map<string, class ObjectOp*>::iterator iter;
	class ObjectOp *Ob;

	iter = DBStore::objectmap.find(bucket);

	if (iter != DBStore::objectmap.end()) {
		// entry already exists
		// return success or replace it or
		// return error ?
		// return success for now
		dbout(L_DEBUG)<<"Objectmap entry already exists for bucket("\
			     <<bucket<<"). Not inserted \n";
		return 0;
	}

	Ob = (class ObjectOp*) ptr;
	Ob->InitializeObjectOps();

	DBStore::objectmap.insert(pair<string, class ObjectOp*>(bucket, Ob));

	return 0;
}

int DBStore::objectmapDelete(string bucket)
{
	map<string, class ObjectOp*>::iterator iter;
	class ObjectOp *Ob;

	iter = DBStore::objectmap.find(bucket);

	if (iter == DBStore::objectmap.end()) {
		// entry doesn't exist
		// return success or return error ?
		// return success for now
		dbout(L_DEBUG)<<"Objectmap entry for bucket("<<bucket<<") "
			     <<"doesnt exist to delete \n";
		return 0;
	}

	Ob = (class ObjectOp*) (iter->second);
	Ob->FreeObjectOps();

	DBStore::objectmap.erase(iter);

	return 0;
}

int DBStore::InitializeParams(string Op, DBOpParams *params)
{
	int ret = -1;

	if (!params)
		goto out;

	//reset params here
	params->user_table = user_table;
	params->bucket_table = bucket_table;

	ret = 0;
out:
	return ret;
}

int DBStore::ProcessOp(string Op, struct DBOpParams *params) {
	int ret = -1;
	class DBOp *db_op;

	Lock();
	db_op = getDBOp(Op, params);

	if (!db_op) {
		dbout(L_ERR)<<"No db_op found for Op("<<Op<<")\n";
		Unlock();
		return ret;
	}
	ret = db_op->Execute(params);

	Unlock();
	if (ret) {
		dbout(L_ERR)<<"In Process op Execute failed for fop(" \
			   <<Op.c_str()<<") \n";
	} else {
		dbout(L_FULLDEBUG)<<"Successfully processed fop(" \
			   <<Op.c_str()<<") \n";
	}

	return ret;
}

int DBStore::get_user(const std::string& query_str, const std::string& query_str_val,
                      RGWUserInfo& uinfo) {
	int ret = 0;

	if (query_str.empty()) {
		// not checking for query_str_val as the query can be to fetch
		// entries with null values
		return -1;
	}

    DBOpParams params = {};
    InitializeParams("GetUser", &params);

	params.op.get_query_str = query_str;

	// validate query_str with UserTable entries names
	if (query_str == "username") {
		params.op.user.uinfo.display_name = query_str_val;
	} else if (query_str == "email") {
		params.op.user.uinfo.user_email = query_str_val;
	} else if (query_str == "access_key") {
        RGWAccessKey k(query_str_val, "");
        map<string, RGWAccessKey> keys;
        keys[query_str_val] = k;
		params.op.user.uinfo.access_keys = keys;
	} else if (query_str == "user_id") {
		params.op.user.uinfo.user_id = uinfo.user_id;
	} else {
		dbout(L_ERR)<<"In GetUser Invalid query string :" <<query_str.c_str()<<") \n";
		return -1;
	}

    ret = ProcessOp("GetUser", &params);

	if (ret)
		goto out;

    uinfo = params.op.user.uinfo;

out:
	return ret;
}

