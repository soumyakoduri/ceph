// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DB_STORE_H
#define DB_STORE_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
// this seems safe to use, at least for now--arguably, we should
// prefer header-only fmt, in general
#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include "fmt/format.h"
#include <map>
#include "dbstore-log.h"
#include "rgw/rgw_sal.h"
#include "rgw/rgw_sal_dbstore.h"
#include "rgw/rgw_common.h"

using namespace std;
class DBstore;

/* Match with UserTable */
struct DBUserInfo {
	string username;
	string tenant;
	string id;
	string ns;
	string user_email;
	int suspended;
	int max_buckets;
	int op_mask;
	int admin;
	int system;
	int bucket_quota_id;
	int user_quota_id;
	int type;
	int mfaids;
	string assumedrolearn;
	map<string, RGWAccessKey> access_keys;
  	map<string, RGWAccessKey> swift_keys;
  	map<string, RGWSubUser> subusers;
	map<string, uint32_t> user_caps;
	list<string> placement_tags;
	map<int, string> temp_url_keys;
};

struct DBOpInfo {
	DBUserInfo uinfo; // maybe array in case of multiple entries in GetUser?
};

struct DBOpParams {
	string user_table;
	string bucket_table;
	string object_table;
	DBOpInfo op;

	/* Below are subject to change */
	string objectdata_table;
	string quota_table;
	string bucket_name;
	string object;
	size_t offset;
	string data;
	size_t datalen;
};

/* Used for prepared schemas.
 * Difference with above structure is that all 
 * the fields are strings here to accommodate any
 * style identifiers used by backend db. By default
 * initialized with sqlitedb style, can be overriden
 * using InitPrepareParams()
 *
 * These identifiers are used in prepare and bind statements
 * to get the right index of each param.
 */
struct DBUserPrepareParams {
	string username = ":username";
	string tenant = ":tenant";
	string id = ":id";
	string ns = ":ns";
	string user_email = ":user_email";
	string suspended = ":suspended";
	string max_buckets = ":max_buckets";
	string op_mask = ":op_mask";
	string admin = ":admin";
	string system = ":system";
	string bucket_quota_id = ":bucket_quota_id";
	string user_quota_id = ":user_quota_id";
	string type = ":type";
	string mfaids = ":mfaids";
	string assumedrolearn = ":assumedrolearn";
	string access_keys = ":access_keys";
	string swift_keys = ":swift_keys";
	string subusers = ":subusers";
	string user_caps = ":user_caps";
	string placement_tags = ":placement_tags";
	string temp_url_keys = ":temp_url_keys";
};

struct DBOpPrepareParams {
	string user_table = ":user_table";
	string bucket_table = ":bucket_table";
	string object_table = ":object_table";
	string objectdata_table = ":objectdata_table";
	string quota_table = ":quota_table";
	DBUserPrepareParams user;

	/* below subject to change */
	string bucket_name = ":bucket";
	string object = ":object";
	string offset = ":offset";
	string data = ":data";
	string datalen = ":datalen";
};

struct DBOps {
	class InsertUserOp *InsertUser;
	class RemoveUserOp *RemoveUser;
	class GetUserOp *GetUser;
	class InsertBucketOp *InsertBucket;
	class RemoveBucketOp *RemoveBucket;
	class ListBucketOp *ListBucket;
};

class ObjectOp {
        public:
        ObjectOp() {};

        virtual ~ObjectOp() {}

        class InsertObjectOp *InsertObject;
        class RemoveObjectOp *RemoveObject;
        class ListObjectOp *ListObject;
        class PutObjectDataOp *PutObjectData;
        class GetObjectDataOp *GetObjectData;
        class DeleteObjectDataOp *DeleteObjectData;

	virtual int InitializeObjectOps() { return 0; }
	virtual int FreeObjectOps() { return 0; }
};

class DBOp {
	private:
	const string CreateUserTableQ =
		"CREATE TABLE IF NOT EXISTS '{}' (	\
			UserName TEXT PRIMARY KEY NOT NULL UNIQUE , \
	       		Tenant TEXT ,		\
			ID TEXT ,		\
			NS TEXT ,		\
			UserEmail TEXT ,	\
			Suspended INTEGER ,	\
			MaxBuckets INTEGER ,	\
			OpMask	INTEGER ,	\
			Admin	INTEGER ,	\
			System INTEGER , 	\
			BucketQuotaID INTEGER ,	\
			UserQuotaID INTEGER ,	\
			TYPE INTEGER ,		\
			MfaIDs INTEGER ,	\
			AssumedRoleARN TEXT ,	\
			AccessKeys BLOB ,	\
			SwiftKeys BLOB ,	\
			SubUsers BLOB ,		\
			UserCaps BLOB ,		\
			PlacementTags BLOB ,	\
		        TempURLKeys BLOB \n);";

	const string CreateBucketTableQ =
		"CREATE TABLE IF NOT EXISTS '{}' ( \
			BucketName TEXT PRIMARY KEY NOT NULL UNIQUE , \
			UserName TEXT NOT NULL, \
			FOREIGN KEY (UserName) \
				REFERENCES '{}' (UserName) ON DELETE CASCADE ON UPDATE CASCADE \n);";
	const string CreateObjectTableQ =
		"CREATE TABLE IF NOT EXISTS '{}' ( \
			BucketName TEXT NOT NULL , \
			ObjectName TEXT NOT NULL , \
			PRIMARY KEY (BucketName, ObjectName), \
			FOREIGN KEY (BucketName) \
				REFERENCES '{}' (BucketName) ON DELETE CASCADE ON UPDATE CASCADE \n);";
        const string CreateObjectDataTableQ =
                "CREATE TABLE IF NOT EXISTS '{}' ( \
			BucketName TEXT NOT NULL , \
			ObjectName TEXT NOT NULL , \
                        Offset   INTEGER NOT NULL, \
                        Data     BLOB,             \
			Size 	 INTEGER NOT NULL, \
			PRIMARY KEY (BucketName, ObjectName, Offset), \
                        FOREIGN KEY (BucketName, ObjectName) \
                                REFERENCES '{}' (BucketName, ObjectName) ON DELETE CASCADE ON UPDATE CASCADE \n);";

	const string CreateQuotaTableQ =
		"CREATE TABLE IF NOT EXISTS '{}' ( \
			ID INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE , \
			MaxSizeSoftThreshold INTEGER ,	\
			MaxObjsSoftThreshold INTEGER ,	\
			MaxSize	INTEGER ,		\
			MaxObjects INTEGER ,		\
			Enabled Boolean ,		\
			CheckOnRaw Boolean \n);";

	const string DropQ = "DROP TABLE IF EXISTS '{}'";
	const string ListAllQ = "SELECT  * from '{}'";

	public:
	DBOp() {};
        virtual ~DBOp() {};

	string CreateTableSchema(string type, DBOpParams *params) {
		if (!type.compare("User"))
			return fmt::format(CreateUserTableQ.c_str(),
				           params->user_table.c_str());
		if (!type.compare("Bucket"))
			return fmt::format(CreateBucketTableQ.c_str(),
				           params->bucket_table.c_str(),
					   params->user_table.c_str());
		if (!type.compare("Object"))
			return fmt::format(CreateObjectTableQ.c_str(),
				           params->object_table.c_str(),
					   params->bucket_table.c_str());
		if (!type.compare("ObjectData"))
			return fmt::format(CreateObjectDataTableQ.c_str(),
				           params->objectdata_table.c_str(),
					   params->object_table.c_str());
		if (!type.compare("Quota"))
			return fmt::format(CreateQuotaTableQ.c_str(),
				           params->quota_table.c_str());

		dbout(L_ERR)<<"Incorrect table type("<<type<<") specified \n";

		return NULL;
	}

	string DeleteTableSchema(string table) {
		return fmt::format(DropQ.c_str(), table.c_str());
	}
	string ListTableSchema(string table) {
		return fmt::format(ListAllQ.c_str(), table.c_str());
	}

	virtual int Prepare(DBOpParams *params) { return 0; }
	virtual int Execute(DBOpParams *params) { return 0; }
};

class InsertUserOp : public DBOp {
	private:
	/* For existing entires, -
	 * (1) INSERT or REPLACE - it will delete previous entry and then
	 * inserts new one. Since it deletes previos enties, it will
	 * trigger all foriegn key cascade deletes or other triggers.
	 * (2) INSERT or UPDATE - this will set NULL values to unassigned
	 * fields.
	 * more info: https://code-examples.net/en/q/377728
	 *
	 * For now using INSERT or REPLACE. If required of updating existing
	 * record, will use another query.
	 */
	const string Query = "INSERT OR REPLACE INTO '{}'	\
	       		(UserName, Tenant, ID, NS, UserEmail, Suspended,  \
			 MaxBuckets, OpMask, Admin, System, BucketQuotaID,\
		 	 UserQuotaID, Type, MfaIDs, AssumedRoleARN, AccessKeys, \
			 SwiftKeys, SubUsers, UserCaps, PlacementTags, TempURLKeys )	\
		         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
				 {}, {}, {}, {}, {}, {}, {}, {}, {}, {});";

	public:
	virtual ~InsertUserOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(), params.user_table.c_str(),
			       params.user.username.c_str(), params.user.tenant,
			       params.user.id, params.user.ns, params.user.user_email,
			       params.user.suspended,
			       params.user.max_buckets, params.user.op_mask,
			       params.user.admin, params.user.system,
			       params.user.bucket_quota_id,
			       params.user.user_quota_id, params.user.type, params.user.mfaids,
			       params.user.assumedrolearn, params.user.access_keys,
			       params.user.swift_keys, params.user.subusers,
			       params.user.user_caps, params.user.placement_tags,
			       params.user.temp_url_keys);
	}

};

class RemoveUserOp: public DBOp {
	private:
	const string Query =
	"DELETE from '{}' where UserName = {}";

	public:
	virtual ~RemoveUserOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(), params.user_table.c_str(),
			       params.user.username.c_str());
	}
};

class GetUserOp: public DBOp {
	private:
	/* If below query columns are updated, make sure to update the indexes
	 * in list_user() cbk in sqliteDB.cc */
	const string Query = "SELECT \
	       		 UserName, Tenant, ID, NS, UserEmail, Suspended,  \
			 MaxBuckets, OpMask, Admin, System, BucketQuotaID,\
		 	 UserQuotaID, Type, MfaIDs, AssumedRoleARN, AccessKeys, \
			 SwiftKeys, SubUsers, UserCaps, PlacementTags, TempURLKeys \
			 from '{}' where UserName = {}";

	public:

	public:
	virtual ~GetUserOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(), params.user_table.c_str(),
				   params.user.username.c_str());
	}
};

class InsertBucketOp: public DBOp {
	private:
	const string Query =
	"INSERT OR REPLACE INTO '{}' (BucketName, UserName) VALUES ({}, {})";

	public:
	virtual ~InsertBucketOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(), params.bucket_table.c_str(),
			       	params.bucket_name.c_str(), params.user.username.c_str());
	}
};

class RemoveBucketOp: public DBOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {}";

	public:
	virtual ~RemoveBucketOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(), params.bucket_table.c_str(),
			       params.bucket_name.c_str());
	}
};

class ListBucketOp: public DBOp {
	private:
	const string Query =
	"SELECT  * from '{}' where BucketName = {}";

	public:
	virtual ~ListBucketOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(), params.bucket_table.c_str(),
			       params.bucket_name.c_str());
	}
};

class InsertObjectOp: public DBOp {
	private:
	const string Query =
	"INSERT OR REPLACE INTO '{}' (BucketName, ObjectName) VALUES ({}, {})";

	public:
	virtual ~InsertObjectOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(),
		 	params.object_table.c_str(), params.bucket_name.c_str(),
		        params.object.c_str());
	}
};

class RemoveObjectOp: public DBOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {} and ObjectName = {}";

	public:
	virtual ~RemoveObjectOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(), params.object_table.c_str(),
			       params.bucket_name.c_str(), params.object.c_str());
	}
};

class ListObjectOp: public DBOp {
	private:
	const string Query =
	"SELECT  * from '{}' where BucketName = {} and ObjectName = {}";
	// XXX: Include queries for specific bucket and user too

	public:
	virtual ~ListObjectOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(), params.object_table.c_str(),
			       params.bucket_name.c_str(), params.object.c_str());
	}
};

class PutObjectDataOp: public DBOp {
	private:
	const string Query =
	"INSERT OR REPLACE INTO '{}' (BucketName, ObjectName, Offset, Data, Size) \
       		VALUES ({}, {}, {}, {}, {})";

	public:
	virtual ~PutObjectDataOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(),
		 	params.objectdata_table.c_str(),
		       	params.bucket_name.c_str(), params.object.c_str(),
		       	params.offset.c_str(), params.data.c_str(),
			params.datalen.c_str());
	}
};

class GetObjectDataOp: public DBOp {
	private:
	const string Query =
	"SELECT * from '{}' where BucketName = {} and ObjectName = {}";

	public:
	virtual ~GetObjectDataOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(),
		 	params.objectdata_table.c_str(), params.bucket_name.c_str(),
		        params.object.c_str());
	}
};

class DeleteObjectDataOp: public DBOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {} and ObjectName = {}";

	public:
	virtual ~DeleteObjectDataOp() {}

	string Schema(DBOpPrepareParams &params) {
		return fmt::format(Query.c_str(),
	 		params.objectdata_table.c_str(), params.bucket_name.c_str(),
	        	params.object.c_str());
	}
};

class DBstore {
	private:
	const string db_name;
	const string user_table;
	const string bucket_table;
	const string quota_table;
	static map<string, class ObjectOp*> objectmap;
	pthread_mutex_t mutex; // to protect objectmap and other shared
       			       // objects if any. This mutex is taken
			       // before processing every fop (i.e, in
			       // ProcessOp()). If required this can be
			       // made further granular by taking separate
			       // locks for objectmap and db operations etc.

        protected:
	void *db;

	public:	
	DBstore(string db_name) : db_name(db_name),
       				user_table(db_name+".user.table"),
			        bucket_table(db_name+".bucket.table"),
			        quota_table(db_name+".quota.table")
       			        {}
/*	DBstore() {}*/

	DBstore() : db_name("default_db"),
       		    user_table("user.table"),
		    bucket_table("bucket.table"),
		    quota_table("quota.table")
       		    {}
	virtual	~DBstore() {}

	const string getDBname() { return db_name + ".db"; }
	const string getUserTable() { return user_table; }
	const string getBucketTable() { return bucket_table; }
	const string getQuotaTable() { return quota_table; }

	map<string, class ObjectOp*> getObjectMap();

	struct DBOps dbops; // DB operations, make it private?

	int Initialize(string logfile, int loglevel);
	int Destroy();
	int LockInit();
	int LockDestroy();
	int Lock();
	int Unlock();

	int InitializeParams(string Op, DBOpParams *params);
	int ProcessOp(string Op, DBOpParams *params);
	DBOp* getDBOp(string Op, struct DBOpParams *params);
	int objectmapInsert(string bucket, void *ptr);
	int objectmapDelete(string bucket);

        virtual void *openDB() { return NULL; }
        virtual int closeDB() { return 0; }
	virtual int createTables() { return 0; }
	virtual int InitializeDBOps() { return 0; }
	virtual int FreeDBOps() { return 0; }
	virtual int InitPrepareParams(DBOpPrepareParams &params) = 0;

        virtual int ListAllBuckets(DBOpParams *params) = 0;
        virtual int ListAllUsers(DBOpParams *params) = 0;
        virtual int ListAllObjects(DBOpParams *params) = 0;

	int get_user(const std::string& query_str, const std::string& query_str_val,
			std::unique_ptr<RGWUser>* user);
};
#endif
