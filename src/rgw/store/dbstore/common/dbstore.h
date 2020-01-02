// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DB_STORE_H
#define DB_STORE_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
#include <fmt/format.h>
#include <map>
#include <dbstore-log.h>

using namespace std;
class DBstore;

struct RGWOpParams {
	string tenant;
	string user_table;
	string bucket_table;
	string object_table;
	string objectdata_table;
	string user_name;
	string bucket_name;
	string object;
	size_t offset;
	string data;
	size_t datalen;
};

/* Used for prepared schemas.
 * Difference with above structure is that all 
 * the fields are strings here to accommodate any
 * style identifiers used by backend db
 */
struct RGWOpPrepareParams {
	string tenant;
	string user_table;
	string bucket_table;
	string object_table;
	string objectdata_table;
	string user_name;
	string bucket_name;
	string object;
	string offset;
	string data;
	string datalen;
};

struct SchemaParams {
	bool is_prepare;
	union {
		struct RGWOpParams *params;
		struct RGWOpPrepareParams *p_params;
	} u;
};

struct RGWOps {
	class InsertUserOp *InsertUser;
	class RemoveUserOp *RemoveUser;
	class ListUserOp *ListUser;
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

class RGWOp {
	private:
	const string CreateUserTableQ =
		"CREATE TABLE IF NOT EXISTS '{}' (	\
			UserName TEXT PRIMARY KEY NOT NULL UNIQUE \n);";
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

	const string DropQ = "DROP TABLE IF EXISTS '{}'";
	const string ListAllQ = "SELECT  * from '{}'";

	public:
	RGWOp() {};

	string CreateTableSchema(string type, RGWOpParams *params);
	string DeleteTableSchema(string table_name);
	string ListTableSchema(string table_name);

	virtual int Prepare(RGWOpParams *params) { return 0; }
	virtual int Execute(RGWOpParams *params) { return 0; }
};

class InsertUserOp : public RGWOp {
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
	const string Query = "INSERT OR REPLACE INTO '{}' (UserName) VALUES ({});";

	public:
	virtual ~InsertUserOp() {}

	string Schema(SchemaParams *s_params);
};

class RemoveUserOp: public RGWOp {
	private:
	const string Query =
	"DELETE from '{}' where UserName = {}";

	public:
	virtual ~RemoveUserOp() {}

	string Schema(SchemaParams *s_params);
};

class ListUserOp: public RGWOp {
	private:
	const string Query =
	"SELECT  * from '{}' where UserName = {}";

	public:
	virtual ~ListUserOp() {}

	string Schema(SchemaParams *s_params);
};

class InsertBucketOp: public RGWOp {
	private:
	const string Query =
	"INSERT OR REPLACE INTO '{}' (BucketName, UserName) VALUES ({}, {})";

	public:
	virtual ~InsertBucketOp() {}

	string Schema(SchemaParams *s_params);
};

class RemoveBucketOp: public RGWOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {}";

	public:
	virtual ~RemoveBucketOp() {}

	string Schema(SchemaParams *s_params);
};

class ListBucketOp: public RGWOp {
	private:
	const string Query =
	"SELECT  * from '{}' where BucketName = {}";

	public:
	virtual ~ListBucketOp() {}

	string Schema(SchemaParams *s_params);
};

class InsertObjectOp: public RGWOp {
	private:
	const string Query =
	"INSERT OR REPLACE INTO '{}' (BucketName, ObjectName) VALUES ({}, {})";

	public:
	virtual ~InsertObjectOp() {}

	string Schema(SchemaParams *s_params);
};

class RemoveObjectOp: public RGWOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {} and ObjectName = {}";

	public:
	virtual ~RemoveObjectOp() {}

	string Schema(SchemaParams *s_params);
};

class ListObjectOp: public RGWOp {
	private:
	const string Query =
	"SELECT  * from '{}' where BucketName = {} and ObjectName = {}";
	// XXX: Include queries for specific bucket and user too

	public:
	virtual ~ListObjectOp() {}

	string Schema(SchemaParams *s_params);
};

class PutObjectDataOp: public RGWOp {
	private:
	const string Query =
	"INSERT OR REPLACE INTO '{}' (BucketName, ObjectName, Offset, Data, Size) \
       		VALUES ({}, {}, {}, {}, {})";

	public:
	virtual ~PutObjectDataOp() {}

	string Schema(SchemaParams *s_params);
};

class GetObjectDataOp: public RGWOp {
	private:
	const string Query =
	"SELECT * from '{}' where BucketName = {} and ObjectName = {}";

	public:
	virtual ~GetObjectDataOp() {}

	string Schema(SchemaParams *s_params);
};

class DeleteObjectDataOp: public RGWOp {
	private:
	const string Query =
	"DELETE from '{}' where BucketName = {} and ObjectName = {}";

	public:
	virtual ~DeleteObjectDataOp() {}

	string Schema(SchemaParams *s_params);
};

class DBstore {
	private:
	const string tenant;
	const string user_table;
	const string bucket_table;
	static map<string, class ObjectOp*> objectmap;
	pthread_mutex_t mutex; // to protect objectmap and other shared
       			       // objects if any. This mutex is taken
			       // before processing every fop (i.e, in
			       // ProcessOp()). If required this can be
			       // made further granular by taking separate
			       // locks for objectmap and db operations etc.

	public:	
	DBstore(string tenant_name) : tenant(tenant_name),
       				user_table(tenant_name+".user.table"),
			        bucket_table(tenant_name+".bucket.table")
       			        {}
	virtual	~DBstore() {}

	string getDBname();
	string getTenant();
	string getUserTable();
	string getBucketTable();
	map<string, class ObjectOp*> getObjectMap();
	void *db; // Backend database handle, make it private?

	struct RGWOps rgwops; // RGW operations, make it private?

	int Initialize(string logfile, int loglevel);
	int Destroy();
	int LockInit();
	int LockDestroy();
	int Lock();
	int Unlock();

	int InitializeParams(string Op, RGWOpParams *params);
	int ProcessOp(string Op, RGWOpParams *params);
	RGWOp* getRGWOp(string Op, struct RGWOpParams *params);
	int objectmapInsert(string bucket, void *ptr);
	int objectmapDelete(string bucket);

        virtual void *openDB() { return NULL; }
        virtual int closeDB() { return 0; }
	virtual int createTables() { return 0; }
	virtual int InitializeRGWOps() { return 0; }
	virtual int FreeRGWOps() { return 0; }

        virtual int ListAllBuckets(RGWOpParams *params) = 0;
        virtual int ListAllUsers(RGWOpParams *params) = 0;
        virtual int ListAllObjects(RGWOpParams *params) = 0;

};
#endif
