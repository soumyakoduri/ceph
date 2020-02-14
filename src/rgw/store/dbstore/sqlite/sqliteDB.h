// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef SQLITE_DB_H
#define SQLITE_DB_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <sqlite3.h>
#include <dbstore.h>

using namespace std;

int InitPrepareParams(DBOpPrepareParams *params);

class SQLiteDB : public DBstore, public DBOp{
	private:
	sqlite3 *db = NULL; // make this private again
	sqlite3_mutex *mutex = NULL;

	public:	
	sqlite3_stmt *stmt = NULL;
	DBOpPrepareParams PrepareParams;

	SQLiteDB(string tenant_name) : DBstore(tenant_name) {
		InitPrepareParams(&PrepareParams);
	}
	SQLiteDB(string tenant_name, sqlite3 *dbi) : DBstore(tenant_name), db(dbi) {
		InitPrepareParams(&PrepareParams);
	}
	~SQLiteDB() {}

	int exec(const char *schema,
		 int (*callback)(void*,int,char**,char**));
	void *openDB();
	int closeDB();
	int Step(sqlite3_stmt *stmt, int (*cbk)(sqlite3_stmt *stmt));
	int Reset(sqlite3_stmt *stmt);
	int InitializeDBOps();
	int FreeDBOps();

	int createTables();
	int createBucketTable(DBOpParams *params);
	int createUserTable(DBOpParams *params);
	int createObjectTable(DBOpParams *params);
	int createObjectDataTable(DBOpParams *params);

	int DeleteBucketTable(DBOpParams *params);
	int DeleteUserTable(DBOpParams *params);
	int DeleteObjectTable(DBOpParams *params);
	int DeleteObjectDataTable(DBOpParams *params);

	int ListAllBuckets(DBOpParams *params);
	int ListAllUsers(DBOpParams *params);
	int ListAllObjects(DBOpParams *params);
};

class SQLObjectOp : public ObjectOp {
	private:
		string tenant;
		sqlite3 **sdb = NULL;

	public:
		SQLObjectOp(string tenant_name, sqlite3 **sdbi) :
       			tenant(tenant_name), sdb(sdbi) {};
		~SQLObjectOp() {}

		int InitializeObjectOps();
		int FreeObjectOps();
};

class SQLInsertUser : public SQLiteDB, public InsertUserOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLInsertUser(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	~SQLInsertUser() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLRemoveUser : public SQLiteDB, public RemoveUserOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLRemoveUser(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	~SQLRemoveUser() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLListUser : public SQLiteDB, public ListUserOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLListUser(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	~SQLListUser() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLInsertBucket : public SQLiteDB, public InsertBucketOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLInsertBucket(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	~SQLInsertBucket() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLRemoveBucket : public SQLiteDB, public RemoveBucketOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLRemoveBucket(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	~SQLRemoveBucket() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLListBucket : public SQLiteDB, public ListBucketOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLListBucket(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	~SQLListBucket() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLInsertObject : public SQLiteDB, public InsertObjectOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLInsertObject(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	SQLInsertObject(string tenant_name, sqlite3 **sdbi) :
		SQLiteDB(tenant_name, *sdbi), sdb(sdbi) {}

	~SQLInsertObject() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLRemoveObject : public SQLiteDB, public RemoveObjectOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLRemoveObject(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	SQLRemoveObject(string tenant_name, sqlite3 **sdbi) :
		SQLiteDB(tenant_name, *sdbi), sdb(sdbi) {}

	~SQLRemoveObject() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLListObject : public SQLiteDB, public ListObjectOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLListObject(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	SQLListObject(string tenant_name, sqlite3 **sdbi) :
		SQLiteDB(tenant_name, *sdbi), sdb(sdbi) {}

	~SQLListObject() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLPutObjectData : public SQLiteDB, public PutObjectDataOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLPutObjectData(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	SQLPutObjectData(string tenant_name, sqlite3 **sdbi) :
		SQLiteDB(tenant_name, *sdbi), sdb(sdbi) {}

	~SQLPutObjectData() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLGetObjectData : public SQLiteDB, public GetObjectDataOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLGetObjectData(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	SQLGetObjectData(string tenant_name, sqlite3 **sdbi) :
		SQLiteDB(tenant_name, *sdbi), sdb(sdbi) {}

	~SQLGetObjectData() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};

class SQLDeleteObjectData : public SQLiteDB, public DeleteObjectDataOp {
	private:
	sqlite3 **sdb = NULL;
	sqlite3_stmt *stmt = NULL; // Prepared statement

	public:
	SQLDeleteObjectData(string tenant_name, class DBstore *dbi) :
	       	SQLiteDB(tenant_name, (sqlite3 *)dbi->db), sdb((sqlite3 **)&(dbi->db)) {}
	SQLDeleteObjectData(string tenant_name, sqlite3 **sdbi) :
		SQLiteDB(tenant_name, *sdbi), sdb(sdbi) {}

	~SQLDeleteObjectData() {
		if (stmt)
			sqlite3_finalize(stmt);
	}
        int Prepare(DBOpParams *params);
        int Execute(DBOpParams *params);
        int Bind(DBOpParams *params);
};
#endif
