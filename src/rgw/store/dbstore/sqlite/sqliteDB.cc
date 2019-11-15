// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "sqliteDB.h"

#define SQL_PREPARE(params, sdb, stmt, ret, Op) 	\
do {							\
	string schema;			   		\
	schema = Schema(&params);	   		\
							\
	sqlite3_prepare_v2 (*sdb, schema.c_str(), 	\
		            -1, &stmt , NULL);		\
	if (!stmt) {					\
	        dout(L_ERR)<<"failed to prepare statement " \
	       		   <<"for Op("<<Op<<"); Errmsg -"\
	       		   <<sqlite3_errmsg(*sdb)<<"\n";\
		ret = -1;				\
		goto out;				\
	}						\
	dout(L_DEBUG)<<"Successfully Prepared stmt for Op("<<Op	\
		 	 <<") schema("<<schema<<") stmt("<<stmt<<")\n";	\
	ret = 0;					\
} while(0);

#define SQL_BIND_INDEX(stmt, index, str, sdb)	\
do {						\
	index = sqlite3_bind_parameter_index(stmt, str);     \
							     \
	if (index <=0)  {				     \
	        dout(L_ERR)<<"failed to fetch bind parameter"\
	       	  	   " index for str("<<str<<") in "   \
			   <<"stmt("<<stmt<<"); Errmsg -"    \
	       		   <<sqlite3_errmsg(*sdb)<<"\n"; 	     \
		rc = -1;				     \
		goto out;				     \
	}						     \
	dout(L_FULLDEBUG)<<"Bind parameter index for str("  \
			 <<str<<") in stmt("<<stmt<<") is "  \
       		         <<index<<"\n";			     \
}while(0);

#define SQL_BIND_TEXT(stmt, index, str, sdb)			\
do {								\
	rc = sqlite3_bind_text(stmt, index, str, -1, SQLITE_STATIC); 	\
								\
	if (rc != SQLITE_OK) {					      	\
	        dout(L_ERR)<<"sqlite bind text failed for index("     	\
			   <<index<<"), str("<<str<<") in stmt("   	\
			   <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
			   <<"\n";				\
		rc = -1;					\
		goto out;					\
	}							\
}while(0);

#define SQL_BIND_INT(stmt, index, num, sdb)			\
do {								\
	rc = sqlite3_bind_int(stmt, index, num);		\
								\
	if (rc != SQLITE_OK) {					\
	        dout(L_ERR)<<"sqlite bind int failed for index("     	\
			   <<index<<"), num("<<num<<") in stmt("   	\
			   <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
			   <<"\n";				\
		rc = -1;					\
		goto out;					\
	}							\
}while(0);

#define SQL_BIND_BLOB(stmt, index, blob, size, sdb)		\
do {								\
	rc = sqlite3_bind_blob(stmt, index, blob, size, NULL);  \
								\
	if (rc != SQLITE_OK) {					\
	        dout(L_ERR)<<"sqlite bind blob failed for index("     	\
			   <<index<<"), blob("<<blob<<") in stmt("   	\
			   <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
			   <<"\n";				\
		rc = -1;					\
		goto out;					\
	}							\
}while(0);

#define SQL_EXECUTE(params, stmt, cbk, args...) \
do{						\
	if (!stmt) {				\
		ret = Prepare(params);		\
	}					\
						\
	if (!stmt) {				\
		dout(L_ERR)<<"No prepared statement \n";	\
		goto out;			\
	}					\
						\
	ret = Bind(params);			\
	if (ret) {				\
		dout(L_ERR)<<"Bind parameters failed for stmt("	\
			   <<stmt<<") \n";		\
		goto out;			\
	}					\
						\
	ret = Step(stmt, cbk);			\
						\
	Reset(stmt);				\
						\
	if (ret) {				\
		dout(L_ERR)<<"Execution failed for stmt("	\
			   <<stmt<<")\n";		\
		goto out;			\
	}					\
}while(0);

static int list_callback(void *None, int argc, char **argv, char **aname)
{
        int i;
        for(i=0; i<argc; i++) {
		string arg = argv[i] ? argv[i] : "NULL";
                cout<<aname[i]<<" = "<<arg<<"\n";
        }
        return 0;
}


static int list_user(sqlite3_stmt *stmt) {
	if (!stmt)
		return -1;

	cout<<sqlite3_column_text(stmt, 0)<<"\n";

	return 0;
}

static int list_bucket(sqlite3_stmt *stmt) {
	if (!stmt)
		return -1;

	cout<<sqlite3_column_text(stmt, 0)<<", ";
	cout<<sqlite3_column_text(stmt, 1)<<"\n";

	return 0;
}

static int list_object(sqlite3_stmt *stmt) {
	if (!stmt)
		return -1;

	cout<<sqlite3_column_text(stmt, 0)<<", ";
	cout<<sqlite3_column_text(stmt, 1)<<"\n";

	return 0;
}

static int get_objectdata(sqlite3_stmt *stmt) {
	if (!stmt)
		return -1;

	int datalen = 0;
	const void *blob = NULL;
	int i = 0;

	blob = sqlite3_column_blob(stmt, 3);
	datalen = sqlite3_column_bytes(stmt, 3);

	cout<<sqlite3_column_text(stmt, 0)<<", ";
	cout<<sqlite3_column_text(stmt, 1)<<",";
	cout<<sqlite3_column_int(stmt, 2)<<",";
	char data[datalen+1] = {};
	if (blob)
		strncpy(data, (const char *)blob, datalen);

	cout<<data<<","<<datalen<<"\n";

	return 0;
}

int SQLiteDB::InitializeRGWOps()
{
	string tenant = getTenant();

        (void)createTables();
        rgwops.InsertUser = new SQLInsertUser(tenant, this);
        rgwops.RemoveUser = new SQLRemoveUser(tenant, this);
        rgwops.ListUser = new SQLListUser(tenant, this);
        rgwops.InsertBucket = new SQLInsertBucket(tenant, this);
        rgwops.RemoveBucket = new SQLRemoveBucket(tenant, this);
        rgwops.ListBucket = new SQLListBucket(tenant, this);

	return 0;
}

int SQLiteDB::FreeRGWOps()
{
        delete rgwops.InsertUser;
        delete rgwops.RemoveUser;
        delete rgwops.ListUser;
        delete rgwops.InsertBucket;
        delete rgwops.RemoveBucket;
        delete rgwops.ListBucket;

	return 0;
}

int InitPrepareParams(RGWOpPrepareParams *params)
{
	if (!params) {
		dout(L_ERR)<<"PrepareParams passed is NULL\n";
		return -1;
	}

	params->tenant = ":tenant";
	params->user_table = ":user_table";
	params->object_table = ":object_table";
	params->objectdata_table = ":objectdata_table";
	params->user_name = ":user";
	params->bucket_name = ":bucket";
	params->object = ":object";
	params->offset = ":offset";
	params->data = ":data";
	params->datalen = ":datalen";

	return 0;
}

void *SQLiteDB::openDB()
{
	string dbname;
        int rc = 0;
	char *errmsg = NULL;

	dbname	= getDBname();
	if (dbname.empty()) {
		dout(L_ERR)<<"dbname is NULL\n";
		goto out;
	}

	rc = sqlite3_open_v2(dbname.c_str(), &db,
			     SQLITE_OPEN_READWRITE |
			     SQLITE_OPEN_CREATE |
			     SQLITE_OPEN_FULLMUTEX,
			     NULL);

        if (rc) {
		dout(L_ERR)<<"Cant open "<<dbname<<"; Errmsg - "\
			   <<sqlite3_errmsg(db)<<"\n";
        } else {
                dout(L_DEBUG)<<"Opened database("<<dbname<<") successfully\n";
        }

	exec("PRAGMA foreign_keys=ON", NULL);

out:
	return db;
}

int SQLiteDB::closeDB()
{
	if (db)
		sqlite3_close((sqlite3 *)db);

	db = NULL;

	return 0;
}

int SQLiteDB::Reset(sqlite3_stmt *stmt)
{
	int ret = -1;

	if (!stmt) {
		return -1;
	}
	sqlite3_clear_bindings(stmt);
	ret = sqlite3_reset(stmt);
out:
	return ret;
}

int SQLiteDB::Step(sqlite3_stmt *stmt, int (*cbk)(sqlite3_stmt *stmt))
{
	int ret = -1;

	if (!stmt)
		return -1;

again:
		ret = sqlite3_step(stmt);

		if ((ret != SQLITE_DONE) && (ret != SQLITE_ROW)) {
			dout(L_ERR)<<"sqlite step failed for stmt("<<stmt \
				   <<"); Errmsg - "<<sqlite3_errmsg(db)<<"\n";
			return -1;
		} else if (ret == SQLITE_ROW) {
			if (cbk)
				(*cbk)(stmt);
			goto again;
		}

	dout(L_FULLDEBUG)<<"sqlite step successfully executed for stmt(" \
			 <<stmt<<") \n";

	return 0;
}

int SQLiteDB::exec(const char *schema,
	       int (*callback)(void*,int,char**,char**))
{
	int ret = -1;
	char *errmsg = NULL;

	if (!db)
		goto out;

	ret = sqlite3_exec(db, schema, callback, 0, &errmsg);
        if (ret != SQLITE_OK) {
		dout(L_ERR)<<"sqlite exec failed for schema("<<schema \
				   <<"); Errmsg - "<<errmsg<<"\n";
                sqlite3_free(errmsg);
		goto out;
        }
	ret = 0;
	dout(L_FULLDEBUG)<<"sqlite exec successfully processed for schema(" \
			<<schema<<")\n";
out:
	return ret;
}

int SQLiteDB::createTables()
{
	int ret = -1;
	int cu, cb = -1;
	RGWOpParams params = {};

	params.user_table = getUserTable();
	params.bucket_table = getBucketTable();

	if (cu = createUserTable(&params))
		goto out;

	if (cb = createBucketTable(&params))
		goto out;

	ret = 0;
out:
	if (ret) {
		if (cu)
			DeleteUserTable(&params);
		if (cb)
			DeleteBucketTable(&params);
		dout(L_ERR)<<"Creation of tables failed \n";
	}

	return ret;
}

int SQLiteDB::createUserTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("User", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dout(L_ERR)<<"CreateUserTable failed \n";

	dout(L_FULLDEBUG)<<"CreateUserTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::createBucketTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("Bucket", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dout(L_ERR)<<"CreateBucketTable failed \n";

	dout(L_FULLDEBUG)<<"CreateBucketTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::createObjectTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("Object", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dout(L_ERR)<<"CreateObjectTable failed \n";

	dout(L_FULLDEBUG)<<"CreateObjectTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::createObjectDataTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("ObjectData", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dout(L_ERR)<<"CreateObjectDataTable failed \n";

	dout(L_FULLDEBUG)<<"CreateObjectDataTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::DeleteUserTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->user_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dout(L_ERR)<<"DeleteUserTable failed \n";

	dout(L_FULLDEBUG)<<"DeleteUserTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::DeleteBucketTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->bucket_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dout(L_ERR)<<"DeletebucketTable failed \n";

	dout(L_FULLDEBUG)<<"DeletebucketTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::DeleteObjectTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->object_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dout(L_ERR)<<"DeleteObjectTable failed \n";

	dout(L_FULLDEBUG)<<"DeleteObjectTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::DeleteObjectDataTable(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->objectdata_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dout(L_ERR)<<"DeleteObjectDataTable failed \n";

	dout(L_FULLDEBUG)<<"DeleteObjectDataTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::ListAllUsers(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = ListTableSchema(params->user_table);
	cout<<"########### Listing all Users #############\n";
	ret = exec(schema.c_str(), &list_callback);
	if (ret)
		dout(L_ERR)<<"ListUsertable failed \n";

	dout(L_FULLDEBUG)<<"ListUserTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::ListAllBuckets(RGWOpParams *params)
{
	int ret = -1;
	string schema;

	schema = ListTableSchema(params->bucket_table);

	cout<<"########### Listing all Buckets #############\n";
	ret = exec(schema.c_str(), &list_callback);
	if (ret)
		dout(L_ERR)<<"Listbuckettable failed \n";

	dout(L_FULLDEBUG)<<"ListbucketTable suceeded \n";
out:
	return ret;
}

int SQLiteDB::ListAllObjects(RGWOpParams *params)
{
	int ret = -1;
	string schema;
        map<string, class ObjectOp*>::iterator iter;
        map<string, class ObjectOp*> objectmap;
	string bucket, tenant;

	tenant = getTenant();
	cout<<"########### Listing all Objects #############\n";

	objectmap = getObjectMap();

	if (objectmap.empty())
		dout(L_DEBUG)<<"objectmap empty \n";

	for (iter = objectmap.begin(); iter != objectmap.end(); ++iter) {
		bucket = iter->first;
		params->object_table = tenant + "." + bucket +
					".object.table";
		schema = ListTableSchema(params->object_table);

		ret = exec(schema.c_str(), &list_callback);
		if (ret)
			dout(L_ERR)<<"ListObjecttable failed \n";

		dout(L_FULLDEBUG)<<"ListObjectTable suceeded \n";
	}

out:
	return ret;
}

int SQLObjectOp::InitializeObjectOps()
{
        InsertObject = new SQLInsertObject(tenant, sdb);
	RemoveObject = new SQLRemoveObject(tenant, sdb);
	ListObject = new SQLListObject(tenant, sdb);
	PutObjectData = new SQLPutObjectData(tenant, sdb);
	GetObjectData = new SQLGetObjectData(tenant, sdb);
	DeleteObjectData = new SQLDeleteObjectData(tenant, sdb);

	return 0;
}

int SQLObjectOp::FreeObjectOps()
{
	delete InsertObject;
	delete RemoveObject;
	delete ListObject;
	delete PutObjectData;
	delete GetObjectData;
	delete DeleteObjectData;

	return 0;
}

int SQLInsertUser::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};

	if (!*sdb) {
		dout(L_ERR)<<"In SQLInsertUser - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.user_table = params->user_table;

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareInsertUser");
out:
	return ret;
}

int SQLInsertUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.user_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->user_name.c_str(), sdb);

out:
	return rc;
}

int SQLInsertUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLRemoveUser::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};

	if (!*sdb) {
		dout(L_ERR)<<"In SQLRemoveUser - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.user_table = params->user_table;

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareRemoveUser");
out:
	return ret;
}

int SQLRemoveUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.user_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->user_name.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLListUser::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};

	if (!*sdb) {
		dout(L_ERR)<<"In SQLListUser - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.user_table = params->user_table;

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareListUser");
out:
	return ret;
}

int SQLListUser::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.user_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->user_name.c_str(), sdb);
out:
	return rc;
}

int SQLListUser::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, list_user);
out:
	return ret;
}

int SQLInsertBucket::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};

	if (!*sdb) {
		dout(L_ERR)<<"In SQLInsertBucket - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.bucket_table = params->bucket_table;

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareInsertBucket");

out:
	return ret;
}

int SQLInsertBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.user_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->user_name.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLInsertBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;
	class SQLObjectOp *ObPtr = NULL;

	ObPtr = new SQLObjectOp(getTenant(), sdb);

	objectmapInsert(params->bucket_name, ObPtr);

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLRemoveBucket::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};

	if (!*sdb) {
		dout(L_ERR)<<"In SQLRemoveBucket - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.bucket_table = params->bucket_table;

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareRemoveBucket");

out:
	return ret;
}

int SQLRemoveBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;
	class SQLObjectOp *ObPtr = NULL;

	objectmapDelete(params->bucket_name);

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLListBucket::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};

	if (!*sdb) {
		dout(L_ERR)<<"In SQLListBucket - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.bucket_table = params->bucket_table;

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareListBucket");

out:
	return ret;
}

int SQLListBucket::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLListBucket::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, list_bucket);
out:
	return ret;
}

int SQLInsertObject::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};
	string tenant_name = getTenant();
	struct RGWOpParams copy = *params;

	if (!*sdb) {
		dout(L_ERR)<<"In SQLInsertObject - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	copy.object_table = tenant_name + "." + params->bucket_name +
				".object.table";

	(void)createObjectTable(&copy);

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareInsertObject");

out:
	return ret;
}

int SQLInsertObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLInsertObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLRemoveObject::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};
	string tenant_name = getTenant();
	struct RGWOpParams copy = *params;

	if (!*sdb) {
		dout(L_ERR)<<"In SQLRemoveObject - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	copy.object_table = tenant_name + "." + params->bucket_name +
				".object.table";

	(void)createObjectTable(&copy);

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareRemoveObject");

out:
	return ret;
}

int SQLRemoveObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLListObject::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};
	string tenant_name = getTenant();
	struct RGWOpParams copy = *params;

	if (!*sdb) {
		dout(L_ERR)<<"In SQLListObject - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	copy.object_table = tenant_name + "." + params->bucket_name +
				".object.table";

	(void)createObjectTable(&copy);


	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareListObject");

out:
	return ret;
}

int SQLListObject::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLListObject::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, list_object);
out:
	return ret;
}

int SQLPutObjectData::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};
	string tenant_name = getTenant();
	struct RGWOpParams copy = *params;

	if (!*sdb) {
		dout(L_ERR)<<"In SQLPutObjectData - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	p_params.objectdata_table = tenant_name + "." + params->bucket_name +
				".objectdata.table";
	copy.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	copy.objectdata_table = tenant_name + "." + params->bucket_name +
				".objectdata.table";

	(void)createObjectDataTable(&copy);

	SQL_PREPARE(s_params, sdb, stmt, ret, "PreparePutObjectData");

out:
	return ret;
}

int SQLPutObjectData::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.offset.c_str(), sdb);

	SQL_BIND_INT(stmt, 3, params->offset, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.data.c_str(), sdb);

	SQL_BIND_BLOB(stmt, index, params->data.c_str(), params->data.length(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.datalen.c_str(), sdb);

	SQL_BIND_INT(stmt, index, params->data.length(), sdb);

out:
	return rc;
}

int SQLPutObjectData::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLGetObjectData::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};
	string tenant_name = getTenant();
	struct RGWOpParams copy = *params;

	if (!*sdb) {
		dout(L_ERR)<<"In SQLGetObjectData - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	p_params.objectdata_table = tenant_name + "." + params->bucket_name +
				".objectdata.table";
	copy.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	copy.objectdata_table = tenant_name + "." + params->bucket_name +
				".objectdata.table";

	(void)createObjectDataTable(&copy);

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareGetObjectData");

out:
	return ret;
}

int SQLGetObjectData::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);
out:
	return rc;
}

int SQLGetObjectData::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, get_objectdata);
out:
	return ret;
}

int SQLDeleteObjectData::Prepare(struct RGWOpParams *params)
{
	int ret = -1;
	struct RGWOpPrepareParams p_params = PrepareParams;
	struct SchemaParams s_params = {};
	string tenant_name = getTenant();
	struct RGWOpParams copy = *params;

	if (!*sdb) {
		dout(L_ERR)<<"In SQLDeleteObjectData - no db\n";
		goto out;
	}

	s_params.is_prepare = true;
	s_params.u.p_params = &p_params;

	p_params.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	p_params.objectdata_table = tenant_name + "." + params->bucket_name +
				".objectdata.table";
	copy.object_table = tenant_name + "." + params->bucket_name +
				".object.table";
	copy.objectdata_table = tenant_name + "." + params->bucket_name +
				".objectdata.table";

	(void)createObjectDataTable(&copy);

	SQL_PREPARE(s_params, sdb, stmt, ret, "PrepareDeleteObjectData");

out:
	return ret;
}

int SQLDeleteObjectData::Bind(struct RGWOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct RGWOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);
out:
	return rc;
}

int SQLDeleteObjectData::Execute(struct RGWOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}
