// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "sqliteDB.h"

#define SQL_PREPARE(params, sdb, stmt, ret, Op) 	\
do {							\
	string schema;			   		\
	schema = Schema(params);	   		\
	sqlite3_prepare_v2 (*sdb, schema.c_str(), 	\
		            -1, &stmt , NULL);		\
	if (!stmt) {					\
	        cout<<"failed to prepare statement " \
	       		   <<"for Op("<<Op<<"); Errmsg -"\
	       		   <<sqlite3_errmsg(*sdb)<<"\n";\
		ret = -1;				\
		goto out;				\
	}						\
	dbout(L_DEBUG)<<"Successfully Prepared stmt for Op("<<Op	\
		 	 <<") schema("<<schema<<") stmt("<<stmt<<")\n";	\
	ret = 0;					\
} while(0);

#define SQL_BIND_INDEX(stmt, index, str, sdb)	\
do {						\
	index = sqlite3_bind_parameter_index(stmt, str);     \
							     \
	if (index <=0)  {				     \
	        cout<<"failed to fetch bind parameter"\
	       	  	   " index for str("<<str<<") in "   \
			   <<"stmt("<<stmt<<"); Errmsg -"    \
	       		   <<sqlite3_errmsg(*sdb)<<"\n"; 	     \
		rc = -1;				     \
		goto out;				     \
	}						     \
	dbout(L_FULLDEBUG)<<"Bind parameter index for str("  \
			 <<str<<") in stmt("<<stmt<<") is "  \
       		         <<index<<"\n";			     \
}while(0);

#define SQL_BIND_TEXT(stmt, index, str, sdb)			\
do {								\
	rc = sqlite3_bind_text(stmt, index, str, -1, SQLITE_STATIC); 	\
								\
	if (rc != SQLITE_OK) {					      	\
	        dbout(L_ERR)<<"sqlite bind text failed for index("     	\
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
	        dbout(L_ERR)<<"sqlite bind int failed for index("     	\
			   <<index<<"), num("<<num<<") in stmt("   	\
			   <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
			   <<"\n";				\
		rc = -1;					\
		goto out;					\
	}							\
}while(0);

#define SQL_BIND_BLOB(stmt, index, blob, size, sdb)		\
do {								\
	rc = sqlite3_bind_blob(stmt, index, blob, size, SQLITE_TRANSIENT);  \
								\
	if (rc != SQLITE_OK) {					\
	        dbout(L_ERR)<<"sqlite bind blob failed for index("     	\
			   <<index<<"), blob("<<blob<<") in stmt("   	\
			   <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
			   <<"\n";				\
		rc = -1;					\
		goto out;					\
	}							\
}while(0);

#define SQL_ENCODE_BLOB_PARAM(stmt, index, param, sdb)		\
do {								\
	bufferlist b;						\
	encode(param, b);					\
	SQL_BIND_BLOB(stmt, index, b.c_str(), b.length(), sdb); \
}while(0);
								
#define SQL_READ_BLOB(stmt, index, void_ptr, len)		\
do {								\
	void_ptr = NULL;					\
	void_ptr = (void *)sqlite3_column_blob(stmt, index);	\
	len = sqlite3_column_bytes(stmt, index);		\
								\
	if (!void_ptr || len == 0) {				\
		dbout(L_FULLDEBUG)<<"Null value for blob index("  \
			 <<index<<") in stmt("<<stmt<<") \n";   \
	}							\
}while(0);

#define SQL_DECODE_BLOB_PARAM(stmt, index, param, sdb)		\
do {								\
	bufferlist b;						\
	void *blob;						\
	int blob_len = 0;					\
								\
        SQL_READ_BLOB(stmt, index, blob, blob_len);		\
								\
	b.append(reinterpret_cast<char *>(blob), blob_len);	\
								\
	decode(param, b);					\
}while(0);
								
#define SQL_EXECUTE(params, stmt, cbk, args...) \
do{						\
	if (!stmt) {				\
		ret = Prepare(params);		\
	}					\
						\
	if (!stmt) {				\
		cout<<"No prepared statement \n";	\
		goto out;			\
	}					\
						\
	ret = Bind(params);			\
	if (ret) {				\
		cout<<"Bind parameters failed for stmt("	\
			   <<stmt<<") \n";		\
		goto out;			\
	}					\
						\
	ret = Step(params->op, stmt, cbk);		\
						\
	Reset(stmt);				\
						\
	if (ret) {				\
		cout<<"Execution failed for stmt("	\
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

static int list_user(DBOpInfo &op, sqlite3_stmt *stmt) {
	if (!stmt)
		return -1;

	cout<<sqlite3_column_text(stmt, 0)<<"\n";
/* Ensure the column names match with the user table defined in dbstore.h                     
                        UserName TEXT PRIMARY KEY NOT NULL UNIQUE , - 0
                        Tenant TEXT ,            - 1
                        ID TEXT ,                - 2
                        NS TEXT ,                - 3
                        UserEmail TEXT ,         - 4
                        Suspended INTEGER ,      - 5
                        MaxBuckets INTEGER ,     - 6
                        OpMask  INTEGER ,        - 7
                        Admin   INTEGER ,        - 8
                        System INTEGER ,         - 9
                        BucketQuotaID INTEGER ,  - 10
                        UserQuotaID INTEGER ,    - 11
                        TYPE INTEGER ,           - 12
                        MfaIDs INTEGER ,         - 13
                        AssumedRoleARN TEXT \n)  - 14
*/

	op.uinfo.username = (const char*)sqlite3_column_text(stmt, 0); // user_name
	op.uinfo.tenant = (const char*)sqlite3_column_text(stmt, 1);
	op.uinfo.id = (const char*)sqlite3_column_text(stmt, 2);
	op.uinfo.ns = (const char*)sqlite3_column_text(stmt, 3);
	op.uinfo.user_email = (const char*)sqlite3_column_text(stmt, 4);

	op.uinfo.suspended = sqlite3_column_int(stmt, 5);
	op.uinfo.max_buckets = sqlite3_column_int(stmt, 6);
	op.uinfo.op_mask = sqlite3_column_int(stmt, 7);


	op.uinfo.admin = sqlite3_column_int(stmt, 8);
	op.uinfo.system = sqlite3_column_int(stmt, 9);

	op.uinfo.bucket_quota_id = sqlite3_column_int(stmt, 10);
	op.uinfo.user_quota_id = sqlite3_column_int(stmt, 11);
	op.uinfo.type = sqlite3_column_int(stmt, 12);
	op.uinfo.mfaids = sqlite3_column_int(stmt, 13);
	op.uinfo.assumedrolearn = (const char*)sqlite3_column_text(stmt, 14);

	SQL_DECODE_BLOB_PARAM(stmt, 15, op.uinfo.access_keys, sdb);
	SQL_DECODE_BLOB_PARAM(stmt, 16, op.uinfo.swift_keys, sdb);
	SQL_DECODE_BLOB_PARAM(stmt, 17, op.uinfo.subusers, sdb);
	SQL_DECODE_BLOB_PARAM(stmt, 18, op.uinfo.user_caps, sdb);
	SQL_DECODE_BLOB_PARAM(stmt, 19, op.uinfo.placement_tags, sdb);
	SQL_DECODE_BLOB_PARAM(stmt, 20, op.uinfo.temp_url_keys, sdb);

	return 0;
}

static int list_bucket(DBOpInfo &op, sqlite3_stmt *stmt) {
	if (!stmt)
		return -1;

	cout<<sqlite3_column_text(stmt, 0)<<", ";
	cout<<sqlite3_column_text(stmt, 1)<<"\n";

	return 0;
}

static int list_object(DBOpInfo &op, sqlite3_stmt *stmt) {
	if (!stmt)
		return -1;

	cout<<sqlite3_column_text(stmt, 0)<<", ";
	cout<<sqlite3_column_text(stmt, 1)<<"\n";

	return 0;
}

static int get_objectdata(DBOpInfo &op, sqlite3_stmt *stmt) {
	if (!stmt)
		return -1;

	int datalen = 0;
	const void *blob = NULL;

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

int SQLiteDB::InitializeDBOps()
{
        (void)createTables();
        dbops.InsertUser = new SQLInsertUser(&this->db);
        dbops.RemoveUser = new SQLRemoveUser(&this->db);
        dbops.GetUser = new SQLGetUser(&this->db);
        dbops.InsertBucket = new SQLInsertBucket(&this->db);
        dbops.RemoveBucket = new SQLRemoveBucket(&this->db);
        dbops.ListBucket = new SQLListBucket(&this->db);

	return 0;
}

int SQLiteDB::FreeDBOps()
{
        delete dbops.InsertUser;
        delete dbops.RemoveUser;
        delete dbops.GetUser;
        delete dbops.InsertBucket;
        delete dbops.RemoveBucket;
        delete dbops.ListBucket;

	return 0;
}

void *SQLiteDB::openDB()
{
	string dbname;
        int rc = 0;

	dbname	= getDBname();
	if (dbname.empty()) {
		dbout(L_ERR)<<"dbname is NULL\n";
		goto out;
	}

	rc = sqlite3_open_v2(dbname.c_str(), (sqlite3**)&db,
			     SQLITE_OPEN_READWRITE |
			     SQLITE_OPEN_CREATE |
			     SQLITE_OPEN_FULLMUTEX,
			     NULL);

        if (rc) {
		dbout(L_ERR)<<"Cant open "<<dbname<<"; Errmsg - "\
			   <<sqlite3_errmsg((sqlite3*)db)<<"\n";
        } else {
                dbout(L_DEBUG)<<"Opened database("<<dbname<<") successfully\n";
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

	return ret;
}

int SQLiteDB::Step(DBOpInfo &op, sqlite3_stmt *stmt,
	           int (*cbk)(DBOpInfo &op, sqlite3_stmt *stmt))
{
	int ret = -1;

	if (!stmt) {
		return -1;
	}

again:
		ret = sqlite3_step(stmt);

		if ((ret != SQLITE_DONE) && (ret != SQLITE_ROW)) {
			dbout(L_ERR)<<"sqlite step failed for stmt("<<stmt \
				   <<"); Errmsg - "<<sqlite3_errmsg((sqlite3*)db)<<"\n";
			return -1;
		} else if (ret == SQLITE_ROW) {
			if (cbk) {
				(*cbk)(op, stmt);
			} else {
			}
			goto again;
		}

	dbout(L_FULLDEBUG)<<"sqlite step successfully executed for stmt(" \
			 <<stmt<<")  ret = " << ret <<"\n";

	return 0;
}

int SQLiteDB::exec(const char *schema,
	       int (*callback)(void*,int,char**,char**))
{
	int ret = -1;
	char *errmsg = NULL;

	if (!db)
		goto out;

	ret = sqlite3_exec((sqlite3*)db, schema, callback, 0, &errmsg);
        if (ret != SQLITE_OK) {
		dbout(L_ERR)<<"sqlite exec failed for schema("<<schema \
				   <<"); Errmsg - "<<errmsg<<"\n";
                sqlite3_free(errmsg);
		goto out;
        }
	ret = 0;
	dbout(L_FULLDEBUG)<<"sqlite exec successfully processed for schema(" \
			<<schema<<")\n";
out:
	return ret;
}

int SQLiteDB::createTables()
{
	int ret = -1;
	int cu, cb = -1;
	DBOpParams params = {};

	params.user_table = getUserTable();
	params.bucket_table = getBucketTable();

	if ((cu = createUserTable(&params)))
		goto out;

	if ((cb = createBucketTable(&params)))
		goto out;

	if ((cb = createQuotaTable(&params)))
		goto out;

	ret = 0;
out:
	if (ret) {
		if (cu)
			DeleteUserTable(&params);
		if (cb)
			DeleteBucketTable(&params);
		dbout(L_ERR)<<"Creation of tables failed \n";
	}

	return ret;
}

int SQLiteDB::createUserTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("User", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"CreateUserTable failed \n";

	dbout(L_FULLDEBUG)<<"CreateUserTable suceeded \n";

	return ret;
}

int SQLiteDB::createBucketTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("Bucket", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"CreateBucketTable failed \n";

	dbout(L_FULLDEBUG)<<"CreateBucketTable suceeded \n";

	return ret;
}

int SQLiteDB::createObjectTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("Object", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"CreateObjectTable failed \n";

	dbout(L_FULLDEBUG)<<"CreateObjectTable suceeded \n";

	return ret;
}

int SQLiteDB::createQuotaTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("Quota", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"CreateQuotaTable failed \n";

	dbout(L_FULLDEBUG)<<"CreateQuotaTable suceeded \n";

	return ret;
}

int SQLiteDB::createObjectDataTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = CreateTableSchema("ObjectData", params);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"CreateObjectDataTable failed \n";

	dbout(L_FULLDEBUG)<<"CreateObjectDataTable suceeded \n";

	return ret;
}

int SQLiteDB::DeleteUserTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->user_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"DeleteUserTable failed \n";

	dbout(L_FULLDEBUG)<<"DeleteUserTable suceeded \n";

	return ret;
}

int SQLiteDB::DeleteBucketTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->bucket_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"DeletebucketTable failed \n";

	dbout(L_FULLDEBUG)<<"DeletebucketTable suceeded \n";

	return ret;
}

int SQLiteDB::DeleteObjectTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->object_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"DeleteObjectTable failed \n";

	dbout(L_FULLDEBUG)<<"DeleteObjectTable suceeded \n";

	return ret;
}

int SQLiteDB::DeleteObjectDataTable(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = DeleteTableSchema(params->objectdata_table);

	ret = exec(schema.c_str(), NULL);
	if (ret)
		dbout(L_ERR)<<"DeleteObjectDataTable failed \n";

	dbout(L_FULLDEBUG)<<"DeleteObjectDataTable suceeded \n";

	return ret;
}

int SQLiteDB::ListAllUsers(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = ListTableSchema(params->user_table);
	cout<<"########### Listing all Users #############\n";
	ret = exec(schema.c_str(), &list_callback);
	if (ret)
		dbout(L_ERR)<<"GetUsertable failed \n";

	dbout(L_FULLDEBUG)<<"GetUserTable suceeded \n";

	return ret;
}

int SQLiteDB::ListAllBuckets(DBOpParams *params)
{
	int ret = -1;
	string schema;

	schema = ListTableSchema(params->bucket_table);

	cout<<"########### Listing all Buckets #############\n";
	ret = exec(schema.c_str(), &list_callback);
	if (ret)
		dbout(L_ERR)<<"Listbuckettable failed \n";

	dbout(L_FULLDEBUG)<<"ListbucketTable suceeded \n";

	return ret;
}

int SQLiteDB::ListAllObjects(DBOpParams *params)
{
	int ret = -1;
	string schema;
        map<string, class ObjectOp*>::iterator iter;
        map<string, class ObjectOp*> objectmap;
	string bucket;

	cout<<"########### Listing all Objects #############\n";

	objectmap = getObjectMap();

	if (objectmap.empty())
		dbout(L_DEBUG)<<"objectmap empty \n";

	for (iter = objectmap.begin(); iter != objectmap.end(); ++iter) {
		bucket = iter->first;
		params->object_table = bucket +
					".object.table";
		schema = ListTableSchema(params->object_table);

		ret = exec(schema.c_str(), &list_callback);
		if (ret)
			dbout(L_ERR)<<"ListObjecttable failed \n";

		dbout(L_FULLDEBUG)<<"ListObjectTable suceeded \n";
	}

	return ret;
}

int SQLObjectOp::InitializeObjectOps()
{
        InsertObject = new SQLInsertObject(sdb);
	RemoveObject = new SQLRemoveObject(sdb);
	ListObject = new SQLListObject(sdb);
	PutObjectData = new SQLPutObjectData(sdb);
	GetObjectData = new SQLGetObjectData(sdb);
	DeleteObjectData = new SQLDeleteObjectData(sdb);

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

int SQLInsertUser::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLInsertUser - no db\n";
		goto out;
	}

	p_params.user_table = params->user_table;

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareInsertUser");
out:
	return ret;
}

int SQLInsertUser::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.user.username.c_str(), sdb);
	SQL_BIND_TEXT(stmt, index, params->op.uinfo.username.c_str(), sdb);

	cout << "username prepare = " << p_params.user.username.c_str() << "\n";
	cout << "username = " << params->op.uinfo.username.c_str() << "\n";

	SQL_BIND_INDEX(stmt, index, p_params.user.tenant.c_str(), sdb);
	SQL_BIND_TEXT(stmt, index, params->op.uinfo.tenant.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.id.c_str(), sdb);
	SQL_BIND_TEXT(stmt, index, params->op.uinfo.id.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.ns.c_str(), sdb);
	SQL_BIND_TEXT(stmt, index, params->op.uinfo.ns.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.user_email.c_str(), sdb);
	SQL_BIND_TEXT(stmt, index, params->op.uinfo.user_email.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.suspended.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.suspended, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.max_buckets.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.max_buckets, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.op_mask.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.op_mask, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.admin.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.admin, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.system.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.system, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.bucket_quota_id.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.bucket_quota_id, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.user_quota_id.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.user_quota_id, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.type.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.type, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.mfaids.c_str(), sdb);
	SQL_BIND_INT(stmt, index, params->op.uinfo.mfaids, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.assumedrolearn.c_str(), sdb);
	SQL_BIND_TEXT(stmt, index, params->op.uinfo.assumedrolearn.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.access_keys.c_str(), sdb);
	SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.uinfo.access_keys, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.swift_keys.c_str(), sdb);
	SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.uinfo.swift_keys, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.subusers.c_str(), sdb);
	SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.uinfo.subusers, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.user_caps.c_str(), sdb);
	SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.uinfo.user_caps, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.placement_tags.c_str(), sdb);
	SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.uinfo.placement_tags, sdb);

	SQL_BIND_INDEX(stmt, index, p_params.user.temp_url_keys.c_str(), sdb);
	SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.uinfo.temp_url_keys, sdb);
out:
	return rc;
}

int SQLInsertUser::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLRemoveUser::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLRemoveUser - no db\n";
		goto out;
	}

	p_params.user_table = params->user_table;

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareRemoveUser");
out:
	return ret;
}

int SQLRemoveUser::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.user.username.c_str(), sdb);
	SQL_BIND_TEXT(stmt, index, params->op.uinfo.username.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveUser::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLGetUser::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLGetUser - no db\n";
		goto out;
	}

	p_params.user_table = params->user_table;
	p_params.user.query_str = params->op.uinfo.query_str;

	if (params->op.uinfo.query_str == "email") { 
		SQL_PREPARE(p_params, sdb, email_stmt, ret, "PrepareGetUser");
	} else { // by default by username
		SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareGetUser");
	}
out:
	return ret;
}

int SQLGetUser::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	if (params->op.uinfo.query_str == "email") { 
		SQL_BIND_INDEX(email_stmt, index, p_params.user.user_email.c_str(), sdb);
		SQL_BIND_TEXT(email_stmt, index, params->op.uinfo.user_email.c_str(), sdb);
	} else { // by default by username
		SQL_BIND_INDEX(stmt, index, p_params.user.username.c_str(), sdb);
		SQL_BIND_TEXT(stmt, index, params->op.uinfo.username.c_str(), sdb);
	}

out:
	return rc;
}

int SQLGetUser::Execute(struct DBOpParams *params)
{
	int ret = -1;

	if (params->op.uinfo.query_str == "email") { 
		SQL_EXECUTE(params, email_stmt, list_user);
	} else { // by default by username
		SQL_EXECUTE(params, stmt, list_user);
	}
	
out:
	return ret;
}

int SQLInsertBucket::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLInsertBucket - no db\n";
		goto out;
	}

	p_params.bucket_table = params->bucket_table;

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareInsertBucket");

out:
	return ret;
}

int SQLInsertBucket::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.user.username.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->op.uinfo.username.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLInsertBucket::Execute(struct DBOpParams *params)
{
	int ret = -1;
	class SQLObjectOp *ObPtr = NULL;

	ObPtr = new SQLObjectOp(sdb);

	objectmapInsert(params->bucket_name, ObPtr);

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLRemoveBucket::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLRemoveBucket - no db\n";
		goto out;
	}

	p_params.bucket_table = params->bucket_table;

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareRemoveBucket");

out:
	return ret;
}

int SQLRemoveBucket::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveBucket::Execute(struct DBOpParams *params)
{
	int ret = -1;

	objectmapDelete(params->bucket_name);

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLListBucket::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLListBucket - no db\n";
		goto out;
	}

	p_params.bucket_table = params->bucket_table;

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareListBucket");

out:
	return ret;
}

int SQLListBucket::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLListBucket::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, list_bucket);
out:
	return ret;
}

int SQLInsertObject::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;
	struct DBOpParams copy = *params;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLInsertObject - no db\n";
		goto out;
	}

	p_params.object_table = params->bucket_name + ".object.table";
	copy.object_table = params->bucket_name + ".object.table";

	(void)createObjectTable(&copy);

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareInsertObject");

out:
	return ret;
}

int SQLInsertObject::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLInsertObject::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLRemoveObject::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;
	struct DBOpParams copy = *params;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLRemoveObject - no db\n";
		goto out;
	}

	p_params.object_table = params->bucket_name + ".object.table";
	copy.object_table = params->bucket_name + ".object.table";

	(void)createObjectTable(&copy);

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareRemoveObject");

out:
	return ret;
}

int SQLRemoveObject::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLRemoveObject::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLListObject::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;
	struct DBOpParams copy = *params;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLListObject - no db\n";
		goto out;
	}

	p_params.object_table = params->bucket_name + ".object.table";
	copy.object_table = params->bucket_name + ".object.table";

	(void)createObjectTable(&copy);


	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareListObject");

out:
	return ret;
}

int SQLListObject::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);

out:
	return rc;
}

int SQLListObject::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, list_object);
out:
	return ret;
}

int SQLPutObjectData::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;
	struct DBOpParams copy = *params;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLPutObjectData - no db\n";
		goto out;
	}

	p_params.object_table = params->bucket_name + ".object.table";
	p_params.objectdata_table = params->bucket_name + ".objectdata.table";
	copy.object_table = params->bucket_name + ".object.table";
	copy.objectdata_table = params->bucket_name + ".objectdata.table";

	(void)createObjectDataTable(&copy);

	SQL_PREPARE(p_params, sdb, stmt, ret, "PreparePutObjectData");

out:
	return ret;
}

int SQLPutObjectData::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

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

int SQLPutObjectData::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}

int SQLGetObjectData::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;
	struct DBOpParams copy = *params;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLGetObjectData - no db\n";
		goto out;
	}

	p_params.object_table = params->bucket_name + ".object.table";
	p_params.objectdata_table = params->bucket_name + ".objectdata.table";
	copy.object_table = params->bucket_name + ".object.table";
	copy.objectdata_table = params->bucket_name + ".objectdata.table";

	(void)createObjectDataTable(&copy);

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareGetObjectData");

out:
	return ret;
}

int SQLGetObjectData::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);
out:
	return rc;
}

int SQLGetObjectData::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, get_objectdata);
out:
	return ret;
}

int SQLDeleteObjectData::Prepare(struct DBOpParams *params)
{
	int ret = -1;
	struct DBOpPrepareParams p_params = PrepareParams;
	struct DBOpParams copy = *params;

	if (!*sdb) {
		dbout(L_ERR)<<"In SQLDeleteObjectData - no db\n";
		goto out;
	}

	p_params.object_table = params->bucket_name + ".object.table";
	p_params.objectdata_table = params->bucket_name + ".objectdata.table";
	copy.object_table = params->bucket_name + ".object.table";
	copy.objectdata_table = params->bucket_name + ".objectdata.table";

	(void)createObjectDataTable(&copy);

	SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareDeleteObjectData");

out:
	return ret;
}

int SQLDeleteObjectData::Bind(struct DBOpParams *params)
{
	int index = -1;
	int rc = 0;
	struct DBOpPrepareParams p_params = PrepareParams;

	SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

	SQL_BIND_INDEX(stmt, index, p_params.bucket_name.c_str(), sdb);

	SQL_BIND_TEXT(stmt, index, params->bucket_name.c_str(), sdb);
out:
	return rc;
}

int SQLDeleteObjectData::Execute(struct DBOpParams *params)
{
	int ret = -1;

	SQL_EXECUTE(params, stmt, NULL);
out:
	return ret;
}
