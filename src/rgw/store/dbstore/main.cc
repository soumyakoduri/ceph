#include <stdio.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <dbstore.h>

#ifdef SQLITE_ENABLED
#include <sqliteDB.h>
#endif


int main(int argc, char *argv[])
{
	string tenant = "Redhat";

	string user1 = "Soumya";
	string bucketa = "rgw";
	string objecta1 = "bugfixing";
	string objecta2 = "zipper";
	string bucketb = "gluster";
	string objectb1 = "bugfixing";
	string objectb2 = "delegations";

	string user2 = "Kasturi";
	string bucketc = "qe";
	string objectc1 = "rhhi";
	string objectc2 = "cns";

	struct RGWOpParams params = {};

	class DBstore *db;
	int rc = 0;

#ifdef SQLITE_ENABLED
	db = new SQLiteDB(tenant);
#else
	db = new DBstore(tenant);
#endif

	db->db = db->openDB();
	db->InitializeRGWOps();

	db->InitializeParams("InsertUser", &params);
	params.user_name = user1;
	db->ProcessOp("InsertUser", &params);

	params.bucket_name = bucketa;
	db->ProcessOp("InsertBucket", &params);

	params.object = objecta1;
	db->ProcessOp("InsertObject", &params);
	params.object = objecta2;
	db->ProcessOp("InsertObject", &params);

	params.bucket_name = bucketb;
	db->ProcessOp("InsertBucket", &params);

	params.object = objectb1;
	db->ProcessOp("InsertObject", &params);
	params.object = objectb2;
	db->ProcessOp("InsertObject", &params);

	params.offset = 0;
	params.data = "Hello World \0";
	params.datalen = params.data.length();
	db->ProcessOp("PutObjectData", &params);

	params.offset = 20;
	params.data = "Hi There \0";
	params.datalen = params.data.length();
	db->ProcessOp("PutObjectData", &params);

	db->ProcessOp("GetObjectData", &params);

	db->ProcessOp("DeleteObjectData", &params);

	db->ProcessOp("GetObjectData", &params);

	params.user_name = user2;
	db->ProcessOp("InsertUser", &params);

	params.bucket_name = bucketc;
	db->ProcessOp("InsertBucket", &params);

	params.object = objectc1;
	db->ProcessOp("InsertObject", &params);
	params.object = objectc2;
	db->ProcessOp("InsertObject", &params);

	db->ProcessOp("ListUser", &params);
	db->ProcessOp("ListBucket", &params);
	db->ProcessOp("ListObject", &params);

	db->ListAllUsers(&params);
	db->ListAllBuckets(&params);
	db->ListAllObjects(&params);

	params.object = objecta1;
	params.bucket_name = bucketa;
	db->ProcessOp("RemoveObject", &params);

	params.bucket_name = bucketb;
	db->ProcessOp("RemoveBucket", &params);

	params.user_name = user2;
	db->ProcessOp("RemoveUser", &params);

	db->ListAllUsers(&params);
	db->ListAllBuckets(&params);
	db->ListAllObjects(&params);

	db->closeDB();

	db->FreeRGWOps();
	delete db;

	return 0;
}
