#include "gtest/gtest.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dbstore.h>
#include <sqliteDB.h>

using namespace std;

namespace gtest {
	class Environment* env;

	class Environment : public ::testing::Environment {
	public:
		Environment(): tenant("RedHat"), db(nullptr),
	       			db_type("SQLite"), ret(-1) {}

		Environment(string tenantname, string db_typename): 
			tenant("tenantname"), db(nullptr),
			db_type("db_typename"), ret(-1) {}

		virtual ~Environment() {}

		void SetUp() override {
			if (!db_type.compare("SQLite")) {
				db = new SQLiteDB(tenant);
				ASSERT_TRUE(db != nullptr);

				ret = db->Initialize("", -1);
				ASSERT_GE(ret, 0);
			}
		}

		void TearDown() override {
			if (!db)
				return;
			db->Destroy();
			delete db;
		}

		string tenant;
		class DBstore *db;
		string db_type;
		int ret;
	};
}

namespace {

	class DBstoreBaseTest : public ::testing::Test {
	protected:
		int ret;
		DBstore *db = nullptr;
		string user1 = "user1";
		string bucket1 = "bucket1";
		string object1 = "object1";
		string data = "Hello World";
		DBOpParams GlobalParams = {};

		DBstoreBaseTest() {}
		void SetUp() {
			db = gtest::env->db;
			ASSERT_TRUE(db != nullptr);

			GlobalParams.op.uinfo.display_name = user1;
			GlobalParams.bucket_name = bucket1;
			GlobalParams.object = object1;
			GlobalParams.offset = 0;
			GlobalParams.data = data;
			GlobalParams.datalen = data.length();

			/* As of now InitializeParams doesnt do anything
			 * special based on fop. Hence its okay to do
			 * global initialization once.
			 */
			ret = db->InitializeParams("", &GlobalParams);
			ASSERT_EQ(ret, 0);
		}

		void TearDown() {
		}
	};
}

TEST_F(DBstoreBaseTest, InsertUser) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	params.op.uinfo.user_id.tenant = "tenant";
	params.op.uinfo.user_email = "user1@dbstore.com";
	params.op.uinfo.user_id.id = "id";
	params.op.uinfo.suspended = 123;
	params.op.uinfo.max_buckets = 456;
	params.op.uinfo.assumed_role_arn = "role";
	params.op.uinfo.placement_tags.push_back("tags");
	RGWAccessKey k1("id1", "key1");
	RGWAccessKey k2("id2", "key2");
	params.op.uinfo.access_keys.insert(make_pair("key1", k1));
	params.op.uinfo.access_keys.insert(make_pair("key2", k2));

	ret = db->ProcessOp("InsertUser", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, GetUser) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("GetUser", &params);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(params.op.uinfo.user_id.tenant, "tenant");
	ASSERT_EQ(params.op.uinfo.user_email, "user1@dbstore.com");
	ASSERT_EQ(params.op.uinfo.user_id.id, "id");
	ASSERT_EQ(params.op.uinfo.suspended, 123);
	ASSERT_EQ(params.op.uinfo.max_buckets, 456);
	ASSERT_EQ(params.op.uinfo.assumed_role_arn, "role");
	ASSERT_EQ(params.op.uinfo.placement_tags.back(), "tags");
	RGWAccessKey k;
	map<string, RGWAccessKey>::iterator it2 = params.op.uinfo.access_keys.begin();
	k = it2->second;
	ASSERT_EQ(k.id, "id1");
	ASSERT_EQ(k.key, "key1");
	it2++;
	k = it2->second;
	ASSERT_EQ(k.id, "id2");
	ASSERT_EQ(k.key, "key2");

}

TEST_F(DBstoreBaseTest, GetUserQuery) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	params.op.get_query_str = "email";
	params.op.uinfo.user_email = "user1@dbstore.com";

	ret = db->ProcessOp("GetUser", &params);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(params.op.uinfo.user_id.tenant, "tenant");
	ASSERT_EQ(params.op.uinfo.user_email, "user1@dbstore.com");
	ASSERT_EQ(params.op.uinfo.user_id.id, "id");
	ASSERT_EQ(params.op.uinfo.suspended, 123);
	ASSERT_EQ(params.op.uinfo.max_buckets, 456);
	ASSERT_EQ(params.op.uinfo.assumed_role_arn, "role");
	ASSERT_EQ(params.op.uinfo.placement_tags.back(), "tags");
	RGWAccessKey k;
	map<string, RGWAccessKey>::iterator it2 = params.op.uinfo.access_keys.begin();
	k = it2->second;
	ASSERT_EQ(k.id, "id1");
	ASSERT_EQ(k.key, "key1");
	it2++;
	k = it2->second;
	ASSERT_EQ(k.id, "id2");
	ASSERT_EQ(k.key, "key2");

}

TEST_F(DBstoreBaseTest, ListAllUsers) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ListAllUsers(&params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, InsertBucket) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("InsertBucket", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, ListBucket) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("ListBucket", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, ListAllBuckets) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ListAllBuckets(&params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, InsertObject) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("InsertObject", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, ListObject) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("ListObject", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, ListAllObjects) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ListAllObjects(&params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, PutObjectData) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("PutObjectData", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, GetObjectData) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("GetObjectData", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, DeleteObjectData) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("DeleteObjectData", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, RemoveObject) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("RemoveObject", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, RemoveBucket) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("RemoveBucket", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, RemoveUser) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("RemoveUser", &params);
	ASSERT_EQ(ret, 0);
}

int main(int argc, char **argv)
{
	int ret = -1;

	::testing::InitGoogleTest(&argc, argv);

	gtest::env = new gtest::Environment();
	::testing::AddGlobalTestEnvironment(gtest::env);

	ret = RUN_ALL_TESTS();

	return ret;
}
