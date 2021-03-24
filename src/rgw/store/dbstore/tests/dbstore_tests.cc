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
		class DBStore *db;
		string db_type;
		int ret;
	};
}

namespace {

	class DBStoreBaseTest : public ::testing::Test {
	protected:
		int ret;
		DBStore *db = nullptr;
		string user1 = "user1";
		string bucket1 = "bucket1";
		string object1 = "object1";
		string data = "Hello World";
		DBOpParams GlobalParams = {};

		DBStoreBaseTest() {}
		void SetUp() {
			db = gtest::env->db;
			ASSERT_TRUE(db != nullptr);

			GlobalParams.op.user.uinfo.display_name = user1;
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

TEST_F(DBStoreBaseTest, InsertUser) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	params.op.user.uinfo.user_id.tenant = "tenant";
	params.op.user.uinfo.user_email = "user1@dbstore.com";
	params.op.user.uinfo.user_id.id = "id";
	params.op.user.uinfo.suspended = 123;
	params.op.user.uinfo.max_buckets = 456;
	params.op.user.uinfo.assumed_role_arn = "role";
	params.op.user.uinfo.placement_tags.push_back("tags");
	RGWAccessKey k1("id1", "key1");
	RGWAccessKey k2("id2", "key2");
	params.op.user.uinfo.access_keys["id1"] = k1;
	params.op.user.uinfo.access_keys["id2"] = k2;

	ret = db->ProcessOp("InsertUser", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, GetUser) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("GetUser", &params);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(params.op.user.uinfo.user_id.tenant, "tenant");
	ASSERT_EQ(params.op.user.uinfo.user_email, "user1@dbstore.com");
	ASSERT_EQ(params.op.user.uinfo.user_id.id, "id");
	ASSERT_EQ(params.op.user.uinfo.suspended, 123);
	ASSERT_EQ(params.op.user.uinfo.max_buckets, 456);
	ASSERT_EQ(params.op.user.uinfo.assumed_role_arn, "role");
	ASSERT_EQ(params.op.user.uinfo.placement_tags.back(), "tags");
	RGWAccessKey k;
	map<string, RGWAccessKey>::iterator it2 = params.op.user.uinfo.access_keys.begin();
	k = it2->second;
	ASSERT_EQ(k.id, "id1");
	ASSERT_EQ(k.key, "key1");
	it2++;
	k = it2->second;
	ASSERT_EQ(k.id, "id2");
	ASSERT_EQ(k.key, "key2");

}

TEST_F(DBStoreBaseTest, GetUserQuery) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	params.op.get_query_str = "email";
	params.op.user.uinfo.user_email = "user1@dbstore.com";

	ret = db->ProcessOp("GetUser", &params);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(params.op.user.uinfo.user_id.tenant, "tenant");
	ASSERT_EQ(params.op.user.uinfo.user_email, "user1@dbstore.com");
	ASSERT_EQ(params.op.user.uinfo.user_id.id, "id");
	ASSERT_EQ(params.op.user.uinfo.suspended, 123);
	ASSERT_EQ(params.op.user.uinfo.max_buckets, 456);
	ASSERT_EQ(params.op.user.uinfo.assumed_role_arn, "role");
	ASSERT_EQ(params.op.user.uinfo.placement_tags.back(), "tags");
	RGWAccessKey k;
	map<string, RGWAccessKey>::iterator it2 = params.op.user.uinfo.access_keys.begin();
	k = it2->second;
	ASSERT_EQ(k.id, "id1");
	ASSERT_EQ(k.key, "key1");
	it2++;
	k = it2->second;
	ASSERT_EQ(k.id, "id2");
	ASSERT_EQ(k.key, "key2");

}

TEST_F(DBStoreBaseTest, GetUserQueryByEmail) {
	int ret = -1;
    RGWUserInfo uinfo;
    string email = "user1@dbstore.com";

	ret = db->get_user("email", email, uinfo);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(uinfo.user_id.tenant, "tenant");
	ASSERT_EQ(uinfo.user_email, "user1@dbstore.com");
	ASSERT_EQ(uinfo.user_id.id, "id");
	ASSERT_EQ(uinfo.suspended, 123);
	ASSERT_EQ(uinfo.max_buckets, 456);
	ASSERT_EQ(uinfo.assumed_role_arn, "role");
	ASSERT_EQ(uinfo.placement_tags.back(), "tags");
	RGWAccessKey k;
	map<string, RGWAccessKey>::iterator it2 = uinfo.access_keys.begin();
	k = it2->second;
	ASSERT_EQ(k.id, "id1");
	ASSERT_EQ(k.key, "key1");
	it2++;
	k = it2->second;
	ASSERT_EQ(k.id, "id2");
	ASSERT_EQ(k.key, "key2");
}

TEST_F(DBStoreBaseTest, GetUserQueryByAccessKey) {
	int ret = -1;
    RGWUserInfo uinfo;
    string key = "id1";

	ret = db->get_user("access_key", key, uinfo);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(uinfo.user_id.tenant, "tenant");
	ASSERT_EQ(uinfo.user_email, "user1@dbstore.com");
	ASSERT_EQ(uinfo.user_id.id, "id");
	ASSERT_EQ(uinfo.suspended, 123);
	ASSERT_EQ(uinfo.max_buckets, 456);
	ASSERT_EQ(uinfo.assumed_role_arn, "role");
	ASSERT_EQ(uinfo.placement_tags.back(), "tags");
	RGWAccessKey k;
	map<string, RGWAccessKey>::iterator it2 = uinfo.access_keys.begin();
	k = it2->second;
	ASSERT_EQ(k.id, "id1");
	ASSERT_EQ(k.key, "key1");
	it2++;
	k = it2->second;
	ASSERT_EQ(k.id, "id2");
	ASSERT_EQ(k.key, "key2");
}

TEST_F(DBStoreBaseTest, GetUserQueryByUserID) {
	int ret = -1;
    RGWUserInfo uinfo;
    uinfo.user_id.tenant = "tenant";
    uinfo.user_id.id = "id";

	ret = db->get_user("user_id", "", uinfo);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(uinfo.user_id.tenant, "tenant");
	ASSERT_EQ(uinfo.user_email, "user1@dbstore.com");
	ASSERT_EQ(uinfo.user_id.id, "id");
	ASSERT_EQ(uinfo.suspended, 123);
	ASSERT_EQ(uinfo.max_buckets, 456);
	ASSERT_EQ(uinfo.assumed_role_arn, "role");
	ASSERT_EQ(uinfo.placement_tags.back(), "tags");
	RGWAccessKey k;
	map<string, RGWAccessKey>::iterator it2 = uinfo.access_keys.begin();
	k = it2->second;
	ASSERT_EQ(k.id, "id1");
	ASSERT_EQ(k.key, "key1");
	it2++;
	k = it2->second;
	ASSERT_EQ(k.id, "id2");
	ASSERT_EQ(k.key, "key2");
}

TEST_F(DBStoreBaseTest, ListAllUsers) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ListAllUsers(&params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, InsertBucket) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("InsertBucket", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, ListBucket) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("ListBucket", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, ListAllBuckets) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ListAllBuckets(&params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, InsertObject) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("InsertObject", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, ListObject) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("ListObject", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, ListAllObjects) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ListAllObjects(&params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, PutObjectData) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("PutObjectData", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, GetObjectData) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("GetObjectData", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, DeleteObjectData) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("DeleteObjectData", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, RemoveObject) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("RemoveObject", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, RemoveBucket) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("RemoveBucket", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, RemoveUser) {
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
