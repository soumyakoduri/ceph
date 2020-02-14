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
			tenant("tenantname"), db_type("db_typename"),
			db(nullptr), ret(-1) {}

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
		string db_type;
		class DBstore *db;
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

			GlobalParams.user_name = user1;
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

	ret = db->ProcessOp("InsertUser", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBstoreBaseTest, ListUser) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("ListUser", &params);
	ASSERT_EQ(ret, 0);
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
