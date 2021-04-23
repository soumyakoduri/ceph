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

ceph::real_time bucket_mtime = real_clock::now();
string marker1;

namespace {

	class DBStoreBaseTest : public ::testing::Test {
	protected:
		int ret;
		DBStore *db = nullptr;
		string user1 = "user1";
        string user_id1 = "user_id1";
		string bucket1 = "bucket1";
		string object1 = "object1";
		string data = "Hello World";
		DBOpParams GlobalParams = {};

		DBStoreBaseTest() {}
		void SetUp() {
			db = gtest::env->db;
			ASSERT_TRUE(db != nullptr);

			GlobalParams.op.user.uinfo.display_name = user1;
			GlobalParams.op.user.uinfo.user_id.id = user_id1;
			GlobalParams.op.bucket.info.bucket.name = bucket1;
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
	ASSERT_EQ(params.op.user.uinfo.user_id.id, "user_id1");
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
	ASSERT_EQ(params.op.user.uinfo.user_id.id, "user_id1");
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
	ASSERT_EQ(uinfo.user_id.id, "user_id1");
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
	ASSERT_EQ(uinfo.user_id.id, "user_id1");
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
    uinfo.user_id.id = "user_id1";

	ret = db->get_user("user_id", "", uinfo);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(uinfo.user_id.tenant, "tenant");
	ASSERT_EQ(uinfo.user_email, "user1@dbstore.com");
	ASSERT_EQ(uinfo.user_id.id, "user_id1");
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

	params.op.bucket.info.bucket.name = "bucket1";
    params.op.bucket.info.bucket.tenant = "tenant";
    params.op.bucket.info.bucket.marker = "marker1";

    params.op.bucket.ent.size = 1024;

    params.op.bucket.info.has_instance_obj = false;
    params.op.bucket.info.objv_tracker.read_version.ver = 1;
    params.op.bucket.info.objv_tracker.read_version.tag = "read_tag";

    params.op.bucket.mtime = bucket_mtime;

	ret = db->ProcessOp("InsertBucket", &params);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, GetBucket) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;

	ret = db->ProcessOp("GetBucket", &params);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(params.op.bucket.info.bucket.name, "bucket1");
	ASSERT_EQ(params.op.bucket.info.bucket.tenant, "tenant");
	ASSERT_EQ(params.op.bucket.ent.size, 1024);
	ASSERT_EQ(params.op.bucket.ent.bucket.name, "bucket1");
	ASSERT_EQ(params.op.bucket.ent.bucket.tenant, "tenant");
	ASSERT_EQ(params.op.bucket.info.has_instance_obj, false);
	ASSERT_EQ(params.op.bucket.info.objv_tracker.read_version.ver, 1);
	ASSERT_EQ(params.op.bucket.info.objv_tracker.read_version.tag, "read_tag");
	ASSERT_EQ(params.op.bucket.mtime, bucket_mtime);
	ASSERT_EQ(params.op.bucket.info.owner.id, "user_id1");
}

TEST_F(DBStoreBaseTest, RemoveBucketAPI) {
	int ret = -1;
    RGWBucketInfo info;

	info.bucket.name = "bucket1";

	ret = db->remove_bucket(info);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, CreateBucket) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;
    RGWBucketInfo info;
    RGWUserInfo owner;
    rgw_bucket bucket;
    obj_version objv;
    rgw_placement_rule rule;
    map<std::string, bufferlist> attrs;

    owner.user_id.id = "user_id1";
    bucket.name = "bucket1";
    bucket.tenant = "tenant";

    objv.ver = 2;
    objv.tag = "write_tag";

    rule.name = "rule1";
    rule.storage_class = "sc1";

	ret = db->create_bucket(owner, bucket, "zid", rule, "swift_ver", NULL,
                            attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
                            null_yield, NULL, false);
	ASSERT_EQ(ret, 0);
    bucket.name = "bucket2";
	ret = db->create_bucket(owner, bucket, "zid", rule, "swift_ver", NULL,
                            attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
                            null_yield, NULL, false);
	ASSERT_EQ(ret, 0);
    bucket.name = "bucket3";
	ret = db->create_bucket(owner, bucket, "zid", rule, "swift_ver", NULL,
                            attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
                            null_yield, NULL, false);
	ASSERT_EQ(ret, 0);
    bucket.name = "bucket4";
	ret = db->create_bucket(owner, bucket, "zid", rule, "swift_ver", NULL,
                            attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
                            null_yield, NULL, false);
	ASSERT_EQ(ret, 0);
    bucket.name = "bucket5";
	ret = db->create_bucket(owner, bucket, "zid", rule, "swift_ver", NULL,
                            attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
                            null_yield, NULL, false);
	ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreBaseTest, GetBucketQueryByName) {
	int ret = -1;
    RGWBucketInfo binfo;
    binfo.bucket.name = "bucket2";

	ret = db->get_bucket_info("name", "", binfo);
	ASSERT_EQ(ret, 0);
	ASSERT_EQ(binfo.bucket.name, "bucket2");
	ASSERT_EQ(binfo.bucket.tenant, "tenant");
	ASSERT_EQ(binfo.owner.id, "user_id1");
	ASSERT_EQ(binfo.objv_tracker.write_version.ver, 2);
	ASSERT_EQ(binfo.objv_tracker.write_version.tag, "write_tag");
	ASSERT_EQ(binfo.zonegroup, "zid");
	ASSERT_EQ(binfo.creation_time, bucket_mtime);
	ASSERT_EQ(binfo.placement_rule.name, "rule1");
	ASSERT_EQ(binfo.placement_rule.storage_class, "sc1");
    marker1 = binfo.bucket.marker;
}

TEST_F(DBStoreBaseTest, ListUserBuckets) {
	struct DBOpParams params = GlobalParams;
	int ret = -1;
    rgw_user owner;
    int max = 2;
    bool need_stats = true;
    bool is_truncated = false;
    RGWUserBuckets ulist;

    owner.id = "user_id1";

    marker1 = "";
    do {
        is_truncated = false;
    	ret = db->list_buckets(owner, marker1, "", max, need_stats, &ulist, &is_truncated);
	    ASSERT_EQ(ret, 0);

        cout << "marker1 :" << marker1 << "\n";

        cout << "is_truncated :" << is_truncated << "\n";

        for (const auto& ent: ulist.get_buckets()) {
          RGWBucketEnt e = ent.second;
          cout << "###################### \n";
          cout << "ent.bucket.id : " << e.bucket.name << "\n";
          cout << "ent.bucket.marker : " << e.bucket.marker << "\n";
          cout << "ent.bucket.bucket_id : " << e.bucket.bucket_id << "\n";
          cout << "ent.size : " << e.size << "\n";
          cout << "ent.rule.name : " << e.placement_rule.name << "\n";

          marker1 = e.bucket.marker;
        }
        ulist.clear();
    } while(is_truncated);
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
