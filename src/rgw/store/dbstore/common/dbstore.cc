// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "dbstore.h"

using namespace std;

string DBstore::getDBname() {
	return tenant + ".db";
}

string DBstore::getTenant() {
	return tenant;
}

string DBstore::getUserTable() {
	return user_table;
}

string DBstore::getBucketTable() {
	return bucket_table;
}

map<string, class ObjectOp*> DBstore::objectmap = {};

map<string, class ObjectOp*> DBstore::getObjectMap() {
	return DBstore::objectmap;
}

string RGWOp::CreateTableSchema(string type, RGWOpParams *params) {
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

	return NULL;
}

string RGWOp::DeleteTableSchema(string table) {
	return fmt::format(DropQ.c_str(), table.c_str());
}

string RGWOp::ListTableSchema(string table) {
	return fmt::format(ListAllQ.c_str(), table.c_str());
}

string InsertUserOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(), pp->user_table.c_str(),
			       pp->user_name.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(), p->user_table.c_str(),
		       p->user_name.c_str());
}

string RemoveUserOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(), pp->user_table.c_str(),
			       pp->user_name.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(), p->user_table.c_str(),
		       p->user_name.c_str());
}

string ListUserOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(), pp->user_table.c_str(),
			       pp->user_name.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(), p->user_table.c_str(),
		       p->user_name.c_str());
}

string InsertBucketOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(), pp->bucket_table.c_str(),
			       	pp->bucket_name.c_str(), pp->user_name.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(), p->bucket_table.c_str(),
		       p->bucket_name.c_str(), p->user_name.c_str());
}

string RemoveBucketOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(), pp->bucket_table.c_str(),
			       pp->bucket_name.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(), p->bucket_table.c_str(),
		       p->bucket_name.c_str());
}

string ListBucketOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(), pp->bucket_table.c_str(),
			       pp->bucket_name.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(), p->bucket_table.c_str(),
		       p->bucket_name.c_str());
}

string InsertObjectOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(),
		 	pp->object_table.c_str(), pp->bucket_name.c_str(),
		        pp->object.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(),
		 p->object_table.c_str(), p->bucket_name.c_str(),
		 p->object.c_str());
}

string RemoveObjectOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(), pp->object_table.c_str(),
			       pp->bucket_name.c_str(), pp->object.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(), p->object_table.c_str(),
		       p->bucket_name.c_str(), p->object.c_str());
}

string ListObjectOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;


	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(), pp->object_table.c_str(),
			       pp->bucket_name.c_str(), pp->object.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(), p->object_table.c_str(),
		       p->bucket_name.c_str(), p->object.c_str());
}

string PutObjectDataOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(),
		 	pp->objectdata_table.c_str(),
		       	pp->bucket_name.c_str(), pp->object.c_str(),
		       	pp->offset.c_str(), pp->data.c_str(),
			pp->datalen.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(),
	 	p->objectdata_table.c_str(), p->bucket_name.c_str(),
	 	p->object.c_str(), p->offset, p->data.c_str(), p->datalen);
}

string GetObjectDataOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(),
		 	pp->objectdata_table.c_str(), pp->bucket_name.c_str(),
		        pp->object.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(),
		 p->objectdata_table.c_str(), p->bucket_name.c_str(),
		 p->object.c_str());
}

string DeleteObjectDataOp::Schema(SchemaParams *s_params) {
	struct RGWOpParams *p;
	struct RGWOpPrepareParams *pp;

	if (!s_params)
		return NULL;

	if (s_params->is_prepare) {
		pp = s_params->u.p_params;

		if (!pp)
			return NULL;

		return fmt::format(Query.c_str(),
		 	pp->objectdata_table.c_str(), pp->bucket_name.c_str(),
		        pp->object.c_str());
	}

	p = s_params->u.params;
	if (!p) /* RGWOpParams */
		return NULL;

	return fmt::format(Query.c_str(),
		 p->objectdata_table.c_str(), p->bucket_name.c_str(),
		 p->object.c_str());
}

int DBstore::Initialize()
{
	int ret = -1;

	db = openDB();

	if (!db)
		return ret;

	ret = LockInit();

	if (ret) {
        	cout<<"Error: mutex is NULL \n";
                closeDB();
                return ret;
        }

	ret = InitializeRGWOps();

	return ret;
}

int DBstore::Destroy()
{
	int ret = -1;

	if (!db)
		return ret;

	ret = closeDB();

	ret = LockDestroy();

	ret = FreeRGWOps();

	return 0;
}

int DBstore::LockInit() {
	int ret;
	
	ret = pthread_mutex_init(&mutex, NULL);

	if (ret)
		cout<<"pthread_mutex_init failed \n";

	return ret;
}

int DBstore::LockDestroy() {
	int ret;
	
	ret = pthread_mutex_destroy(&mutex);

	if (ret)
		cout<<"pthread_mutex_destroy failed \n";

	return ret;
}

int DBstore::Lock() {
	int ret;

	ret = pthread_mutex_lock(&mutex);

	if (ret)
		cout<<"pthread_mutex_lock failed \n";

	return ret;
}

int DBstore::Unlock() {
	int ret;

	ret = pthread_mutex_unlock(&mutex);

	if (ret)
		cout<<"pthread_mutex_unlock failed \n";

	return ret;
}

RGWOp * DBstore::getRGWOp(string Op, struct RGWOpParams *params)
{
	if (!Op.compare("InsertUser"))
		return rgwops.InsertUser;
	if (!Op.compare("RemoveUser"))
		return rgwops.RemoveUser;
	if (!Op.compare("ListUser"))
		return rgwops.ListUser;
	if (!Op.compare("InsertBucket"))
		return rgwops.InsertBucket;
	if (!Op.compare("RemoveBucket"))
		return rgwops.RemoveBucket;
	if (!Op.compare("ListBucket"))
		return rgwops.ListBucket;

	/* Object Operations */
	map<string, class ObjectOp*>::iterator iter;
	class ObjectOp* Ob;

	iter = DBstore::objectmap.find(params->bucket_name);

	if (iter == DBstore::objectmap.end()) {
		cout<<"No objectmap found for bucket: "<<params->bucket_name<<"\n";
		/* not found */
		return NULL;
	}

	Ob = iter->second;

	if (!Op.compare("InsertObject"))
		return Ob->InsertObject;
	if (!Op.compare("RemoveObject"))
		return Ob->RemoveObject;
	if (!Op.compare("ListObject"))
		return Ob->ListObject;
	if (!Op.compare("PutObjectData"))
		return Ob->PutObjectData;
	if (!Op.compare("GetObjectData"))
		return Ob->GetObjectData;
	if (!Op.compare("DeleteObjectData"))
		return Ob->DeleteObjectData;

	return NULL;
}

int DBstore::objectmapInsert(string bucket, void *ptr)
{
	map<string, class ObjectOp*>::iterator iter;
	class ObjectOp *Ob;

	iter = DBstore::objectmap.find(bucket);

	if (iter != DBstore::objectmap.end()) {
		// entry already exists
		// return success or replace it or
		// return error ?
		// return success for now
		return 0;
	}

	Ob = (class ObjectOp*) ptr;
	Ob->InitializeObjectOps();

	DBstore::objectmap.insert(pair<string, class ObjectOp*>(bucket, Ob));

	return 0;
}

int DBstore::objectmapDelete(string bucket)
{
	map<string, class ObjectOp*>::iterator iter;
	class ObjectOp *Ob;

	iter = DBstore::objectmap.find(bucket);

	if (iter == DBstore::objectmap.end()) {
		// entry doesn't exist
		// return success or return error ?
		// return success for now
		return 0;
	}

	Ob = (class ObjectOp*) (iter->second);
	Ob->FreeObjectOps();

	DBstore::objectmap.erase(iter);

	return 0;
}

int DBstore::InitializeParams(string Op, RGWOpParams *params)
{
	int ret = -1;

	if (!params)
		goto out;

	//reset params here
	params->tenant = tenant;
	params->user_table = user_table;
	params->bucket_table = bucket_table;

out:
	return ret;
}

int DBstore::ProcessOp(string Op, struct RGWOpParams *params) {
	int ret = -1;
	class RGWOp *rgw_op;

	Lock();
	rgw_op = getRGWOp(Op, params);

	if (!rgw_op) {
		cout<<"No rgw_op for Op: "<<Op<<"\n";
		Unlock();
		return ret;
	}
	ret = rgw_op->Execute(params);

	Unlock();
	if (ret)
		cout<<"In Process op Execute failed for fop : "<<Op.c_str()<<" \n";

	return ret;
}
