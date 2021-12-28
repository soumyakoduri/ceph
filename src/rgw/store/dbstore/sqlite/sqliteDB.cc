// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "sqliteDB.h"

#define SQL_PREPARE(dpp, params, sdb, stmt, ret, Op) 	\
  do {							\
    string schema;			   		\
    schema = Schema(params);	   		\
    sqlite3_prepare_v2 (*sdb, schema.c_str(), 	\
        -1, &stmt , NULL);		\
    if (!stmt) {					\
      ldpp_dout(dpp, 0) <<"failed to prepare statement " \
      <<"for Op("<<Op<<"); Errmsg -"\
      <<sqlite3_errmsg(*sdb)<< dendl;\
      ret = -1;				\
      goto out;				\
    }						\
    ldpp_dout(dpp, 20)<<"Successfully Prepared stmt for Op("<<Op	\
    <<") schema("<<schema<<") stmt("<<stmt<<")"<< dendl;	\
    ret = 0;					\
  } while(0);

#define MAX_RETRIES 200

#define EXECUTE_STMT(dpp, rc, stmt) \
  do {                          \
    int retries = 0;                  \
    do {                        \
      rc = stmt;                  \
      retries++;                    \
      if (retries > 1) {  \
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); \
      }   \
    } while ((rc == SQLITE_BUSY) && (retries < MAX_RETRIES));    \
    if (retries > 1) {  \
        ldpp_dout(dpp, 20)<<"No. of retries = " << retries << dendl; \
    } \
  } while(0);

    //ldpp_dout(dpp, 20)<<"No. of retries = " << retries << " for stmt ("<<stmt	
    //<<")" << dendl; 
#define SQL_BIND_INDEX(dpp, stmt, index, str, sdb)	\
  do {						\
    EXECUTE_STMT(dpp, index, sqlite3_bind_parameter_index(stmt, str));    \
    \
    if (index <=0)  {				     \
      ldpp_dout(dpp, 0) <<"failed to fetch bind parameter"\
      " index for str("<<str<<") in "   \
      <<"stmt("<<stmt<<"); Errmsg -"    \
      <<sqlite3_errmsg(*sdb)<< dendl; 	     \
      rc = -1;				     \
      goto out;				     \
    }						     \
    ldpp_dout(dpp, 20)<<"Bind parameter index for str("  \
    <<str<<") in stmt("<<stmt<<") is "  \
    <<index<< dendl;			     \
  }while(0);

#define SQL_BIND_TEXT(dpp, stmt, index, str, sdb)			\
  do {		\
    if (strcmp(str, "null") == 0) {          \
      EXECUTE_STMT(dpp, rc, sqlite3_bind_text(stmt, index, "", -1, SQLITE_TRANSIENT));    \
    } else {                                                       \
      EXECUTE_STMT(dpp, rc, sqlite3_bind_text(stmt, index, str, -1, SQLITE_TRANSIENT));    \
    }  \
    if (rc != SQLITE_OK) {					      	\
      ldpp_dout(dpp, 0)<<"sqlite bind text failed for index("     	\
      <<index<<"), str("<<str<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      << dendl;				\
      rc = -1;					\
      goto out;					\
    }							\
  }while(0);

#define SQL_BIND_INT(dpp, stmt, index, num, sdb)			\
  do {								\
    EXECUTE_STMT(dpp, rc, sqlite3_bind_int(stmt, index, num));    \
    \
    if (rc != SQLITE_OK) {					\
      ldpp_dout(dpp, 0)<<"sqlite bind int failed for index("     	\
      <<index<<"), num("<<num<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      << dendl;				\
      rc = -1;					\
      goto out;					\
    }							\
  }while(0);

#define SQL_BIND_BLOB(dpp, stmt, index, blob, size, sdb)		\
  do {								\
    EXECUTE_STMT(dpp, rc, sqlite3_bind_blob(stmt, index, blob, size, SQLITE_TRANSIENT));    \
    \
    if (rc != SQLITE_OK) {					\
      ldpp_dout(dpp, 0)<<"sqlite bind blob failed for index("     	\
      <<index<<"), blob("<<blob<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      << dendl;				\
      rc = -1;					\
      goto out;					\
    }							\
  }while(0);

#define SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, param, sdb)		\
  do {								\
    bufferlist b;						\
    encode(param, b);					\
    SQL_BIND_BLOB(dpp, stmt, index, b.c_str(), b.length(), sdb); \
  }while(0);

#define SQL_READ_BLOB(dpp, stmt, index, void_ptr, len)		\
  do {								\
    void_ptr = NULL;					\
    void_ptr = (void *)sqlite3_column_blob(stmt, index);	\
    len = sqlite3_column_bytes(stmt, index);		\
    \
    if (!void_ptr || len == 0) {				\
      ldpp_dout(dpp, 20)<<"Null value for blob index("  \
      <<index<<") in stmt("<<stmt<<") "<< dendl;   \
    }							\
  }while(0);

#define SQL_DECODE_BLOB_PARAM(dpp, stmt, index, param, sdb)		\
  do {								\
    bufferlist b;						\
    void *blob;						\
    int blob_len = 0;					\
    \
    SQL_READ_BLOB(dpp, stmt, index, blob, blob_len);		\
    \
    b.append(reinterpret_cast<char *>(blob), blob_len);	\
    \
    decode(param, b);					\
  }while(0);

#define SQL_EXECUTE(dpp, params, stmt, cbk, args...) \
  do{						\
    if (!stmt) {				\
      ret = Prepare(dpp, params);		\
    }					\
    \
    if (!stmt) {				\
      ldpp_dout(dpp, 0) <<"No prepared statement "<< dendl;	\
      goto out;			\
    }					\
    \
    ret = Bind(dpp, params);			\
    if (ret) {				\
      ldpp_dout(dpp, 0) <<"Bind parameters failed for stmt(" <<stmt<<") "<< dendl;		\
      goto out;			\
    }					\
    \
    ret = Step(dpp, params->op, stmt, cbk);		\
    \
    Reset(dpp, stmt);				\
    \
    if (ret) {				\
      ldpp_dout(dpp, 0) <<"Execution failed for stmt(" <<stmt<<")"<< dendl;		\
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

enum GetUser {
  UserID = 0,
  Tenant,
  NS,
  DisplayName,
  UserEmail,
  AccessKeysID,
  AccessKeysSecret,
  AccessKeys,
  SwiftKeys,
  SubUsers,
  Suspended,
  MaxBuckets,
  OpMask,
  UserCaps,
  Admin,
  System,
  PlacementName,
  PlacementStorageClass,
  PlacementTags,
  BucketQuota,
  TempURLKeys,
  UserQuota,
  TYPE,
  MfaIDs,
  AssumedRoleARN,
  UserAttrs,
  UserVersion,
  UserVersionTag,
};

enum GetBucket {
  BucketName = 0,
  Bucket_Tenant, //Tenant
  Marker,
  BucketID,
  Size,
  SizeRounded,
  CreationTime,
  Count,
  Bucket_PlacementName,
  Bucket_PlacementStorageClass,
  OwnerID,
  Flags,
  Zonegroup,
  HasInstanceObj,
  Quota,
  RequesterPays,
  HasWebsite,
  WebsiteConf,
  SwiftVersioning,
  SwiftVerLocation,
  MdsearchConfig,
  NewBucketInstanceID,
  ObjectLock,
  SyncPolicyInfoGroups,
  BucketAttrs,
  BucketVersion,
  BucketVersionTag,
  Mtime,
  Bucket_User_NS
};

enum GetObject {
  ObjName,
  ObjInstance,
  ObjNS,
  ObjBucketName,
  ACLs,
  IndexVer,
  Tag,
  ObjFlags,
  VersionedEpoch,
  ObjCategory,
  Etag,
  Owner,
  OwnerDisplayName,
  StorageClass,
  Appendable,
  ContentType,
  IndexHashSource,
  ObjSize,
  AccountedSize,
  ObjMtime,
  Epoch,
  ObjTag,
  TailTag,
  WriteTag,
  FakeTag,
  ShadowObj,
  HasData,
  IsOLH,
  OLHTag,
  PGVer,
  ZoneShortID,
  ObjVersion,
  ObjVersionTag,
  ObjAttrs,
  HeadSize,
  MaxHeadSize,
  Prefix,
  TailInstance,
  HeadPlacementRuleName,
  HeadPlacementRuleStorageClass,
  TailPlacementRuleName,
  TailPlacementStorageClass,
  ManifestPartObjs,
  ManifestPartRules,
  Omap,
  IsMultipart,
  MPPartsList,
  HeadData
};

enum GetObjectData {
  ObjDataName,
  ObjDataInstance,
  ObjDataNS,
  ObjDataBucketName,
  MultipartPartStr,
  PartNum,
  Offset,
  ObjDataSize,
  ObjData
};

enum GetLCEntry {
  LCEntryIndex,
  LCEntryBucketName,
  LCEntryStartTime,
  LCEntryStatus
};

enum GetLCHead {
  LCHeadIndex,
  LCHeadMarker,
  LCHeadStartDate
};

static int list_user(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.user.uinfo.user_id.tenant = (const char*)sqlite3_column_text(stmt, Tenant);
  op.user.uinfo.user_id.id = (const char*)sqlite3_column_text(stmt, UserID);
  op.user.uinfo.user_id.ns = (const char*)sqlite3_column_text(stmt, NS);
  op.user.uinfo.display_name = (const char*)sqlite3_column_text(stmt, DisplayName); // user_name
  op.user.uinfo.user_email = (const char*)sqlite3_column_text(stmt, UserEmail);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, AccessKeys, op.user.uinfo.access_keys, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, SwiftKeys, op.user.uinfo.swift_keys, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, SubUsers, op.user.uinfo.subusers, sdb);

  op.user.uinfo.suspended = sqlite3_column_int(stmt, Suspended);
  op.user.uinfo.max_buckets = sqlite3_column_int(stmt, MaxBuckets);
  op.user.uinfo.op_mask = sqlite3_column_int(stmt, OpMask);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, UserCaps, op.user.uinfo.caps, sdb);

  op.user.uinfo.admin = sqlite3_column_int(stmt, Admin);
  op.user.uinfo.system = sqlite3_column_int(stmt, System);

  op.user.uinfo.default_placement.name = (const char*)sqlite3_column_text(stmt, PlacementName);

  op.user.uinfo.default_placement.storage_class = (const char*)sqlite3_column_text(stmt, PlacementStorageClass);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, PlacementTags, op.user.uinfo.placement_tags, sdb);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, BucketQuota, op.user.uinfo.bucket_quota, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, TempURLKeys, op.user.uinfo.temp_url_keys, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, UserQuota, op.user.uinfo.user_quota, sdb);

  op.user.uinfo.type = sqlite3_column_int(stmt, TYPE);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, MfaIDs, op.user.uinfo.mfa_ids, sdb);

  op.user.uinfo.assumed_role_arn = (const char*)sqlite3_column_text(stmt, AssumedRoleARN);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, UserAttrs, op.user.user_attrs, sdb);
  op.user.user_version.ver = sqlite3_column_int(stmt, UserVersion);
  op.user.user_version.tag = (const char*)sqlite3_column_text(stmt, UserVersionTag);

  return 0;
}

static int list_bucket(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.bucket.ent.bucket.name = (const char*)sqlite3_column_text(stmt, BucketName);
  op.bucket.ent.bucket.tenant = (const char*)sqlite3_column_text(stmt, Bucket_Tenant);
  op.bucket.ent.bucket.marker = (const char*)sqlite3_column_text(stmt, Marker);
  op.bucket.ent.bucket.bucket_id = (const char*)sqlite3_column_text(stmt, BucketID);
  op.bucket.ent.size = sqlite3_column_int(stmt, Size);
  op.bucket.ent.size_rounded = sqlite3_column_int(stmt, SizeRounded);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, CreationTime, op.bucket.ent.creation_time, sdb);
  op.bucket.ent.count = sqlite3_column_int(stmt, Count);
  op.bucket.ent.placement_rule.name = (const char*)sqlite3_column_text(stmt, Bucket_PlacementName);
  op.bucket.ent.placement_rule.storage_class = (const char*)sqlite3_column_text(stmt, Bucket_PlacementStorageClass);

  op.bucket.info.bucket = op.bucket.ent.bucket;
  op.bucket.info.placement_rule = op.bucket.ent.placement_rule;
  op.bucket.info.creation_time = op.bucket.ent.creation_time;

  op.bucket.info.owner.id = (const char*)sqlite3_column_text(stmt, OwnerID);
  op.bucket.info.owner.tenant = op.bucket.ent.bucket.tenant;

  if (op.name == "GetBucket") {
    op.bucket.info.owner.ns = (const char*)sqlite3_column_text(stmt, Bucket_User_NS);
  }

  op.bucket.info.flags = sqlite3_column_int(stmt, Flags);
  op.bucket.info.zonegroup = (const char*)sqlite3_column_text(stmt, Zonegroup);
  op.bucket.info.has_instance_obj = sqlite3_column_int(stmt, HasInstanceObj);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, Quota, op.bucket.info.quota, sdb);
  op.bucket.info.requester_pays = sqlite3_column_int(stmt, RequesterPays);
  op.bucket.info.has_website = sqlite3_column_int(stmt, HasWebsite);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, WebsiteConf, op.bucket.info.website_conf, sdb);
  op.bucket.info.swift_versioning = sqlite3_column_int(stmt, SwiftVersioning);
  op.bucket.info.swift_ver_location = (const char*)sqlite3_column_text(stmt, SwiftVerLocation);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, MdsearchConfig, op.bucket.info.mdsearch_config, sdb);
  op.bucket.info.new_bucket_instance_id = (const char*)sqlite3_column_text(stmt, NewBucketInstanceID);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjectLock, op.bucket.info.obj_lock, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, SyncPolicyInfoGroups, op.bucket.info.sync_policy, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, BucketAttrs, op.bucket.bucket_attrs, sdb);
  op.bucket.bucket_version.ver = sqlite3_column_int(stmt, BucketVersion);
  op.bucket.bucket_version.tag = (const char*)sqlite3_column_text(stmt, BucketVersionTag);

  /* Read bucket version into info.objv_tracker.read_ver. No need
   * to set write_ver as its not used anywhere. Still keeping its
   * value same as read_ver */
  op.bucket.info.objv_tracker.read_version = op.bucket.bucket_version;
  op.bucket.info.objv_tracker.write_version = op.bucket.bucket_version;

  SQL_DECODE_BLOB_PARAM(dpp, stmt, Mtime, op.bucket.mtime, sdb);

  op.bucket.list_entries.push_back(op.bucket.ent);

  return 0;
}

static int list_object(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  //cout<<sqlite3_column_text(stmt, 0)<<", ";
  //cout<<sqlite3_column_text(stmt, 1) << "\n";

  op.obj.state.exists = true;
  op.obj.state.obj.key.name = (const char*)sqlite3_column_text(stmt, ObjName);
  op.bucket.info.bucket.name = (const char*)sqlite3_column_text(stmt, ObjBucketName);
  op.obj.state.obj.key.instance = (const char*)sqlite3_column_text(stmt, ObjInstance);
  op.obj.state.obj.key.ns = (const char*)sqlite3_column_text(stmt, ObjNS);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ACLs, op.obj.acls, sdb);
  op.obj.index_ver = sqlite3_column_int(stmt, IndexVer);
  op.obj.tag = (const char*)sqlite3_column_text(stmt, Tag);
  op.obj.flags = sqlite3_column_int(stmt, ObjFlags); 
  op.obj.versioned_epoch = sqlite3_column_int(stmt, VersionedEpoch);
  op.obj.category = (RGWObjCategory)sqlite3_column_int(stmt, ObjCategory); 
  op.obj.etag = (const char*)sqlite3_column_text(stmt, Etag);
  op.obj.owner = (const char*)sqlite3_column_text(stmt, Owner);
  op.obj.owner_display_name = (const char*)sqlite3_column_text(stmt, OwnerDisplayName);
  op.obj.storage_class = (const char*)sqlite3_column_text(stmt, StorageClass);
  op.obj.appendable = sqlite3_column_int(stmt, Appendable); 
  op.obj.content_type = (const char*)sqlite3_column_text(stmt, ContentType);
  op.obj.state.obj.index_hash_source = (const char*)sqlite3_column_text(stmt, IndexHashSource);
  op.obj.state.size = sqlite3_column_int(stmt, ObjSize); 
  op.obj.state.accounted_size = sqlite3_column_int(stmt, AccountedSize); 
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjMtime, op.obj.state.mtime, sdb);
  op.obj.state.epoch = sqlite3_column_int(stmt, Epoch);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjTag, op.obj.state.obj_tag, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, TailTag, op.obj.state.tail_tag, sdb);
  op.obj.state.write_tag = (const char*)sqlite3_column_text(stmt, WriteTag);
  op.obj.state.fake_tag = sqlite3_column_int(stmt, FakeTag);
  op.obj.state.shadow_obj = (const char*)sqlite3_column_text(stmt, ShadowObj);
  op.obj.state.has_data = sqlite3_column_int(stmt, HasData); 
  op.obj.state.is_olh = sqlite3_column_int(stmt, IsOLH); 
  SQL_DECODE_BLOB_PARAM(dpp, stmt, OLHTag, op.obj.state.olh_tag, sdb);
  op.obj.state.pg_ver = sqlite3_column_int(stmt, PGVer); 
  op.obj.state.zone_short_id = sqlite3_column_int(stmt, ZoneShortID); 
  op.obj.state.objv_tracker.read_version.ver = sqlite3_column_int(stmt, ObjVersion); 
  op.obj.state.objv_tracker.read_version.tag = (const char*)sqlite3_column_text(stmt, ObjVersionTag);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjAttrs, op.obj.state.attrset, sdb);
  op.obj.head_size = sqlite3_column_int(stmt, HeadSize); 
  op.obj.max_head_size = sqlite3_column_int(stmt, MaxHeadSize); 
  op.obj.prefix = (const char*)sqlite3_column_text(stmt, Prefix);
  op.obj.tail_instance = (const char*)sqlite3_column_text(stmt, TailInstance);
  op.obj.head_placement_rule.name = (const char*)sqlite3_column_text(stmt, HeadPlacementRuleName);
  op.obj.head_placement_rule.storage_class = (const char*)sqlite3_column_text(stmt, HeadPlacementRuleStorageClass);
  op.obj.tail_placement.placement_rule.name = (const char*)sqlite3_column_text(stmt, TailPlacementRuleName);
  op.obj.tail_placement.placement_rule.storage_class = (const char*)sqlite3_column_text(stmt, TailPlacementStorageClass);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ManifestPartObjs, op.obj.objs, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ManifestPartRules, op.obj.rules, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, Omap, op.obj.omap, sdb);
  op.obj.is_multipart = sqlite3_column_int(stmt, IsMultipart);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, MPPartsList, op.obj.mp_parts, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, HeadData, op.obj.head_data, sdb);
  op.obj.state.data = op.obj.head_data;

  rgw_bucket_dir_entry dent;
  dent.key.name = op.obj.state.obj.key.name;
  dent.key.instance = op.obj.state.obj.key.instance;
  dent.tag = op.obj.tag;
  dent.flags = op.obj.flags;
  dent.versioned_epoch = op.obj.versioned_epoch;
  dent.index_ver = op.obj.index_ver;
  dent.exists = true;
  dent.meta.category = op.obj.category;
  dent.meta.size = op.obj.state.size;
  dent.meta.accounted_size = op.obj.state.accounted_size;
  dent.meta.mtime = op.obj.state.mtime;
  dent.meta.etag = op.obj.etag;
  dent.meta.owner = op.obj.owner;
  dent.meta.owner_display_name = op.obj.owner_display_name;
  dent.meta.content_type = op.obj.content_type;
  dent.meta.storage_class = op.obj.storage_class;
  dent.meta.appendable = op.obj.appendable;

  op.obj.list_entries.push_back(dent);
  return 0;
}

static int get_objectdata(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.obj.state.obj.key.name = (const char*)sqlite3_column_text(stmt, ObjName);
  op.bucket.info.bucket.name = (const char*)sqlite3_column_text(stmt, ObjBucketName);
  op.obj.state.obj.key.instance = (const char*)sqlite3_column_text(stmt, ObjInstance);
  op.obj.state.obj.key.ns = (const char*)sqlite3_column_text(stmt, ObjNS);
  op.obj_data.part_num = sqlite3_column_int(stmt, PartNum);
  op.obj_data.offset = sqlite3_column_int(stmt, Offset);
  op.obj_data.size = sqlite3_column_int(stmt, ObjDataSize);
  op.obj_data.multipart_part_str = (const char*)sqlite3_column_text(stmt, MultipartPartStr);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjData, op.obj_data.data, sdb);

  return 0;
}

static int list_lc_entry(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.lc_entry.index = (const char*)sqlite3_column_text(stmt, LCEntryIndex);
  op.lc_entry.entry.bucket = (const char*)sqlite3_column_text(stmt, LCEntryBucketName);
  op.lc_entry.entry.start_time = sqlite3_column_int(stmt, LCEntryStartTime);
  op.lc_entry.entry.status = sqlite3_column_int(stmt, LCEntryStatus);
 
  op.lc_entry.list_entries.push_back(op.lc_entry.entry);

  return 0;
}

static int list_lc_head(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.lc_head.index = (const char*)sqlite3_column_text(stmt, LCHeadIndex);
  op.lc_head.head.marker = (const char*)sqlite3_column_text(stmt, LCHeadMarker);
 
  SQL_DECODE_BLOB_PARAM(dpp, stmt, LCHeadStartDate, op.lc_head.head.start_date, sdb);

  return 0;
}

int SQLiteDB::InitializeDBOps(const DoutPrefixProvider *dpp)
{
  (void)createTables(dpp);
  dbops.InsertUser = new SQLInsertUser(&this->db, this->getDBname(), cct);
  dbops.RemoveUser = new SQLRemoveUser(&this->db, this->getDBname(), cct);
  dbops.GetUser = new SQLGetUser(&this->db, this->getDBname(), cct);
  dbops.InsertBucket = new SQLInsertBucket(&this->db, this->getDBname(), cct);
  dbops.UpdateBucket = new SQLUpdateBucket(&this->db, this->getDBname(), cct);
  dbops.RemoveBucket = new SQLRemoveBucket(&this->db, this->getDBname(), cct);
  dbops.GetBucket = new SQLGetBucket(&this->db, this->getDBname(), cct);
  dbops.ListUserBuckets = new SQLListUserBuckets(&this->db, this->getDBname(), cct);
  dbops.InsertLCEntry = new SQLInsertLCEntry(&this->db, this->getDBname(), cct);
  dbops.RemoveLCEntry = new SQLRemoveLCEntry(&this->db, this->getDBname(), cct);
  dbops.GetLCEntry = new SQLGetLCEntry(&this->db, this->getDBname(), cct);
  dbops.ListLCEntries = new SQLListLCEntries(&this->db, this->getDBname(), cct);
  dbops.InsertLCHead = new SQLInsertLCHead(&this->db, this->getDBname(), cct);
  dbops.RemoveLCHead = new SQLRemoveLCHead(&this->db, this->getDBname(), cct);
  dbops.GetLCHead = new SQLGetLCHead(&this->db, this->getDBname(), cct);

  return 0;
}

int SQLiteDB::FreeDBOps(const DoutPrefixProvider *dpp)
{
  delete dbops.InsertUser;
  delete dbops.RemoveUser;
  delete dbops.GetUser;
  delete dbops.InsertBucket;
  delete dbops.UpdateBucket;
  delete dbops.RemoveBucket;
  delete dbops.GetBucket;
  delete dbops.ListUserBuckets;
  delete dbops.InsertLCEntry;
  delete dbops.RemoveLCEntry;
  delete dbops.GetLCEntry;
  delete dbops.ListLCEntries;
  delete dbops.InsertLCHead;
  delete dbops.RemoveLCHead;
  delete dbops.GetLCHead;

  return 0;
}

void *SQLiteDB::openDB(const DoutPrefixProvider *dpp)
{
  string dbname;
  int rc = 0;

  dbname = getDBfile();
  if (dbname.empty()) {
    ldpp_dout(dpp, 0)<<"dbname is NULL" << dendl;
    goto out;
  }

//  do {
    rc = sqlite3_open_v2(dbname.c_str(), (sqlite3**)&db,
        SQLITE_OPEN_READWRITE |
        SQLITE_OPEN_CREATE |
        SQLITE_OPEN_FULLMUTEX,
        NULL);
  //} while(rc == SQLITE_BUSY);

  if (rc) {
    ldpp_dout(dpp, 0) <<"Cant open "<<dbname<<"; Errmsg - "\
      <<sqlite3_errmsg((sqlite3*)db) <<  dendl;
  } else {
    ldpp_dout(dpp, 0) <<"Opened database("<<dbname<<") successfully" <<  dendl;
  }

  exec(dpp, "PRAGMA foreign_keys=ON", NULL);
  exec(dpp, "PRAGMA journal_mode=WAL", NULL);

out:
  return db;
}

int SQLiteDB::closeDB(const DoutPrefixProvider *dpp)
{
  if (db)
    sqlite3_close((sqlite3 *)db);

  db = NULL;

  return 0;
}

int SQLiteDB::Reset(const DoutPrefixProvider *dpp, sqlite3_stmt *stmt)
{
  int ret = -1;

  if (!stmt) {
    return -1;
  }
  sqlite3_clear_bindings(stmt);

  do {
    ret = sqlite3_reset(stmt);
  } while(ret == SQLITE_BUSY);

  return ret;
}

int SQLiteDB::Step(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt,
    int (*cbk)(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt))
{
  int ret = -1;

  if (!stmt) {
    return -1;
  }

again:
  EXECUTE_STMT(dpp, ret, sqlite3_step(stmt));

  if ((ret != SQLITE_DONE) && (ret != SQLITE_ROW)) {
    ldpp_dout(dpp, 0)<<"sqlite step failed for stmt("<<stmt \
      <<"); Errmsg - "<<sqlite3_errmsg((sqlite3*)db) << dendl;
    return -1;
  } else if (ret == SQLITE_ROW) {
    if (cbk) {
      (*cbk)(dpp, op, stmt);
    } else {
    }
    goto again;
  }

  ldpp_dout(dpp, 20)<<"sqlite step successfully executed for stmt(" \
    <<stmt<<")  ret = " << ret << dendl;

  return 0;
}

int SQLiteDB::exec(const DoutPrefixProvider *dpp, const char *schema,
    int (*callback)(void*,int,char**,char**))
{
  int ret = -1;
  char *errmsg = NULL;

  if (!db)
    goto out;

  EXECUTE_STMT(dpp, ret, sqlite3_exec((sqlite3*)db, schema, callback, 0, &errmsg));
  if (ret != SQLITE_OK) {
    ldpp_dout(dpp, 0) <<"sqlite exec failed for schema("<<schema \
       <<"); Errmsg - "<<errmsg <<  dendl;
    sqlite3_free(errmsg);
    goto out;
  }

  ret = 0;
  ldpp_dout(dpp, 10) <<"sqlite exec successfully processed for schema(" \
    <<schema<<")" <<  dendl;
out:
  return ret;
}

int SQLiteDB::createTables(const DoutPrefixProvider *dpp)
{
  int ret = -1;
  int cu = 0, cb = 0, cq = 0;
  DBOpParams params = {};

  params.user_table = getUserTable();
  params.bucket_table = getBucketTable();

  if ((cu = createUserTable(dpp, &params)))
    goto out;

  if ((cb = createBucketTable(dpp, &params)))
    goto out;

  if ((cq = createQuotaTable(dpp, &params)))
    goto out;

  ret = 0;
out:
  if (ret) {
    if (cu)
      DeleteUserTable(dpp, &params);
    if (cb)
      DeleteBucketTable(dpp, &params);
    ldpp_dout(dpp, 0)<<"Creation of tables failed" << dendl;
  }

  return ret;
}

int SQLiteDB::createUserTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("User", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateUserTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateUserTable suceeded" << dendl;

  return ret;
}

int SQLiteDB::createBucketTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Bucket", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateBucketTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateBucketTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::createObjectTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Object", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateObjectTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateObjectTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::createQuotaTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Quota", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateQuotaTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateQuotaTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::createObjectDataTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("ObjectData", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateObjectDataTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateObjectDataTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::createLCTables(const DoutPrefixProvider *dpp)
{
  int ret = -1;
  string schema;
  DBOpParams params = {};

  params.lc_entry_table = getLCEntryTable();
  params.lc_head_table = getLCHeadTable();
  params.bucket_table = getBucketTable();

  schema = CreateTableSchema("LCEntry", &params);
  ret = exec(dpp, schema.c_str(), NULL);
  if (ret) {
    ldpp_dout(dpp, 0)<<"CreateLCEntryTable failed" << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20)<<"CreateLCEntryTable suceeded" << dendl;

  schema = CreateTableSchema("LCHead", &params);
  ret = exec(dpp, schema.c_str(), NULL);
  if (ret) {
    ldpp_dout(dpp, 0)<<"CreateLCHeadTable failed" << dendl;
    (void)DeleteLCEntryTable(dpp, &params);
  }
  ldpp_dout(dpp, 20)<<"CreateLCHeadTable suceeded" << dendl;

  return ret;
}

int SQLiteDB::DeleteUserTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->user_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteUserTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteUserTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteBucketTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->bucket_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeletebucketTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeletebucketTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteObjectTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->object_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteObjectTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteObjectTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteObjectDataTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->objectdata_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteObjectDataTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteObjectDataTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteQuotaTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->quota_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteQuotaTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteQuotaTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteLCEntryTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->lc_entry_table);
  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteLCEntryTable failed " << dendl;
  ldpp_dout(dpp, 20)<<"DeleteLCEntryTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteLCHeadTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->lc_head_table);
  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteLCHeadTable failed " << dendl;
  ldpp_dout(dpp, 20)<<"DeleteLCHeadTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::ListAllUsers(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = ListTableSchema(params->user_table);
  ret = exec(dpp, schema.c_str(), &list_callback);
  if (ret)
    ldpp_dout(dpp, 0)<<"GetUsertable failed " << dendl;

  ldpp_dout(dpp, 20)<<"GetUserTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::ListAllBuckets(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = ListTableSchema(params->bucket_table);

  ret = exec(dpp, schema.c_str(), &list_callback);
  if (ret)
    ldpp_dout(dpp, 0)<<"Listbuckettable failed " << dendl;

  ldpp_dout(dpp, 20)<<"ListbucketTable suceeded " << dendl;

  return ret;
}

int SQLiteDB::ListAllObjects(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;
  map<string, class ObjectOp*>::iterator iter;
  map<string, class ObjectOp*> objectmap;
  string bucket;

  objectmap = getObjectMap();

  if (objectmap.empty())
    ldpp_dout(dpp, 20)<<"objectmap empty " << dendl;

  for (iter = objectmap.begin(); iter != objectmap.end(); ++iter) {
    bucket = iter->first;
    params->object_table = getObjectTable(bucket);
    schema = ListTableSchema(params->object_table);

    ret = exec(dpp, schema.c_str(), &list_callback);
    if (ret)
      ldpp_dout(dpp, 0)<<"ListObjecttable failed " << dendl;

    ldpp_dout(dpp, 20)<<"ListObjectTable suceeded " << dendl;
  }

  return ret;
}

int SQLObjectOp::InitializeObjectOps(string db_name, const DoutPrefixProvider *dpp)
{
  PutObject = new SQLPutObject(sdb, db_name, cct);
  DeleteObject = new SQLDeleteObject(sdb, db_name, cct);
  GetObject = new SQLGetObject(sdb, db_name, cct);
  UpdateObject = new SQLUpdateObject(sdb, db_name, cct);
  ListBucketObjects = new SQLListBucketObjects(sdb, db_name, cct);
  PutObjectData = new SQLPutObjectData(sdb, db_name, cct);
  UpdateObjectData = new SQLUpdateObjectData(sdb, db_name, cct);
  GetObjectData = new SQLGetObjectData(sdb, db_name, cct);
  DeleteObjectData = new SQLDeleteObjectData(sdb, db_name, cct);

  return 0;
}

int SQLObjectOp::FreeObjectOps(const DoutPrefixProvider *dpp)
{
  delete PutObject;
  delete DeleteObject;
  delete GetObject;
  delete UpdateObject;
  delete PutObjectData;
  delete UpdateObjectData;
  delete GetObjectData;
  delete DeleteObjectData;

  return 0;
}

int SQLInsertUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertUser - no db" << dendl;
    goto out;
  }

  p_params.user_table = params->user_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertUser");
out:
  return ret;
}

int SQLInsertUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.tenant.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.tenant.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.ns.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.ns.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.display_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.display_name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_email.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_email.c_str(), sdb);

  if (!params->op.user.uinfo.access_keys.empty()) {
    string access_key;
    string key;
    map<string, RGWAccessKey>::const_iterator it =
      params->op.user.uinfo.access_keys.begin();
    const RGWAccessKey& k = it->second;
    access_key = k.id;
    key = k.key;

    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.access_keys_id.c_str(), sdb);
    SQL_BIND_TEXT(dpp, stmt, index, access_key.c_str(), sdb);

    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.access_keys_secret.c_str(), sdb);
    SQL_BIND_TEXT(dpp, stmt, index, key.c_str(), sdb);

    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.access_keys.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.access_keys, sdb);
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.swift_keys.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.swift_keys, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.subusers.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.subusers, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.suspended.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.suspended, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.max_buckets.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.max_buckets, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.op_mask.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.op_mask, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_caps.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.caps, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.admin.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.admin, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.system.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.system, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.placement_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.default_placement.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.placement_storage_class.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.default_placement.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.placement_tags.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.placement_tags, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.bucket_quota.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.bucket_quota, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.temp_url_keys.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.temp_url_keys, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_quota.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.user_quota, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.type.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.type, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.mfa_ids.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.mfa_ids, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.assumed_role_arn.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.assumed_role_arn.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_attrs.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.user_attrs, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_ver.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.user_version.ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_ver_tag.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.user_version.tag.c_str(), sdb);

out:
  return rc;
}

int SQLInsertUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveUser - no db" << dendl;
    goto out;
  }

  p_params.user_table = params->user_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveUser");
out:
  return ret;
}

int SQLRemoveUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetUser - no db" << dendl;
    goto out;
  }

  p_params.user_table = params->user_table;
  p_params.op.query_str = params->op.query_str;

  if (params->op.query_str == "email") { 
    SQL_PREPARE(dpp, p_params, sdb, email_stmt, ret, "PrepareGetUser");
  } else if (params->op.query_str == "access_key") { 
    SQL_PREPARE(dpp, p_params, sdb, ak_stmt, ret, "PrepareGetUser");
  } else if (params->op.query_str == "user_id") { 
    SQL_PREPARE(dpp, p_params, sdb, userid_stmt, ret, "PrepareGetUser");
  } else { // by default by userid
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetUser");
  }
out:
  return ret;
}

int SQLGetUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.query_str == "email") { 
    SQL_BIND_INDEX(dpp, email_stmt, index, p_params.op.user.user_email.c_str(), sdb);
    SQL_BIND_TEXT(dpp, email_stmt, index, params->op.user.uinfo.user_email.c_str(), sdb);
  } else if (params->op.query_str == "access_key") { 
    if (!params->op.user.uinfo.access_keys.empty()) {
      string access_key;
      map<string, RGWAccessKey>::const_iterator it =
        params->op.user.uinfo.access_keys.begin();
      const RGWAccessKey& k = it->second;
      access_key = k.id;

      SQL_BIND_INDEX(dpp, ak_stmt, index, p_params.op.user.access_keys_id.c_str(), sdb);
      SQL_BIND_TEXT(dpp, ak_stmt, index, access_key.c_str(), sdb);
    }
  } else if (params->op.query_str == "user_id") { 
    SQL_BIND_INDEX(dpp, userid_stmt, index, p_params.op.user.tenant.c_str(), sdb);
    SQL_BIND_TEXT(dpp, userid_stmt, index, params->op.user.uinfo.user_id.tenant.c_str(), sdb);

    SQL_BIND_INDEX(dpp, userid_stmt, index, p_params.op.user.user_id.c_str(), sdb);
    SQL_BIND_TEXT(dpp, userid_stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

    SQL_BIND_INDEX(dpp, userid_stmt, index, p_params.op.user.ns.c_str(), sdb);
    SQL_BIND_TEXT(dpp, userid_stmt, index, params->op.user.uinfo.user_id.ns.c_str(), sdb);
  } else { // by default by userid
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id.c_str(), sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);
  }

out:
  return rc;
}

int SQLGetUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  if (params->op.query_str == "email") { 
    SQL_EXECUTE(dpp, params, email_stmt, list_user);
  } else if (params->op.query_str == "access_key") { 
    SQL_EXECUTE(dpp, params, ak_stmt, list_user);
  } else if (params->op.query_str == "user_id") { 
    SQL_EXECUTE(dpp, params, userid_stmt, list_user);
  } else { // by default by userid
    SQL_EXECUTE(dpp, params, stmt, list_user);
  }

out:
  return ret;
}

int SQLInsertBucket::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertBucket - no db" << dendl;
    goto out;
  }

  p_params.bucket_table = params->bucket_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertBucket");

out:
  return ret;
}

int SQLInsertBucket::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.tenant.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.tenant.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.marker.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_id.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.bucket_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.size.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.ent.size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.size_rounded.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.ent.size_rounded, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.creation_time.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.creation_time, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.count.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.ent.count, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.placement_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.placement_rule.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.placement_storage_class.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.placement_rule.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.flags.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.flags, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.zonegroup.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.zonegroup.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.has_instance_obj.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.has_instance_obj, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.quota.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.quota, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.requester_pays.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.requester_pays, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.has_website.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.has_website, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.website_conf.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.website_conf, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.swift_versioning.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.swift_versioning, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.swift_ver_location.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.swift_ver_location.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.mdsearch_config.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.mdsearch_config, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.new_bucket_instance_id.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.new_bucket_instance_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.obj_lock.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.obj_lock, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.sync_policy_info_groups.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.sync_policy, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_attrs.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.bucket_attrs, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_ver.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.bucket_version.ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_ver_tag.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.bucket_version.tag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.mtime.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.mtime, sdb);

out:
  return rc;
}

int SQLInsertBucket::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  class SQLObjectOp *ObPtr = NULL;
  string bucket_name = params->op.bucket.info.bucket.name;

  ObPtr = new SQLObjectOp(sdb, ctx());

  objectmapInsert(dpp, bucket_name, ObPtr);

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLUpdateBucket::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLUpdateBucket - no db" << dendl;
    goto out;
  }

  p_params.op.query_str = params->op.query_str;
  p_params.bucket_table = params->bucket_table;

  if (params->op.query_str == "attrs") { 
    SQL_PREPARE(dpp, p_params, sdb, attrs_stmt, ret, "PrepareUpdateBucket");
  } else if (params->op.query_str == "owner") { 
    SQL_PREPARE(dpp, p_params, sdb, owner_stmt, ret, "PrepareUpdateBucket");
  } else if (params->op.query_str == "info") { 
    SQL_PREPARE(dpp, p_params, sdb, info_stmt, ret, "PrepareUpdateBucket");
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "" << dendl;
    goto out;
  }

out:
  return ret;
}

int SQLUpdateBucket::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  sqlite3_stmt** stmt = NULL; // Prepared statement

  /* All below fields for attrs */
  if (params->op.query_str == "attrs") { 
    stmt = &attrs_stmt;
  } else if (params->op.query_str == "owner") { 
    stmt = &owner_stmt;
  } else if (params->op.query_str == "info") { 
    stmt = &info_stmt;
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "" << dendl;
    goto out;
  }

  if (params->op.query_str == "attrs") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_attrs.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.bucket_attrs, sdb);
  } else if (params->op.query_str == "owner") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.creation_time.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.creation_time, sdb);
  } else if (params->op.query_str == "info") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.tenant.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.tenant.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.marker.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.marker.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_id.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.bucket_id.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.creation_time.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.creation_time, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.count.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.ent.count, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.placement_name.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.placement_rule.name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.placement_storage_class.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.placement_rule.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.flags.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.flags, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.zonegroup.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.zonegroup.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.has_instance_obj.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.has_instance_obj, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.quota.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.quota, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.requester_pays.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.requester_pays, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.has_website.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.has_website, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.website_conf.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.website_conf, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.swift_versioning.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.swift_versioning, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.swift_ver_location.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.swift_ver_location.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.mdsearch_config.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.mdsearch_config, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.new_bucket_instance_id.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.new_bucket_instance_id.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.obj_lock.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.obj_lock, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.sync_policy_info_groups.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.sync_policy, sdb);
  }

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_ver.c_str(), sdb);
  SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.bucket_version.ver, sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.mtime.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.mtime, sdb);

out:
  return rc;
}

int SQLUpdateBucket::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  sqlite3_stmt** stmt = NULL; // Prepared statement

  if (params->op.query_str == "attrs") { 
    stmt = &attrs_stmt;
  } else if (params->op.query_str == "owner") { 
    stmt = &owner_stmt;
  } else if (params->op.query_str == "info") { 
    stmt = &info_stmt;
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "" << dendl;
    goto out;
  }

  SQL_EXECUTE(dpp, params, *stmt, NULL);
out:
  return ret;
}

int SQLRemoveBucket::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveBucket - no db" << dendl;
    goto out;
  }

  p_params.bucket_table = params->bucket_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveBucket");

out:
  return ret;
}

int SQLRemoveBucket::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveBucket::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  objectmapDelete(dpp, params->op.bucket.info.bucket.name);

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetBucket::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetBucket - no db" << dendl;
    goto out;
  }

  p_params.bucket_table = params->bucket_table;
  p_params.user_table = params->user_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetBucket");

out:
  return ret;
}

int SQLGetBucket::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLGetBucket::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  class SQLObjectOp *ObPtr = NULL;

  params->op.name = "GetBucket";

  ObPtr = new SQLObjectOp(sdb, ctx());

  /* For the case when the  server restarts, need to reinsert objectmap*/
  objectmapInsert(dpp, params->op.bucket.info.bucket.name, ObPtr);
  SQL_EXECUTE(dpp, params, stmt, list_bucket);
out:
  return ret;
}

int SQLListUserBuckets::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListUserBuckets - no db" << dendl;
    goto out;
  }

  p_params.bucket_table = params->bucket_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListUserBuckets");

out:
  return ret;
}

int SQLListUserBuckets::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.min_marker.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.min_marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);

out:
  return rc;
}

int SQLListUserBuckets::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_bucket);
out:
  return ret;
}

int SQLPutObject::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name = params->op.bucket.info.bucket.name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLPutObject - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    p_params.object_table = getObjectTable(bucket_name);
  }
  params->object_table = p_params.object_table;
  (void)createObjectTable(dpp, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PreparePutObject");

out:
  return ret;
}

int SQLPutObject::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_ns.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.ns.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.acls.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.acls, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.index_ver.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.index_ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tag.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.tag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.flags.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.flags, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.versioned_epoch.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.versioned_epoch, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_category.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, (uint8_t)(params->op.obj.category), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.etag.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.etag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.owner.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.owner.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.owner_display_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.owner_display_name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.storage_class.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.appendable.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.appendable, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.content_type.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.content_type.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.index_hash_source.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.index_hash_source.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_size.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.accounted_size.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.accounted_size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.mtime.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.mtime, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.epoch.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.epoch, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_tag.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.obj_tag, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tail_tag.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.tail_tag, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.write_tag.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.write_tag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.fake_tag.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.fake_tag, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.shadow_obj.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.shadow_obj.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.has_data.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.has_data, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.is_olh.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.is_olh, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.olh_tag.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.olh_tag, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.pg_ver.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.pg_ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.zone_short_id.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.zone_short_id, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_version.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.objv_tracker.read_version.ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_version_tag.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.objv_tracker.read_version.tag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_attrs.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.attrset, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.head_size.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.head_size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.max_head_size.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.max_head_size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.prefix.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.prefix.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tail_instance.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.tail_instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.head_placement_rule_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.head_placement_rule.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.head_placement_storage_class.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.head_placement_rule.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tail_placement_rule_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.tail_placement.placement_rule.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tail_placement_storage_class.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.tail_placement.placement_rule.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.manifest_part_objs.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.objs, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.manifest_part_rules.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.rules, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.omap.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.omap, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.is_multipart.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.is_multipart, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.mp_parts.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.mp_parts, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.head_data.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.head_data, sdb);


out:
  return rc;
}

int SQLPutObject::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLDeleteObject::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name = params->op.bucket.info.bucket.name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLDeleteObject - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    p_params.object_table = getObjectTable(bucket_name);
  }
  params->object_table = p_params.object_table;
  (void)createObjectTable(dpp, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareDeleteObject");

out:
  return ret;
}

int SQLDeleteObject::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);
out:
  return rc;
}

int SQLDeleteObject::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetObject::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name = params->op.bucket.info.bucket.name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetObject - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    p_params.object_table = getObjectTable(bucket_name);
  }
  params->object_table = p_params.object_table;
  (void)createObjectTable(dpp, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetObject");

out:
  return ret;
}

int SQLGetObject::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

out:
  return rc;
}

int SQLGetObject::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_object);
out:
  return ret;
}

int SQLUpdateObject::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObject - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    bucket_name = params->op.bucket.info.bucket.name;
    p_params.object_table = getObjectTable(bucket_name);
  }

  p_params.op.query_str = params->op.query_str;

  if (params->op.query_str == "omap") {
    SQL_PREPARE(dpp, p_params, sdb, omap_stmt, ret, "PrepareUpdateObject");
  } else if (params->op.query_str == "attrs") {
    SQL_PREPARE(dpp, p_params, sdb, attrs_stmt, ret, "PrepareUpdateObject");
  } else if (params->op.query_str == "meta") {
    SQL_PREPARE(dpp, p_params, sdb, meta_stmt, ret, "PrepareUpdateObject");
  } else if (params->op.query_str == "mp") {
    SQL_PREPARE(dpp, p_params, sdb, mp_stmt, ret, "PrepareUpdateObject");
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObject invalid query_str:" <<
      params->op.query_str << dendl;
    goto out;
  }

out:
  return ret;
}

int SQLUpdateObject::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  sqlite3_stmt** stmt = NULL; // Prepared statement

  /* All below fields for attrs */
  if (params->op.query_str == "omap") { 
    stmt = &omap_stmt;
  } else if (params->op.query_str == "attrs") { 
    stmt = &attrs_stmt;
  } else if (params->op.query_str == "meta") { 
    stmt = &meta_stmt;
  } else if (params->op.query_str == "mp") { 
    stmt = &mp_stmt;
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObject invalid query_str:" <<
      params->op.query_str << dendl;
    goto out;
  }

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_instance.c_str(), sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.mtime.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.mtime, sdb);

  if (params->op.query_str == "omap") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.omap.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.omap, sdb);
  }
  if (params->op.query_str == "attrs") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_attrs.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.attrset, sdb);
  }
  if (params->op.query_str == "mp") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.mp_parts.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.mp_parts, sdb);
  }
  if (params->op.query_str == "meta") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_ns.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.obj.key.ns.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.acls.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.acls, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.index_ver.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.index_ver, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tag.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.tag.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.flags.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.flags, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.versioned_epoch.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.versioned_epoch, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_category.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, (uint8_t)(params->op.obj.category), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.etag.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.etag.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.owner.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.owner.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.owner_display_name.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.owner_display_name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.storage_class.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.appendable.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.appendable, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.content_type.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.content_type.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.index_hash_source.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.obj.index_hash_source.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_size.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.size, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.accounted_size.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.accounted_size, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.epoch.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.epoch, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_tag.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.obj_tag, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tail_tag.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.tail_tag, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.write_tag.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.write_tag.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.fake_tag.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.fake_tag, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.shadow_obj.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.shadow_obj.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.has_data.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.has_data, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.is_olh.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.is_olh, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.olh_tag.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.olh_tag, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.pg_ver.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.pg_ver, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.zone_short_id.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.zone_short_id, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_version.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.objv_tracker.read_version.ver, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_version_tag.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.objv_tracker.read_version.tag.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_attrs.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.attrset, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.head_size.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.head_size, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.max_head_size.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.max_head_size, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.prefix.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.prefix.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tail_instance.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.tail_instance.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.head_placement_rule_name.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.head_placement_rule.name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.head_placement_storage_class.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.head_placement_rule.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tail_placement_rule_name.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.tail_placement.placement_rule.name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tail_placement_storage_class.c_str(), sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.tail_placement.placement_rule.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.manifest_part_objs.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.objs, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.manifest_part_rules.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.rules, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.omap.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.omap, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.is_multipart.c_str(), sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.is_multipart, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.mp_parts.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.mp_parts, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.head_data.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.head_data, sdb);
  }

out:
  return rc;
}

int SQLUpdateObject::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  sqlite3_stmt** stmt = NULL; // Prepared statement

  if (params->op.query_str == "omap") { 
    stmt = &omap_stmt;
  } else if (params->op.query_str == "attrs") { 
    stmt = &attrs_stmt;
  } else if (params->op.query_str == "meta") { 
    stmt = &meta_stmt;
  } else if (params->op.query_str == "mp") { 
    stmt = &mp_stmt;
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObject invalid query_str:" <<
      params->op.query_str << dendl;
    goto out;
  }

  SQL_EXECUTE(dpp, params, *stmt, NULL);
out:
  return ret;
}

int SQLListBucketObjects::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListBucketObjects - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    bucket_name = params->op.bucket.info.bucket.name;
    p_params.object_table = getObjectTable(bucket_name);
  }

  /* XXX: instead of creating..maybe keep object count in bucket info
   * and return if there is no object table created.
   */
  params->object_table = p_params.object_table;
  (void)createObjectTable(dpp, params);

  p_params.op.query_str = params->op.query_str;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListBucketObjects");

out:
  return ret;
}

int SQLListBucketObjects::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.min_marker.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.min_marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);

out:
  return rc;
}

int SQLListBucketObjects::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_object);
out:
  return ret;
}

int SQLPutObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name = params->op.bucket.info.bucket.name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLPutObjectData - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    p_params.object_table = getObjectTable(bucket_name);
  }
  if (p_params.objectdata_table.empty()) {
    p_params.objectdata_table = getObjectDataTable(bucket_name);
  }
  params->bucket_table = p_params.bucket_table;
  params->object_table = p_params.object_table;
  params->objectdata_table = p_params.objectdata_table;
  (void)createObjectDataTable(dpp, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PreparePutObjectData");

out:
  return ret;
}

int SQLPutObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_ns.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.ns.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.part_num.c_str(), sdb);

  SQL_BIND_INT(dpp, stmt, index, params->op.obj_data.part_num, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.offset.c_str(), sdb);

  SQL_BIND_INT(dpp, stmt, index, params->op.obj_data.offset, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.data.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj_data.data, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.size.c_str(), sdb);

  SQL_BIND_INT(dpp, stmt, index, params->op.obj_data.size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.multipart_part_str.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj_data.multipart_part_str.c_str(), sdb);

out:
  return rc;
}

int SQLPutObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLUpdateObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name = params->op.bucket.info.bucket.name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObjectData - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    p_params.object_table = getObjectTable(bucket_name);
  }
  if (p_params.objectdata_table.empty()) {
    p_params.objectdata_table = getObjectDataTable(bucket_name);
  }
  params->bucket_table = p_params.bucket_table;
  params->object_table = p_params.object_table;
  params->objectdata_table = p_params.objectdata_table;
  (void)createObjectDataTable(dpp, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareUpdateObjectData");

out:
  return ret;
}

int SQLUpdateObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_ns.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.ns.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.new_obj_name.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.new_obj_key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.new_obj_instance.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.new_obj_key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.new_obj_ns.c_str(), sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.new_obj_key.ns.c_str(), sdb);

out:
  return rc;
}

int SQLUpdateObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name = params->op.bucket.info.bucket.name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetObjectData - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    p_params.object_table = getObjectTable(bucket_name);
  }
  if (p_params.objectdata_table.empty()) {
    p_params.objectdata_table = getObjectDataTable(bucket_name);
  }
  params->object_table = p_params.object_table;
  params->objectdata_table = p_params.objectdata_table;
  (void)createObjectDataTable(dpp, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetObjectData");

out:
  return ret;
}

int SQLGetObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

out:
  return rc;
}

int SQLGetObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, get_objectdata);
out:
  return ret;
}

int SQLDeleteObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name = params->op.bucket.info.bucket.name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLDeleteObjectData - no db" << dendl;
    goto out;
  }

  if (p_params.object_table.empty()) {
    p_params.object_table = getObjectTable(bucket_name);
  }
  if (p_params.objectdata_table.empty()) {
    p_params.objectdata_table = getObjectDataTable(bucket_name);
  }
  params->object_table = p_params.object_table;
  params->objectdata_table = p_params.objectdata_table;
  (void)createObjectDataTable(dpp, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareDeleteObjectData");

out:
  return ret;
}

int SQLDeleteObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);
out:
  return rc;
}

int SQLDeleteObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLInsertLCEntry::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertLCEntry - no db" << dendl;
    goto out;
  }

  p_params.lc_entry_table = params->lc_entry_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertLCEntry");

out:
  return ret;
}

int SQLInsertLCEntry::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.index.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.entry.bucket.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.status.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.lc_entry.entry.status, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.start_time.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.lc_entry.entry.start_time, sdb);

out:
  return rc;
}

int SQLInsertLCEntry::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveLCEntry::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveLCEntry - no db" << dendl;
    goto out;
  }

  p_params.lc_entry_table = params->lc_entry_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveLCEntry");

out:
  return ret;
}

int SQLRemoveLCEntry::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.index.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.entry.bucket.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveLCEntry::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetLCEntry::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  sqlite3_stmt** pstmt = NULL; // Prepared statement
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetLCEntry - no db" << dendl;
    goto out;
  }

  p_params.lc_entry_table = params->lc_entry_table;
  p_params.op.query_str = params->op.query_str;

  if (params->op.query_str == "get_next_entry") {
    pstmt = &next_stmt;
  } else {
    pstmt = &stmt;
  }
  SQL_PREPARE(dpp, p_params, sdb, *pstmt, ret, "PrepareGetLCEntry");

out:
  return ret;
}

int SQLGetLCEntry::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  sqlite3_stmt** pstmt = NULL; // Prepared statement

  if (params->op.query_str == "get_next_entry") {
    pstmt = &next_stmt;
  } else {
    pstmt = &stmt;
  }
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.index.c_str(), sdb);
  SQL_BIND_TEXT(dpp, *pstmt, index, params->op.lc_entry.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(dpp, *pstmt, index, params->op.lc_entry.entry.bucket.c_str(), sdb);

out:
  return rc;
}

int SQLGetLCEntry::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  sqlite3_stmt** pstmt = NULL; // Prepared statement

  if (params->op.query_str == "get_next_entry") {
    pstmt = &next_stmt;
  } else {
    pstmt = &stmt;
  }

  SQL_EXECUTE(dpp, params, *pstmt, list_lc_entry);
out:
  return ret;
}

int SQLListLCEntries::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListLCEntries - no db" << dendl;
    goto out;
  }

  p_params.lc_entry_table = params->lc_entry_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListLCEntries");

out:
  return ret;
}

int SQLListLCEntries::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.index.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.min_marker.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.min_marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count.c_str(), sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);

out:
  return rc;
}

int SQLListLCEntries::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_lc_entry);
out:
  return ret;
}

int SQLInsertLCHead::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertLCHead - no db" << dendl;
    goto out;
  }

  p_params.lc_head_table = params->lc_head_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertLCHead");

out:
  return ret;
}

int SQLInsertLCHead::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.index.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_head.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.marker.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_head.head.marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.start_date.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.lc_head.head.start_date, sdb);

out:
  return rc;
}

int SQLInsertLCHead::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveLCHead::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveLCHead - no db" << dendl;
    goto out;
  }

  p_params.lc_head_table = params->lc_head_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveLCHead");

out:
  return ret;
}

int SQLRemoveLCHead::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.index.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_head.index.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveLCHead::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetLCHead::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetLCHead - no db" << dendl;
    goto out;
  }

  p_params.lc_head_table = params->lc_head_table;

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetLCHead");

out:
  return ret;
}

int SQLGetLCHead::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.index.c_str(), sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_head.index.c_str(), sdb);

out:
  return rc;
}

int SQLGetLCHead::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  // clear the params before fetching the entry
  params->op.lc_head.head = {};
  SQL_EXECUTE(dpp, params, stmt, list_lc_head);
out:
  return ret;
}
