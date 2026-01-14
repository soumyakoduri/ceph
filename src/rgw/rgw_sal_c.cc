// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "include/rgw/rgw_sal_c.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_sal_rados.h"
#include "rgw_common.h"
#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/dout.h"

#include <memory>
#include <string>
#include <vector>
#include <cstring>
#include <boost/asio/io_context.hpp>

#define dout_subsys ceph_subsys_rgw

using namespace rgw::sal;

/* Internal wrapper structures */
struct rgw_sal_driver_wrapper {
  std::unique_ptr<Driver> driver;
  CephContext* cct;
  std::unique_ptr<boost::asio::io_context> io_context;
  rgw::SiteConfig site_config;
  DoutPrefixProvider* dpp;
  
  rgw_sal_driver_wrapper() : driver(nullptr), cct(nullptr), dpp(nullptr) {}
  ~rgw_sal_driver_wrapper() {
    if (driver) {
      driver->finalize();
    }
    if (cct) {
      cct->put();
    }
    if (dpp) {
      delete dpp;
    }
  }
};

struct rgw_sal_user_wrapper {
  std::unique_ptr<User> user;
  rgw_sal_driver_t driver;
  
  rgw_sal_user_wrapper() : user(nullptr), driver(nullptr) {}
};

struct rgw_sal_bucket_wrapper {
  std::unique_ptr<Bucket> bucket;
  rgw_sal_driver_t driver;
  
  rgw_sal_bucket_wrapper() : bucket(nullptr), driver(nullptr) {}
};

struct rgw_sal_object_wrapper {
  std::unique_ptr<Object> object;
  rgw_sal_driver_t driver;
  
  rgw_sal_object_wrapper() : object(nullptr), driver(nullptr) {}
};

struct rgw_sal_writer_wrapper {
  std::unique_ptr<Writer> writer;
  rgw_sal_driver_t driver;
  
  rgw_sal_writer_wrapper() : writer(nullptr), driver(nullptr) {}
};

/* Helper function to get driver from wrapper */
static Driver* get_driver(rgw_sal_driver_t driver) {
  if (!driver) return nullptr;
  auto wrapper = static_cast<rgw_sal_driver_wrapper*>(driver);
  return wrapper->driver.get();
}

/* Helper function to get user from wrapper */
static User* get_user(rgw_sal_user_t user) {
  if (!user) return nullptr;
  auto wrapper = static_cast<rgw_sal_user_wrapper*>(user);
  return wrapper->user.get();
}

/* Helper function to get bucket from wrapper */
static Bucket* get_bucket(rgw_sal_bucket_t bucket) {
  if (!bucket) return nullptr;
  auto wrapper = static_cast<rgw_sal_bucket_wrapper*>(bucket);
  return wrapper->bucket.get();
}

/* Helper function to get object from wrapper */
static Object* get_object(rgw_sal_object_t object) {
  if (!object) return nullptr;
  auto wrapper = static_cast<rgw_sal_object_wrapper*>(object);
  return wrapper->object.get();
}

/* Helper function to get writer from wrapper */
static Writer* get_writer(rgw_sal_writer_t writer) {
  if (!writer) return nullptr;
  auto wrapper = static_cast<rgw_sal_writer_wrapper*>(writer);
  return wrapper->writer.get();
}

/* ============================================
 * Driver Operations
 * ============================================ */

extern "C" int rgw_sal_driver_create(const char* driver_name, const char* cct_path, rgw_sal_driver_t* driver)
{
  if (!driver_name || !driver) {
    return -EINVAL;
  }

  try {
    auto wrapper = new rgw_sal_driver_wrapper();
    
    // Initialize Ceph context
    CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
    CephContext* cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
    if (!cct) {
      delete wrapper;
      return -ENOMEM;
    }
    
    if (cct_path) {
      cct->_conf.parse_file(cct_path);
    }
    cct->_conf.parse_env(cct->get_module_type());
    cct->_conf.apply_changes(nullptr);
    
    wrapper->cct = cct;
    wrapper->io_context = std::make_unique<boost::asio::io_context>();
    wrapper->dpp = new DoutPrefix(cct, dout_subsys, "rgw_sal_c: ");
    
    // Create driver based on name
    DriverManager::Config cfg;
    cfg.store_name = driver_name;
    cfg.filter_name = "base";
    
    Driver* d = DriverManager::get_raw_storage(wrapper->dpp, cct, cfg, 
                                               *wrapper->io_context, 
                                               wrapper->site_config);
    
    if (!d) {
      delete wrapper;
      return -ENOENT;
    }
    
    wrapper->driver.reset(d);
    
    *driver = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_driver_initialize(rgw_sal_driver_t driver)
{
  if (!driver) {
    return -EINVAL;
  }
  
  auto wrapper = static_cast<rgw_sal_driver_wrapper*>(driver);
  if (!wrapper->driver) {
    return -EINVAL;
  }
  
  // Driver is already initialized in get_raw_storage
  return 0;
}

extern "C" const char* rgw_sal_driver_get_name(rgw_sal_driver_t driver)
{
  Driver* d = get_driver(driver);
  if (!d) return nullptr;
  
  static thread_local std::string name;
  name = d->get_name();
  return name.c_str();
}

extern "C" int rgw_sal_driver_get_cluster_stat(rgw_sal_driver_t driver, struct rgw_sal_cluster_stat* stats)
{
  if (!driver || !stats) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  if (!d) return -EINVAL;
  
  RGWClusterStat cstat;
  int ret = d->cluster_stat(cstat);
  if (ret < 0) {
    return ret;
  }
  
  stats->kb = cstat.kb;
  stats->kb_used = cstat.kb_used;
  stats->kb_avail = cstat.kb_avail;
  stats->num_objects = cstat.num_objects;
  
  return 0;
}

extern "C" int rgw_sal_driver_get_cluster_id(rgw_sal_driver_t driver, char* cluster_id, size_t cluster_id_len)
{
  if (!driver || !cluster_id || cluster_id_len == 0) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  if (!d) return -EINVAL;
  
  auto wrapper = static_cast<rgw_sal_driver_wrapper*>(driver);
  std::string id = d->get_cluster_id(wrapper->dpp, null_yield);
  
  if (id.length() >= cluster_id_len) {
    return -ERANGE;
  }
  
  strncpy(cluster_id, id.c_str(), cluster_id_len - 1);
  cluster_id[cluster_id_len - 1] = '\0';
  
  return 0;
}

extern "C" void rgw_sal_driver_destroy(rgw_sal_driver_t driver)
{
  if (driver) {
    delete static_cast<rgw_sal_driver_wrapper*>(driver);
  }
}

/* ============================================
 * User Operations
 * ============================================ */

extern "C" int rgw_sal_get_user(rgw_sal_driver_t driver, const char* user_id, const char* tenant, rgw_sal_user_t* user)
{
  if (!driver || !user_id || !user) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  if (!d) return -EINVAL;
  
  try {
    rgw_user u;
    u.id = user_id;
    if (tenant) {
      u.tenant = tenant;
    }
    
    auto wrapper = new rgw_sal_user_wrapper();
    wrapper->driver = driver;
    wrapper->user = d->get_user(u);
    
    if (!wrapper->user) {
      delete wrapper;
      return -ENOENT;
    }
    
    *user = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_get_user_by_access_key(rgw_sal_driver_t driver, const char* access_key, rgw_sal_user_t* user)
{
  if (!driver || !access_key || !user) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  if (!d) return -EINVAL;
  
  try {
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(driver);
    std::unique_ptr<User> u;
    int ret = d->get_user_by_access_key(wrapper_driver->dpp, access_key, null_yield, &u);
    if (ret < 0) {
      return ret;
    }
    
    auto wrapper = new rgw_sal_user_wrapper();
    wrapper->driver = driver;
    wrapper->user = std::move(u);
    
    *user = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_get_user_by_email(rgw_sal_driver_t driver, const char* email, rgw_sal_user_t* user)
{
  if (!driver || !email || !user) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  if (!d) return -EINVAL;
  
  try {
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(driver);
    std::unique_ptr<User> u;
    int ret = d->get_user_by_email(wrapper_driver->dpp, email, null_yield, &u);
    if (ret < 0) {
      return ret;
    }
    
    auto wrapper = new rgw_sal_user_wrapper();
    wrapper->driver = driver;
    wrapper->user = std::move(u);
    
    *user = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_user_load(rgw_sal_user_t user, struct rgw_sal_user_info* info)
{
  if (!user || !info) {
    return -EINVAL;
  }
  
  User* u = get_user(user);
  if (!u) return -EINVAL;
  
  try {
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(static_cast<rgw_sal_user_wrapper*>(user)->driver);
    int ret = u->load_user(wrapper_driver->dpp, null_yield);
    if (ret < 0) {
      return ret;
    }
    
    // Fill in info structure
    const rgw_user& uid = u->get_id();
    info->user_id = strdup(uid.id.c_str());
    info->tenant = strdup(uid.tenant.c_str());
    info->display_name = strdup(u->get_display_name().c_str());
    info->max_buckets = u->get_max_buckets();
    
    // Try to get email from attrs if available
    Attrs& attrs = u->get_attrs();
    auto it = attrs.find(RGW_ATTR_USER_EMAIL);
    if (it != attrs.end()) {
      std::string email;
      try {
        decode(email, it->second);
        info->email = strdup(email.c_str());
      } catch (...) {
        info->email = nullptr;
      }
    } else {
      info->email = nullptr;
    }
    
    info->suspended = 0; // TODO: get from user info
    
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_user_store(rgw_sal_user_t user, const struct rgw_sal_user_info* info, int exclusive)
{
  if (!user || !info) {
    return -EINVAL;
  }
  
  User* u = get_user(user);
  if (!u) return -EINVAL;
  
  try {
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(static_cast<rgw_sal_user_wrapper*>(user)->driver);
    
    // Set user info
    if (info->display_name) {
      u->get_display_name() = info->display_name;
    }
    if (info->max_buckets >= 0) {
      u->set_max_buckets(info->max_buckets);
    }
    
    // Store email in attrs if provided
    if (info->email) {
      Attrs& attrs = u->get_attrs();
      bufferlist bl;
      encode(std::string(info->email), bl);
      attrs[RGW_ATTR_USER_EMAIL] = bl;
    }
    
    int ret = u->store_user(wrapper_driver->dpp, null_yield, exclusive != 0);
    return ret;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_user_remove(rgw_sal_user_t user)
{
  if (!user) {
    return -EINVAL;
  }
  
  User* u = get_user(user);
  if (!u) return -EINVAL;
  
  try {
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(static_cast<rgw_sal_user_wrapper*>(user)->driver);
    int ret = u->remove_user(wrapper_driver->dpp, null_yield);
    return ret;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_user_get_id(rgw_sal_user_t user, char* user_id, size_t user_id_len)
{
  if (!user || !user_id || user_id_len == 0) {
    return -EINVAL;
  }
  
  User* u = get_user(user);
  if (!u) return -EINVAL;
  
  const rgw_user& uid = u->get_id();
  if (uid.id.length() >= user_id_len) {
    return -ERANGE;
  }
  
  strncpy(user_id, uid.id.c_str(), user_id_len - 1);
  user_id[user_id_len - 1] = '\0';
  
  return 0;
}

extern "C" int rgw_sal_user_get_display_name(rgw_sal_user_t user, char* display_name, size_t display_name_len)
{
  if (!user || !display_name || display_name_len == 0) {
    return -EINVAL;
  }
  
  User* u = get_user(user);
  if (!u) return -EINVAL;
  
  std::string name = u->get_display_name();
  if (name.length() >= display_name_len) {
    return -ERANGE;
  }
  
  strncpy(display_name, name.c_str(), display_name_len - 1);
  display_name[display_name_len - 1] = '\0';
  
  return 0;
}

extern "C" void rgw_sal_user_destroy(rgw_sal_user_t user)
{
  if (user) {
    delete static_cast<rgw_sal_user_wrapper*>(user);
  }
}

/* ============================================
 * Bucket Operations
 * ============================================ */

extern "C" int rgw_sal_get_bucket(rgw_sal_driver_t driver, const char* bucket_name, const char* tenant, rgw_sal_bucket_t* bucket)
{
  if (!driver || !bucket_name || !bucket) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  if (!d) return -EINVAL;
  
  try {
    rgw_bucket b;
    b.name = bucket_name;
    if (tenant) {
      b.tenant = tenant;
    }
    
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(driver);
    std::unique_ptr<Bucket> bu;
    int ret = d->load_bucket(wrapper_driver->dpp, b, &bu, null_yield);
    if (ret < 0) {
      return ret;
    }
    
    auto wrapper = new rgw_sal_bucket_wrapper();
    wrapper->driver = driver;
    wrapper->bucket = std::move(bu);
    
    *bucket = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_create_bucket(rgw_sal_driver_t driver, const char* bucket_name, const char* tenant, const char* owner_id, rgw_sal_bucket_t* bucket)
{
  if (!driver || !bucket_name || !owner_id || !bucket) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  if (!d) return -EINVAL;
  
  try {
    rgw_bucket b;
    b.name = bucket_name;
    if (tenant) {
      b.tenant = tenant;
    }
    
    rgw_user owner;
    owner.id = owner_id;
    
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(driver);
    std::unique_ptr<User> u = d->get_user(owner);
    
    rgw_placement_rule rule;
    Attrs attrs;
    RGWBucketInfo binfo;
    
    int ret = u->create_bucket(wrapper_driver->dpp, b, tenant ? tenant : "", rule, attrs, nullptr, nullptr, null_yield, &binfo);
    if (ret < 0) {
      return ret;
    }
    
    std::unique_ptr<Bucket> bu = d->get_bucket(binfo);
    
    auto wrapper = new rgw_sal_bucket_wrapper();
    wrapper->driver = driver;
    wrapper->bucket = std::move(bu);
    
    *bucket = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_bucket_load(rgw_sal_bucket_t bucket, struct rgw_sal_bucket_info* info)
{
  if (!bucket || !info) {
    return -EINVAL;
  }
  
  Bucket* b = get_bucket(bucket);
  if (!b) return -EINVAL;
  
  try {
    const RGWBucketInfo& binfo = b->get_info();
    
    info->name = strdup(binfo.bucket.name.c_str());
    info->tenant = strdup(binfo.bucket.tenant.c_str());
    info->marker = strdup(binfo.bucket.marker.c_str());
    info->bucket_id = strdup(binfo.bucket.bucket_id.c_str());
    info->size = 0; // TODO: get from bucket stats
    info->size_rounded = 0;
    info->creation_time = binfo.creation_time.sec();
    info->owner_id = strdup(to_string(binfo.owner).c_str());
    
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_list_buckets(rgw_sal_driver_t driver, const char* owner_id, const char* tenant, const char* marker, uint64_t max, struct rgw_sal_bucket_list* list)
{
  if (!driver || !owner_id || !list) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  if (!d) return -EINVAL;
  
  try {
    rgw_owner owner;
    owner.id = owner_id;
    
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(driver);
    BucketList blist;
    
    int ret = d->list_buckets(wrapper_driver->dpp, owner, tenant ? tenant : "", marker ? marker : "", "", max, true, blist, null_yield);
    if (ret < 0) {
      return ret;
    }
    
    list->count = blist.buckets.size();
    list->buckets = (struct rgw_sal_bucket_info*)calloc(list->count, sizeof(struct rgw_sal_bucket_info));
    if (!list->buckets) {
      return -ENOMEM;
    }
    
    for (size_t i = 0; i < list->count; i++) {
      const RGWBucketEnt& ent = blist.buckets[i];
      list->buckets[i].name = strdup(ent.bucket.name.c_str());
      list->buckets[i].tenant = strdup(ent.bucket.tenant.c_str());
      list->buckets[i].marker = strdup(ent.bucket.marker.c_str());
      list->buckets[i].bucket_id = strdup(ent.bucket.bucket_id.c_str());
      list->buckets[i].size = ent.size;
      list->buckets[i].size_rounded = ent.size_rounded;
      list->buckets[i].creation_time = ent.creation_time.sec();
      list->buckets[i].owner_id = nullptr; // TODO: get from bucket info
    }
    
    list->next_marker = blist.next_marker.empty() ? nullptr : strdup(blist.next_marker.c_str());
    list->is_truncated = !blist.next_marker.empty();
    
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" void rgw_sal_bucket_list_free(struct rgw_sal_bucket_list* list)
{
  if (!list) return;
  
  if (list->buckets) {
    for (size_t i = 0; i < list->count; i++) {
      free(list->buckets[i].name);
      free(list->buckets[i].tenant);
      free(list->buckets[i].marker);
      free(list->buckets[i].bucket_id);
      free(list->buckets[i].owner_id);
    }
    free(list->buckets);
  }
  
  free(list->next_marker);
  memset(list, 0, sizeof(*list));
}

extern "C" int rgw_sal_bucket_remove(rgw_sal_bucket_t bucket)
{
  if (!bucket) {
    return -EINVAL;
  }
  
  Bucket* b = get_bucket(bucket);
  if (!b) return -EINVAL;
  
  try {
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(static_cast<rgw_sal_bucket_wrapper*>(bucket)->driver);
    int ret = b->remove(wrapper_driver->dpp, null_yield);
    return ret;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_bucket_get_name(rgw_sal_bucket_t bucket, char* name, size_t name_len)
{
  if (!bucket || !name || name_len == 0) {
    return -EINVAL;
  }
  
  Bucket* b = get_bucket(bucket);
  if (!b) return -EINVAL;
  
  const rgw_bucket& bu = b->get_bucket();
  if (bu.name.length() >= name_len) {
    return -ERANGE;
  }
  
  strncpy(name, bu.name.c_str(), name_len - 1);
  name[name_len - 1] = '\0';
  
  return 0;
}

extern "C" void rgw_sal_bucket_destroy(rgw_sal_bucket_t bucket)
{
  if (bucket) {
    delete static_cast<rgw_sal_bucket_wrapper*>(bucket);
  }
}

/* ============================================
 * Object Operations
 * ============================================ */

extern "C" int rgw_sal_bucket_get_object(rgw_sal_bucket_t bucket, const char* object_name, const char* instance, rgw_sal_object_t* object)
{
  if (!bucket || !object_name || !object) {
    return -EINVAL;
  }
  
  Bucket* b = get_bucket(bucket);
  if (!b) return -EINVAL;
  
  try {
    rgw_obj_key key(object_name, instance);
    std::unique_ptr<Object> obj = b->get_object(key);
    
    auto wrapper = new rgw_sal_object_wrapper();
    wrapper->driver = static_cast<rgw_sal_bucket_wrapper*>(bucket)->driver;
    wrapper->object = std::move(obj);
    
    *object = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_bucket_create_object(rgw_sal_bucket_t bucket, const char* object_name, rgw_sal_object_t* object)
{
  if (!bucket || !object_name || !object) {
    return -EINVAL;
  }
  
  Bucket* b = get_bucket(bucket);
  if (!b) return -EINVAL;
  
  try {
    rgw_obj_key key(object_name);
    std::unique_ptr<Object> obj = b->get_object(key);
    
    auto wrapper = new rgw_sal_object_wrapper();
    wrapper->driver = static_cast<rgw_sal_bucket_wrapper*>(bucket)->driver;
    wrapper->object = std::move(obj);
    
    *object = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_object_load(rgw_sal_object_t object, struct rgw_sal_object_info* info)
{
  if (!object || !info) {
    return -EINVAL;
  }
  
  Object* obj = get_object(object);
  if (!obj) return -EINVAL;
  
  try {
    const rgw_obj_key& key = obj->get_key();
    info->name = strdup(key.name.c_str());
    info->instance = key.instance.empty() ? nullptr : strdup(key.instance.c_str());
    
    // TODO: Load actual object state to get size, mtime, etc.
    info->size = 0;
    info->mtime = 0;
    info->etag = nullptr;
    info->content_type = nullptr;
    info->owner_id = nullptr;
    
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_object_read(rgw_sal_object_t object, uint64_t offset, uint64_t len, rgw_sal_data_cb data_cb, void* user_data)
{
  if (!object || !data_cb) {
    return -EINVAL;
  }
  
  Object* obj = get_object(object);
  if (!obj) return -EINVAL;
  
  try {
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(static_cast<rgw_sal_object_wrapper*>(object)->driver);
    Object::Read read_op(obj.get());
    
    int ret = read_op.prepare(wrapper_driver->dpp);
    if (ret < 0) {
      return ret;
    }
    
    bufferlist bl;
    ret = read_op.read(offset, len, bl, wrapper_driver->dpp);
    if (ret < 0) {
      return ret;
    }
    
    // Call callback with data
    for (auto& it : bl.buffers()) {
      ret = data_cb(it.c_str(), it.length(), offset, user_data);
      if (ret < 0) {
        return ret;
      }
      offset += it.length();
    }
    
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_object_write(rgw_sal_object_t object, const void* data, size_t len, uint64_t offset)
{
  if (!object || !data) {
    return -EINVAL;
  }
  
  Object* obj = get_object(object);
  if (!obj) return -EINVAL;
  
  // This is a simplified write - full implementation would use Writer
  return -ENOTSUP;
}

extern "C" int rgw_sal_object_delete(rgw_sal_object_t object)
{
  if (!object) {
    return -EINVAL;
  }
  
  Object* obj = get_object(object);
  if (!obj) return -EINVAL;
  
  try {
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(static_cast<rgw_sal_object_wrapper*>(object)->driver);
    Object::Delete del_op(obj.get());
    
    int ret = del_op.delete_obj(wrapper_driver->dpp);
    return ret;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_bucket_list_objects(rgw_sal_bucket_t bucket, const char* prefix, const char* marker, uint64_t max, struct rgw_sal_object_list* list)
{
  if (!bucket || !list) {
    return -EINVAL;
  }
  
  Bucket* b = get_bucket(bucket);
  if (!b) return -EINVAL;
  
  // Simplified implementation - full version would use Bucket::List
  list->count = 0;
  list->objects = nullptr;
  list->next_marker = nullptr;
  list->is_truncated = 0;
  
  return -ENOTSUP;
}

extern "C" void rgw_sal_object_list_free(struct rgw_sal_object_list* list)
{
  if (!list) return;
  
  if (list->objects) {
    for (size_t i = 0; i < list->count; i++) {
      free(list->objects[i].name);
      free(list->objects[i].instance);
      free(list->objects[i].etag);
      free(list->objects[i].content_type);
      free(list->objects[i].owner_id);
    }
    free(list->objects);
  }
  
  free(list->next_marker);
  memset(list, 0, sizeof(*list));
}

extern "C" int rgw_sal_object_get_name(rgw_sal_object_t object, char* name, size_t name_len)
{
  if (!object || !name || name_len == 0) {
    return -EINVAL;
  }
  
  Object* obj = get_object(object);
  if (!obj) return -EINVAL;
  
  const rgw_obj_key& key = obj->get_key();
  if (key.name.length() >= name_len) {
    return -ERANGE;
  }
  
  strncpy(name, key.name.c_str(), name_len - 1);
  name[name_len - 1] = '\0';
  
  return 0;
}

extern "C" int rgw_sal_object_get_size(rgw_sal_object_t object, uint64_t* size)
{
  if (!object || !size) {
    return -EINVAL;
  }
  
  Object* obj = get_object(object);
  if (!obj) return -EINVAL;
  
  // TODO: Get actual size from object state
  *size = 0;
  return -ENOTSUP;
}

extern "C" void rgw_sal_object_destroy(rgw_sal_object_t object)
{
  if (object) {
    delete static_cast<rgw_sal_object_wrapper*>(object);
  }
}

/* ============================================
 * Writer Operations
 * ============================================ */

extern "C" int rgw_sal_get_atomic_writer(rgw_sal_driver_t driver, rgw_sal_bucket_t bucket, const char* object_name, const char* owner_id, rgw_sal_writer_t* writer)
{
  if (!driver || !bucket || !object_name || !owner_id || !writer) {
    return -EINVAL;
  }
  
  Driver* d = get_driver(driver);
  Bucket* b = get_bucket(bucket);
  if (!d || !b) return -EINVAL;
  
  try {
    rgw_obj_key key(object_name);
    std::unique_ptr<Object> obj = b->get_object(key);
    
    ACLOwner owner;
    owner.id = owner_id;
    
    auto wrapper_driver = static_cast<rgw_sal_driver_wrapper*>(driver);
    std::unique_ptr<Writer> w = d->get_atomic_writer(wrapper_driver->dpp, null_yield, obj.get(), owner, nullptr, 0, "");
    
    if (!w) {
      return -ENOMEM;
    }
    
    auto wrapper = new rgw_sal_writer_wrapper();
    wrapper->driver = driver;
    wrapper->writer = std::move(w);
    
    *writer = wrapper;
    return 0;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_writer_prepare(rgw_sal_writer_t writer)
{
  if (!writer) {
    return -EINVAL;
  }
  
  Writer* w = get_writer(writer);
  if (!w) return -EINVAL;
  
  try {
    int ret = w->prepare(null_yield);
    return ret;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_writer_write(rgw_sal_writer_t writer, const void* data, size_t len, uint64_t offset)
{
  if (!writer || !data) {
    return -EINVAL;
  }
  
  Writer* w = get_writer(writer);
  if (!w) return -EINVAL;
  
  try {
    bufferlist bl;
    bl.append((const char*)data, len);
    int ret = w->process(std::move(bl), offset);
    return ret;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" int rgw_sal_writer_complete(rgw_sal_writer_t writer, const char* etag)
{
  if (!writer) {
    return -EINVAL;
  }
  
  Writer* w = get_writer(writer);
  if (!w) return -EINVAL;
  
  try {
    std::string etag_str = etag ? etag : "";
    std::map<std::string, bufferlist> attrs;
    ceph::real_time mtime;
    req_context rctx(nullptr, nullptr);
    
    int ret = w->complete(0, etag_str, &mtime, mtime, attrs, std::nullopt, ceph::real_time(), nullptr, nullptr, nullptr, nullptr, nullptr, rctx, 0);
    return ret;
  } catch (const std::exception& e) {
    return -EFAULT;
  }
}

extern "C" void rgw_sal_writer_destroy(rgw_sal_writer_t writer)
{
  if (writer) {
    delete static_cast<rgw_sal_writer_wrapper*>(writer);
  }
}

/* ============================================
 * Utility Functions
 * ============================================ */

extern "C" const char* rgw_sal_version(int* major, int* minor, int* extra)
{
  if (major) *major = RGW_SAL_C_VER_MAJOR;
  if (minor) *minor = RGW_SAL_C_VER_MINOR;
  if (extra) *extra = RGW_SAL_C_VER_EXTRA;
  return "rgw_sal_c 1.0.0";
}

extern "C" void rgw_sal_user_info_free(struct rgw_sal_user_info* info)
{
  if (!info) return;
  
  free(info->user_id);
  free(info->tenant);
  free(info->display_name);
  free(info->email);
  memset(info, 0, sizeof(*info));
}

extern "C" void rgw_sal_bucket_info_free(struct rgw_sal_bucket_info* info)
{
  if (!info) return;
  
  free(info->name);
  free(info->tenant);
  free(info->marker);
  free(info->bucket_id);
  free(info->owner_id);
  memset(info, 0, sizeof(*info));
}

extern "C" void rgw_sal_object_info_free(struct rgw_sal_object_info* info)
{
  if (!info) return;
  
  free(info->name);
  free(info->instance);
  free(info->etag);
  free(info->content_type);
  free(info->owner_id);
  memset(info, 0, sizeof(*info));
}
