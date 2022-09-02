import logging
import json
import tempfile
import random
import threading
import subprocess
import socket
import time
import os
from http import server as http_server
from random import randint
from .tests import get_realm, \
    ZonegroupConns, \
    zonegroup_meta_checkpoint, \
    zone_meta_checkpoint, \
    zone_bucket_checkpoint, \
    zone_data_checkpoint, \
    zonegroup_bucket_checkpoint, \
    check_bucket_eq, \
    gen_bucket_name, \
    get_user, \
    get_tenant, \
    new_key, \
    get_bucket
from nose.plugins.attrib import attr
from .multisite import User
from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal
import boto.s3.tagging

# configure logging for the tests module
log = logging.getLogger(__name__)

skip_push_tests = True

realm = None
zonegroup = None


def sync_info(cluster, bucket = None):
    cmd = ['sync', 'info']
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to get sync policy'

    return json.loads(result_json)

def get_sync_policy(cluster, bucket = None):
    cmd = ['sync', 'policy', 'get']
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to get sync policy'

    return json.loads(result_json)

def create_sync_policy_group(cluster, group, status = "allowed", bucket = None):
    cmd = ['sync', 'group', 'create', '--group-id', group, '--status' , status]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to create sync policy group id=%s, bucket=%s' % (group, bucket)
    return json.loads(result_json)

def set_sync_policy_group_status(cluster, group, status, bucket = None):
    cmd = ['sync', 'group', 'modify', '--group-id', group, '--status' , status]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to set sync policy group id=%s, bucket=%s' % (group, bucket)
    return json.loads(result_json)

def get_sync_policy_group(cluster, group, bucket = None):
    cmd = ['sync', 'group', 'get', '--group-id', group]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to get sync policy group id=%s, bucket=%s' % (group, bucket)
    return json.loads(result_json)

def remove_sync_policy_group(cluster, group, bucket = None):
    cmd = ['sync', 'group', 'remove', '--group-id', group]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to remove sync policy group id=%s, bucket=%s' % (group, bucket)
    return json.loads(result_json)

def create_sync_group_flow_symmetrical(cluster, group, flow_id, zones, bucket = None):
    cmd = ['sync', 'group', 'flow', 'create', '--group-id', group, '--flow-id' , flow_id, '--flow-type', 'symmetrical', '--zones=%s' % zones]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to create sync group flow symmetrical groupid=%s, flow_id=%s, zones=%s, bucket=%s' % (group, flow_id, zones, bucket)
    return json.loads(result_json)

def create_sync_group_flow_directional(cluster, group, flow_id, src_zones, dest_zones, bucket = None):
    cmd = ['sync', 'group', 'flow', 'create', '--group-id', group, '--flow-id' , flow_id, '--flow-type', 'directional', '--source-zone=%s' % src_zones, '--dest-zone=%s' % dest_zones]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to create sync group flow directional groupid=%s, flow_id=%s, src_zones=%s, dest_zones=%s, bucket=%s' % (group, flow_id, src_zones, dest_zones, bucket)
    return json.loads(result_json)

def remove_sync_group_flow_symmetrical(cluster, group, flow_id, zones = None, bucket = None):
    cmd = ['sync', 'group', 'flow', 'remove', '--group-id', group, '--flow-id' , flow_id, '--flow-type', 'symmetrical']
    if zones:
        cmd += ['--zones=%s' % zones]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to remove sync group flow symmetrical groupid=%s, flow_id=%s, zones=%s, bucket=%s' % (group, flow_id, zones, bucket)
    return json.loads(result_json)

def remove_sync_group_flow_directional(cluster, group, flow_id, src_zones, dest_zones, bucket = None):
    cmd = ['sync', 'group', 'flow', 'remove', '--group-id', group, '--flow-id' , flow_id, '--flow-type', 'directional', '--source-zone=%s' % src_zones, '--dest-zone=%s' % dest_zones]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to remove sync group flow directional groupid=%s, flow_id=%s, src_zones=%s, dest_zones=%s, bucket=%s' % (group, flow_id, src_zones, dest_zones, bucket)
    return json.loads(result_json)

def create_sync_group_pipe(cluster, group, pipe_id, src_zones, dest_zones, bucket = None, args = []):
    cmd = ['sync', 'group', 'pipe', 'create', '--group-id', group, '--pipe-id' , pipe_id, '--source-zones=%s' % src_zones, '--dest-zones=%s' % dest_zones]
    if bucket:
        b_args = '--bucket=' + bucket
        cmd.append(b_args)
    if args:
        cmd += args
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to create sync group pipe groupid=%s, pipe_id=%s, src_zones=%s, dest_zones=%s, bucket=%s' % (group, pipe_id, src_zones, dest_zones, bucket)
    return json.loads(result_json)

def remove_sync_group_pipe(cluster, group, pipe_id, bucket = None, args = None):
    cmd = ['sync', 'group', 'pipe', 'remove', '--group-id', group, '--pipe-id' , pipe_id]
    if bucket:
        b_args = '--bucket=' + bucket
        cmd.append(b_args)
    if args:
        cmd.append(args)
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to remove sync group pipe groupid=%s, pipe_id=%s, src_zones=%s, dest_zones=%s, bucket=%s' % (group, pipe_id, src_zones, dest_zones, bucket)
    return json.loads(result_json)

def create_zone_bucket(zone):
    b_name = gen_bucket_name()
    log.info('create bucket zone=%s name=%s', zone.name, b_name)
    bucket = zone.create_bucket(b_name)
    return bucket

def create_object(zone_conn, bucket, objname, content):
    k = new_key(zone_conn, bucket.name, objname)
    k.set_contents_from_string(content)

def create_objects(zone_conn, bucket, obj_arr, content):
    for objname in obj_arr:
        create_object(zone_conn, bucket, objname, content)

def check_object_exists(bucket, objname, content = None):
    k = bucket.get_key(objname)
    assert_not_equal(k, None)
    if (content != None):
        assert_equal(k.get_contents_as_string(encoding='ascii'), content)

def check_objects_exist(bucket, obj_arr, content = None):
    for objname in obj_arr:
        check_object_exists(bucket, objname, content)

def check_object_not_exists(bucket, objname):
    k = bucket.get_key(objname)
    assert_equal(k, None)

def check_objects_not_exist(bucket, obj_arr):
    for objname in obj_arr:
        check_object_not_exists(bucket, objname)

@attr('sync_policy')
def test_sync_policy_config_zonegroup():
    """
    test_sync_policy_config_zonegroup:
        test configuration of all sync commands
    """
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)
    z1, z2 = zonegroup.zones[0:2]
    c1, c2 = (z1.cluster, z2.cluster)

    zones = z1.name+","+z2.name

    c1.admin(['sync', 'policy', 'get'])

    # (a) zonegroup level
    create_sync_policy_group(c1, "sync-group")
    set_sync_policy_group_status(c1, "sync-group", "enabled")
    get_sync_policy_group(c1, "sync-group")

    get_sync_policy(c1)

    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    create_sync_group_flow_directional(c1, "sync-group", "sync-flow2", z1.name, z2.name)

    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)
    get_sync_policy_group(c1, "sync-group")

    zonegroup.period.update(z1, commit=True)

    # (b)  bucket level
    zc1, zc2 = zonegroup_conns.zones[0:2]
    bucket = create_zone_bucket(zc1)
    bucket_name = bucket.name

    create_sync_policy_group(c1, "sync-bucket", "allowed", bucket_name)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucket_name)
    get_sync_policy_group(c1, "sync-bucket", bucket_name)

    get_sync_policy(c1, bucket_name)

    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flow1", zones, bucket_name)
    create_sync_group_flow_directional(c1, "sync-bucket", "sync-flow2", z1.name, z2.name, bucket_name)

    create_sync_group_pipe(c1, "sync-bucket", "sync-pipe", zones, zones, bucket_name)
    get_sync_policy_group(c1, "sync-bucket", bucket_name)

    zonegroup_meta_checkpoint(zonegroup)

    remove_sync_group_pipe(c1, "sync-bucket", "sync-pipe", bucket_name)
    remove_sync_group_flow_directional(c1, "sync-bucket", "sync-flow2", z1.name, z2.name, bucket_name)
    remove_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flow1", zones, bucket_name)
    remove_sync_policy_group(c1, "sync-bucket", bucket_name)

    get_sync_policy(c1, bucket_name)

    zonegroup_meta_checkpoint(zonegroup)

    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    remove_sync_group_flow_directional(c1, "sync-group", "sync-flow2", z1.name, z2.name)
    remove_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1")
    remove_sync_policy_group(c1, "sync-group")

    get_sync_policy(c1)

    zonegroup.period.update(z1, commit=True)

    return

@attr('sync_policy')
def test_sync_flow_symmetrical_zonegroup_all():
    """
    test_sync_flow_symmetrical_zonegroup_all:
        allows sync from all the zones to all other zones (default case)
    """

    realm = get_realm()

    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    zones = zoneA.name + ',' + zoneB.name
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)
    set_sync_policy_group_status(c1, "sync-group", "enabled")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    objnames = [ 'obj1', 'obj2' ]
    content = 'asdasd'
    buckets = []

    # create bucket & object in all zones
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    create_object(zcA, bucketA, objnames[0], content)

    bucketB = create_zone_bucket(zcB)
    buckets.append(bucketB)
    create_object(zcB, bucketB, objnames[1], content)

    zonegroup_meta_checkpoint(zonegroup)
    # 'zonegroup_data_checkpoint' currently fails for the zones not
    # allowed to sync. So as a workaround, data checkpoint is done
    # for only the ones configured.
    zone_data_checkpoint(zoneB, zoneA)

    # verify if objects are synced accross the zone
    bucket = get_bucket(zcB, bucketA.name)
    check_object_exists(bucket, objnames[0], content)

    bucket = get_bucket(zcA, bucketB.name)
    check_object_exists(bucket, objnames[1], content)

    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    remove_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1")
    remove_sync_policy_group(c1, "sync-group")
    get_sync_policy(c1)

    zonegroup.period.update(zoneA, commit=True)
    return

@attr('sync_policy')
def test_sync_flow_symmetrical_zonegroup_select():
    """
    test_sync_flow_symmetrical_zonegroup_select:
        allow sync between zoneA & zoneB
        verify zoneC doesnt sync the data
    """

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    if len(zonegroup.zones) < 3:
        raise SkipTest("test_sync_flow_symmetrical_zonegroup_select skipped. Requires 3 or more zones in master zonegroup.")

    zonegroup_meta_checkpoint(zonegroup)

    (zoneA, zoneB, zoneC) = zonegroup.zones[0:3]
    (zcA, zcB, zcC) = zonegroup_conns.zones[0:3]

    c1 = zoneA.cluster

    # configure sync policy
    zones = zoneA.name + ',' + zoneB.name
    c1.admin(['sync', 'policy', 'get'])
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow", zones)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)
    set_sync_policy_group_status(c1, "sync-group", "enabled")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    buckets = []
    content = 'asdasd'

    # create bucketA & objects in zoneA
    objnamesA = [ 'obj1', 'obj2', 'obj3' ]
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    create_objects(zcA, bucketA, objnamesA, content)

    # create bucketB & objects in zoneB
    objnamesB = [ 'obj4', 'obj5', 'obj6' ]
    bucketB = create_zone_bucket(zcB)
    buckets.append(bucketB)
    create_objects(zcB, bucketB, objnamesB, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)
    zone_data_checkpoint(zoneA, zoneB)

    # verify if objnamesA synced to only zoneB but not zoneC
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesA, content)

    bucket = get_bucket(zcC, bucketA.name)
    check_objects_not_exist(bucket, objnamesA)

    # verify if objnamesB synced to only zoneA but not zoneC
    bucket = get_bucket(zcA, bucketB.name)
    check_objects_exist(bucket, objnamesB, content)

    bucket = get_bucket(zcC, bucketB.name)
    check_objects_not_exist(bucket, objnamesB)

    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    remove_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow")
    remove_sync_policy_group(c1, "sync-group")
    get_sync_policy(c1)

    zonegroup.period.update(zoneA, commit=True)
    return

@attr('sync_policy')
def test_sync_flow_directional_zonegroup_select():
    """
    test_sync_flow_directional_zonegroup_select:
        allow sync from only zoneA to zoneB
        
        verify that data doesn't get synced to zoneC and
        zoneA shouldn't sync data from zoneB either
    """

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    if len(zonegroup.zones) < 3:
        raise SkipTest("test_sync_flow_symmetrical_zonegroup_select skipped. Requires 3 or more zones in master zonegroup.")

    zonegroup_meta_checkpoint(zonegroup)

    (zoneA, zoneB, zoneC) = zonegroup.zones[0:3]
    (zcA, zcB, zcC) = zonegroup_conns.zones[0:3]

    c1 = zoneA.cluster

    # configure sync policy
    zones = zoneA.name + ',' + zoneB.name
    c1.admin(['sync', 'policy', 'get'])
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_directional(c1, "sync-group", "sync-flow", zoneA.name, zoneB.name)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zoneA.name, zoneB.name)
    set_sync_policy_group_status(c1, "sync-group", "enabled")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    buckets = []
    content = 'asdasd'

    # create bucketA & objects in zoneA
    objnamesA = [ 'obj1', 'obj2', 'obj3' ]
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    create_objects(zcA, bucketA, objnamesA, content)

    # create bucketB & objects in zoneB
    objnamesB = [ 'obj4', 'obj5', 'obj6' ]
    bucketB = create_zone_bucket(zcB)
    buckets.append(bucketB)
    create_objects(zcB, bucketB, objnamesB, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify if objnamesA synced to only zoneB but not zoneC
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesA, content)

    bucket = get_bucket(zcC, bucketA.name)
    check_objects_not_exist(bucket, objnamesA)

    # verify if objnamesB are not synced to either zoneA or zoneC
    bucket = get_bucket(zcA, bucketB.name)
    check_objects_not_exist(bucket, objnamesB)

    bucket = get_bucket(zcC, bucketB.name)
    check_objects_not_exist(bucket, objnamesB)

    """
        verify the same at bucketA level
        configure another policy at bucketA level with src and dest
        zones specified to zoneA and zoneB resp.

        verify zoneA bucketA syncs to zoneB BucketA but not viceversa.
    """
    # reconfigure zonegroup pipe & flow
    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    remove_sync_group_flow_directional(c1, "sync-group", "sync-flow", zoneA.name, zoneB.name)
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    args = ['--source-bucket=*', '--dest-bucket=*']
    create_sync_group_pipe(c1, "sync-bucket", "sync-pipe", zoneA.name, zoneB.name, bucketA.name, args)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1, bucketA.name)

    zonegroup_meta_checkpoint(zonegroup)

    # create objects in bucketA in zoneA and zoneB
    objnamesC = [ 'obj7', 'obj8', 'obj9' ]
    objnamesD = [ 'obj10', 'obj11', 'obj12' ]
    create_objects(zcA, bucketA, objnamesC, content)
    create_objects(zcB, bucketA, objnamesD, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that objnamesC are synced to bucketA in zoneB
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesC, content)

    # verify that objnamesD are not synced to bucketA in zoneA
    bucket = get_bucket(zcA, bucketA.name)
    check_objects_not_exist(bucket, objnamesD)

    remove_sync_group_pipe(c1, "sync-bucket", "sync-pipe")
    remove_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA")
    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1")
    remove_sync_policy_group(c1, "sync-group")
    get_sync_policy(c1)

    zonegroup.period.update(zoneA, commit=True)
    return

@attr('sync_policy')
def test_sync_single_bucket():
    """
    test_sync_single_bucket:
        Allow data sync for only bucketA but not for other buckets via
        below 2 methods

        (a) zonegroup: symmetrical flow but configure pipe for only bucketA.
        (b) bucket level: configure policy for bucketA
    """

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    zones = zoneA.name + ',' + zoneB.name
    get_sync_policy(c1)

    objnames = [ 'obj1', 'obj2', 'obj3' ]
    content = 'asdasd'
    buckets = []

    # create bucketA & bucketB in zoneA
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    bucketB = create_zone_bucket(zcA)
    buckets.append(bucketB)

    zonegroup_meta_checkpoint(zonegroup)

    """
        Method (a): configure pipe for only bucketA
    """
    # configure sync policy & pipe for only bucketA
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    args = ['--source-bucket=' +  bucketA.name, '--dest-bucket=' + bucketA.name]

    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones, None, args)
    set_sync_policy_group_status(c1, "sync-group", "enabled")
    get_sync_policy(c1)
    zonegroup.period.update(zoneA, commit=True)
    
    sync_info(c1)

    # create objects in bucketA & bucketB
    create_objects(zcA, bucketA, objnames, content)
    create_object(zcA, bucketB, objnames, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify if bucketA objects are synced
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnames, content)

    # bucketB objects should not be synced
    bucket = get_bucket(zcB, bucketB.name)
    check_objects_not_exist(bucket, objnames)


    """
        Method (b): configure policy at only bucketA level 
    """
    # reconfigure group pipe
    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    remove_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1")
    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)


    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    create_sync_group_pipe(c1, "sync-bucket", "sync-pipe", zones, zones, bucketA.name)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1, bucketA.name)

    # create object in bucketA
    create_object(zcA, bucketA, objnames[2], content)

    # create object in bucketA too
    create_object(zcA, bucketB, objnames[2], content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify if bucketA objects are synced
    bucket = get_bucket(zcB, bucketA.name)
    check_object_exists(bucket, objnames[2], content)

    # bucketB objects should not be synced
    bucket = get_bucket(zcB, bucketB.name)
    check_object_not_exists(bucket, objnames[2])

    remove_sync_group_pipe(c1, "sync-bucket", "sync-pipe")
    remove_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA")
    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_different_buckets():
    """
    test_sync_different_buckets:
        sync zoneA bucketA to zoneB bucketB via below methods

        (a) zonegroup: directional flow but configure pipe for zoneA bucketA to zoneB bucketB
        (b) bucket: configure another policy at bucketA level with pipe set to
        another bucket(bucketB) in target zone.

        sync zoneA bucketA from zoneB bucketB
        (c) configure another policy at bucketA level with pipe set from
        another bucket(bucketB) in source zone.

    """

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]
    zones = zoneA.name + ',' + zoneB.name

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    objnames = [ 'obj1', 'obj2' ]
    objnamesB = [ 'obj3', 'obj4' ]
    content = 'asdasd'
    buckets = []

    # create bucketA & bucketB in zoneA
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    bucketB = create_zone_bucket(zcA)
    buckets.append(bucketB)

    zonegroup_meta_checkpoint(zonegroup)

    """
        Method (a): zonegroup - configure pipe for only bucketA
    """
    # configure pipe from zoneA bucketA to zoneB bucketB
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    args = ['--source-bucket=' +  bucketA.name, '--dest-bucket=' + bucketB.name]
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zoneA.name, zoneB.name, None, args)
    set_sync_policy_group_status(c1, "sync-group", "enabled")
    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # create objects in bucketA
    create_objects(zcA, bucketA, objnames, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that objects are synced to bucketB in zoneB
    # but not to bucketA
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_not_exist(bucket, objnames)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnames, content)
    """
        Method (b): configure policy at only bucketA level with pipe
        set to bucketB in target zone
    """

    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    args = ['--source-bucket=*', '--dest-bucket=' + bucketB.name]
    create_sync_group_pipe(c1, "sync-bucket", "sync-pipeA", zones, zones, bucketA.name, args)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1, bucketA.name)

    objnamesC = [ 'obj5', 'obj6' ]

    zonegroup_meta_checkpoint(zonegroup)
    # create objects in bucketA
    create_objects(zcA, bucketA, objnamesC, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    """
    # verify that objects are synced to bucketB in zoneB
    # but not to bucketA
    """
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_not_exist(bucket, objnamesC)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnamesC, content)

    remove_sync_group_pipe(c1, "sync-bucket", "sync-pipeA")
    remove_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA")
    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    zonegroup_meta_checkpoint(zonegroup)
    get_sync_policy(c1, bucketA.name)

    """
        Method (c): configure policy at only bucketA level with pipe
        set from bucketB in source zone
        verify zoneA bucketA syncs from zoneB BucketB but not bucketA
    """

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    args = ['--source-bucket=' +  bucketB.name, '--dest-bucket=' + '*']
    create_sync_group_pipe(c1, "sync-bucket", "sync-pipe", zones, zones, bucketA.name, args)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1, bucketA.name)

    # create objects in bucketA & B in ZoneB
    objnamesD = [ 'obj7', 'obj8' ]
    objnamesE = [ 'obj9', 'obj10' ]

    create_objects(zcB, bucketA, objnamesD, content)
    create_objects(zcB, bucketB, objnamesE, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneA, zoneB)
    """
    # verify that objects from only bucketB are synced to
    # bucketA in zoneA
    """
    bucket = get_bucket(zcA, bucketA.name)
    check_objects_not_exist(bucket, objnamesD)
    check_objects_exist(bucket, objnamesE, content)

    remove_sync_group_pipe(c1, "sync-bucket", "sync-pipeA")
    remove_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA")
    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    remove_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1")
    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_multiple_buckets_to_single():
    """
    test_sync_multiple_buckets_to_single:
        directional flow
        (a) pipe: sync zoneA bucketA,bucketB to zoneB bucketB

        (b) configure another policy at bucketA level with pipe configured
        to sync from multiple buckets (bucketA & bucketB)

        verify zoneA bucketA & bucketB syncs to zoneB BucketB
    """

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]
    zones = zoneA.name + ',' + zoneB.name

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    objnamesA = [ 'obj1', 'obj2' ]
    objnamesB = [ 'obj3', 'obj4' ]
    content = 'asdasd'
    buckets = []

    # create bucketA & bucketB in zoneA
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    bucketB = create_zone_bucket(zcA)
    buckets.append(bucketB)

    zonegroup_meta_checkpoint(zonegroup)

    # configure pipe from zoneA bucketA,bucketB to zoneB bucketB
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_directional(c1, "sync-group", "sync-flow", zoneA.name, zoneB.name)
    source_buckets = [ bucketA.name, bucketB.name ]
    for source_bucket in source_buckets:
        args = ['--source-bucket=' +  source_bucket, '--dest-bucket=' + bucketB.name]
        create_sync_group_pipe(c1, "sync-group", "sync-pipe-%s" % source_bucket, zoneA.name, zoneB.name, None, args)

    set_sync_policy_group_status(c1, "sync-group", "enabled")
    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # create objects in bucketA & bucketB
    create_objects(zcA, bucketA, objnamesA, content)
    create_objects(zcA, bucketB, objnamesB, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that both zoneA bucketA & bucketB objects are synced to
    # bucketB in zoneB but not to bucketA
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_not_exist(bucket, objnamesA)
    check_objects_not_exist(bucket, objnamesB)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnamesA, content)
    check_objects_exist(bucket, objnamesB, content)

    """
        Method (b): configure at bucket level
    """
    # reconfigure pipe & flow
    for source_bucket in source_buckets:
        remove_sync_group_pipe(c1, "sync-group", "sync-pipe-%s" % source_bucket)
    remove_sync_group_flow_directional(c1, "sync-group", "sync-flow", zoneA.name, zoneB.name)
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    objnamesC = [ 'obj5', 'obj6' ]
    objnamesD = [ 'obj7', 'obj8' ]

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    source_buckets = [ bucketA.name, bucketB.name ]
    for source_bucket in source_buckets:
        args = ['--source-bucket=' +  source_bucket, '--dest-bucket=' + '*']
        create_sync_group_pipe(c1, "sync-bucket", "sync-pipe-%s" % source_bucket, zoneA.name, zoneB.name, bucketA.name, args)

    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1)

    zonegroup_meta_checkpoint(zonegroup)
    # create objects in bucketA
    create_objects(zcA, bucketA, objnamesC, content)
    create_objects(zcA, bucketB, objnamesD, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that both zoneA bucketA & bucketB objects are synced to
    # bucketA in zoneB but not to bucketB
    bucket = get_bucket(zcB, bucketB.name)
    check_objects_not_exist(bucket, objnamesC)
    check_objects_not_exist(bucket, objnamesD)

    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesD, content)
    check_objects_exist(bucket, objnamesD, content)

    for source_bucket in source_buckets:
        remove_sync_group_pipe(c1, "sync-bucket", "sync-pipe-%s" % source_bucket)
    remove_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA")
    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_group_flow_directional(c1, "sync-group", "sync-flow1", zoneA.name, zoneB.name)
    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_single_bucket_to_multiple():
    """
    test_sync_single_bucket_to_multiple:
        directional flow
        (a) pipe: sync zoneA bucketA to zoneB bucketA & bucketB

        (b) configure another policy at bucketA level with pipe configured
        to sync to multiple buckets (bucketA & bucketB)

        verify zoneA bucketA syncs to zoneB bucketA & bucketB
    """

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]
    zones = zoneA.name + ',' + zoneB.name

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    objnamesA = [ 'obj1', 'obj2' ]
    content = 'asdasd'
    buckets = []

    # create bucketA & bucketB in zoneA
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    bucketB = create_zone_bucket(zcA)
    buckets.append(bucketB)

    zonegroup_meta_checkpoint(zonegroup)

    # configure pipe from zoneA bucketA to zoneB bucketA, bucketB
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)

    dest_buckets = [ bucketA.name, bucketB.name ]
    for dest_bucket in dest_buckets:
        args = ['--source-bucket=' +  bucketA.name, '--dest-bucket=' + dest_bucket]
        create_sync_group_pipe(c1, "sync-group", "sync-pipe-%s" % dest_bucket, zoneA.name, zoneB.name, None, args)

    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zoneA.name, zoneB.name, None, args)
    set_sync_policy_group_status(c1, "sync-group", "enabled")
    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # create objects in bucketA
    create_objects(zcA, bucketA, objnamesA, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that objects from zoneA bucketA are synced to both
    # bucketA & bucketB in zoneB
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesA, content)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnamesA, content)

    """
        Method (b): configure at bucket level
    """
    for dest_bucket in dest_buckets:
        remove_sync_group_pipe(c1, "sync-group", "sync-pipe-%s" % dest_bucket)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", '*', '*')

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    objnamesB = [ 'obj3', 'obj4' ]

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    dest_buckets = [ bucketA.name, bucketB.name ]
    for dest_bucket in dest_buckets:
        args = ['--source-bucket=' + '*', '--dest-bucket=' + dest_bucket]
        create_sync_group_pipe(c1, "sync-bucket", "sync-pipe-%s" % dest_bucket, zoneA.name, zoneB.name, bucketA.name, args)

    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1)

    zonegroup_meta_checkpoint(zonegroup)
    # create objects in bucketA
    create_objects(zcA, bucketA, objnamesB, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that objects from zoneA bucketA are synced to both
    # bucketA & bucketB in zoneB
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesB, content)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnamesB, content)

    for dest_bucket in dest_buckets:
        remove_sync_group_pipe(c1, "sync-bucket", "sync-pipe-%s" % dest_bucket)
    remove_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA")
    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1")
    remove_sync_policy_group(c1, "sync-group")
    return
