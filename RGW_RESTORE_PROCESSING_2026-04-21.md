# RGW Restore Processing Analysis

**Date:** 2026-04-21
**Files Analyzed:** `src/rgw/rgw_restore.h`, `src/rgw/rgw_restore.cc`

## Overview

The restore mechanism handles restoring objects from cloud-tier (e.g., S3 Glacier, cloud-s3) back to local RGW storage. It uses an asynchronous processing model with sharded storage for restore entries.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RGW Instance                              │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Restore Class                             ││
│  │  ┌─────────────────┐    ┌──────────────────────────────────┐││
│  │  │  RestoreWorker  │    │  Shards (RADOS objects)          │││
│  │  │  (1 thread)     │───▶│  restore.0, restore.1, ...       │││
│  │  └─────────────────┘    │  restore.31 (default 32 shards)  │││
│  │                         └──────────────────────────────────┘││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      Another RGW Instance                        │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  RestoreWorker (1 thread) ──▶ Same RADOS shards             ││
│  │                              (coordinated via RADOS locks)   ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rgw_restore_max_objs` | **32** | Number of shards for storing restore entries. Affects concurrency as shards can be processed in parallel. |
| `rgw_restore_processor_period` | **15 minutes** | Time between consecutive restore processing cycles |
| `rgw_restore_lock_max_time` | **90 seconds** | Maximum time to hold lock on a shard |
| `rgw_restore_debug_interval` | -1 (disabled) | Debug mode: seconds per simulated "day" for testing |

### Configuration Location

```yaml
# src/common/options/rgw.yaml.in

- name: rgw_restore_max_objs
  type: int
  level: advanced
  desc: Number of shards for restore processing
  default: 32

- name: rgw_restore_processor_period
  type: int
  level: advanced
  desc: Restore cycle run time
  default: 15_min

- name: rgw_restore_lock_max_time
  type: int
  level: dev
  default: 90
```

## Worker Threads

### Single Worker Thread Per RGW Instance

Each RGW instance runs exactly **1 restore worker thread**:

```cpp
// rgw_restore.h:104
std::unique_ptr<Restore::RestoreWorker> worker;

// rgw_restore.cc:231-235
void Restore::start_processor()
{
  worker = std::make_unique<Restore::RestoreWorker>(this, cct, this);
  worker->create("rgw_restore");
}
```

- Thread name: `"rgw_restore"`
- Single worker handles all restore processing for that RGW instance
- Worker runs continuously in a loop, sleeping between cycles

### Worker Loop

```cpp
// rgw_restore.cc:273-297
void *Restore::RestoreWorker::entry() {
  do {
    ceph_timespec start = ceph::real_clock::to_ceph_timespec(real_clock::now());

    int r = restore->process(this, null_yield);

    if (restore->going_down())
      break;

    int secs = cct->_conf->rgw_restore_processor_period;  // 15 minutes default

    // If processing took longer than period, immediately start next round
    if (secs < d)
      continue;

    // Otherwise, wait for the remaining time
    std::unique_lock locker{lock};
    cond.wait_for(locker, std::chrono::seconds(secs));

  } while (!restore->going_down());

  return NULL;
}
```

## Sharding Mechanism

### Number of Shards

- Default: **32 shards** (`rgw_restore_max_objs`)
- Maximum: **7877** (`HASH_PRIME`)
- Shard names: `restore.0`, `restore.1`, ... `restore.N`

```cpp
// rgw_restore.cc:164-173
max_objs = cct->_conf->rgw_restore_max_objs;
if (max_objs > HASH_PRIME)
  max_objs = HASH_PRIME;

for (int i = 0; i < max_objs; i++) {
  std::string s = fmt::format("{}.{}", restore_oid_prefix, i);
  obj_names.push_back(s);  // "restore.0", "restore.1", etc.
}
```

### Hash-Based Distribution

Entries are distributed across shards using a hash of bucket + object name:

```cpp
// rgw_restore.cc:266-271
int Restore::choose_oid(const RestoreEntry& e) {
  int index;
  const auto& name = e.bucket.name + e.obj_key.name + e.obj_key.instance;
  index = ((ceph_str_hash_linux(name.data(), name.size())) % max_objs);
  return static_cast<int>(index);
}
```

## Entries Processed Per Batch

### Batch Size: 100 Entries (Hardcoded)

```cpp
// rgw_restore.cc:373-377
do {
  int max = 100;  // Hardcoded batch size
  std::vector<RestoreEntry> entries;

  ret = sal_restore->list(this, y, index, marker, &next_marker, max, entries, &truncated);

  // Process entries...

} while (truncated);  // Continue if more entries exist
```

- Lists up to **100 entries** per shard per iteration
- Uses marker-based pagination
- Continues fetching batches if `truncated == true`
- After processing all entries, trims completed and re-adds in-progress

## Processing Flow

### Main Processing Loop

```cpp
// rgw_restore.cc:299-311
int Restore::process(RestoreWorker* worker, optional_yield y)
{
  int max_secs = cct->_conf->rgw_restore_lock_max_time;

  // Random start shard to distribute load across multiple RGW instances
  const int start = ceph::util::generate_random_number(0, max_objs - 1);

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process(index, max_secs, y);
    if (ret < 0)
      return ret;
  }
  return 0;
}
```

### Per-Shard Processing

```cpp
// rgw_restore.cc:333-446
int Restore::process(int index, int max_secs, optional_yield y)
{
  // 1. Acquire lock on shard
  std::unique_ptr<rgw::sal::RestoreSerializer> serializer =
      sal_restore->get_serializer("restore_process", obj_names[index], worker->thr_name());

  int ret = serializer->try_lock(this, time, y);
  if (ret == -EBUSY || ret == -EEXIST) {
    return -EBUSY;  // Skip - another RGW has this shard
  }

  // 2. List and process entries in batches of 100
  do {
    int max = 100;
    ret = sal_restore->list(this, y, index, marker, &next_marker, max, entries, &truncated);

    for (auto& entry : entries) {
      ret = process_restore_entry(entry, y);

      // Re-queue if still in progress
      if (entry.status == RGWRestoreStatus::RestoreAlreadyInProgress) {
        r_entries.push_back(entry);
      }
    }
  } while (truncated);

  // 3. Trim processed entries
  ret = sal_restore->trim_entries(this, y, index, marker);

  // 4. Re-add in-progress entries for retry
  if (!r_entries.empty()) {
    ret = sal_restore->add_entries(this, y, index, r_entries);
  }

  // 5. Release lock
  lock.unlock();
}
```

### Processing Flow Diagram

```
┌──────────────────┐
│ Worker wakes up  │
│ (every 15 min)   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Pick random      │
│ start shard      │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌─────────────────────┐
│ Try lock shard   │────▶│ Lock busy?          │
└────────┬─────────┘     │ Skip to next shard  │
         │ Success       └─────────────────────┘
         ▼
┌──────────────────┐
│ List 100 entries │◀───┐
└────────┬─────────┘    │
         │              │
         ▼              │
┌──────────────────┐    │
│ Process each     │    │
│ entry            │    │
└────────┬─────────┘    │
         │              │
         ▼              │
┌──────────────────┐    │
│ More entries?    │────┘
│ (truncated)      │ Yes
└────────┬─────────┘
         │ No
         ▼
┌──────────────────┐
│ Trim processed   │
│ Re-add pending   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Release lock     │
│ Next shard       │
└──────────────────┘
```

## Multi-RGW Server Synchronization

### RADOS-Based Locking

Multiple RGW instances coordinate using RADOS exclusive locks on each shard:

```cpp
// rgw_restore.cc:341-368
std::unique_ptr<rgw::sal::RestoreSerializer> serializer =
    sal_restore->get_serializer(
        std::string(restore_index_lock_name),  // "restore_process"
        std::string(obj_names[index]),          // "restore.N"
        worker->thr_name()                      // Thread identifier
    );

const ceph::timespan time = std::chrono::seconds(max_secs);  // 90 seconds
int ret = serializer->try_lock(this, time, y);

if (ret == -EBUSY || ret == -EEXIST) {
  // Already locked by another RGW processor
  ldpp_dout(this, 0) << "failed to acquire lock on " << obj_names[index] << dendl;
  return -EBUSY;
}
```

### Synchronization Features

| Feature | Value/Implementation |
|---------|---------------------|
| Lock type | RADOS exclusive lock |
| Lock name | `"restore_process"` |
| Lock scope | Per shard (per RADOS object) |
| Lock timeout | 90 seconds (`rgw_restore_lock_max_time`) |
| Conflict handling | Skip shard, continue to next |
| Load balancing | Random start shard per cycle |

### How Multiple RGWs Coordinate

```
Time ─────────────────────────────────────────────────────────▶

RGW-1:  [Lock shard 5][Process][Unlock]     [Lock shard 12][Process][Unlock]
                                    │
RGW-2:        [Try shard 5: BUSY]   │   [Lock shard 5][Process][Unlock]
              [Lock shard 7][Process][Unlock]
                                    │
RGW-3:              [Lock shard 5: BUSY]    [Try shard 7: BUSY]
                    [Lock shard 0][Process][Unlock]
```

- Each RGW starts at a **random shard** to distribute load
- If a shard is locked, RGW **skips it** and tries the next
- No retry mechanism - will pick up skipped shards in next cycle
- Lock timeout (90s) prevents deadlocks if RGW crashes

## Zone Awareness

### Temp Copies Processed Only by Source Zone

For temporary restored copies (with expiration days), only the originating zone processes them:

```cpp
// rgw_restore.cc:463-469
if (days) { // temp copy (has expiration)
  auto& zone_id = entry.zone_id;
  if (driver->get_zone()->get_id() != zone_id) {
    // Skip - this is not the source zone
    return 0;
  }
}
```

- Each `RestoreEntry` stores `zone_id` of the zone where restore was initiated
- Only the source zone processes temp copies
- Prevents duplicate processing in multi-zone deployments
- Permanent restores (no days) can be processed by any zone

## Entry Lifecycle

### RestoreEntry Structure

```cpp
// rgw_restore.h:36-67
struct RestoreEntry {
  rgw_bucket bucket;
  rgw_obj_key obj_key;
  std::optional<uint64_t> days;  // Expiration days (empty = permanent)
  std::string zone_id;           // Source zone
  rgw::sal::RGWRestoreStatus status;
};
```

### Status Values

| Status | Description |
|--------|-------------|
| `None` | Initial state when entry is first created |
| `RestoreAlreadyInProgress` | Restore operation is ongoing |
| `CloudRestored` | Restore completed successfully |
| `RestoreFailed` | Restore failed |

### Entry Flow

```
┌──────────────────┐
│ RestoreObject    │
│ API called       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Set object attr  │
│ RESTORE_IN_PROG  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Add entry to     │
│ shard queue      │
│ status=None      │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Wake worker      │◀─── (immediate for cloud-s3 tier)
│ (optional)       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌─────────────────────┐
│ Worker picks up  │────▶│ Still in progress?  │
│ entry            │     └──────────┬──────────┘
└──────────────────┘                │
                           Yes      │      No
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
         ┌──────────────────┐            ┌──────────────────┐
         │ Re-add to queue  │            │ Trim from queue  │
         │ for retry        │            │ Send notification│
         └──────────────────┘            │ Notify waiters   │
                                         └──────────────────┘
```

## Immediate Wake for Cloud-S3 Tier

For cloud-s3 tier (not Glacier), the worker is woken immediately:

```cpp
// rgw_restore.cc:761-766
if (tier && tier->get_tier_type() == "cloud-s3") {
  ldpp_dout(this, 10) << "Waking restore worker for immediate processing" << dendl;
  wake_worker();
}
```

This allows faster restore for non-Glacier S3 tiers that don't have retrieval delays.

## Notifications

### Events Sent

| Event | When |
|-------|------|
| `ObjectRestoreInitiated` | When restore request is accepted |
| `ObjectRestoreCompleted` | When restore finishes successfully |

```cpp
// rgw_restore.cc:563-565
send_notification(this, driver, obj.get(), bucket.get(), etag, size,
                  obj->get_key().instance,
                  {rgw::notify::ObjectRestoreCompleted}, y);
```

## Summary Table

| Aspect | Value |
|--------|-------|
| **Worker threads per RGW** | 1 |
| **Thread name** | `"rgw_restore"` |
| **Shards (queues)** | 32 (default, max 7877) |
| **Entries per batch** | 100 (hardcoded) |
| **Processing interval** | 15 minutes |
| **Lock timeout** | 90 seconds |
| **Lock type** | RADOS exclusive lock per shard |
| **Multi-RGW sync** | Skip locked shards, random start |
| **Zone handling** | Source zone only for temp copies |
| **Hash function** | `ceph_str_hash_linux` on bucket+object |

## Performance Considerations

1. **Throughput**: With 32 shards and multiple RGWs, theoretical max parallelism is 32 concurrent restore operations cluster-wide

2. **Batch size**: 100 entries per batch balances memory usage vs. iteration overhead

3. **Lock contention**: Random start shard reduces collision probability

4. **Processing period**: 15-minute default balances responsiveness vs. overhead

5. **Immediate wake**: Cloud-s3 tier triggers immediate processing for faster restores
