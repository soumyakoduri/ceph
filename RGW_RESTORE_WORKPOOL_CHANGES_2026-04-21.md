# RGW Restore Processing - Multi-Worker and Workpool Implementation

**Date:** 2026-04-21
**Branch:** wip-s3vector-backend-options

## Overview

This document summarizes the changes made to enhance RGW restore processing with multiple worker threads and coroutine-based workpools for concurrent entry processing, similar to the Lifecycle (LC) implementation.

## Changes Summary

### 1. New Configuration Options

Added to `src/common/options/rgw.yaml.in`:

```yaml
- name: rgw_restore_max_worker
  type: int
  level: advanced
  desc: Number of RestoreWorker threads that will be run in parallel
  default: 3

- name: rgw_restore_max_wp_worker
  type: int
  level: advanced
  desc: Number of workpool coroutines per RestoreWorker
  default: 128
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rgw_restore_max_worker` | **3** | Number of RestoreWorker threads running in parallel |
| `rgw_restore_max_wp_worker` | **128** | Number of coroutines per worker for concurrent entry processing |

### 2. Header Changes (`src/rgw/rgw_restore.h`)

**New Includes:**
```cpp
#include <mutex>
#include <boost/asio/spawn.hpp>
```

**RestoreWorker Class Updates:**
- Added worker index (`int ix`) for identification
- Changed from `ceph::mutex` to `std::mutex` for compatibility
- Updated constructor to accept worker index
- Added `get_ix()` method

```cpp
class RestoreWorker : public Thread
{
  const DoutPrefixProvider *dpp;
  CephContext *cct;
  rgw::restore::Restore *restore;
  int ix;  // worker index
  std::mutex lock;
  std::condition_variable cond;

public:
  RestoreWorker(const DoutPrefixProvider* _dpp, CephContext *_cct,
                rgw::restore::Restore *_restore, int _ix);
  int get_ix() const { return ix; }
  // ...
};
```

**Changed from Single Worker to Vector of Workers:**
```cpp
// Before
std::unique_ptr<Restore::RestoreWorker> worker;

// After
std::vector<std::unique_ptr<Restore::RestoreWorker>> workers;
```

**New Function Signatures:**
```cpp
// Updated to include worker parameter
int process(int index, int max_secs, RestoreWorker* worker, optional_yield y);

// New coroutine-based functions
int process_shard(int index, int max_secs, RestoreWorker* worker,
                  boost::asio::yield_context yield);
int process_restore_entry(rgw::restore::RestoreEntry& entry,
                          boost::asio::yield_context yield);
```

### 3. Implementation Changes (`src/rgw/rgw_restore.cc`)

**New Includes:**
```cpp
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include "common/async/spawn_throttle.h"
```

**start_processor() - Creates Multiple Workers:**
```cpp
void Restore::start_processor()
{
  auto maxw = cct->_conf->rgw_restore_max_worker;
  workers.reserve(maxw);
  for (int ix = 0; ix < maxw; ++ix) {
    auto worker = std::make_unique<Restore::RestoreWorker>(this, cct, this, ix);
    worker->create((std::string{"rgw_restore_"} + std::to_string(ix)).c_str());
    workers.emplace_back(std::move(worker));
  }
}
```

**stop_processor() - Stops All Workers:**
```cpp
void Restore::stop_processor()
{
  down_flag = true;
  for (auto& worker : workers) {
    worker->stop();
    worker->join();
  }
  workers.clear();
}
```

**wake_worker() - Wakes All Workers:**
```cpp
void Restore::wake_worker()
{
  for (auto& worker : workers) {
    if (worker) {
      std::lock_guard<std::mutex> lock(worker->lock);
      worker->cond.notify_one();
    }
  }
}
```

**process() - Spawns Coroutine for Shard Processing:**
```cpp
int Restore::process(int index, int max_secs, RestoreWorker* worker, optional_yield y)
{
  int ret = 0;

  // Spawn a coroutine for process_shard() so it can use spawn_throttle
  boost::asio::io_context context;
  boost::asio::spawn(context,
      [this, index, max_secs, worker, &ret] (boost::asio::yield_context yield) {
        ret = process_shard(index, max_secs, worker, yield);
      },
      [] (std::exception_ptr eptr) {
        if (eptr) std::rethrow_exception(eptr);
      });
  context.run();

  return ret;
}
```

**process_shard() - Uses spawn_throttle for Concurrent Entry Processing:**
```cpp
int Restore::process_shard(int index, int max_secs, RestoreWorker* worker,
                           boost::asio::yield_context yield)
{
  // ...

  // Use spawn_throttle for concurrent entry processing within this shard
  size_t wp_limit = cct->_conf.get_val<int64_t>("rgw_restore_max_wp_worker");
  auto workpool = ceph::async::spawn_throttle{yield, wp_limit};

  // ...

  // Spawn coroutines for each entry to process concurrently
  for (auto& entry : entries) {
    workpool.spawn(
      [this, entry_copy = entry, &r_entries, &r_entries_lock, ...]
      (boost::asio::yield_context entry_yield) mutable {
        int entry_ret = process_restore_entry(entry_copy, entry_yield);
        // Handle result...
      });
  }

  // Wait for all coroutines to complete
  workpool.wait();

  // ...
}
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           RGW Instance                                   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      Restore Class                               │    │
│  │                                                                  │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │    │
│  │  │RestoreWorker │  │RestoreWorker │  │RestoreWorker │          │    │
│  │  │    (ix=0)    │  │    (ix=1)    │  │    (ix=2)    │          │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │    │
│  │         │                 │                 │                   │    │
│  │         ▼                 ▼                 ▼                   │    │
│  │  ┌──────────────────────────────────────────────────────┐      │    │
│  │  │              Shards (RADOS objects)                   │      │    │
│  │  │  restore.0, restore.1, ... restore.31 (32 default)    │      │    │
│  │  └──────────────────────────────────────────────────────┘      │    │
│  │                                                                  │    │
│  │  Per-Worker Workpool (spawn_throttle):                          │    │
│  │  ┌────────────────────────────────────────┐                     │    │
│  │  │  Coroutines (up to 128 per worker)     │                     │    │
│  │  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐      │                     │    │
│  │  │  │Entry│ │Entry│ │Entry│ │Entry│ ...  │                     │    │
│  │  │  │  1  │ │  2  │ │  3  │ │  4  │      │                     │    │
│  │  │  └─────┘ └─────┘ └─────┘ └─────┘      │                     │    │
│  │  └────────────────────────────────────────┘                     │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Processing Flow

```
┌──────────────────────┐
│  start_processor()   │
│  Creates 3 workers   │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐     ┌──────────────────────┐
│   Worker Thread 0    │     │   Worker Thread 1    │ ...
│   (rgw_restore_0)    │     │   (rgw_restore_1)    │
└──────────┬───────────┘     └──────────┬───────────┘
           │                            │
           ▼                            ▼
┌──────────────────────┐     ┌──────────────────────┐
│ process(worker, y)   │     │ process(worker, y)   │
│ Iterates shards      │     │ Iterates shards      │
└──────────┬───────────┘     └──────────────────────┘
           │
           ▼
┌──────────────────────┐
│ process(index, ...)  │
│ Spawns io_context    │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ process_shard()      │
│ - Acquires lock      │
│ - Creates workpool   │
│   (128 coroutines)   │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ For each entry:      │
│ workpool.spawn(...)  │
│   └─▶ process_       │
│       restore_entry()│
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ workpool.wait()      │
│ Trim & re-add        │
│ Release lock         │
└──────────────────────┘
```

## Concurrency Model

| Level | Mechanism | Default | Purpose |
|-------|-----------|---------|---------|
| **Worker Threads** | `std::vector<RestoreWorker>` | 3 | Process different shards in parallel |
| **Per-Shard Workpool** | `spawn_throttle` | 128 coroutines | Process entries within a shard concurrently |
| **Cross-RGW Sync** | RADOS locks | Per shard | Prevent multiple RGWs processing same shard |

## Thread Safety

- **r_entries vector**: Protected by `std::mutex r_entries_lock` for concurrent access from workpool coroutines
- **Error handling**: Uses `std::atomic<int> last_error` and `std::atomic<bool> should_stop` for safe cross-coroutine communication
- **Shard locking**: RADOS-based locks prevent concurrent processing of the same shard across workers/RGWs

## Performance Impact

| Scenario | Before | After |
|----------|--------|-------|
| Worker threads | 1 | 3 (configurable) |
| Concurrent entries per shard | 1 (sequential) | 128 (configurable) |
| Max theoretical parallelism | 1 | 3 workers × 128 coroutines = 384 |

## Configuration Examples

**Default Configuration (3 workers, 128 coroutines each):**
```ini
# No configuration needed, uses defaults
```

**High-Throughput Configuration:**
```ini
[client.rgw]
rgw_restore_max_worker = 5
rgw_restore_max_wp_worker = 256
```

**Low-Resource Configuration:**
```ini
[client.rgw]
rgw_restore_max_worker = 1
rgw_restore_max_wp_worker = 32
```

## Files Modified

| File | Changes |
|------|---------|
| `src/common/options/rgw.yaml.in` | Added `rgw_restore_max_worker` and `rgw_restore_max_wp_worker` |
| `src/rgw/rgw_restore.h` | Updated RestoreWorker class, changed to vector of workers, added new function signatures |
| `src/rgw/rgw_restore.cc` | Implemented multi-worker support, spawn_throttle workpool, coroutine-based processing |

## Comparison with LC Implementation

| Aspect | LC | Restore (New) |
|--------|----|----|
| Config: max workers | `rgw_lc_max_worker` (default: 3) | `rgw_restore_max_worker` (default: 3) |
| Config: workpool size | `rgw_lc_max_wp_worker` (default: 128) | `rgw_restore_max_wp_worker` (default: 128) |
| Workpool mechanism | `ceph::async::spawn_throttle` | `ceph::async::spawn_throttle` |
| Worker vector | `std::vector<std::unique_ptr<LCWorker>>` | `std::vector<std::unique_ptr<RestoreWorker>>` |
