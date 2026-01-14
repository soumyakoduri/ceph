# RGW SAL C API Example

This directory contains an example program demonstrating the usage of the
RGW SAL (Storage Abstraction Layer) C bindings.

## Overview

The `rgw_sal_c_example.c` file shows how to use the C interface to interact
with Ceph's RGW storage backend. It demonstrates:

- **Driver Operations**: Creating and initializing storage drivers, getting
  cluster information and statistics
- **User Operations**: Looking up users, getting user information
- **Bucket Operations**: Listing buckets, getting bucket details
- **Object Operations**: Working with objects in buckets

## Building

### As part of Ceph build

The example can be built as part of the main Ceph build process. From your
Ceph build directory:

```bash
cd /path/to/ceph/build
ninja rgw_sal_c_example
```

### Standalone build

You can also build the example separately after Ceph has been built:

```bash
cd /path/to/ceph/examples/rgw/sal_c_example
mkdir build && cd build
cmake .. -DCEPH_SOURCE_DIR=/path/to/ceph -DCEPH_BUILD_DIR=/path/to/ceph/build
make
```

### Manual compilation

```bash
gcc -o rgw_sal_c_example rgw_sal_c_example.c \
    -I/path/to/ceph/src/include \
    -L/path/to/ceph/build/lib \
    -lrgw_sal -lrados -lpthread
```

## Running

Before running, ensure the library path includes the Ceph build directory:

```bash
export LD_LIBRARY_PATH=/path/to/ceph/build/lib:$LD_LIBRARY_PATH
```

Run with an optional ceph.conf path:

```bash
# Using default configuration
./rgw_sal_c_example

# With explicit config file
./rgw_sal_c_example /etc/ceph/ceph.conf
```

## API Reference

### Driver API

| Function | Description |
|----------|-------------|
| `rgw_sal_version()` | Get library version |
| `rgw_sal_driver_create()` | Create a driver instance |
| `rgw_sal_driver_initialize()` | Initialize the driver |
| `rgw_sal_driver_destroy()` | Destroy a driver instance |
| `rgw_sal_driver_get_name()` | Get driver name |
| `rgw_sal_driver_get_cluster_id()` | Get cluster ID |
| `rgw_sal_driver_get_cluster_stat()` | Get cluster statistics |

### User API

| Function | Description |
|----------|-------------|
| `rgw_sal_get_user()` | Get a user by ID or access key |
| `rgw_sal_user_destroy()` | Destroy a user object |
| `rgw_sal_user_get_id()` | Get user ID |
| `rgw_sal_user_get_display_name()` | Get user display name |
| `rgw_sal_user_load()` | Load full user info |
| `rgw_sal_user_info_free()` | Free user info strings |

### Bucket API

| Function | Description |
|----------|-------------|
| `rgw_sal_get_bucket()` | Get a bucket by name |
| `rgw_sal_list_buckets()` | List buckets for a user |
| `rgw_sal_bucket_destroy()` | Destroy a bucket object |
| `rgw_sal_bucket_get_name()` | Get bucket name |
| `rgw_sal_bucket_load()` | Load full bucket info |
| `rgw_sal_bucket_create_object()` | Create a new object |
| `rgw_sal_bucket_get_object()` | Get an existing object |
| `rgw_sal_bucket_info_free()` | Free bucket info strings |
| `rgw_sal_bucket_list_free()` | Free bucket list |

### Object API

| Function | Description |
|----------|-------------|
| `rgw_sal_object_destroy()` | Destroy an object handle |
| `rgw_sal_object_get_name()` | Get object name |
| `rgw_sal_object_load()` | Load object metadata |
| `rgw_sal_object_info_free()` | Free object info strings |

## Error Handling

All functions that can fail return an integer:
- **0**: Success
- **< 0**: Error (negated errno value)

Common error codes:
- `-ENOENT`: User/bucket/object not found
- `-EINVAL`: Invalid argument
- `-ENOMEM`: Out of memory
- `-EEXIST`: Already exists

## Example Output

```
RGW SAL C Interface Example
============================

Using config file: /etc/ceph/ceph.conf

RGW SAL C Library: 1.0.0
Version: 1.0.0

=== Driver Info Example ===
Driver name: rados
Cluster ID: 12345678-1234-1234-1234-123456789abc
Cluster Stats:
  Total: 1000000 KB
  Used:  500000 KB
  Free:  500000 KB
  Objects: 1234
Driver info example completed successfully.

=== User Operations Example ===
User 'testuser' not found (ret=-2) - this is expected for new clusters
User operations example completed.

...
```

## See Also

- `/src/include/rgw/rgw_sal_c.h` - C API header file
- `/src/rgw/rgw_sal_c.cc` - C API implementation
- `/src/rgw/rgw_sal.h` - C++ SAL interface
