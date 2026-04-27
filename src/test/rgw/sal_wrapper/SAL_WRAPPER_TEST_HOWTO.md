# Testing SAL Wrapper Backend from Within RGW

This guide explains how to test the SAL wrapper from within a running RGW instance using the `?sal-wrapper-test` endpoint.

## Quick Start

### Step 1: Add the test endpoint to rgw_rest_s3.cc

Add the include at the top:
```cpp
#include "rgw_rest_sal_wrapper_test.h"
```

Add the handler check in `RGWHandler_REST_Bucket_S3::op_post()`:
```cpp
RGWOp *RGWHandler_REST_Bucket_S3::op_post()
{
  // Add this check for sal-wrapper-test
  if (s->info.args.exists("sal-wrapper-test")) {
    return new RGWSALWrapperTest();
  }

  if (s->info.args.exists("delete")) {
    return new RGWDeleteMultiObj_ObjStore_S3;
  }
  // ... rest of the function
}
```

### Step 2: Add to CMakeLists.txt

Add these files to the RGW build:
```cmake
# In src/rgw/CMakeLists.txt
set(rgw_common_srcs
  # ... existing files ...
  rgw_rest_sal_wrapper_test.cc
  rgw_sal_wrapper.cc
)
```

### Step 3: Build RGW

```bash
cd /home/skoduri/Documents/IBM/lancedb/lancedb-code/ceph/build
ninja radosgw
```

### Step 4: Start test cluster

```bash
cd build
../src/vstart.sh -d -n -x --rgw

# Create test user
./bin/radosgw-admin user create --uid=testuser --display-name="Test User" \
    --access-key=testkey --secret-key=testsecret

# Create bucket via S3 API
AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret \
  aws --endpoint-url http://localhost:8000 s3 mb s3://sal-wrapper-test
```

### Step 5: Run tests

```bash
# Get endpoint info
curl http://localhost:8000/sal-wrapper-test?sal-wrapper-test

# Run all tests (default config)
curl -X POST http://localhost:8000/sal-wrapper-test?sal-wrapper-test \
  -H "Content-Type: application/json" \
  -d '{}'

# Run specific test type with custom config
curl -X POST http://localhost:8000/sal-wrapper-test?sal-wrapper-test \
  -H "Content-Type: application/json" \
  -d '{
    "test": "put_get",
    "iterations": 100,
    "object_size": 4096
  }'

# Run all tests with AWS credentials
AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret \
  aws --endpoint-url http://localhost:8000 \
  s3api put-object --bucket sal-wrapper-test --key dummy --body /dev/null

curl -X POST "http://localhost:8000/sal-wrapper-test?sal-wrapper-test" \
  -H "Content-Type: application/json" \
  -d '{"test": "all", "iterations": 10}'
```

## Test Types

| Test Type | Description |
|-----------|-------------|
| `all` | Run all tests |
| `put_get` | Test put and get operations with data verification |
| `list` | Test listing objects with prefix |
| `copy` | Test object copy operations |
| `delete` | Test object deletion |

## Sample Output

```json
{
  "results": {
    "success": true,
    "tests_run": 6,
    "tests_passed": 6,
    "tests_failed": 0,
    "details": [
      {
        "test": {
          "name": "put_get_basic",
          "passed": true,
          "duration_ms": 45.23
        }
      },
      {
        "test": {
          "name": "delete_basic",
          "passed": true,
          "duration_ms": 12.45
        }
      }
    ]
  }
}
```

## Testing the Rust Crate with Real SAL

Once the test endpoint works, you can also test the Rust crate:

```bash
# Set environment
export CEPH_BUILD_DIR=/home/skoduri/Documents/IBM/lancedb/lancedb-code/ceph/build
export CEPH_SRC_DIR=/home/skoduri/Documents/IBM/lancedb/lancedb-code/ceph

# Build without mock-sal (requires linking to real Ceph libs)
cd rust/ceph-lancedb-rgw
cargo build  # No --features mock-sal

# For now, unit tests still use mock:
cargo test --features mock-sal
```

## Troubleshooting

### "sal-wrapper-test not found"
Make sure the endpoint is registered in `op_post()` and RGW is rebuilt.

### "Permission denied"
The endpoint requires authentication. Use AWS credentials or run as admin.

### "Bucket not found"
Create the test bucket first via S3 API:
```bash
AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret \
  aws --endpoint-url http://localhost:8000 s3 mb s3://sal-wrapper-test
```
