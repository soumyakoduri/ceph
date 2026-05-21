# Testing SAL Wrapper Backend from Within RGW

This guide explains how to test the SAL wrapper from within a running RGW instance using the `?sal-wrapper-test` endpoint.

## Quick Start

### Step 1: Build RGW

The SAL wrapper test endpoint is already integrated into RGW. The following files provide the functionality:

- `src/rgw/rgw_rest_sal_wrapper_test.h` - Test endpoint header
- `src/rgw/rgw_rest_sal_wrapper_test.cc` - Test endpoint implementation
- `src/rgw/rgw_sal_wrapper.h` - SAL wrapper C API header
- `src/rgw/rgw_sal_wrapper.cc` - SAL wrapper C API implementation

Build RGW (requires `WITH_RADOSGW_LANCEDB=ON`):

```bash
cd <ceph-source>/build
ninja radosgw
```

### Step 2: Start test cluster

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

### Step 3: Make user an admin

The SAL wrapper test endpoint requires admin privileges because it writes and deletes test objects:

```bash
./bin/radosgw-admin user modify --uid=testuser --admin
```

### Step 4: Run tests

**Note:** POST requests to run tests require admin user credentials. GET requests for endpoint info do not require authentication.

```bash
# Get endpoint info (no auth required) - note: must include bucket name in path
curl http://localhost:8000/sal-wrapper-test?sal-wrapper-test

# Use the Python test script for proper presigned URL authentication:
python3 src/test/rgw/sal_wrapper/test_sal_wrapper_endpoint.py

# The script supports environment variable overrides:
RGW_ENDPOINT=http://localhost:8000 \
  AWS_ACCESS_KEY_ID=testkey \
  AWS_SECRET_ACCESS_KEY=testsecret \
  RGW_TEST_BUCKET=sal-wrapper-test \
  python3 src/test/rgw/sal_wrapper/test_sal_wrapper_endpoint.py
```

## Test Types

| Test Type | Description |
|-----------|-------------|
| `all` | Run all tests (put_get, delete, head, list, copy, range_read) |
| `put_get` | Test put and get operations with data verification |
| `list` | Test listing objects with prefix |
| `copy` | Test object copy operations |

When running `all`, the following individual tests are executed:
- `put_get_basic` - Put/get with data verification
- `delete_basic` - Delete object and verify removal
- `head_basic` - Head object metadata
- `list_basic` - List objects with prefix
- `copy_basic` - Copy object within bucket
- `range_read` - Range read (partial object fetch)

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
      },
      {
        "test": {
          "name": "head_basic",
          "passed": true,
          "duration_ms": 8.12
        }
      },
      {
        "test": {
          "name": "list_basic",
          "passed": true,
          "duration_ms": 156.78
        }
      },
      {
        "test": {
          "name": "copy_basic",
          "passed": true,
          "duration_ms": 23.45
        }
      },
      {
        "test": {
          "name": "range_read",
          "passed": true,
          "duration_ms": 18.90
        }
      }
    ]
  }
}
```

## Using the Python Test Script

A Python test script is available for automated testing with presigned URL authentication:

```bash
cd src/test/rgw/sal_wrapper
python3 test_sal_wrapper_endpoint.py
```

The script:
1. Creates the test bucket if needed
2. Tests basic S3 operations via AWS CLI
3. Queries the endpoint info (GET request)
4. Runs all SAL wrapper tests (POST request with presigned URL auth)

Configure via environment variables or edit `test_sal_wrapper_endpoint.py`:
```bash
export RGW_ENDPOINT='http://localhost:8000'
export AWS_ACCESS_KEY_ID='your-access-key'
export AWS_SECRET_ACCESS_KEY='your-secret-key'
export RGW_TEST_BUCKET='sal-wrapper-test'
```

## Testing the Rust Crate with Real SAL

Once the test endpoint works, you can also test the Rust crate:

```bash
# Set environment (adjust paths for your setup)
export CEPH_BUILD_DIR=<ceph-source>/build
export CEPH_SRC_DIR=<ceph-source>

# Build without mock-sal (requires linking to real Ceph libs)
cd rust/ceph-lancedb-rgw
cargo build  # No --features mock-sal

# For now, unit tests still use mock:
cargo test --features mock-sal
```

## Troubleshooting

### "sal-wrapper-test not found"
Make sure RGW is rebuilt with the latest code including `rgw_rest_sal_wrapper_test.cc`.

### "Permission denied"
The endpoint requires authentication. Use AWS credentials or run as admin.

### "Bucket not found"
Create the test bucket first via S3 API:
```bash
AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret \
  aws --endpoint-url http://localhost:8000 s3 mb s3://sal-wrapper-test
```
