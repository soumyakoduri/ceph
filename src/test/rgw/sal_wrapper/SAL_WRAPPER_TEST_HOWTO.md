# Testing SAL Wrapper Backend from Within RGW

This guide explains how to test the SAL wrapper from within a running RGW instance using the `?sal-wrapper-test` endpoint.

## Quick Start

### Step 1: Build RGW

The SAL wrapper test endpoint is already integrated into RGW. The following files provide the functionality:

- `src/rgw/rgw_rest_sal_wrapper_test.h` - Test endpoint header
- `src/rgw/rgw_rest_sal_wrapper_test.cc` - Test endpoint implementation
- `src/rgw/rgw_sal_wrapper.h` - SAL wrapper C API header
- `src/rgw/rgw_sal_wrapper.cc` - SAL wrapper C API implementation

Build RGW:

```bash
cd /home/skoduri/Documents/IBM/lancedb/lancedb-code/ceph/build
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

### Step 3: Run tests

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
| `all` | Run all tests (put_get, delete, head, list, copy, range_read) |
| `put_get` | Test put and get operations with data verification |
| `list` | Test listing objects with prefix |
| `copy` | Test object copy operations |
| `multipart` | Test multipart upload operations |

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

To configure the script, edit the variables at the top of `test_sal_wrapper_endpoint.py`:
```python
ENDPOINT = 'http://localhost:8000'
ACCESS_KEY = '0555b35654ad1656d804'   # Use your RGW user's access key
SECRET_KEY = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
BUCKET = 'sal-wrapper-test'
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
Make sure RGW is rebuilt with the latest code including `rgw_rest_sal_wrapper_test.cc`.

### "Permission denied"
The endpoint requires authentication. Use AWS credentials or run as admin.

### "Bucket not found"
Create the test bucket first via S3 API:
```bash
AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret \
  aws --endpoint-url http://localhost:8000 s3 mb s3://sal-wrapper-test
```
