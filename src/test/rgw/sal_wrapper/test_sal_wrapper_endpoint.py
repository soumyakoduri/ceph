#!/usr/bin/env python3
"""
Test script for the SAL wrapper test endpoint.

Usage:
    python3 test_sal_wrapper_endpoint.py
"""

import json
import subprocess
import tempfile
import os
import hashlib
import hmac
import datetime
from urllib.parse import urlencode, quote
import requests

# Configuration
ENDPOINT = 'http://localhost:8000'
ACCESS_KEY = 'testkey'
SECRET_KEY = 'testsecret'
BUCKET = 'sal-wrapper-test'
REGION = 'us-east-1'
SERVICE = 's3'


def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def get_signature_key(key, date_stamp, region_name, service_name):
    k_date = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    k_region = sign(k_date, region_name)
    k_service = sign(k_region, service_name)
    k_signing = sign(k_service, 'aws4_request')
    return k_signing


def make_presigned_request(method, path, query_params=None, payload=None):
    """Make a request using query string authentication (presigned URL style)."""

    host = 'localhost:8000'
    canonical_uri = path

    # Timestamps
    t = datetime.datetime.now(datetime.timezone.utc)
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = t.strftime('%Y%m%d')

    # Credential scope
    credential_scope = f'{date_stamp}/{REGION}/{SERVICE}/aws4_request'
    credential = f'{ACCESS_KEY}/{credential_scope}'

    # Query parameters for signing
    query = query_params.copy() if query_params else {}
    query['X-Amz-Algorithm'] = 'AWS4-HMAC-SHA256'
    query['X-Amz-Credential'] = credential
    query['X-Amz-Date'] = amz_date
    query['X-Amz-Expires'] = '3600'
    query['X-Amz-SignedHeaders'] = 'host'

    # Sort and encode query string
    sorted_params = sorted(query.items())
    canonical_querystring = '&'.join([f'{quote(k, safe="")}={quote(str(v), safe="")}' for k, v in sorted_params])

    # Payload hash - for presigned URLs use UNSIGNED-PAYLOAD
    payload_hash = 'UNSIGNED-PAYLOAD'

    # Canonical headers
    canonical_headers = f'host:{host}\n'
    signed_headers = 'host'

    # Canonical request
    canonical_request = '\n'.join([
        method,
        canonical_uri,
        canonical_querystring,
        canonical_headers,
        signed_headers,
        payload_hash
    ])

    # String to sign
    algorithm = 'AWS4-HMAC-SHA256'
    string_to_sign = '\n'.join([
        algorithm,
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    ])

    # Signature
    signing_key = get_signature_key(SECRET_KEY, date_stamp, REGION, SERVICE)
    signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    # Build final URL
    final_querystring = canonical_querystring + f'&X-Amz-Signature={signature}'
    url = f'{ENDPOINT}{canonical_uri}?{final_querystring}'

    # Make request
    headers = {'x-amz-content-sha256': payload_hash}
    if payload:
        headers['Content-Type'] = 'application/json'
        body = json.dumps(payload)
    else:
        body = None

    if method == 'GET':
        response = requests.get(url, headers=headers)
    elif method == 'POST':
        response = requests.post(url, headers=headers, data=body)
    else:
        raise ValueError(f"Unsupported method: {method}")

    return response


def run_aws_cmd(args, input_data=None):
    """Run an AWS CLI command with proper credentials."""
    env = os.environ.copy()
    env['AWS_ACCESS_KEY_ID'] = ACCESS_KEY
    env['AWS_SECRET_ACCESS_KEY'] = SECRET_KEY
    env['AWS_DEFAULT_REGION'] = REGION

    cmd = ['aws', '--endpoint-url', ENDPOINT] + args

    result = subprocess.run(cmd, env=env, capture_output=True, text=True, input=input_data)
    return result


def create_bucket():
    """Create the test bucket if it doesn't exist."""
    result = run_aws_cmd(['s3', 'mb', f's3://{BUCKET}'])
    if result.returncode == 0:
        print(f"Created bucket: {BUCKET}")
    elif 'BucketAlreadyOwnedByYou' in result.stderr or 'BucketAlreadyExists' in result.stderr:
        print(f"Bucket already exists: {BUCKET}")
    else:
        print(f"Bucket creation: {result.stderr}")


def run_sal_wrapper_test(test_type="all", iterations=5, object_size=1024):
    """Run the SAL Wrapper SAL wrapper tests using presigned URL authentication."""
    payload = {
        "test": test_type,
        "iterations": iterations,
        "object_size": object_size
    }

    print(f"\nRunning SAL Wrapper tests: {test_type}")
    print(f"Payload: {json.dumps(payload)}")

    # Use presigned URL authentication
    response = make_presigned_request(
        'POST',
        f'/{BUCKET}',
        query_params={'sal-wrapper-test': ''},
        payload=payload
    )

    print(f"\nStatus: {response.status_code}")
    try:
        print(f"Response:\n{json.dumps(response.json(), indent=2)}")
    except:
        print(f"Response:\n{response.text}")

    return response


def get_endpoint_info():
    """Get info about the test endpoint using presigned URL authentication."""
    print(f"\nGetting endpoint info...")

    response = make_presigned_request(
        'GET',
        f'/{BUCKET}',
        query_params={'sal-wrapper-test': ''}
    )

    print(f"Status: {response.status_code}")
    try:
        print(f"Response:\n{json.dumps(response.json(), indent=2)}")
    except:
        print(f"Response:\n{response.text}")

    return response


def test_basic_s3_ops():
    """Test basic S3 operations via AWS CLI."""
    print("\n--- Testing Basic S3 Operations ---")

    # Put object
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write("test data")
        temp_file = f.name

    try:
        result = run_aws_cmd(['s3', 'cp', temp_file, f's3://{BUCKET}/test.txt'])
        print(f"Put: {'OK' if result.returncode == 0 else 'FAILED'}")

        result = run_aws_cmd(['s3', 'ls', f's3://{BUCKET}/'])
        print(f"List: {'OK' if result.returncode == 0 else 'FAILED'}")

        result = run_aws_cmd(['s3', 'rm', f's3://{BUCKET}/test.txt'])
        print(f"Delete: {'OK' if result.returncode == 0 else 'FAILED'}")
    finally:
        os.unlink(temp_file)


if __name__ == "__main__":
    print("=" * 60)
    print("SAL Wrapper SAL Wrapper Test")
    print("=" * 60)

    # Step 1: Create bucket
    create_bucket()

    # Step 2: Test basic S3 operations
    test_basic_s3_ops()

    # Step 3: Get endpoint info (GET request with presigned URL)
    get_endpoint_info()

    # Step 4: Run all tests (POST request with presigned URL)
    run_sal_wrapper_test(test_type="all", iterations=3, object_size=1024)
