# S3 Vector Backend Implementation Summary

**Date:** 2026-04-21
**Branch:** wip-s3vector-backend-options

## Overview

This document summarizes the implementation of configurable backend support for S3 Vectors (LanceDB integration) in Ceph RGW. The implementation allows vector data to be stored either on the local filesystem or in S3-compatible storage (local RGW or external S3 service).

## Feature Requirements

1. **Configurable Backend**: Support both local filesystem and S3 storage backends
2. **S3 Bucket Naming**: S3 bucket name = vector bucket name (same name, auto-created)
3. **Local Filesystem**: Configurable root directory (default: `/tmp/lancedb`)
4. **S3 Backend Options**:
   - **With explicit config**: Use provided endpoint, credentials, and region
   - **Without config (local RGW)**: Auto-detect from local RGW settings:
     - Endpoint: `localhost:<port>` from `rgw_frontends` config
     - SSL: Detected from `ssl_port=` vs `port=` in frontend config
     - Region: Zonegroup name
     - Credentials: Bucket owner's access keys

## Configuration Options

Added to `src/common/options/rgw.yaml.in`:

```yaml
# S3 Vectors (LanceDB) configuration
- name: rgw_s3vector_backend
  type: str
  default: local
  desc: Storage backend for S3 vectors - "local" for filesystem or "s3" for S3 storage

- name: rgw_s3vector_local_path
  type: str
  default: /tmp/lancedb
  desc: Root directory for local vector storage (when backend=local)

- name: rgw_s3vector_s3_endpoint
  type: str
  default: ""
  desc: S3 endpoint URL for vector storage (empty = use local RGW via loopback)

- name: rgw_s3vector_s3_access_key
  type: str
  default: ""
  desc: S3 access key for vector storage (empty = use bucket owner's credentials)

- name: rgw_s3vector_s3_secret_key
  type: str
  default: ""
  desc: S3 secret key for vector storage

- name: rgw_s3vector_s3_region
  type: str
  default: ""
  desc: S3 region for vector storage (empty = use zonegroup name)
```

## File Changes

### 1. `src/rgw/rgw_s3vector.h`

**New Types Added:**

```cpp
// Backend type for S3 Vector storage
enum class BackendType {
  LOCAL,  // Local filesystem storage (default)
  S3      // S3 storage backend (local RGW or external S3 service)
};

// S3 connection configuration for vector storage
struct S3ConnConfig {
  std::string endpoint;      // S3 endpoint URL
  std::string access_key;    // S3 access key
  std::string secret_key;    // S3 secret key
  std::string region;        // S3 region
  bool use_ssl = false;      // Use SSL for connection
  bool allow_insecure = true; // Allow insecure SSL (for loopback)

  bool has_credentials() const;
  bool has_endpoint() const;
};
```

**New Functions Added:**

```cpp
// Get the backend type from configuration
BackendType get_backend_type(CephContext* cct);

// Get the database path for a vector bucket based on configuration
std::string get_db_path(CephContext* cct, const std::string& vector_bucket_name);

// Check if the backend is S3 (requires S3 bucket creation)
bool is_s3_backend(CephContext* cct);

// Build S3 connection config from RGW settings (for local RGW mode)
S3ConnConfig build_local_rgw_config(int rgw_port, bool use_ssl,
                                     const std::string& zonegroup_name,
                                     const std::string& access_key,
                                     const std::string& secret_key);
```

**Updated Function Signatures:**

All s3vector functions now accept an optional `const S3ConnConfig* s3_config = nullptr` parameter:

- `create_index()`, `delete_index()`, `get_index()`, `list_indexes()`
- `create_vector_bucket()`, `delete_vector_bucket()`
- `put_vectors()`, `get_vectors()`, `list_vectors()`, `delete_vectors()`, `query_vectors()`

### 2. `src/rgw/rgw_s3vector.cc`

**New Helper Functions:**

```cpp
// Parse RGW frontend config to extract port and SSL settings
// Returns (port, use_ssl) tuple
// Parses config like "beast port=8000" or "beast ssl_port=443"
std::pair<int, bool> parse_rgw_frontend_config(CephContext* cct);

// Build S3 connection config for local RGW mode
S3ConnConfig build_local_rgw_config(int rgw_port, bool use_ssl,
                                     const std::string& zonegroup_name,
                                     const std::string& access_key,
                                     const std::string& secret_key);

// Apply S3 connection config to LanceDB builder
void apply_s3_config(LanceDBConnectBuilder*& builder, const S3ConnConfig& config,
                     DoutPrefixProvider* dpp);
```

**Updated `connect()` Function:**

The connect function now handles three scenarios:

1. **Explicit S3 config provided via parameter** - Uses the provided config
2. **Config file has endpoint/credentials** - Uses config file settings
3. **Auto-detect from local RGW** - Parses frontend config for port/SSL

```cpp
LanceDBConnection* connect(DoutPrefixProvider* dpp,
                           const std::string& vector_bucket_name,
                           const S3ConnConfig* s3_config = nullptr);
```

**Updated `get_db_path()` Function:**

Returns appropriate path based on backend type:
- Local: `{local_path}/{vector_bucket_name}`
- S3: `s3://{vector_bucket_name}`

### 3. `src/rgw/rgw_rest_s3vector.cc`

**New Helper Method in `RGWS3VectorBase`:**

```cpp
class RGWS3VectorBase : public RGWDefaultResponseOp {
protected:
  std::optional<rgw::s3vector::S3ConnConfig> cached_s3_config;

  // Build S3ConnConfig from req_state for S3 backend with local RGW
  // Returns nullptr if S3 backend is not enabled or explicit config exists
  const rgw::s3vector::S3ConnConfig* get_s3_config() {
    // 1. Check if S3 backend is enabled
    // 2. Check if explicit config exists in config file
    // 3. Auto-detect from local RGW:
    //    - Parse rgw_frontends for port and SSL
    //    - Get user credentials from s->user->get_info().access_keys
    //    - Use s->zonegroup_name as region
    // 4. Cache and return the config
  }
};
```

**S3 Bucket Auto-Creation:**

In `RGWS3VectorCreateVectorBucket::execute()`:
- When S3 backend is enabled, automatically creates a corresponding S3 bucket with the same name as the vector bucket
- This bucket stores the LanceDB vector data

In `RGWS3VectorDeleteVectorBucket::execute()`:
- When S3 backend is enabled, also deletes the corresponding S3 bucket

**Updated Handler Methods:**

All handler `execute()` methods now call `get_s3_config()` and pass it to s3vector functions:

```cpp
void execute(optional_yield y) override {
  // ... load vector bucket ...
  op_ret = rgw::s3vector::create_index(configuration, this, y, get_s3_config());
}
```

## How It Works

### Backend Selection Flow

```
rgw_s3vector_backend config
         |
         v
    +---------+
    | "local" |-----> Use local filesystem at rgw_s3vector_local_path
    +---------+
         |
         v "s3"
    +------------------+
    | Check for        |
    | explicit config  |
    +------------------+
         |
    +----+----+
    |         |
    v         v
Has config    No config
    |              |
    v              v
Use config    Auto-detect from local RGW
settings      - Port from rgw_frontends
              - SSL from rgw_frontends
              - Region from zonegroup
              - Credentials from bucket owner
```

### S3 Connection Configuration Priority

1. **Highest**: S3ConnConfig passed from REST handler (auto-detected settings)
2. **Medium**: Config file settings (rgw_s3vector_s3_endpoint, etc.)
3. **Lowest**: Auto-detect from rgw_frontends (port, SSL only)

### Database Path Generation

| Backend | Path Format |
|---------|-------------|
| Local | `/tmp/lancedb/{vector_bucket_name}` |
| S3 | `s3://{vector_bucket_name}` |

## Usage Examples

### Local Backend (Default)

```bash
# No configuration needed, uses default /tmp/lancedb
# Vector data stored at /tmp/lancedb/{vector_bucket_name}/
```

### S3 Backend with Local RGW (Auto-detect)

```bash
# In ceph.conf:
[client.rgw]
rgw_s3vector_backend = s3
rgw_frontends = beast port=8000

# Auto-detects:
# - Endpoint: http://localhost:8000
# - Region: from zonegroup
# - Credentials: from bucket owner's keys
```

### S3 Backend with SSL Local RGW

```bash
# In ceph.conf:
[client.rgw]
rgw_s3vector_backend = s3
rgw_frontends = beast ssl_port=443 ssl_certificate=/path/to/cert.pem

# Auto-detects:
# - Endpoint: https://localhost:443
# - SSL enabled with insecure mode (for loopback)
```

### S3 Backend with External S3

```bash
# In ceph.conf:
[client.rgw]
rgw_s3vector_backend = s3
rgw_s3vector_s3_endpoint = https://s3.amazonaws.com
rgw_s3vector_s3_access_key = AKIAIOSFODNN7EXAMPLE
rgw_s3vector_s3_secret_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
rgw_s3vector_s3_region = us-east-1
```

## Testing

The implementation maintains backward compatibility:
- Default backend is "local" with path `/tmp/lancedb`
- Existing tests continue to work without configuration changes

To test S3 backend:
1. Set `rgw_s3vector_backend = s3` in config
2. Ensure RGW has proper frontend configuration
3. Run s3vector tests - they should create S3 buckets with same name as vector buckets

## Commits

1. Initial backend configuration implementation
2. S3 bucket auto-creation for vector buckets
3. Auto-detection of RGW port and SSL settings
4. Integration of S3ConnConfig with REST handlers for credentials and zonegroup

## Files Modified

| File | Changes |
|------|---------|
| `src/common/options/rgw.yaml.in` | Added S3 vector backend config options |
| `src/rgw/rgw_s3vector.h` | Added BackendType, S3ConnConfig, helper functions |
| `src/rgw/rgw_s3vector.cc` | Implemented backend selection, S3 config, auto-detection |
| `src/rgw/rgw_rest_s3vector.cc` | Added get_s3_config(), S3 bucket creation/deletion |
