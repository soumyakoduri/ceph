# RGW Stack Management Changes Summary

This document summarizes the changes made to implement segmented stack support and stack overflow detection for the RGW (RADOS Gateway) frontend coroutines.

---

## Overview

Two major features were implemented:

1. **Segmented Stacks Support** - Optional compile-time feature for dynamically growing coroutine stacks
2. **Stack Guard** - Runtime stack overflow detection to prevent memory corruption

---

## 1. Segmented Stacks Support

### What Are Segmented Stacks?

Segmented stacks (also called split stacks) allow coroutine stacks to grow dynamically in non-contiguous memory segments rather than allocating a large fixed-size block upfront.

| Aspect | Fixed Stack (Current Default) | Segmented Stack |
|--------|-------------------------------|-----------------|
| Memory allocation | Full size at creation | Grows on demand |
| Memory efficiency | May waste memory | Highly efficient |
| Overhead | None | Small per-function check |
| Concurrent connections | Limited by memory | Scales to thousands |

### Files Changed

#### `cmake/modules/FindSegmentedStack.cmake` (NEW)

CMake module that detects compiler and Boost support for segmented stacks:

```cmake
# Checks for:
# 1. GCC -fsplit-stack compiler flag support
# 2. Boost.Context segmented_stack availability

# Sets variables:
#   SEGMENTED_STACK_FOUND
#   SEGMENTED_STACK_FLAGS

# Creates imported target:
#   SegmentedStack::SegmentedStack
```

#### `src/rgw/CMakeLists.txt`

Added build option for segmented stacks:

```cmake
option(WITH_RGW_SEGMENTED_STACKS "Enable segmented stacks for RGW coroutines (experimental)" OFF)

if(WITH_RGW_SEGMENTED_STACKS)
  find_package(SegmentedStack)
  if(SEGMENTED_STACK_FOUND)
    message(STATUS "RGW: Segmented stacks enabled")
  else()
    message(WARNING "RGW: Segmented stacks requested but not available")
    set(WITH_RGW_SEGMENTED_STACKS OFF)
  endif()
endif()
```

#### `src/rgw/rgw_asio_frontend.cc`

Updated stack allocator to conditionally use segmented stacks:

```cpp
// Added include
#if defined(RGW_USE_SEGMENTED_STACKS) && defined(BOOST_USE_SEGMENTED_STACKS)
#include <boost/context/segmented_stack.hpp>
#endif

// Conditional stack allocator
#if defined(RGW_USE_SEGMENTED_STACKS) && defined(BOOST_USE_SEGMENTED_STACKS)
auto make_stack_allocator([[maybe_unused]] size_t stack_size) {
  return boost::context::segmented_stack{};
}
using stack_allocator_t = boost::context::segmented_stack;
#else
auto make_stack_allocator(size_t stack_size) {
  return boost::context::protected_fixedsize_stack{stack_size};
}
using stack_allocator_t = boost::context::protected_fixedsize_stack;
#endif
```

#### `src/rgw/driver/rados/rgw_bl_rados.cc`

Updated bucket logging coroutines with same conditional stack allocator pattern.

#### `src/rgw/driver/rados/rgw_notify.cc`

Updated notification coroutines with same conditional stack allocator pattern.

### How to Enable

```bash
# Build with segmented stacks
cmake -DWITH_RGW_SEGMENTED_STACKS=ON ..
make

# Requirements:
# - GCC with -fsplit-stack support
# - Boost.Context compiled with segmented stack support
# - All linked libraries must be split-stack aware
```

---

## 2. Stack Guard (Overflow Detection)

### Purpose

Proactively detect when a coroutine is approaching its stack limit and gracefully abort the request with HTTP 503 instead of corrupting memory or crashing.

### Files Changed

#### `src/rgw/rgw_stack_guard.h` (NEW)

New header providing stack overflow detection utilities:

```cpp
namespace rgw {

// Error codes
enum class stack_error {
  success = 0,
  stack_overflow_imminent = 1,
  stack_depth_exceeded = 2
};

// Main class for stack monitoring
class StackGuard {
public:
  // Create with known stack bounds
  StackGuard(void* stack_base, size_t stack_size,
             size_t safety_margin = 16*1024,
             size_t max_depth = 1000);

  // Create by inferring stack position
  explicit StackGuard(size_t stack_size,
                      size_t safety_margin = 16*1024,
                      size_t max_depth = 1000);

  // Check if stack is within safe limits
  boost::system::error_code check_stack() const;

  // Check stack and increment depth counter
  boost::system::error_code check_and_push();

  // Decrement depth counter
  void pop();

  // Diagnostics
  size_t remaining_stack() const;
  double stack_usage_percent() const;
  size_t current_depth() const;
};

// RAII helper for depth tracking
class ScopedStackDepth { ... };

// Convenience macros
#define RGW_CHECK_STACK(guard, ec_var)
#define RGW_CHECK_STACK_OR_RETURN(guard, retval)

} // namespace rgw
```

#### `src/common/options/rgw.yaml.in`

Added three new configuration options:

```yaml
- name: rgw_frontend_stack_guard_enabled
  type: bool
  level: advanced
  desc: Enable stack overflow detection for frontend coroutines.
  default: true

- name: rgw_frontend_stack_safety_margin
  type: size
  level: advanced
  desc: Minimum stack space to keep free before aborting request.
  default: 16_K
  min: 4_K
  max: 128_K

- name: rgw_frontend_max_call_depth
  type: int
  level: advanced
  desc: Maximum call depth for request processing.
  default: 500
  min: 0
  max: 10000
```

#### `src/rgw/rgw_asio_frontend.cc`

Integrated stack guard into connection handling:

```cpp
// Added include
#include "rgw_stack_guard.h"

// Configuration structure
struct StackGuardConfig {
  bool enabled = true;
  size_t stack_size = 512 * 1024;
  size_t safety_margin = 16 * 1024;
  size_t max_depth = 500;
};

// Updated handle_connection signature
template <typename Stream>
void handle_connection(...,
                       const StackGuardConfig& stack_config,
                       ...);

// Stack check at connection start
std::optional<rgw::StackGuard> stack_guard;
if (stack_config.enabled) {
  stack_guard.emplace(stack_config.stack_size,
                      stack_config.safety_margin,
                      stack_config.max_depth);
  auto stack_ec = stack_guard->check_stack();
  if (stack_ec) {
    // Log and return error
  }
}

// Stack check before request processing
if (stack_guard) {
  auto stack_ec = stack_guard->check_stack();
  if (stack_ec) {
    // Send HTTP 503 response
    http::response<http::string_body> response;
    response.result(http::status::service_unavailable);
    response.body() = "Service temporarily unavailable: server resource limit reached";
    // ...
  }
}

// AsioFrontend class updated
class AsioFrontend {
  StackGuardConfig stack_guard_config;  // New member

  AsioFrontend(...) {
    // Initialize from config
    stack_guard_config.enabled = ctx()->_conf->rgw_frontend_stack_guard_enabled;
    stack_guard_config.stack_size = coroutine_stack_size;
    stack_guard_config.safety_margin = ctx()->_conf->rgw_frontend_stack_safety_margin;
    stack_guard_config.max_depth = ctx()->_conf->rgw_frontend_max_call_depth;
  }
};
```

---

## Configuration Reference

### Existing Options (Updated Documentation)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `rgw_frontend_coroutine_stack_size` | size | 512KB | Stack size for coroutines (ignored when segmented stacks enabled) |

### New Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `rgw_frontend_stack_guard_enabled` | bool | true | Enable stack overflow detection |
| `rgw_frontend_stack_safety_margin` | size | 16KB | Minimum stack space to keep free |
| `rgw_frontend_max_call_depth` | int | 500 | Maximum recursion depth allowed |

### Example Configuration

```ini
# /etc/ceph/ceph.conf
[client.rgw.mygateway]
# Stack size (for fixed-size stacks)
rgw_frontend_coroutine_stack_size = 1048576  # 1MB

# Stack guard settings
rgw_frontend_stack_guard_enabled = true
rgw_frontend_stack_safety_margin = 32768     # 32KB safety margin
rgw_frontend_max_call_depth = 300            # Conservative depth limit
```

---

## Runtime Behavior

### Startup Logging

```
beast frontend using fixed-size stacks: 524288 bytes
beast frontend stack guard enabled: safety_margin=16384 bytes, max_depth=500
```

Or with segmented stacks:
```
beast frontend using segmented stacks (dynamically growing)
beast frontend stack guard enabled: safety_margin=16384 bytes, max_depth=500
```

### When Stack Limit Is Reached

**Log output:**
```
ERROR: stack overflow imminent before request processing, remaining: 8192 bytes, usage: 98.4%, aborting request
====== req done http_status=503 (stack limit) ======
```

**HTTP Response:**
```
HTTP/1.1 503 Service Unavailable
Content-Type: text/plain

Service temporarily unavailable: server resource limit reached
```

---

## Build Instructions

### Standard Build (Fixed-Size Stacks + Stack Guard)

```bash
cd ceph/build
cmake ..
make radosgw
```

### With Segmented Stacks (Experimental)

```bash
cd ceph/build
cmake -DWITH_RGW_SEGMENTED_STACKS=ON ..
make radosgw
```

**Requirements for segmented stacks:**
- GCC compiler (Clang has limited support)
- `-fsplit-stack` compiler flag support
- Boost.Context built with segmented stack support
- All linked libraries must be split-stack aware

---

## File Summary

| File | Status | Description |
|------|--------|-------------|
| `cmake/modules/FindSegmentedStack.cmake` | NEW | CMake detection module |
| `src/rgw/rgw_stack_guard.h` | NEW | Stack overflow detection utility |
| `docs/boost_library_guide.md` | NEW | Comprehensive Boost library guide |
| `docs/rgw_stack_management_changes.md` | NEW | This summary document |
| `src/rgw/CMakeLists.txt` | MODIFIED | Added build option |
| `src/rgw/rgw_asio_frontend.cc` | MODIFIED | Integrated both features |
| `src/rgw/driver/rados/rgw_bl_rados.cc` | MODIFIED | Segmented stack support |
| `src/rgw/driver/rados/rgw_notify.cc` | MODIFIED | Segmented stack support |
| `src/common/options/rgw.yaml.in` | MODIFIED | Added 3 new config options |

---

## Testing Recommendations

1. **Stack Guard Testing**
   - Set `rgw_frontend_stack_safety_margin` to a high value (e.g., 256KB) to trigger guards
   - Verify HTTP 503 responses are returned
   - Check logs for stack usage metrics

2. **Segmented Stack Testing**
   - Build with `-DWITH_RGW_SEGMENTED_STACKS=ON`
   - Monitor memory usage with many concurrent connections
   - Compare memory footprint vs fixed-size stacks

3. **Performance Testing**
   - Benchmark request latency with stack guard enabled vs disabled
   - Measure overhead of stack checks (expected to be minimal)

---

## Rollback Instructions

To disable these features:

```ini
# Disable stack guard at runtime
rgw_frontend_stack_guard_enabled = false
```

To build without segmented stack support (default):
```bash
cmake -DWITH_RGW_SEGMENTED_STACKS=OFF ..
```

---

## References

- [Boost.Context Documentation](https://www.boost.org/doc/libs/release/libs/context/doc/html/index.html)
- [GCC Split Stacks](https://gcc.gnu.org/wiki/SplitStacks)
- [Ceph RGW Configuration](https://docs.ceph.com/en/latest/radosgw/config-ref/)
