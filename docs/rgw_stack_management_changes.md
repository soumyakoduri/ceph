# RGW Stack Management Changes Summary (Simplified)

This document summarizes the changes made to implement segmented stack support for the RGW (RADOS Gateway) frontend coroutines.

---

## Overview

This branch implements **optional segmented stack support** for RGW coroutines, while relying on Boost's built-in guard pages for stack overflow protection.

### Stack Overflow Protection Strategy

The RGW frontend uses `boost::context::protected_fixedsize_stack` which provides:

1. **Guard Pages**: Allocated via `mmap` with `mprotect` to create protected memory regions
2. **Hardware Protection**: Stack overflow triggers `SIGSEGV` when hitting the guard page
3. **Memory Corruption Prevention**: Guard pages prevent writes beyond stack boundaries

This is simpler than custom software-based stack checking and leverages hardware memory protection.

---

## Changes Made

### 1. CMake Module: `cmake/modules/FindSegmentedStack.cmake` (NEW)

Detects compiler and Boost support for segmented stacks:

```cmake
# Checks for:
# - GCC -fsplit-stack compiler flag support
# - Boost.Context segmented_stack availability

# Sets:
#   SEGMENTED_STACK_FOUND
#   SEGMENTED_STACK_FLAGS

# Creates target:
#   SegmentedStack::SegmentedStack
```

### 2. Build Option: `src/rgw/CMakeLists.txt`

Added optional segmented stack support:

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

### 3. Stack Allocator: `src/rgw/rgw_asio_frontend.cc`

Conditional stack allocator based on compile-time option:

```cpp
#include <boost/context/protected_fixedsize_stack.hpp>
#if defined(RGW_USE_SEGMENTED_STACKS) && defined(BOOST_USE_SEGMENTED_STACKS)
#include <boost/context/segmented_stack.hpp>
#endif

#if defined(RGW_USE_SEGMENTED_STACKS) && defined(BOOST_USE_SEGMENTED_STACKS)
// Segmented stack - grows dynamically on demand
auto make_stack_allocator([[maybe_unused]] size_t stack_size) {
  return boost::context::segmented_stack{};
}
using stack_allocator_t = boost::context::segmented_stack;
#else
// Protected fixed-size stack with guard pages (default)
auto make_stack_allocator(size_t stack_size) {
  return boost::context::protected_fixedsize_stack{stack_size};
}
using stack_allocator_t = boost::context::protected_fixedsize_stack;
#endif
```

### 4. Other Files Updated

- `src/rgw/driver/rados/rgw_bl_rados.cc` - Same conditional allocator
- `src/rgw/driver/rados/rgw_notify.cc` - Same conditional allocator
- `src/common/options/rgw.yaml.in` - Documentation comments

---

## Configuration

### Existing Option

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `rgw_frontend_coroutine_stack_size` | size | 512KB | Stack size for coroutines |

### Example Configuration

```ini
# /etc/ceph/ceph.conf
[client.rgw.mygateway]
# Increase stack size if needed (for fixed-size stacks)
rgw_frontend_coroutine_stack_size = 1048576  # 1MB
```

---

## Build Instructions

### Standard Build (Protected Fixed-Size Stacks)

```bash
cd ceph/build
cmake ..
make radosgw
```

This uses `boost::context::protected_fixedsize_stack` with:
- Configurable stack size (default 512KB)
- Guard pages for overflow protection
- SIGSEGV on stack overflow (prevents corruption)

### With Segmented Stacks (Experimental)

```bash
cd ceph/build
cmake -DWITH_RGW_SEGMENTED_STACKS=ON ..
make radosgw
```

**Requirements:**
- GCC compiler with `-fsplit-stack` support
- Boost.Context with segmented stack support
- All linked libraries must be split-stack aware

---

## Runtime Behavior

### Startup Logging

**Fixed-size stacks (default):**
```
beast frontend using protected fixed-size stacks: 524288 bytes (with guard pages)
```

**Segmented stacks:**
```
beast frontend using segmented stacks (dynamically growing)
```

### Stack Overflow Behavior

| Stack Type | Overflow Behavior |
|------------|-------------------|
| Protected fixed-size | SIGSEGV (process crash, no corruption) |
| Segmented | Stack grows automatically (until system limits) |

---

## Comparison with Full Stack Guard Branch

| Feature | This Branch (Simple) | couroutine-testing Branch |
|---------|---------------------|---------------------------|
| Segmented stacks | Yes | Yes |
| Guard pages | Yes (Boost) | Yes (Boost) |
| Pre-emptive detection | No | Yes |
| HTTP 503 on overflow | No (crash) | Yes |
| Config options | 1 | 4 |
| Code complexity | Low | Higher |

---

## File Summary

| File | Status | Description |
|------|--------|-------------|
| `cmake/modules/FindSegmentedStack.cmake` | NEW | CMake detection module |
| `docs/boost_library_guide.md` | NEW | Boost library reference |
| `docs/rgw_stack_management_changes.md` | NEW | This document |
| `src/rgw/CMakeLists.txt` | MODIFIED | Build option |
| `src/rgw/rgw_asio_frontend.cc` | MODIFIED | Conditional allocator |
| `src/rgw/driver/rados/rgw_bl_rados.cc` | MODIFIED | Conditional allocator |
| `src/rgw/driver/rados/rgw_notify.cc` | MODIFIED | Conditional allocator |
| `src/common/options/rgw.yaml.in` | MODIFIED | Documentation |

---

## References

- [Boost.Context protected_fixedsize_stack](https://www.boost.org/doc/libs/release/libs/context/doc/html/context/stack/protected_fixedsize.html)
- [Boost.Context segmented_stack](https://www.boost.org/doc/libs/release/libs/context/doc/html/context/stack/segmented.html)
- [GCC Split Stacks](https://gcc.gnu.org/wiki/SplitStacks)
