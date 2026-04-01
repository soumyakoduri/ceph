# C++ Boost Library - Most Used Features Guide

This document covers the most commonly used features of the C++ Boost library with concise examples.

---

## Table of Contents

1. [Smart Pointers](#1-smart-pointers)
2. [Boost.Asio (Asynchronous I/O)](#2-boostasio-asynchronous-io)
3. [Boost.Coroutine/Context](#3-boostcoroutinecontext)
4. [Boost.Optional](#4-boostoptional)
5. [Boost.Variant](#5-boostvariant)
6. [Boost.Filesystem](#6-boostfilesystem)
7. [Boost.Thread](#7-boostthread)
8. [Boost.Container](#8-boostcontainer)
9. [Boost.Intrusive](#9-boostintrusive)
10. [Boost.Beast (HTTP/WebSocket)](#10-boostbeast-httpwebsocket)
11. [Boost.Serialization](#11-boostserialization)
12. [Boost.Regex](#12-boostregex)
13. [Segmented Stacks](#13-segmented-stacks)
14. [RGW Frontend Segmented Stack Changes](#14-rgw-frontend-segmented-stack-changes)
15. [Stack Overflow Detection (Stack Guard)](#15-stack-overflow-detection-stack-guard)

---

## 1. Smart Pointers

Boost smart pointers provide automatic memory management.

```cpp
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/weak_ptr.hpp>

// shared_ptr - reference counted pointer
boost::shared_ptr<MyClass> ptr = boost::make_shared<MyClass>(arg1, arg2);

// weak_ptr - non-owning observer
boost::weak_ptr<MyClass> weak = ptr;
if (auto locked = weak.lock()) {
    locked->doSomething();
}

// intrusive_ptr - for objects with embedded reference count
class RefCounted {
    std::atomic<int> ref_count{0};
    friend void intrusive_ptr_add_ref(RefCounted* p) { ++p->ref_count; }
    friend void intrusive_ptr_release(RefCounted* p) {
        if (--p->ref_count == 0) delete p;
    }
};
boost::intrusive_ptr<RefCounted> iptr{new RefCounted()};
```

---

## 2. Boost.Asio (Asynchronous I/O)

Core library for network and asynchronous programming.

### Basic TCP Server

```cpp
#include <boost/asio.hpp>

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

int main() {
    asio::io_context io_ctx;
    tcp::acceptor acceptor(io_ctx, tcp::endpoint(tcp::v4(), 8080));

    tcp::socket socket(io_ctx);
    acceptor.accept(socket);

    std::string message = "Hello from server!";
    asio::write(socket, asio::buffer(message));

    io_ctx.run();
}
```

### Async Operations

```cpp
#include <boost/asio.hpp>

void async_read_handler(const boost::system::error_code& ec, std::size_t bytes) {
    if (!ec) {
        std::cout << "Read " << bytes << " bytes\n";
    }
}

// Async read
std::array<char, 1024> buffer;
socket.async_read_some(asio::buffer(buffer), async_read_handler);

// Using lambda
socket.async_write_some(asio::buffer(data),
    [](const boost::system::error_code& ec, std::size_t bytes) {
        if (!ec) std::cout << "Wrote " << bytes << " bytes\n";
    });
```

### Timers

```cpp
#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>

asio::io_context io_ctx;
asio::steady_timer timer(io_ctx, std::chrono::seconds(5));

// Synchronous wait
timer.wait();

// Asynchronous wait
timer.async_wait([](const boost::system::error_code& ec) {
    if (!ec) std::cout << "Timer expired!\n";
});
io_ctx.run();
```

### Strands (Serialized Execution)

```cpp
#include <boost/asio.hpp>

asio::io_context io_ctx;
auto strand = asio::make_strand(io_ctx);

// Post work to strand - ensures serialized execution
asio::post(strand, []() { /* handler 1 */ });
asio::post(strand, []() { /* handler 2 - runs after handler 1 */ });
```

---

## 3. Boost.Coroutine/Context

Stackful coroutines for cooperative multitasking.

### Using spawn() with yield_context

```cpp
#include <boost/asio/spawn.hpp>
#include <boost/asio.hpp>

namespace asio = boost::asio;

void coroutine_handler(asio::yield_context yield) {
    asio::io_context& io_ctx =
        static_cast<asio::io_context&>(yield.get_executor().context());

    asio::steady_timer timer(io_ctx);
    timer.expires_after(std::chrono::seconds(1));

    boost::system::error_code ec;
    timer.async_wait(yield[ec]);  // Suspend here, resume when timer fires

    if (!ec) {
        std::cout << "Timer completed!\n";
    }
}

int main() {
    asio::io_context io_ctx;
    asio::spawn(io_ctx, coroutine_handler);
    io_ctx.run();
}
```

### Custom Stack Allocator

```cpp
#include <boost/asio/spawn.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>

// Protected fixed-size stack with guard pages
auto make_stack_allocator(size_t stack_size) {
    return boost::context::protected_fixedsize_stack{stack_size};
}

asio::spawn(io_ctx,
    std::allocator_arg,
    make_stack_allocator(512 * 1024),  // 512KB stack
    [](asio::yield_context yield) {
        // coroutine body
    },
    [](std::exception_ptr eptr) {
        if (eptr) std::rethrow_exception(eptr);
    });
```

### Stackless Coroutines (BOOST_ASIO_CORO_REENTER)

```cpp
#include <boost/asio/coroutine.hpp>

class MyCoroutine : boost::asio::coroutine {
public:
    void operator()(boost::system::error_code ec = {}) {
        BOOST_ASIO_CORO_REENTER(this) {
            // First step
            BOOST_ASIO_CORO_YIELD async_operation1(std::move(*this));

            // Second step (after async_operation1 completes)
            BOOST_ASIO_CORO_YIELD async_operation2(std::move(*this));

            // Final step
            complete();
        }
    }
};
```

---

## 4. Boost.Optional

Represents optional (nullable) values.

```cpp
#include <boost/optional.hpp>

boost::optional<int> find_value(const std::map<std::string, int>& m,
                                 const std::string& key) {
    auto it = m.find(key);
    if (it != m.end()) {
        return it->second;
    }
    return boost::none;
}

// Usage
auto result = find_value(my_map, "key");
if (result) {
    std::cout << "Found: " << *result << "\n";
} else {
    std::cout << "Not found\n";
}

// With default value
int value = result.value_or(42);
```

---

## 5. Boost.Variant

Type-safe union container.

```cpp
#include <boost/variant.hpp>

// Define variant type
using MyVariant = boost::variant<int, std::string, double>;

MyVariant v = 42;
v = "hello";
v = 3.14;

// Visitor pattern
struct MyVisitor : boost::static_visitor<void> {
    void operator()(int i) const { std::cout << "int: " << i << "\n"; }
    void operator()(const std::string& s) const { std::cout << "string: " << s << "\n"; }
    void operator()(double d) const { std::cout << "double: " << d << "\n"; }
};

boost::apply_visitor(MyVisitor(), v);

// Get specific type (throws if wrong type)
try {
    int i = boost::get<int>(v);
} catch (const boost::bad_get& e) {
    // Handle type mismatch
}
```

---

## 6. Boost.Filesystem

Cross-platform filesystem operations.

```cpp
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

// Path operations
fs::path p = "/home/user/documents";
fs::path file = p / "report.txt";  // Path concatenation

// Check existence
if (fs::exists(file)) {
    std::cout << "File size: " << fs::file_size(file) << "\n";
}

// Create directories
fs::create_directories("/tmp/my/nested/dir");

// Iterate directory
for (const auto& entry : fs::directory_iterator(p)) {
    if (fs::is_regular_file(entry)) {
        std::cout << entry.path().filename() << "\n";
    }
}

// Recursive iteration
for (const auto& entry : fs::recursive_directory_iterator(p)) {
    std::cout << entry.path() << "\n";
}

// Remove files
fs::remove(file);
fs::remove_all(p);  // Recursive remove
```

---

## 7. Boost.Thread

Threading primitives and synchronization.

```cpp
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

// Basic thread
boost::thread t([]() {
    std::cout << "Running in thread\n";
});
t.join();

// Thread group
boost::thread_group tg;
for (int i = 0; i < 4; ++i) {
    tg.create_thread([i]() {
        std::cout << "Thread " << i << "\n";
    });
}
tg.join_all();

// Mutex and lock guard
boost::mutex mtx;
{
    boost::lock_guard<boost::mutex> lock(mtx);
    // Critical section
}

// Shared mutex (read-write lock)
boost::shared_mutex rw_mtx;
{
    boost::shared_lock<boost::shared_mutex> read_lock(rw_mtx);  // Reader
    // Multiple readers allowed
}
{
    boost::unique_lock<boost::shared_mutex> write_lock(rw_mtx);  // Writer
    // Exclusive access
}

// Condition variable
boost::condition_variable cv;
bool ready = false;

// Producer
{
    boost::unique_lock<boost::mutex> lock(mtx);
    ready = true;
    cv.notify_one();
}

// Consumer
{
    boost::unique_lock<boost::mutex> lock(mtx);
    cv.wait(lock, [&]() { return ready; });
}
```

---

## 8. Boost.Container

Extended container types.

```cpp
#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/container/static_vector.hpp>

// flat_map - sorted vector-based map (cache-friendly)
boost::container::flat_map<std::string, int> fmap;
fmap["one"] = 1;
fmap["two"] = 2;

// flat_set - sorted vector-based set
boost::container::flat_set<int> fset = {3, 1, 4, 1, 5};

// small_vector - stack-allocated for small sizes, heap for larger
boost::container::small_vector<int, 8> sv;  // 8 elements on stack
for (int i = 0; i < 20; ++i) sv.push_back(i);  // Moves to heap after 8

// static_vector - fixed capacity, no heap allocation
boost::container::static_vector<int, 100> static_vec;
static_vec.push_back(42);  // Always on stack, max 100 elements
```

---

## 9. Boost.Intrusive

Intrusive containers - objects contain their own hooks.

```cpp
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

namespace bi = boost::intrusive;

// List hook embedded in object
class MyItem : public bi::list_base_hook<> {
public:
    int value;
    MyItem(int v) : value(v) {}
};

// Create intrusive list
bi::list<MyItem> my_list;

MyItem a(1), b(2), c(3);
my_list.push_back(a);
my_list.push_back(b);
my_list.push_back(c);

// Iterate - no memory allocation for container nodes!
for (auto& item : my_list) {
    std::cout << item.value << "\n";
}

// Remove without destroying object
my_list.erase(my_list.iterator_to(b));
```

---

## 10. Boost.Beast (HTTP/WebSocket)

High-level HTTP and WebSocket library built on Asio.

```cpp
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;

// HTTP Request
http::request<http::string_body> req{http::verb::get, "/", 11};
req.set(http::field::host, "example.com");
req.set(http::field::user_agent, "boost-beast");

// HTTP Response
http::response<http::string_body> res;
res.result(http::status::ok);
res.set(http::field::content_type, "text/html");
res.body() = "<html><body>Hello</body></html>";
res.prepare_payload();

// Read HTTP request (async with yield_context)
beast::flat_buffer buffer;
http::request<http::string_body> request;
http::async_read(socket, buffer, request, yield[ec]);

// Write HTTP response
http::async_write(socket, response, yield[ec]);

// WebSocket
websocket::stream<tcp::socket> ws{std::move(socket)};
ws.async_accept(yield[ec]);
ws.async_write(asio::buffer("Hello WebSocket"), yield[ec]);
```

---

## 11. Boost.Serialization

Object serialization to various formats.

```cpp
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <fstream>

class MyClass {
    friend class boost::serialization::access;

    int x;
    std::string name;
    std::vector<int> data;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & x;
        ar & name;
        ar & data;
    }
public:
    MyClass() = default;
    MyClass(int x, std::string n) : x(x), name(std::move(n)) {}
};

// Save
{
    std::ofstream ofs("data.txt");
    boost::archive::text_oarchive oa(ofs);
    MyClass obj(42, "test");
    oa << obj;
}

// Load
{
    std::ifstream ifs("data.txt");
    boost::archive::text_iarchive ia(ifs);
    MyClass obj;
    ia >> obj;
}
```

---

## 12. Boost.Regex

Regular expression library.

```cpp
#include <boost/regex.hpp>

std::string text = "The email is user@example.com";
boost::regex pattern(R"((\w+)@(\w+)\.(\w+))");

// Search
boost::smatch match;
if (boost::regex_search(text, match, pattern)) {
    std::cout << "Full match: " << match[0] << "\n";
    std::cout << "User: " << match[1] << "\n";
    std::cout << "Domain: " << match[2] << "\n";
}

// Replace
std::string result = boost::regex_replace(text, pattern, "REDACTED");

// Match entire string
if (boost::regex_match("user@example.com", pattern)) {
    std::cout << "Valid email format\n";
}
```

---

## 13. Segmented Stacks

### What are Segmented Stacks?

Segmented stacks (also called split stacks or growable stacks) are a mechanism where the stack grows dynamically in non-contiguous segments rather than using a single large contiguous memory block.

### How They Work

1. **Initial Small Stack**: Coroutine starts with a small stack (e.g., 4KB)
2. **Prologue Check**: Each function's prologue checks available stack space
3. **Dynamic Growth**: If space is insufficient, a new segment is allocated and linked
4. **Segment Chain**: Multiple segments form a linked list

```
Traditional Fixed Stack:          Segmented Stack:
+------------------+              +--------+
|                  |              | Seg 3  |--+
|   512KB Fixed    |              +--------+  |
|                  |                          v
|                  |              +--------+
+------------------+              | Seg 2  |--+
                                  +--------+  |
                                              v
                                  +--------+
                                  | Seg 1  |
                                  +--------+
```

### Advantages

| Aspect | Fixed Stack | Segmented Stack |
|--------|-------------|-----------------|
| Memory Usage | Full size allocated upfront | Grows on demand |
| Scalability | Limited by memory | Thousands of coroutines |
| Overhead | None | Small per-function check |
| Complexity | Simple | More complex |

### Compiler Support

Segmented stacks require compiler support via the `-fsplit-stack` flag:

```bash
# GCC
g++ -fsplit-stack -o myapp myapp.cpp

# Clang (limited support)
clang++ -fsplit-stack -o myapp myapp.cpp
```

### Boost.Context Segmented Stack Allocator

```cpp
#include <boost/context/segmented_stack.hpp>

// Create segmented stack allocator
auto make_segmented_stack_allocator() {
    return boost::context::segmented_stack{};
}

// Use with Boost.Asio spawn
asio::spawn(io_ctx,
    std::allocator_arg,
    boost::context::segmented_stack{},
    [](asio::yield_context yield) {
        // Coroutine with dynamically growing stack
    },
    [](std::exception_ptr eptr) {
        if (eptr) std::rethrow_exception(eptr);
    });
```

### Limitations

1. **Compiler Requirement**: Requires `-fsplit-stack` flag for the entire codebase
2. **Platform Support**: Best on Linux with GCC; limited on other platforms
3. **Runtime Overhead**: Each function call has a small stack check overhead
4. **Library Compatibility**: All linked libraries must also use split-stack or be "split-stack aware"
5. **Debugging**: Stack traces can be harder to follow across segments

### Checking Support

```cpp
#include <boost/context/detail/config.hpp>

#if defined(BOOST_USE_SEGMENTED_STACKS)
    // Segmented stacks available
    using stack_allocator = boost::context::segmented_stack;
#else
    // Fall back to protected fixed-size
    using stack_allocator = boost::context::protected_fixedsize_stack;
#endif
```

---

## 14. RGW Frontend Segmented Stack Changes

The RGW (RADOS Gateway) frontend currently uses **protected fixed-size stacks** for its coroutines. Here's an analysis of what changes would be needed to implement segmented stacks.

### Current Implementation

**File: `src/rgw/rgw_asio_frontend.cc`**

```cpp
// Current: Fixed-size stack allocator with guard pages
auto make_stack_allocator(size_t stack_size) {
    return boost::context::protected_fixedsize_stack{stack_size};
}

// Configuration (src/common/options/rgw.yaml.in)
- name: rgw_frontend_coroutine_stack_size
  type: size
  default: 512_K
  min: 64_K
  max: 16_M
```

Coroutines are spawned with:
```cpp
boost::asio::spawn(make_strand(context),
    std::allocator_arg,
    make_stack_allocator(coroutine_stack_size),
    [this, ...](boost::asio::yield_context yield) {
        // Handle connection
    },
    [](std::exception_ptr eptr) { ... });
```

### Changes Required for Segmented Stacks

#### 1. Build System Changes (`CMakeLists.txt`)

```cmake
# Add compiler flag detection
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-fsplit-stack" HAVE_SPLIT_STACK)

option(WITH_SEGMENTED_STACKS "Enable segmented stacks for coroutines" OFF)

if(WITH_SEGMENTED_STACKS)
    if(NOT HAVE_SPLIT_STACK)
        message(FATAL_ERROR "Segmented stacks requested but -fsplit-stack not supported")
    endif()

    # Add to global compile flags
    add_compile_options(-fsplit-stack)
    add_definitions(-DRGW_USE_SEGMENTED_STACKS)

    # IMPORTANT: Link with split-stack aware libraries
    # All dependencies must be compiled with -fsplit-stack
endif()
```

#### 2. Stack Allocator Changes (`src/rgw/rgw_asio_frontend.cc`)

```cpp
#include <boost/context/protected_fixedsize_stack.hpp>

#if defined(RGW_USE_SEGMENTED_STACKS) && defined(BOOST_USE_SEGMENTED_STACKS)
#include <boost/context/segmented_stack.hpp>
#endif

// Conditional stack allocator
#if defined(RGW_USE_SEGMENTED_STACKS) && defined(BOOST_USE_SEGMENTED_STACKS)

auto make_stack_allocator([[maybe_unused]] size_t stack_size) {
    // Segmented stack - size is just initial hint, grows dynamically
    return boost::context::segmented_stack{};
}

#else

// Current implementation - protected fixed-size stack
auto make_stack_allocator(size_t stack_size) {
    return boost::context::protected_fixedsize_stack{stack_size};
}

#endif
```

#### 3. Configuration Changes (`src/common/options/rgw.yaml.in`)

```yaml
- name: rgw_frontend_use_segmented_stacks
  type: bool
  level: advanced
  default: false
  desc: >
    Use segmented (growable) stacks for beast frontend coroutines.
    Requires compilation with -fsplit-stack. When enabled,
    rgw_frontend_coroutine_stack_size is used as initial size only.
  long_desc: >
    Segmented stacks allow coroutines to start with small stacks that
    grow on demand, reducing memory usage when handling many concurrent
    connections. However, this adds per-function-call overhead for
    stack boundary checks.

- name: rgw_frontend_coroutine_initial_stack_size
  type: size
  level: advanced
  default: 8_K
  min: 4_K
  max: 64_K
  desc: Initial stack size for segmented stacks (only used when segmented stacks enabled)
```

#### 4. Runtime Detection (`src/rgw/rgw_asio_frontend.cc`)

For runtime selection between fixed and segmented stacks:

```cpp
class AsioFrontend {
    // ...
    bool use_segmented_stacks;
    size_t coroutine_stack_size;
    size_t initial_stack_size;  // For segmented stacks

public:
    AsioFrontend() {
        use_segmented_stacks = ctx()->_conf->rgw_frontend_use_segmented_stacks;
        coroutine_stack_size = ctx()->_conf->rgw_frontend_coroutine_stack_size;
        initial_stack_size = ctx()->_conf->rgw_frontend_coroutine_initial_stack_size;

#if !defined(RGW_USE_SEGMENTED_STACKS) || !defined(BOOST_USE_SEGMENTED_STACKS)
        if (use_segmented_stacks) {
            ldout(cct, 0) << "WARNING: Segmented stacks requested but not compiled in. "
                          << "Using fixed-size stacks." << dendl;
            use_segmented_stacks = false;
        }
#endif
    }
};

// Template-based spawning for type erasure
template<typename StackAlloc, typename Handler>
void spawn_with_allocator(asio::io_context& ctx, StackAlloc&& alloc, Handler&& handler) {
    asio::spawn(make_strand(ctx),
        std::allocator_arg,
        std::forward<StackAlloc>(alloc),
        std::forward<Handler>(handler),
        [](std::exception_ptr eptr) {
            if (eptr) std::rethrow_exception(eptr);
        });
}

void spawn_connection_handler(/*...*/) {
#if defined(RGW_USE_SEGMENTED_STACKS) && defined(BOOST_USE_SEGMENTED_STACKS)
    if (use_segmented_stacks) {
        spawn_with_allocator(context,
            boost::context::segmented_stack{},
            [this, ...](asio::yield_context yield) {
                handle_connection(...);
            });
        return;
    }
#endif
    spawn_with_allocator(context,
        boost::context::protected_fixedsize_stack{coroutine_stack_size},
        [this, ...](asio::yield_context yield) {
            handle_connection(...);
        });
}
```

#### 5. Other Files to Update

The same changes apply to other RGW files using coroutines:

| File | Current Stack Size |
|------|-------------------|
| `src/rgw/driver/rados/rgw_bl_rados.cc` | 128KB fixed |
| `src/rgw/driver/rados/rgw_notify.cc` | 128KB fixed |

#### 6. Testing Considerations

```cpp
// Add to test suite
#ifdef RGW_USE_SEGMENTED_STACKS
TEST(RGWFrontend, SegmentedStackDeepRecursion) {
    // Test that segmented stacks handle deep call stacks
    // that would overflow a small fixed stack
}

TEST(RGWFrontend, SegmentedStackManyCoroutines) {
    // Test memory efficiency with many concurrent connections
    // Should use less memory than fixed stacks
}
#endif
```

### Memory Comparison

| Scenario | Fixed Stack (512KB each) | Segmented Stack (8KB initial) |
|----------|-------------------------|-------------------------------|
| 100 connections | 50 MB | ~1 MB (grows as needed) |
| 1000 connections | 500 MB | ~10 MB |
| 10000 connections | 5 GB | ~100 MB |

### Recommendations

1. **Keep Fixed Stacks as Default**: Segmented stacks add overhead and complexity
2. **Offer as Build Option**: For memory-constrained deployments
3. **Thorough Testing**: All code paths must be tested with segmented stacks
4. **Document Dependencies**: All linked libraries need split-stack support
5. **Monitor Performance**: The per-call overhead may impact latency-sensitive operations

### Implementation Priority

1. **Phase 1**: Add CMake detection and build flag
2. **Phase 2**: Implement conditional allocator in `rgw_asio_frontend.cc`
3. **Phase 3**: Add configuration options
4. **Phase 4**: Update other RGW coroutine users
5. **Phase 5**: Testing and documentation

---

## 15. Stack Overflow Detection (Stack Guard)

In addition to segmented stacks, the RGW frontend includes a **Stack Guard** feature that proactively detects stack overflow conditions before they cause memory corruption.

### How It Works

The Stack Guard monitors stack usage by:
1. Tracking the stack base address and size at coroutine creation
2. Checking remaining stack space before critical operations
3. Aborting requests with HTTP 503 if stack is nearly exhausted

```cpp
#include "rgw_stack_guard.h"

// Create a stack guard at coroutine entry
rgw::StackGuard guard(stack_size, safety_margin, max_depth);

// Check before intensive operations
auto ec = guard.check_stack();
if (ec) {
    // Stack overflow imminent - abort safely
    return send_503_response();
}

// Use RAII for depth tracking
rgw::ScopedStackDepth depth_guard(guard);
if (auto ec = depth_guard.enter(); ec) {
    // Max depth exceeded
    return send_503_response();
}
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `rgw_frontend_stack_guard_enabled` | bool | true | Enable stack overflow detection |
| `rgw_frontend_stack_safety_margin` | size | 16KB | Minimum stack space to keep free |
| `rgw_frontend_max_call_depth` | int | 500 | Maximum recursion depth allowed |

### Example Configuration

```ini
# ceph.conf
[client.rgw]
rgw_frontend_stack_guard_enabled = true
rgw_frontend_stack_safety_margin = 32768   # 32KB safety margin
rgw_frontend_max_call_depth = 300          # Lower depth limit
rgw_frontend_coroutine_stack_size = 1048576  # 1MB stack
```

### Benefits

1. **Prevents Memory Corruption**: Catches stack overflow before it happens
2. **Graceful Degradation**: Returns HTTP 503 instead of crashing
3. **Diagnostic Logging**: Logs stack usage percentage for debugging
4. **Minimal Overhead**: Only a few pointer comparisons per check

### When Stack Guard Triggers

The guard returns an error when:
- Remaining stack space falls below `safety_margin` bytes
- Call depth exceeds `max_call_depth` (for recursive operations)

Log message when triggered:
```
ERROR: stack overflow imminent before request processing, remaining: 8192 bytes, usage: 98.4%, aborting request
```

---

## References

- [Boost C++ Libraries Documentation](https://www.boost.org/doc/libs/)
- [Boost.Asio](https://www.boost.org/doc/libs/release/doc/html/boost_asio.html)
- [Boost.Context](https://www.boost.org/doc/libs/release/libs/context/doc/html/index.html)
- [GCC Split Stacks](https://gcc.gnu.org/wiki/SplitStacks)
- [Ceph RGW Documentation](https://docs.ceph.com/en/latest/radosgw/)
