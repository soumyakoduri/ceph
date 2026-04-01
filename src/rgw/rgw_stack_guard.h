// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#ifndef RGW_STACK_GUARD_H
#define RGW_STACK_GUARD_H

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <boost/system/error_code.hpp>

/**
 * Stack Guard Utility for RGW Coroutines
 *
 * This utility provides stack overflow detection for coroutines to prevent
 * memory corruption. It tracks stack usage and can detect when a request
 * is approaching the stack limit.
 *
 * Usage:
 *   1. Create StackGuard at coroutine entry with stack bounds
 *   2. Call check_stack() at strategic points (deep operations, recursion)
 *   3. If check fails, return error to client instead of corrupting memory
 *
 * Note: For segmented stacks, overflow detection is less critical as stacks
 * grow dynamically, but we still provide a maximum depth check.
 */

namespace rgw {

// Error codes for stack guard
enum class stack_error {
  success = 0,
  stack_overflow_imminent = 1,
  stack_depth_exceeded = 2
};

// Custom error category for stack errors
class stack_error_category : public boost::system::error_category {
public:
  const char* name() const noexcept override {
    return "rgw_stack";
  }

  std::string message(int ev) const override {
    switch (static_cast<stack_error>(ev)) {
      case stack_error::success:
        return "success";
      case stack_error::stack_overflow_imminent:
        return "stack overflow imminent - request aborted for safety";
      case stack_error::stack_depth_exceeded:
        return "maximum call depth exceeded - request too complex";
      default:
        return "unknown stack error";
    }
  }
};

inline const stack_error_category& get_stack_error_category() {
  static stack_error_category instance;
  return instance;
}

inline boost::system::error_code make_error_code(stack_error e) {
  return {static_cast<int>(e), get_stack_error_category()};
}

/**
 * StackGuard - Monitors stack usage within a coroutine
 *
 * This class tracks the stack boundaries and provides methods to check
 * if the stack is approaching its limit. It should be instantiated at
 * the entry point of a coroutine and checked periodically.
 */
class StackGuard {
public:
  // Default safety margin - leave this much space before considering overflow
  static constexpr size_t DEFAULT_SAFETY_MARGIN = 16 * 1024;  // 16KB

  // Maximum recursion/call depth for segmented stacks
  static constexpr size_t DEFAULT_MAX_DEPTH = 1000;

private:
  // Stack grows downward on most architectures
  const uintptr_t stack_base_;      // High address (start of stack)
  const uintptr_t stack_limit_;     // Low address (end of usable stack)
  const size_t stack_size_;
  const size_t safety_margin_;
  const size_t max_depth_;

  std::atomic<size_t> current_depth_{0};

  // Get current stack pointer approximation
  static uintptr_t get_stack_pointer() {
    // Use address of local variable as stack pointer approximation
    volatile int stack_probe;
    return reinterpret_cast<uintptr_t>(&stack_probe);
  }

public:
  /**
   * Construct a StackGuard with known stack bounds
   *
   * @param stack_base    High address of stack (where it starts)
   * @param stack_size    Total size of the stack in bytes
   * @param safety_margin Minimum bytes to keep free before warning
   * @param max_depth     Maximum call depth for depth-based checking
   */
  StackGuard(void* stack_base, size_t stack_size,
             size_t safety_margin = DEFAULT_SAFETY_MARGIN,
             size_t max_depth = DEFAULT_MAX_DEPTH)
    : stack_base_(reinterpret_cast<uintptr_t>(stack_base)),
      stack_limit_(stack_base_ - stack_size + safety_margin),
      stack_size_(stack_size),
      safety_margin_(safety_margin),
      max_depth_(max_depth)
  {}

  /**
   * Construct a StackGuard by inferring stack position
   *
   * This constructor estimates the stack base from the current position.
   * Less accurate but works when exact bounds are unknown.
   *
   * @param stack_size    Configured stack size
   * @param safety_margin Minimum bytes to keep free
   * @param max_depth     Maximum call depth
   */
  explicit StackGuard(size_t stack_size,
                      size_t safety_margin = DEFAULT_SAFETY_MARGIN,
                      size_t max_depth = DEFAULT_MAX_DEPTH)
    : stack_base_(get_stack_pointer() + 4096), // Approximate: we're near the top
      stack_limit_(stack_base_ - stack_size + safety_margin),
      stack_size_(stack_size),
      safety_margin_(safety_margin),
      max_depth_(max_depth)
  {}

  /**
   * Check if stack usage is within safe limits
   *
   * @return error_code - success if safe, stack_overflow_imminent if dangerous
   */
  boost::system::error_code check_stack() const {
    uintptr_t current_sp = get_stack_pointer();

    // Stack grows downward: if current SP is below limit, we're in danger
    if (current_sp <= stack_limit_) {
      return make_error_code(stack_error::stack_overflow_imminent);
    }

    return {};
  }

  /**
   * Check stack and increment depth counter (for recursion tracking)
   *
   * @return error_code - success if safe, error if limits exceeded
   */
  boost::system::error_code check_and_push() {
    // Check stack space first
    auto ec = check_stack();
    if (ec) {
      return ec;
    }

    // Check depth limit
    size_t depth = current_depth_.fetch_add(1, std::memory_order_relaxed);
    if (depth >= max_depth_) {
      current_depth_.fetch_sub(1, std::memory_order_relaxed);
      return make_error_code(stack_error::stack_depth_exceeded);
    }

    return {};
  }

  /**
   * Decrement depth counter (call when leaving a tracked scope)
   */
  void pop() {
    current_depth_.fetch_sub(1, std::memory_order_relaxed);
  }

  /**
   * Get remaining stack space in bytes
   */
  size_t remaining_stack() const {
    uintptr_t current_sp = get_stack_pointer();
    if (current_sp <= stack_limit_) {
      return 0;
    }
    return current_sp - stack_limit_;
  }

  /**
   * Get percentage of stack used
   */
  double stack_usage_percent() const {
    uintptr_t current_sp = get_stack_pointer();
    if (current_sp >= stack_base_) {
      return 0.0;
    }
    size_t used = stack_base_ - current_sp;
    return (static_cast<double>(used) / stack_size_) * 100.0;
  }

  /**
   * Get current recursion depth
   */
  size_t current_depth() const {
    return current_depth_.load(std::memory_order_relaxed);
  }

  size_t stack_size() const { return stack_size_; }
  size_t safety_margin() const { return safety_margin_; }
  size_t max_depth() const { return max_depth_; }
};

/**
 * RAII helper for depth tracking
 */
class ScopedStackDepth {
  StackGuard& guard_;
  bool pushed_;

public:
  explicit ScopedStackDepth(StackGuard& guard)
    : guard_(guard), pushed_(false)
  {}

  boost::system::error_code enter() {
    auto ec = guard_.check_and_push();
    if (!ec) {
      pushed_ = true;
    }
    return ec;
  }

  ~ScopedStackDepth() {
    if (pushed_) {
      guard_.pop();
    }
  }

  // Non-copyable
  ScopedStackDepth(const ScopedStackDepth&) = delete;
  ScopedStackDepth& operator=(const ScopedStackDepth&) = delete;
};

/**
 * Lightweight stack check macro for critical sections
 *
 * Usage:
 *   RGW_CHECK_STACK(stack_guard, error_code_var);
 *   if (error_code_var) {
 *     return handle_error(error_code_var);
 *   }
 */
#define RGW_CHECK_STACK(guard, ec_var) \
  do { (ec_var) = (guard).check_stack(); } while(0)

/**
 * Quick stack check that returns immediately if stack is low
 *
 * Usage:
 *   RGW_CHECK_STACK_OR_RETURN(stack_guard, return_value);
 */
#define RGW_CHECK_STACK_OR_RETURN(guard, retval) \
  do { \
    auto _stack_ec = (guard).check_stack(); \
    if (_stack_ec) { return (retval); } \
  } while(0)

} // namespace rgw

// Enable ADL for make_error_code
namespace boost::system {
template<>
struct is_error_code_enum<rgw::stack_error> : std::true_type {};
}

#endif // RGW_STACK_GUARD_H
