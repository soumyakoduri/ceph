# FindSegmentedStack.cmake
# -------------------------
# Detect compiler support for segmented (split) stacks.
#
# This module sets the following variables:
#   SEGMENTED_STACK_FOUND       - True if segmented stacks are supported
#   SEGMENTED_STACK_FLAGS       - Compiler flags needed for segmented stacks
#
# And defines the following imported target:
#   SegmentedStack::SegmentedStack - Interface library for segmented stack support

include(CheckCXXCompilerFlag)
include(CMakePushCheckState)

# Check for -fsplit-stack support
cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_FLAGS "-fsplit-stack")
check_cxx_compiler_flag("-fsplit-stack" HAVE_SPLIT_STACK_FLAG)
cmake_pop_check_state()

if(HAVE_SPLIT_STACK_FLAG)
  # Additional check: verify that Boost.Context supports segmented stacks
  # This requires BOOST_USE_SEGMENTED_STACKS to be defined
  include(CheckCXXSourceCompiles)
  cmake_push_check_state(RESET)
  set(CMAKE_REQUIRED_FLAGS "-fsplit-stack")
  set(CMAKE_REQUIRED_DEFINITIONS "-DBOOST_USE_SEGMENTED_STACKS")
  set(CMAKE_REQUIRED_INCLUDES "${Boost_INCLUDE_DIRS}")
  check_cxx_source_compiles("
    #define BOOST_USE_SEGMENTED_STACKS
    #include <boost/context/segmented_stack.hpp>
    int main() {
      boost::context::segmented_stack alloc;
      return 0;
    }
  " HAVE_BOOST_SEGMENTED_STACK)
  cmake_pop_check_state()

  if(HAVE_BOOST_SEGMENTED_STACK)
    set(SEGMENTED_STACK_FOUND TRUE)
    set(SEGMENTED_STACK_FLAGS "-fsplit-stack")
  else()
    set(SEGMENTED_STACK_FOUND FALSE)
    message(STATUS "Boost.Context segmented_stack not available")
  endif()
else()
  set(SEGMENTED_STACK_FOUND FALSE)
endif()

# Create imported target
if(SEGMENTED_STACK_FOUND AND NOT TARGET SegmentedStack::SegmentedStack)
  add_library(SegmentedStack::SegmentedStack INTERFACE IMPORTED)
  set_target_properties(SegmentedStack::SegmentedStack PROPERTIES
    INTERFACE_COMPILE_OPTIONS "${SEGMENTED_STACK_FLAGS}"
    INTERFACE_COMPILE_DEFINITIONS "BOOST_USE_SEGMENTED_STACKS;RGW_USE_SEGMENTED_STACKS"
  )
endif()

# Report results
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SegmentedStack
  REQUIRED_VARS SEGMENTED_STACK_FOUND
  FAIL_MESSAGE "Segmented stacks require GCC with -fsplit-stack support and Boost.Context"
)

mark_as_advanced(
  SEGMENTED_STACK_FOUND
  SEGMENTED_STACK_FLAGS
  HAVE_SPLIT_STACK_FLAG
  HAVE_BOOST_SEGMENTED_STACK
)
