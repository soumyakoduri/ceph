# CMakeLists.txt for RGW SAL C Example
#
# This is a standalone CMakeLists.txt for building the example
# outside of the main Ceph build tree.
#
# Build instructions:
#   mkdir build && cd build
#   cmake .. -DCEPH_SOURCE_DIR=/path/to/ceph -DCEPH_BUILD_DIR=/path/to/ceph/build
#   make
#

cmake_minimum_required(VERSION 3.14.0)
project(rgw_sal_c_example C)

# Option to specify Ceph directories
set(CEPH_SOURCE_DIR "" CACHE PATH "Path to Ceph source directory")
set(CEPH_BUILD_DIR "" CACHE PATH "Path to Ceph build directory")

if(NOT CEPH_SOURCE_DIR)
    message(FATAL_ERROR "CEPH_SOURCE_DIR must be specified")
endif()

if(NOT CEPH_BUILD_DIR)
    set(CEPH_BUILD_DIR "${CEPH_SOURCE_DIR}/build")
endif()

# Include directories
include_directories(
    ${CEPH_SOURCE_DIR}/src/include
    ${CEPH_SOURCE_DIR}/src
    ${CEPH_BUILD_DIR}/include
)

# Library directories
link_directories(
    ${CEPH_BUILD_DIR}/lib
)

# Build the example
add_executable(rgw_sal_c_example rgw_sal_c_example.c)

target_link_libraries(rgw_sal_c_example
    rgw_sal
    rados
    pthread
)

# Set RPATH for finding libraries at runtime
set_target_properties(rgw_sal_c_example PROPERTIES
    INSTALL_RPATH "${CEPH_BUILD_DIR}/lib"
    BUILD_WITH_INSTALL_RPATH TRUE
)

# Installation (optional)
install(TARGETS rgw_sal_c_example DESTINATION bin)
