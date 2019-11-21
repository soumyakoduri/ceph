cmake_minimum_required(VERSION 3.14.0)
project(dbstore_tests)

#find_package(gtest REQUIRED)
set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} gtest)

set(dbstore_tests_srcs
    dbstore-tests.cc)

include_directories(${CMAKE_INCLUDE_DIR})

add_executable(dbstore_tests ${dbstore_tests_srcs})
target_link_libraries(dbstore_tests ${CMAKE_LINK_LIBRARIES})
