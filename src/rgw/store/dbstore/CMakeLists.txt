#need to update cmake version here
cmake_minimum_required(VERSION 3.14.0)
project(dbstore)

option(DEBUG_SYMS "Enable debug symbols to binaries (-g option)" OFF)
option(USE_SQLITE "Enable SQLITE DB" ON)

IF(DEBUG_SYMS)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
  message(STATUS "Enabling debug symbols")
ENDIF(DEBUG_SYMS)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

find_package(fmt REQUIRED)
if(NOT fmt_FOUND)
	message(FATAL_ERROR "Could not find fmt, will build it")
endif()

set (CMAKE_INCLUDE_DIR ${CMAKE_INCLUDE_DIR} "${CMAKE_SOURCE_DIR}/common")

set(dbstore_srcs
    common/dbstore-log.h
    common/dbstore.h
    common/dbstore.cc)

set(main_srcs
    dbstore_mgr.h
    dbstore_mgr.cc
    main.cc)

add_library(dbstore_lib ${dbstore_srcs})
target_link_libraries(dbstore_lib fmt)
set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} dbstore_lib)

IF(USE_SQLITE)
  add_subdirectory(sqlite)
  set(CMAKE_INCLUDE_DIR ${CMAKE_INCLUDE_DIR} "${CMAKE_SOURCE_DIR}/sqlite")
  add_compile_definitions(SQLITE_ENABLED=1)
  set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} sqlite_db)
ENDIF()

# add pthread library
set (CMAKE_LINK_LIBRARIES ${CMAKE_LINK_LIBRARIES} pthread)

find_package(gtest QUIET)
if(NOT gtest_FOUND)
	message(WARNING "Could not find gtest")
endif()
add_subdirectory(tests)

include_directories(${CMAKE_INCLUDE_DIR})
add_executable(dbstore ${main_srcs})
target_link_libraries(dbstore ${CMAKE_LINK_LIBRARIES})
