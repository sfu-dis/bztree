# No in-source build
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif ()
include(FetchContent)
project(bztree)
cmake_minimum_required(VERSION 3.11)

# Set backend to PMDK by default to build persistent version. This will be passed
# in directly to PMwCAS which provides persistence support.
set(PMEM_BACKEND "PMDK" CACHE STRING "Persistent memory backend type")
string(TOUPPER ${PMEM_BACKEND} PMEM_BACKEND)

# Both volatile and persistent versions are supported, by setting PMEM_BACKEND to:
# PMDK    : use PMDK for persistence
# EMU     : use simple shared memory for emulating persistent memory. This
#           should only be used for experimental and profiling purpose. No real
#           persistence is guaranteed.
# VOLATILE: turn off persistence and build a volatile version, no persistence
#           whatsoever. Equivalent to the original MwCAS operation.
#
# If persistent memory support is turned on, in the code we define both PMEM and
# the corresponding macro for the backend. Code that is agnostic to the backend
# is wrapped by PMEM; code that is specific to the backend is wrapped around by
# PMEMEMU (for using emulation) or PMDK (for using PMDK).
#
# Note: these macros definitions follow strictly those in PMwCAS and are needed
# to make sure of the inclusion of code in PMwCAS headers in included by BzTree.
if (${PMEM_BACKEND} STREQUAL "PMDK")
  add_definitions(-DPMEM)
  add_definitions(-DPMDK)
  message(STATUS "Persistence support: PMDK")
elseif (${PMEM_BACKEND} STREQUAL "EMU")
  add_definitions(-DPMEM)
  add_definitions(-DPMEMEMU)
  message(STATUS "Persistence support: emulation")
elseif (${PMEM_BACKEND} STREQUAL "VOLATILE")
  message(STATUS "Persistence support: off")
else ()
  message(FATAL_ERROR "Unsupported persistent memory backend: ${PMEM_BACKEND}")
endif ()

option(GOOGLE_FRAMEWORK "Use glog, gflags and gtest" ON)
if (${GOOGLE_FRAMEWORK})
  add_definitions(-DGOOGLE_FRAMEWORK)
  message(STATUS "GOOGLE_FRAMEWORK is defined, will use glog, gflags and gtest")
else ()
  message(STATUS "GOOGLE_FRAMEWORK is not defined, will not use glog, gflags and gtest")
endif ()

option(BUILD_TESTS, "Build test cases using gflags, glog and gtest" ON)
if (${BUILD_TESTS})
  add_definitions(-DBUILD_TESTS)
  message(STATUS "BUILD_TESTS: defined")
else ()
  message(STATUS "BUILD_TESTS: not defined")
endif ()

# PMwCAS
FetchContent_Declare(
        pmwcas
        GIT_REPOSITORY git@github.com:HaoPatrick/pmwcas.git
        GIT_TAG master
)
FetchContent_GetProperties(pmwcas)
if (NOT pmwcas_POPULATED)
  FetchContent_Populate(pmwcas)
  add_subdirectory(${pmwcas_SOURCE_DIR} ${pmwcas_BINARY_DIR})
  include_directories(${pmwcas_SOURCE_DIR}/include)
  include_directories(${pmwcas_SOURCE_DIR}/src)
endif ()

# Google Test
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.8.1
)
FetchContent_GetProperties(googletest)
if (NOT googletest_POPULATED)
  FetchContent_Populate(googletest)
  add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif ()
include(GoogleTest)

# Google Log
FetchContent_Declare(
        glog
        GIT_REPOSITORY https://github.com/google/glog.git
        GIT_TAG v0.4.0
)
FetchContent_GetProperties(glog)
if (NOT glog_POPULATED)
  FetchContent_Populate(glog)
  add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})
endif ()
####################################################

set(CMAKE_CXX_STANDARD 14)
set(LINK_FLAGS "-lnuma -lpthread -pthread -lrt")
link_libraries(${LINK_FLAGS})

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

set_property(GLOBAL APPEND PROPERTY BZTREE_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/bztree.cc
        )
get_property(BZTREE_LIB_SRC GLOBAL PROPERTY BZTREE_SRC)

add_library(bztree SHARED ${BZTREE_LIB_SRC} basenode.cpp)
target_include_directories(bztree PUBLIC ${pmwcas_SOURCE_DIR})
target_link_libraries(bztree pmwcas glog::glog gtest_main)

add_dependencies(bztree cpplint)

if (${BUILD_TESTS})
  if ((${PMEM_BACKEND} STREQUAL "PMDK"))
    add_executable(bztree_pmdk_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_pmdk_tests.cc)
    target_link_libraries(bztree_pmdk_tests bztree gtest_main)
  else ()
    add_executable(bztree_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_tests.cc)
    add_executable(bztree_thread_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_multithread_tests.cc)
    target_link_libraries(bztree_tests bztree gtest_main)
    target_link_libraries(bztree_thread_tests bztree gtest_main)
    add_dependencies(bztree_tests cpplint)
  endif ()
endif ()

add_custom_target(
        cpplint ALL
        COMMAND python2 ${CMAKE_CURRENT_SOURCE_DIR}/third-party/cpplint/cpplint.py
        --linelength=120
        --filter=-runtime/references,-build/header_guard,-build/include
        ${CMAKE_CURRENT_SOURCE_DIR}/bztree.h
        ${CMAKE_CURRENT_SOURCE_DIR}/bztree.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_tests.cc
        || (exit 0)
)

set(MAX_FREEZE_RETRY 1)
target_compile_definitions(bztree PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})

set(ENABLE_MERGE 0)
message(STATUS "ENABLE_MERGE: " ${ENABLE_MERGE})
target_compile_definitions(bztree PRIVATE ENABLE_MERGE=${ENABLE_MERGE})
