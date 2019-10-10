if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)

# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

include(ExternalProject)
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
if(${PMEM_BACKEND} STREQUAL "PMDK")
  add_definitions(-DPMEM)
  add_definitions(-DPMDK)
  message(STATUS "Persistence support: PMDK")
elseif(${PMEM_BACKEND} STREQUAL "EMU")
  add_definitions(-DPMEM)
  add_definitions(-DPMEMEMU)
  message(STATUS "Persistence support: emulation")
elseif(${PMEM_BACKEND} STREQUAL "VOLATILE")
  message(STATUS "Persistence support: off")
else()
  message(FATAL_ERROR "Unsupported persistent memory backend: ${PMEM_BACKEND}")
endif()

option(GOOGLE_FRAMEWORK "Use glog, gflags and gtest" ON)
if(${GOOGLE_FRAMEWORK})
  add_definitions(-DGOOGLE_FRAMEWORK)
  message(STATUS "GOOGLE_FRAMEWORK is defined, will use glog, gflags and gtest")
else()
  message(STATUS "GOOGLE_FRAMEWORK is not defined, will not use glog, gflags and gtest")
endif()

option(BUILD_TESTS, "Build test cases using gflags, glog and gtest" ON)
if(${BUILD_TESTS})
  add_definitions(-DBUILD_TESTS)
  message(STATUS "BUILD_TESTS: defined")
else()
  message(STATUS "BUILD_TESTS: not defined")
endif()

##################### PMwCAS #########################
add_definitions(-DDESC_CAP=16)
set(PMWCAS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/pmwcas")
ExternalProject_Add(PMWCAS
  PREFIX "${PMWCAS_PREFIX}"
  GIT_REPOSITORY git@github.com:XiangpengHao/pmwcas.git
  GIT_TAG master
  INSTALL_COMMAND ""
  BUILD_COMMAND $(MAKE)
  CMAKE_ARGS -DPMEM_BACKEND=${PMEM_BACKEND} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DDESC_CAP=16 -DBUILD_APPS=${BUILD_TESTS} -DGOOGLE_FRAMEWORK=${GOOGLE_FRAMEWORK}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON
)

##################### PiBench #########################
FetchContent_Declare(
        pibench 
        GIT_REPOSITORY git@github.com:wangtzh/pibench.git
        GIT_TAG master 
)
if (NOT pibench_POPULATED)
  FetchContent_Populate(pibench)
  include_directories(${pibench_SOURCE_DIR}/include)
endif ()


# PMwCAS
include_directories(
  ${PMWCAS_PREFIX}/src/PMWCAS
  ${PMWCAS_PREFIX}/src/PMWCAS/src/
  ${PMWCAS_PREFIX}/src/PMWCAS/include
)
link_directories(${PMWCAS_PREFIX}/src/PMWCAS-build/)

if(${GOOGLE_FRAMEWORK})

######################## glog #####################
# Google Log
FetchContent_Declare(
        glog
        GIT_REPOSITORY https://github.com/google/glog.git
        GIT_TAG v0.4.0
)
FetchContent_GetProperties(glog)
if (NOT glog_POPULATED)
  FetchContent_Populate(glog)
  set(WITH_GFLAGS OFF CACHE BOOL "we don't use gflags")
  add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})
endif ()


######################## gtest #####################
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
  include_directories(${googletest_SOURCE_DIR}/GTestExternal/googletest/include)
endif ()
include(GoogleTest)

endif()
####################################################

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
set(LINK_FLAGS "-lnuma -lpthread -pthread -lrt")
link_libraries(${LINK_FLAGS})

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

if(${CMAKE_BUILD_TYPE} STREQUAL "Release")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -march=native")
endif()

set_property(GLOBAL APPEND PROPERTY BZTREE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/bztree.cc
)
get_property(BZTREE_LIB_SRC GLOBAL PROPERTY BZTREE_SRC)

add_library(bztree_static STATIC ${BZTREE_LIB_SRC})
add_library(bztree SHARED ${BZTREE_LIB_SRC})
add_dependencies(bztree cpplint)
add_dependencies(bztree PMWCAS)

set (BZTREE_LINK_LIBS
  bztree
)

if(${GOOGLE_FRAMEWORK})

list(APPEND BZTREE_LINK_LIBS
  glog::glog
  gtest_main
)
target_link_libraries(bztree glog::glog gtest_main)
target_link_libraries(bztree_static glog::glog gtest_main)

endif()

if(${PMEM_BACKEND} STREQUAL "PMDK")
  set(PMDK_LIB_PATH "/usr/local/lib" CACHE STRING "PMDK lib install path")
  add_library(pmemobj STATIC IMPORTED)
  set_property(TARGET pmemobj PROPERTY IMPORTED_LOCATION ${PMDK_LIB_PATH}/libpmemobj.a)
  list(APPEND BZTREE_LINK_LIBS
    pmemobj
  )
endif()

if(${BUILD_TESTS})
  if((${PMEM_BACKEND} STREQUAL "PMDK"))
    add_executable(bztree_pmdk_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_pmdk_tests.cc)
    target_link_libraries(bztree_pmdk_tests ${BZTREE_LINK_LIBS})
  else()
    add_executable(bztree_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_tests.cc)
    add_executable(bztree_thread_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_multithread_tests.cc)
    target_link_libraries(bztree_tests ${BZTREE_LINK_LIBS})
    target_link_libraries(bztree_thread_tests ${BZTREE_LINK_LIBS})
    add_dependencies(bztree_tests cpplint)
  endif()
endif()

target_link_libraries(bztree pmwcas)
target_link_libraries(bztree_static pmwcas)
set_property(TARGET bztree_static PROPERTY POSITION_INDEPENDENT_CODE ON)

add_library(bztree_pibench_wrapper SHARED tests/bztree_pibench_wrapper.cc)
target_link_libraries(bztree_pibench_wrapper bztree_static)

add_custom_target(
  cpplint ALL
  COMMAND python2 ${CMAKE_CURRENT_SOURCE_DIR}/third-party/cpplint/cpplint.py
    --linelength=100
    --filter=-runtime/references,-build/header_guard,-build/include
    ${CMAKE_CURRENT_SOURCE_DIR}/bztree.h
    ${CMAKE_CURRENT_SOURCE_DIR}/bztree.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_tests.cc
  || (exit 0)
)

set(MAX_FREEZE_RETRY 1 CACHE STRING "MAX retry on frozen node")
target_compile_definitions(bztree PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})
target_compile_definitions(bztree_static PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})

set(ENABLE_MERGE 0 CACHE STRING "MAX retry on frozen node")
message(STATUS "ENABLE_MERGE: " ${ENABLE_MERGE})
target_compile_definitions(bztree PRIVATE ENABLE_MERGE=${ENABLE_MERGE})
target_compile_definitions(bztree_static PUBLIC ENABLE_MERGE=${ENABLE_MERGE})
