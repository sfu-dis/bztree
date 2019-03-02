# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

include(ExternalProject)
project(bztree)
cmake_minimum_required(VERSION 3.2.2)

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

##################### PMwCAS #########################
set(PMWCAS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/pmwcas")
ExternalProject_Add(PMWCAS
  PREFIX "${PMWCAS_PREFIX}"
  GIT_REPOSITORY git@github.com:HaoPatrick/pmwcas.git
  GIT_TAG master
  INSTALL_COMMAND ""
  BUILD_COMMAND $(MAKE)
  CMAKE_ARGS -DPMEM_BACKEND=${PMEM_BACKEND} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON
)

# PMwCAS and PMDK paths
include_directories(
  ${PMWCAS_PREFIX}/src/PMWCAS
  ${PMWCAS_PREFIX}/src/PMWCAS/src/
  ${PMWCAS_PREFIX}/src/PMWCAS/include
  ${PMWCAS_PREFIX}/src/PMWCAS-build/pmdk/src/PMDK/src/include
)
link_directories(${PMWCAS_PREFIX}/src/PMWCAS-build/)

####################################################

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
set(LINK_FLAGS "-lnuma -lpthread -pthread -lrt -lglog -lgtest -lgflags -lpmwcas")
if(${PMEM_BACKEND} STREQUAL "PMDK")
  set(LINK_FLAGS "${LINK_FLAGS} -lpmemobj")
  if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    link_directories(${PMWCAS_PREFIX}/src/PMWCAS-build/pmdk/src/PMDK/src/debug)
  else()
    link_directories(${PMWCAS_PREFIX}/src/PMWCAS-build/pmdk/src/PMDK/src/nondebug)
  endif()
endif()
link_libraries(${LINK_FLAGS})

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

if(${CMAKE_BUILD_TYPE} STREQUAL "Profile")
  add_compile_definitions("PROFILE")
  link_libraries("-lprofiler")
endif()

set_property(GLOBAL APPEND PROPERTY BZTREE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/bztree.cc
)
get_property(BZTREE_LIB_SRC GLOBAL PROPERTY BZTREE_SRC)

add_library(bztree SHARED ${BZTREE_LIB_SRC})

if((${PMEM_BACKEND} STREQUAL "PMDK"))
  add_executable(bztree_pmem_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_pmem_test.cc)
  target_link_libraries(bztree_pmem_tests bztree)
else()
  add_executable(bztree_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_tests.cc)
  add_executable(bztree_thread_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/bztree_multithread_tests.cc)
  target_link_libraries(bztree_tests bztree)
  target_link_libraries(bztree_thread_tests bztree)
  add_dependencies(bztree_tests cpplint)
endif()

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

set(MAX_FREEZE_RETRY 1000 CACHE STRING "MAX retry on frozen node")
target_compile_definitions(bztree PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})

add_dependencies(bztree cpplint)
add_dependencies(bztree PMWCAS)

