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
  GIT_REPOSITORY git@github.com:HaoPatrick/pmwcas.git
  GIT_TAG master
  INSTALL_COMMAND ""
  BUILD_COMMAND $(MAKE)
  CMAKE_ARGS -DPMEM_BACKEND=${PMEM_BACKEND} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DDESC_CAP=16 -DBUILD_APPS=${BUILD_TESTS} -DGOOGLE_FRAMEWORK=${GOOGLE_FRAMEWORK}
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

if(${GOOGLE_FRAMEWORK})
###################### gflags ######################
  set(GFLAGS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gflags")
  set(GFLAGS_LOCATION "${GFLAGS_PREFIX}/src/GFlagsExternal-build")
  set(GFLAGS_INCLUDES "${GFLAGS_PREFIX}/src/GFlagsExternal-build/include")

  ExternalProject_Add(GFlagsExternal
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third-party/gflags-2.1.2/gflags
    PREFIX "${GFLAGS_PREFIX}"

    # Disable install step
    INSTALL_COMMAND ""

    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON
  )

  set(GFLAGS_IMPORTED_LOCATION
      IMPORTED_LOCATION "${GFLAGS_LOCATION}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gflags${CMAKE_STATIC_LIBRARY_SUFFIX}")
  link_directories(${GFLAGS_LOCATION}/lib)

# the gtest include directory exists only after it is build, but it is used/needed
# for the set_target_properties call below, so make it to avoid an error
  file(MAKE_DIRECTORY ${GFLAGS_INCLUDES})

  add_library(GFlags IMPORTED STATIC GLOBAL)

  set_target_properties(GFlags PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES     "${GFLAGS_INCLUDES}"
      IMPORTED_LINK_INTERFACE_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}"
      ${GFLAGS_IMPORTED_LOCATION}
  )
  include_directories(${GFLAGS_INCLUDES})
  add_dependencies(GFlags GFlagsExternal)

###################### glog ######################
# variables to help keep track of glog paths
  set(GLOG_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/glog")
  set(GLOG_LOCATION "${GLOG_PREFIX}/src/GLogExternal-build")
  set(APPEND GLOG_INCLUDES "${GLOG_PREFIX}/src/GLogExternal/src")
  file(MAKE_DIRECTORY "${GLOG_PREFIX}/src/GLogExternal/src")
  include_directories("${GLOG_PREFIX}/src/GLogExternal/src")

  set(APPEND GLOG_INCLUDES "${GLOG_PREFIX}/src/GLogExternal-build")
  file(MAKE_DIRECTORY "${GLOG_PREFIX}/src/GLogExternal-build")
  include_directories("${GLOG_PREFIX}/src/GLogExternal-build")

  ExternalProject_Add(GLogExternal
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third-party/glog-0.3.4
    PREFIX "${GLOG_PREFIX}"
    DEPENDS GFlagsExternal
    # cmake arguments
    CMAKE_ARGS -DCMAKE_PREFIX_PATH=${GFLAGS_LOCATION}

    # Disable install step
    INSTALL_COMMAND ""

    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON
  )
  set(GLOG_IMPORTED_LOCATION
    IMPORTED_LOCATION "${GLOG_LOCATION}/${CMAKE_STATIC_LIBRARY_PREFIX}glog${CMAKE_STATIC_LIBRARY_SUFFIX}")
  link_directories(${GLOG_LOCATION})

# define imported library GFlags
  add_library(GLog IMPORTED STATIC GLOBAL)
  set_target_properties(GLog PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES     "${GLOG_INCLUDES}"
    IMPORTED_LINK_INTERFACE_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}"
    ${GLOG_IMPORTED_LOCATION}
  )
  add_dependencies(GLog GLogExternal)

######################## gtest #####################
# Install gtest as en external project
  set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gtest")
  set(GTEST_LOCATION "${GTEST_PREFIX}/src/GTestExternal-build")
  set(GTEST_INCLUDES "${GTEST_PREFIX}/src/GTestExternal/googletest/include")
  set(gtest_force_shared_crt ON CACHE BOOL "Force gtest to use dynamic standard library" )
  include_directories(${GTEST_INCLUDES})

# external project download and build
  ExternalProject_Add(GTestExternal
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third-party/googletest
    PREFIX "${GTEST_PREFIX}"

    # cmake arguments
    CMAKE_ARGS -Dgtest_force_shared_crt=ON

    # Disable install step
    INSTALL_COMMAND ""

    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON
  )

  set(GTEST_IMPORTED_LOCATION
      IMPORTED_LOCATION "${GTEST_LOCATION}/googlemock/gtest")
  set(GTESTMAIN_IMPORTED_LOCATION
      IMPORTED_LOCATION "${GTEST_LOCATION}/googlemock/gtest_main")
  link_directories(${GTEST_LOCATION})
  link_directories(${GTEST_LOCATION}/googlemock/gtest)

# the gtest include directory exists only after it is built
  file(MAKE_DIRECTORY ${GTEST_INCLUDES})

# define imported library GTest
  add_library(GTest IMPORTED STATIC GLOBAL)
  set_target_properties(GTest PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES     "${GTEST_INCLUDES}"
      IMPORTED_LINK_INTERFACE_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}"
      ${GTEST_IMPORTED_LOCATION}
  )

# define imported library GTestMain
  add_library(GTestMain IMPORTED STATIC GLOBAL)
  set_target_properties(GTestMain PROPERTIES
      IMPORTED_LINK_INTERFACE_LIBRARIES GTest
      ${GTESTMAIN_IMPORTED_LOCATION}
      )

# make GTest depend on GTestExternal
  add_dependencies(GTest GTestExternal)
endif()
####################################################

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
set(LINK_FLAGS "-lnuma -lpthread -pthread -lrt -lpmwcas")
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
add_dependencies(bztree cpplint)
add_dependencies(bztree PMWCAS)

set (BZTREE_LINK_LIBS
  bztree
)

if(${GOOGLE_FRAMEWORK})
add_dependencies(bztree GLog)
add_dependencies(bztree GFlags)
add_dependencies(bztree GTest)

set(APPEND BZTREE_LINK_LIBS
  glog
  gflags
  gtest
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

set(ENABLE_MERGE 1 CACHE STRING "MAX retry on frozen node")
target_compile_definitions(bztree PRIVATE ENABLE_MERGE=${ENABLE_MERGE})

target_compile_definitions(bztree PRIVATE DESC_CAP=4)
target_compile_definitions(bztree PRIVATE GOOGLE_FRAMEWORK=1)
