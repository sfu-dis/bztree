# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

project(ermia)
cmake_minimum_required(VERSION 2.8)

# PMwCAS paths
set(PMWCAS_SRC_PATH "" CACHE STRING "PMwCAS source path")
include_directories(
  ${PMWCAS_SRC_PATH}
  ${PMWCAS_SRC_PATH}/src
)

# Define PMEM for PMwCAS
add_definitions(-DPMEM)

set(PMWCAS_LIB_PATH "" CACHE STRING "PMwCAS shared library path")
link_directories(${PMWCAS_LIB_PATH})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(LINK_FLAGS "-lnuma -lpthread -pthread -lrt -lglog -lgtest -lgflags -lpmwcas")
link_libraries(${LINK_FLAGS})

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
endif()

set_property(GLOBAL APPEND PROPERTY BZTREE_SRC 
  ${CMAKE_CURRENT_SOURCE_DIR}/bztree.cc
)
get_property(BZTREE_LIB_SRC GLOBAL PROPERTY BZTREE_SRC)

add_library(bztree SHARED ${BZTREE_LIB_SRC})

add_executable(bztree_tests ${CMAKE_CURRENT_SOURCE_DIR}/bztree_tests.cc)
target_link_libraries(bztree_tests bztree)

add_custom_target(
  cpplint ALL
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/third-party/cpplint/cpplint.py
    --linelength=100
    ${CMAKE_CURRENT_SOURCE_DIR}/bztree.h
    ${CMAKE_CURRENT_SOURCE_DIR}/bztree.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/bztree_tests.cc
  || (exit 0)
)

add_dependencies(bztree cpplint)
add_dependencies(bztree_tests cpplint)
