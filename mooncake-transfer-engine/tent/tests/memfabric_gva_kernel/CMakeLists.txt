# Copyright 2026 KVCache.AI
# SPDX-License-Identifier: Apache-2.0
# Standalone optional fixture; configure this directory explicitly on an NPU host.

cmake_minimum_required(VERSION 3.16)
project(gva_kernel_smoke LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(RUN_MODE npu)
set(SOC_VERSION "Ascend910B3" CACHE STRING "Ascend target SoC")
set(ASCEND_CANN_PACKAGE_PATH "/usr/local/Ascend/cann" CACHE PATH
    "CANN installation directory")
include(
  ${ASCEND_CANN_PACKAGE_PATH}/compiler/tikcpp/ascendc_kernel_cmake/ascendc.cmake)
ascendc_library(gva_device STATIC ${CMAKE_CURRENT_SOURCE_DIR}/copy.cpp)
add_library(gva_kernel SHARED wrapper.cpp)
target_include_directories(gva_kernel PRIVATE ${ASCEND_CANN_PACKAGE_PATH}/include)
target_link_directories(gva_kernel PRIVATE ${ASCEND_CANN_PACKAGE_PATH}/lib64)
target_link_libraries(gva_kernel PRIVATE gva_device ascendcl)
add_executable(normal_buffer_test normal_buffer_test.cpp)
target_include_directories(normal_buffer_test
                           PRIVATE ${ASCEND_CANN_PACKAGE_PATH}/include)
target_link_directories(normal_buffer_test
                        PRIVATE ${ASCEND_CANN_PACKAGE_PATH}/lib64)
target_link_libraries(normal_buffer_test PRIVATE gva_kernel ascendcl)
