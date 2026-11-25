add_library(
  memory_engine_objects OBJECT
  src/client/memory_engine_client.cpp
  src/common/memory_engine_common.cpp
  src/common/memory_engine_rpc.cpp
  src/server/memory_engine_server.cpp)
set_target_properties(memory_engine_objects PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_compile_features(memory_engine_objects PUBLIC cxx_std_20)
target_include_directories(
  memory_engine_objects PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/common
          ${PROJECT_SOURCE_DIR}/mooncake-transfer-engine/tent/include)
target_link_libraries(memory_engine_objects PRIVATE tent_interface
                      glog::glog yalantinglibs::yalantinglibs)
if(USE_MEMFABRIC)
  target_compile_definitions(memory_engine_objects PRIVATE USE_MEMFABRIC)
endif()

add_library(mooncake_memory_engine SHARED)
target_link_libraries(mooncake_memory_engine PRIVATE memory_engine_objects
                      PUBLIC tent_shared)
target_include_directories(
  mooncake_memory_engine PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/mooncake-transfer-engine/tent/include>
  $<INSTALL_INTERFACE:include>)
set_target_properties(mooncake_memory_engine PROPERTIES INSTALL_RPATH "$ORIGIN")
install(TARGETS mooncake_memory_engine LIBRARY DESTINATION lib)
install(FILES include/memory_engine_client.h include/memory_engine_server.h
        DESTINATION include)
install(FILES ${PROJECT_SOURCE_DIR}/mooncake-transfer-engine/tent/include/tent/common/status.h
        DESTINATION include/tent/common)

if(BUILD_UNIT_TESTS AND USE_MEMFABRIC)
  add_executable(
    memory_engine_test tests/memory_engine_test.cpp
    ${PROJECT_SOURCE_DIR}/mooncake-transfer-engine/tent/src/transport/memfabric/memfabric_under_api.cpp)
  target_compile_definitions(memory_engine_test PRIVATE TENT_MEMFABRIC_TEST_GVA_API)
  target_include_directories(
    memory_engine_test PRIVATE
    ${PROJECT_SOURCE_DIR}/mooncake-transfer-engine/tent/include
    ${PROJECT_SOURCE_DIR}/mooncake-transfer-engine/tent/tests/mock_acl)
  target_link_libraries(memory_engine_test PRIVATE memory_engine_objects gtest
                        gtest_main tent_link_group glog::glog)
  add_test(NAME memory_engine_test COMMAND memory_engine_test)
endif()
