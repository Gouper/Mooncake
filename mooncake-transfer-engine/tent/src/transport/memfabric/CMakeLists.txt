if(USE_MEMFABRIC)
  add_library(
    tent_xport_memfabric STATIC memfabric_transport.cpp memfabric_under_api.cpp
                                memfabric_allocator.cpp memfabric_shared_memory.cpp
                                memfabric_gva_api.cpp)
  find_path(MEMFABRIC_HYBM_INCLUDE_DIR hybm_big_mem.h
            HINTS ENV MEMFABRIC_HOME PATH_SUFFIXES include include/host)
  if(MEMFABRIC_HYBM_INCLUDE_DIR)
    include(CheckCXXSourceCompiles)
    set(CMAKE_REQUIRED_INCLUDES ${MEMFABRIC_HYBM_INCLUDE_DIR})
    check_cxx_source_compiles("
      #include <hybm_big_mem.h>
      int main() {
        static_assert(BM_GVA_NOT_FOUND == -12);
        static_assert(sizeof(&hybm_gva_to_va) > 0);
        static_assert(sizeof(&hybm_get_slice_size) > 0);
        static_assert(sizeof(&hybm_unmap_rank) > 0);
        static_assert(sizeof(&hybm_remove_imported) > 0);
        static_assert(sizeof(&hybm_entity_reach_types) > 0);
        hybm_options options{};
        options.enable56BitsGva = false;
        options.dramShmFd = -1;
        return 0;
      }" MEMFABRIC_HYBM_GVA_REGISTRY_HEADERS)
    unset(CMAKE_REQUIRED_INCLUDES)
    if(MEMFABRIC_HYBM_GVA_REGISTRY_HEADERS)
      target_include_directories(tent_xport_memfabric PRIVATE ${MEMFABRIC_HYBM_INCLUDE_DIR})
      target_compile_definitions(tent_xport_memfabric PRIVATE TENT_WITH_HYBM_GVA=1)
    endif()
  endif()
  if(NOT MEMFABRIC_HYBM_GVA_REGISTRY_HEADERS)
    message(STATUS "HYBM headers unavailable; MemFabric trans remains enabled without GVA")
  endif()
  if(BUILD_UNIT_TESTS AND USE_MEMFABRIC)
    target_include_directories(
      tent_xport_memfabric
      PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../tests/mock_acl)
    target_link_libraries(tent_xport_memfabric PUBLIC tent_interface dl)
    message(STATUS "Using stub ACL headers for tent_xport_memfabric unit tests")
  else()
    target_link_libraries(tent_xport_memfabric PUBLIC tent_interface dl
                                                      ascendcl)
  endif()
  target_include_directories(
    tent_xport_memfabric PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../../include)
endif()
