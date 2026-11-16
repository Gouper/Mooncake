// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

// These CPU-only tests link the full engine, including its Ascend platform.
// Supply only the ACL entry points referenced by that platform and the
// memfabric adapter. Device allocations deliberately fail so an accidental
// hardware path cannot pass as a successful CPU test.
#include <acl/acl.h>

#include <cstdlib>
#include <cstring>

extern "C" {

aclError aclrtGetDevice(int* device) {
    *device = 0;
    return ACL_SUCCESS;
}

aclError aclrtSetDevice(int device) { return device == 0 ? ACL_SUCCESS : 1; }

aclError aclrtPointerGetAttributes(const void*,
                                   aclrtPtrAttributes* attributes) {
    *attributes = {};
    attributes->location.id = 0;
    attributes->location.type = ACL_MEM_LOCATION_TYPE_HOST;
    return ACL_SUCCESS;
}

aclError aclrtMalloc(void** pointer, size_t, aclrtMemMallocPolicy) {
    *pointer = nullptr;
    return 1;
}

aclError aclrtFree(void*) { return 1; }

aclError aclrtMallocHost(void** pointer, size_t size) {
    *pointer = std::malloc(size);
    return *pointer ? ACL_SUCCESS : 1;
}

aclError aclrtFreeHost(void* pointer) {
    std::free(pointer);
    return ACL_SUCCESS;
}

aclError aclrtMemcpy(void* destination, size_t capacity, const void* source,
                     size_t size, aclrtMemcpyKind) {
    if (size > capacity || (size && (!destination || !source))) return 1;
    if (size) std::memcpy(destination, source, size);
    return ACL_SUCCESS;
}

}  // extern "C"
