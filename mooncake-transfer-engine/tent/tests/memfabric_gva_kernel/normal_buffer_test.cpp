// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "acl/acl.h"
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
extern "C" int gva_kernel_copy(void *, void *, void *);
#define CHECK(call)                                                 \
    do {                                                            \
        int result = (call);                                        \
        if (result != 0) {                                          \
            std::fprintf(stderr, "%s failed: %d\n", #call, result); \
            return 1;                                               \
        }                                                           \
    } while (0)
int main(int argc, char **argv) {
    const int device = argc > 1 ? std::atoi(argv[1]) : 7;
    CHECK(aclInit(nullptr));
    CHECK(aclrtSetDevice(device));
    aclrtStream stream = nullptr;
    CHECK(aclrtCreateStream(&stream));
    void *source = nullptr;
    void *destination = nullptr;
    CHECK(aclrtMalloc(&source, 32, ACL_MEM_MALLOC_HUGE_FIRST));
    CHECK(aclrtMalloc(&destination, 32, ACL_MEM_MALLOC_HUGE_FIRST));
    uint32_t expected[8] = {0x10203040, 0x50607080, 0x90a0b0c0, 0xd0e0f000,
                            0x01234567, 0x89abcdef, 0xdeadbeef, 0xcafebabe};
    uint32_t actual[8] = {};
    CHECK(aclrtMemcpy(source, 32, expected, 32, ACL_MEMCPY_HOST_TO_DEVICE));
    CHECK(aclrtMemset(destination, 32, 0, 32));
    CHECK(gva_kernel_copy(source, destination, stream));
    CHECK(aclrtMemcpy(actual, 32, destination, 32, ACL_MEMCPY_DEVICE_TO_HOST));
    if (std::memcmp(expected, actual, 32) != 0) {
        for (int i = 0; i < 8; ++i)
            std::fprintf(stderr, "word %d: %08x != %08x\n", i, actual[i],
                         expected[i]);
        return 2;
    }
    std::printf(
        "PASS normal_buffer_test: device=%d, bytes=32, source=%p, "
        "destination=%p, AscendC GlobalTensor/DataCopy matched all 8 words\n",
        device, source, destination);
    CHECK(aclrtFree(source));
    CHECK(aclrtFree(destination));
    CHECK(aclrtDestroyStream(stream));
    CHECK(aclrtResetDevice(device));
    CHECK(aclFinalize());
    return 0;
}
