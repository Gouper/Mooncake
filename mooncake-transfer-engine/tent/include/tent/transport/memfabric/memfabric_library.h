// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#ifndef TENT_MEMFABRIC_LIBRARY_H
#define TENT_MEMFABRIC_LIBRARY_H

#include <dlfcn.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "memfabric_types.h"

namespace mooncake {
namespace tent {

// Common dynamic-library loader.
// All MemFabric function tables share this loader. Tables still choose their
// own required symbols: old trans libraries need not expose the HYBM GVA API.
class MemFabricLibrary {
   public:
    static std::shared_ptr<MemFabricLibrary> load(
        const std::string& name, int flags = RTLD_NOW | RTLD_LOCAL,
        std::string* error = nullptr) {
        auto& cache = libraries();
        std::lock_guard<std::mutex> lock(cache.mutex);
        // Visibility is part of the key so a local load cannot accidentally
        // replace the global-symbol contract of the existing trans loader.
        const auto key = std::make_pair(name, flags);
        const auto found = cache.entries.find(key);
        if (found != cache.entries.end()) {
            if (auto library = found->second.lock()) return library;
        }
        auto library = std::shared_ptr<MemFabricLibrary>(new MemFabricLibrary);
        library->handle_ = dlopen(name.c_str(), flags);
        if (!library->handle_) {
            if (error) {
                const char* detail = dlerror();
                *error = detail ? detail : "dlopen failed";
            }
            return nullptr;
        }
        cache.entries[key] = library;
        return library;
    }

    ~MemFabricLibrary() {
        if (handle_) (void)dlclose(handle_);
    }
    MemFabricLibrary(const MemFabricLibrary&) = delete;
    MemFabricLibrary& operator=(const MemFabricLibrary&) = delete;

    // An aliasing shared_ptr can expose this native handle while keeping its
    // loader alive, as required by the existing trans MemoryApi and its mocks.
    void* handle() const { return handle_; }

    template <typename Function>
    Function symbol(const char* name) const {
        return resolve<Function>(handle_, name);
    }

    template <typename Function>
    static Function resolve(void* handle, const char* name) {
        static_assert(
            std::is_pointer<Function>::value &&
                std::is_function<
                    typename std::remove_pointer<Function>::type>::value,
            "MemFabric symbols must use function pointer types");
        return handle ? reinterpret_cast<Function>(dlsym(handle, name))
                      : nullptr;
    }

   private:
    MemFabricLibrary() = default;
    struct Cache {
        std::mutex mutex;
        std::map<std::pair<std::string, int>, std::weak_ptr<MemFabricLibrary>>
            entries;
    };
    static Cache& libraries() {
        static Cache cache;
        return cache;
    }
    void* handle_ = nullptr;
};

// Trans API: libmf_smem.so.
using smemTransInitFunc = int32_t (*)(uint32_t, uint32_t);
using smemTransUninitFunc = void (*)(uint32_t);
using smemTransConfigInitFunc = int32_t (*)(smem_trans_config_t*);
using smemTransCreateFunc = smem_trans_t (*)(const smem_trans_config_t*,
                                             uint32_t);
using smemTransDestroyFunc = void (*)(smem_trans_t, uint32_t);
using smemTransGetRpcPortFunc = int32_t (*)(smem_trans_t, uint16_t*);
using smemTransRegisterMemFunc = int32_t (*)(smem_trans_t, void*, size_t,
                                             uint32_t);
using smemTransBatchRegisterMemFunc = int32_t (*)(smem_trans_t, void*[],
                                                  size_t[], uint32_t, uint32_t);
using smemTransDeregisterMemFunc = int32_t (*)(smem_trans_t, void*);
using smemTransBatchCopyFunc = int32_t (*)(smem_trans_t,
                                           smem_trans_batch_params_t*,
                                           uint64_t*);
using smemTransBatchStatusQueryFunc = int32_t (*)(smem_trans_t, uint64_t,
                                                  int32_t*, uint32_t);
using smemSetLogLevelFunc = int32_t (*)(int);

using smemMemAllocFunc = int32_t (*)(uint16_t, uint64_t, uint32_t, void**);
using smemMemFreeFunc = int32_t (*)(void*);

class MemFabricUnderApi {
    friend class MemFabricTransportTest;

   public:
    static bool LoadLibrary();
    static void CleanupLibrary();
    static bool IsLoaded() { return gLoaded; }
    static int32_t SmemMemAlloc(uint16_t deviceId, uint64_t size,
                                uint32_t flags, void** address);
    // managed distinguishes fabric allocations from ACL Host buffers.
    static int32_t SmemMemFree(void* address, bool& managed);

    static inline int32_t SmemTransInit(uint32_t deviceId, uint32_t flags) {
        if (pSmemTransInit == nullptr) return -1;
        return pSmemTransInit(deviceId, flags);
    }
    static inline void SmemTransUninit(uint32_t flags) {
        if (pSmemTransUninit) pSmemTransUninit(flags);
    }
    static inline int32_t SmemTransConfigInit(smem_trans_config_t* config) {
        if (pSmemTransConfigInit == nullptr) return -1;
        return pSmemTransConfigInit(config);
    }
    static inline smem_trans_t SmemTransCreate(
        const smem_trans_config_t* config, uint32_t flags) {
        if (pSmemTransCreate == nullptr) return nullptr;
        return pSmemTransCreate(config, flags);
    }
    static inline void SmemTransDestroy(smem_trans_t handle, uint32_t flags) {
        if (pSmemTransDestroy) pSmemTransDestroy(handle, flags);
    }
    static inline int32_t SmemTransGetRpcPort(smem_trans_t handle,
                                              uint16_t* port) {
        if (pSmemTransGetRpcPort == nullptr) return -1;
        return pSmemTransGetRpcPort(handle, port);
    }
    static inline int32_t SmemTransRegisterMem(smem_trans_t handle,
                                               void* address, size_t capacity,
                                               uint32_t flags) {
        if (pSmemTransRegisterMem == nullptr) return -1;
        return pSmemTransRegisterMem(handle, address, capacity, flags);
    }
    static inline int32_t SmemTransBatchRegisterMem(smem_trans_t handle,
                                                    void* addresses[],
                                                    size_t capacities[],
                                                    uint32_t count,
                                                    uint32_t flags) {
        if (pSmemTransBatchRegisterMem == nullptr) return -1;
        return pSmemTransBatchRegisterMem(handle, addresses, capacities, count,
                                          flags);
    }
    static inline int32_t SmemTransDeregisterMem(smem_trans_t handle,
                                                 void* address) {
        if (pSmemTransDeregisterMem == nullptr) return -1;
        return pSmemTransDeregisterMem(handle, address);
    }
    static inline int32_t SmemTransBatchCopy(smem_trans_t handle,
                                             smem_trans_batch_params_t* param,
                                             uint64_t* batchId) {
        if (pSmemTransBatchCopy == nullptr) return -1;
        return pSmemTransBatchCopy(handle, param, batchId);
    }
    static inline int32_t SmemTransBatchStatusQuery(smem_trans_t handle,
                                                    uint64_t batchId,
                                                    int32_t* result,
                                                    uint32_t flags) {
        if (pSmemTransBatchStatusQuery == nullptr) return -1;
        return pSmemTransBatchStatusQuery(handle, batchId, result, flags);
    }
    static inline int32_t SmemSetLogLevel(int level) {
        if (pSmemSetLogLevel == nullptr) return -1;
        return pSmemSetLogLevel(level);
    }

   private:
    // Keep allocation functions and their library alive with outstanding
    // buffers. The native-handle alias owns the shared MemFabricLibrary loader.
    struct MemoryApi {
        std::shared_ptr<void> library;
        smemMemAllocFunc allocate = nullptr;
        smemMemFreeFunc free = nullptr;
    };

    static bool LoadLibraryLocked();
    static void ResetSymbols();
    static std::mutex gMutex;
    static bool gLoaded;
    static void* gLibHandle;
    static std::shared_ptr<const MemoryApi> gMemoryApi;
    static std::weak_ptr<const MemoryApi> gMemoryApiWeak;
    static std::unordered_map<void*, std::shared_ptr<const MemoryApi>>
        gAllocations;

    static smemTransInitFunc pSmemTransInit;
    static smemTransUninitFunc pSmemTransUninit;
    static smemTransConfigInitFunc pSmemTransConfigInit;
    static smemTransCreateFunc pSmemTransCreate;
    static smemTransDestroyFunc pSmemTransDestroy;
    static smemTransGetRpcPortFunc pSmemTransGetRpcPort;
    static smemTransRegisterMemFunc pSmemTransRegisterMem;
    static smemTransBatchRegisterMemFunc pSmemTransBatchRegisterMem;
    static smemTransDeregisterMemFunc pSmemTransDeregisterMem;
    static smemTransBatchCopyFunc pSmemTransBatchCopy;
    static smemTransBatchStatusQueryFunc pSmemTransBatchStatusQuery;
    static smemSetLogLevelFunc pSmemSetLogLevel;
};

// HYBM GVA API: libmf_hybm_core.so.
struct MemFabricGvaConfig {
    bool enabled = false;
    bool explicit_copy = false;
    std::string group;
    std::string protocol = "device_sdma";
    uint32_t rank_count = 0, rank = 0;
    uint16_t device = 0, entity = 64;
    uint64_t hbm_window = 0, dram_window = 0, hbm_size = 0, dram_size = 0;
};
// This adapter uses official HYBM headers in its implementation. No native ABI
// structures or implementation-dependent handles appear in wire metadata.
class MemFabricGvaApi {
   public:
    // Symbols are shared for process lifetime, including failed teardown.
    // No CleanupLibrary: unloading could invalidate live entities or workers.
    static bool LoadLibrary();
    static bool IsLoaded();
    static bool supportsCopy();
    static int copy(void* entity, uint64_t source, uint64_t destination,
                    size_t size, uint32_t direction);
    static int wait(void* entity);
    static int bind(uint16_t device);
    static int initialize(uint16_t device);
    static void uninitialize();
    static void* create(const MemFabricGvaConfig& config);
    static void destroy(void* entity);
    static int reserve(void* entity);
    static int unreserve(void* entity);
    static void* allocate(void* entity, uint32_t memory_type, uint64_t size);
    static int free(void* entity, void* slice);
    static uint64_t address(void* entity, void* slice);
    static uint64_t size(void* entity, void* slice);
    static uint64_t base(void* entity, uint32_t memory_type);
    static int translate(uint64_t address, uint32_t memory_type, uint64_t& out);
    static int exportBlob(void* entity, void* slice,
                          std::vector<uint8_t>& blob);
    static int importBlob(void* entity, bool entity_info,
                          const std::vector<uint8_t>& blob);
    static int map(void* entity);
    static bool supportsConnect();
    static int connect(void* entity, uint32_t rank);
    static int reach(void* entity, uint32_t rank, uint32_t& out);
    static int remove(void* entity, uint32_t rank);
    static int unmap(void* entity, uint32_t rank);

   private:
    MemFabricGvaApi() = delete;
};

}  // namespace tent
}  // namespace mooncake
#endif
