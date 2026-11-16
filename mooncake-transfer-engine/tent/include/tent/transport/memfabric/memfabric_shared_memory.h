// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#ifndef TENT_MEMFABRIC_SHARED_MEMORY_H
#define TENT_MEMFABRIC_SHARED_MEMORY_H
#include <array>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/thirdparty/nlohmann/json.h"
namespace mooncake {
namespace tent {
struct MemFabricGvaConfig {
    bool enabled = false;
    bool explicit_copy = false;
    std::string group;
    uint32_t rank_count = 0, rank = 0;
    uint16_t device = 0, entity = 64;
    uint64_t hbm_window = 0, dram_window = 0, hbm_size = 0, dram_size = 0;
};
// This adapter uses official HYBM headers in its implementation. No native ABI
// structures or implementation-dependent handles appear in wire metadata.
class MemFabricGvaApi {
   public:
    virtual ~MemFabricGvaApi() = default;
    static std::shared_ptr<MemFabricGvaApi> load();
    virtual int bind(uint16_t) = 0;
    virtual int initialize(uint16_t) = 0;
    virtual void uninitialize() = 0;
    virtual void* create(const MemFabricGvaConfig&) = 0;
    virtual void destroy(void*) = 0;
    virtual int reserve(void*) = 0;
    virtual int unreserve(void*) = 0;
    virtual void* allocate(void*, uint32_t, uint64_t) = 0;
    virtual int free(void*, void*) = 0;
    virtual uint64_t address(void*, void*) = 0;
    virtual uint64_t size(void*, void*) = 0;
    virtual int translate(uint64_t, uint32_t, uint64_t&) = 0;
    virtual int exportBlob(void*, void*, std::vector<uint8_t>&) = 0;
    virtual int importBlob(void*, bool, const std::vector<uint8_t>&) = 0;
    virtual int map(void*) = 0;
    virtual int reach(void*, uint32_t, uint32_t&) = 0;
    virtual int remove(void*, uint32_t) = 0;
    virtual int unmap(void*, uint32_t) = 0;
};
class MemFabricSharedMemory {
   public:
    MemFabricSharedMemory(MemFabricGvaConfig config, std::string owner,
                          std::shared_ptr<MemFabricGvaApi> api);
    ~MemFabricSharedMemory();
    bool available() const { return config_.enabled && bool(api_); }
    Status allocate(void**, size_t, uint32_t memory_type, uint64_t requested);
    Status free(void*, size_t);
    bool describe(void*, size_t, MemoryOptions&) const;
    bool overlaps(uint64_t, size_t) const;
    Status publish(void*, size_t, std::string&);
    Status unpublish(void*);
    Status map(const std::string& owner, const std::string& descriptor,
               uint64_t address, size_t size);
    Status unmap(const std::string& owner, uint64_t address);
    // Resolve only live local pools or successfully mapped, active peer pools.
    Status resolve(uint64_t address, size_t size, void** device_address);
    Status close();

   private:
    using Json = nlohmann::json;
    struct Pool {
        void* slice = nullptr;
        uint64_t address = 0, length = 0, host_address = 0, device_address = 0;
        bool published = false;
        Json descriptor;
    };
    struct Peer {
        std::string owner, generation;
        Json entity_blob;
        std::array<Json, 2> descriptors;
        std::array<bool, 2> active{{false, false}};
        std::array<bool, 2> imported{{false, false}};
        bool cleaned = false, mapped = false;
    };
    Status ensureLocked();
    Status validateLocked(const Json&, const std::string&) const;
    Status recordLocked(const Json&, const std::string&);
    Status importLocked(const Json&, const std::string&);
    Status verifyLocked(const Peer&);
    Status cleanupLocked(uint32_t, Peer&);
    Json identityLocked() const;
    mutable std::mutex mutex_;
    MemFabricGvaConfig config_;
    std::string owner_, generation_;
    std::shared_ptr<MemFabricGvaApi> api_;
    void* entity_ = nullptr;
    bool initialized_ = false;
    std::array<Pool, 2> pools_;
    std::array<uint64_t, 2> consumed_{{0, 0}};
    std::vector<void*> rollback_;  // failed frees stay owned for retry
    std::map<uint32_t, Peer> peers_;
    Json entity_blob_;
    uint64_t pool_generation_ = 0;
};
}  // namespace tent
}  // namespace mooncake
#endif
