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
#include "tent/transport/memfabric/memfabric_library.h"
namespace mooncake {
namespace tent {
class MemFabricSharedMemory {
   public:
    MemFabricSharedMemory(MemFabricGvaConfig config, std::string owner,
                          std::shared_ptr<MemFabricGvaApi> api);
    ~MemFabricSharedMemory();
    bool available() const { return config_.enabled && bool(api_); }
    // Initialize the entity and reserve windows before allocation or mapping.
    Status initialize();
    Status allocate(void**, size_t, uint32_t memory_type, uint64_t requested);
    Status allocate(void**, size_t, uint32_t memory_type, MemoryOptions&);
    Status free(void*, size_t);
    bool describe(void*, size_t, MemoryOptions&) const;
    bool overlaps(uint64_t, size_t) const;
    Status publish(void*, size_t, std::string&);
    Status unpublish(void*);
    Status map(const std::string& owner, const std::string& descriptor,
               uint64_t address, size_t size);
    Status prepare(const std::string& owner, const std::string& descriptor,
                   uint64_t address, size_t size);
    Status unmap(const std::string& owner, uint64_t address);
    // Resolve only live local pools or successfully mapped, active peer pools.
    Status resolve(uint64_t address, size_t size, void** device_address);
    Status validateCopyTarget(const std::string& owner,
                              const std::string& descriptor) const;
    Status beginCopy(uint64_t source, uint64_t destination, size_t size,
                     uint32_t& direction);
    Status runCopy(uint64_t source, uint64_t destination, size_t size,
                   uint32_t direction);
    void cancelCopy();
    void failCopy();
    Status close();

   private:
    using Json = nlohmann::json;
    struct Pool {
        void* slice = nullptr;
        uint64_t address = 0, length = 0, host_address = 0, device_address = 0;
        uint32_t memory_type = 0;
        bool published = false;
        Json descriptor;
    };
    struct RemotePool {
        Json descriptor;
        bool active = false, imported = false, mapped = false,
             copy_ready = false;
    };
    // Native imports grouped by rank, not a peer discovery/connection list.
    // owner/generation identify the publication; pools retain per-allocation
    // import, active-map and copy-ready state that HYBM cannot query for TENT.
    struct ImportedRank {
        std::string owner, generation;
        Json entity_blob;
        std::map<uint64_t, RemotePool> pools;
        bool cleaned = false;
    };
    Status mapOrPrepare(const std::string&, const std::string&, uint64_t,
                        size_t, bool);
    Status validateConfig() const;
    // Resource helpers below require mutex_ to be held by the caller.
    // Configuration is immutable; device binding belongs to each native caller.
    Status initializeContext();
    Status bindDevice();
    Status validateDescriptor(const Json&, const std::string&) const;
    Status recordPeer(const Json&, const std::string&);
    Status importPeer(const Json&, const std::string&);
    Status verifyPeerMapping(uint32_t, const RemotePool&);
    Status cleanupPeer(uint32_t, ImportedRank&);
    Json makeIdentity() const;
    mutable std::mutex mutex_;
    MemFabricGvaConfig config_;
    Status config_status_ = Status::OK();
    std::string owner_, generation_;
    std::shared_ptr<MemFabricGvaApi> api_;
    void* entity_ = nullptr;
    bool initialized_ = false;
    std::map<uint64_t, Pool> pools_;
    std::array<uint64_t, 2> window_bases_{{0, 0}};
    std::array<uint64_t, 2> consumed_{{0, 0}};
    std::vector<void*> rollback_;  // failed frees stay owned for retry
    // Retain failed imports for retry/cleanup. Native rank cleanup waits until
    // its last active allocation exits, including logically unmapped
    // companions.
    std::map<uint32_t, ImportedRank> imported_ranks_;
    Json entity_blob_;
    uint64_t pool_generation_ = 0;
    size_t pending_copies_ = 0;
    bool copy_failed_ = false;
};
}  // namespace tent
}  // namespace mooncake
#endif
