// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#ifndef TENT_MEMFABRIC_SHARED_MEMORY_H
#define TENT_MEMFABRIC_SHARED_MEMORY_H
#include <array>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/memfabric/memfabric_under_api.h"
namespace mooncake {
namespace tent {
// Optional backend injection for deterministic CPU tests. Production calls the
// process-wide static API directly; no native virtual adapter is instantiated.
class MemFabricGvaBackend {
   public:
    virtual ~MemFabricGvaBackend() = default;
    virtual bool supportsCopy() const { return true; }
    virtual int copy(void*, uint64_t, uint64_t, size_t, uint32_t) { return -1; }
    virtual bool supportsBatchCopy() const { return false; }
    virtual int batchCopy(void*, const std::vector<MemFabricGvaCopy>&,
                          uint32_t) {
        return -1;
    }
    virtual int wait(void*) { return -1; }
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
    virtual uint64_t base(void*, uint32_t) { return 0; }
    virtual int translate(uint64_t, uint32_t, uint64_t&) = 0;
    virtual int exportBlob(void*, void*, std::vector<uint8_t>&) = 0;
    virtual int importBlob(void*, bool, const std::vector<uint8_t>&) = 0;
    virtual int map(void*) = 0;
    virtual bool supportsConnect() const { return false; }
    virtual int connect(void*, uint32_t) { return -1; }
    virtual int reach(void*, uint32_t, uint32_t&) = 0;
    virtual int remove(void*, uint32_t) = 0;
    virtual int unmap(void*, uint32_t) = 0;
};
class MemFabricGvaCalls {
   public:
    MemFabricGvaCalls() : available_(MemFabricGvaApi::LoadLibrary()) {}
    MemFabricGvaCalls(std::nullptr_t) : available_(false) {}
    template <typename Backend>
    MemFabricGvaCalls(std::shared_ptr<Backend> backend)
        : backend_(std::move(backend)), available_(bool(backend_)) {}
    explicit operator bool() const { return available_; }
    const MemFabricGvaCalls* operator->() const { return this; }
    bool supportsCopy() const {
        return backend_ ? backend_->supportsCopy()
                        : MemFabricGvaApi::supportsCopy();
    }
    int copy(void* entity, uint64_t source, uint64_t destination, size_t size,
             uint32_t direction) const {
        return backend_ ? backend_->copy(entity, source, destination, size,
                                         direction)
                        : MemFabricGvaApi::copy(entity, source, destination,
                                                size, direction);
    }
    bool supportsBatchCopy() const {
        return backend_ ? backend_->supportsBatchCopy()
                        : MemFabricGvaApi::supportsBatchCopy();
    }
    int batchCopy(void* entity, const std::vector<MemFabricGvaCopy>& copies,
                  uint32_t direction) const {
        return backend_ ? backend_->batchCopy(entity, copies, direction)
                        : MemFabricGvaApi::batchCopy(entity, copies, direction);
    }
    int wait(void* entity) const {
        return backend_ ? backend_->wait(entity)
                        : MemFabricGvaApi::wait(entity);
    }
    int bind(uint16_t device) const {
        return backend_ ? backend_->bind(device)
                        : MemFabricGvaApi::bind(device);
    }
    int initialize(uint16_t device) const {
        return backend_ ? backend_->initialize(device)
                        : MemFabricGvaApi::initialize(device);
    }
    void uninitialize() const {
        if (backend_)
            backend_->uninitialize();
        else
            MemFabricGvaApi::uninitialize();
    }
    void* create(const MemFabricGvaConfig& config) const {
        return backend_ ? backend_->create(config)
                        : MemFabricGvaApi::create(config);
    }
    void destroy(void* entity) const {
        if (backend_)
            backend_->destroy(entity);
        else
            MemFabricGvaApi::destroy(entity);
    }
    int reserve(void* entity) const {
        return backend_ ? backend_->reserve(entity)
                        : MemFabricGvaApi::reserve(entity);
    }
    int unreserve(void* entity) const {
        return backend_ ? backend_->unreserve(entity)
                        : MemFabricGvaApi::unreserve(entity);
    }
    void* allocate(void* entity, uint32_t memory_type, uint64_t size) const {
        return backend_ ? backend_->allocate(entity, memory_type, size)
                        : MemFabricGvaApi::allocate(entity, memory_type, size);
    }
    int free(void* entity, void* slice) const {
        return backend_ ? backend_->free(entity, slice)
                        : MemFabricGvaApi::free(entity, slice);
    }
    uint64_t address(void* entity, void* slice) const {
        return backend_ ? backend_->address(entity, slice)
                        : MemFabricGvaApi::address(entity, slice);
    }
    uint64_t size(void* entity, void* slice) const {
        return backend_ ? backend_->size(entity, slice)
                        : MemFabricGvaApi::size(entity, slice);
    }
    uint64_t base(void* entity, uint32_t memory_type) const {
        return backend_ ? backend_->base(entity, memory_type)
                        : MemFabricGvaApi::base(entity, memory_type);
    }
    int translate(uint64_t address, uint32_t memory_type, uint64_t& out) const {
        return backend_ ? backend_->translate(address, memory_type, out)
                        : MemFabricGvaApi::translate(address, memory_type, out);
    }
    int exportBlob(void* entity, void* slice,
                   std::vector<uint8_t>& blob) const {
        return backend_ ? backend_->exportBlob(entity, slice, blob)
                        : MemFabricGvaApi::exportBlob(entity, slice, blob);
    }
    int importBlob(void* entity, bool entity_info,
                   const std::vector<uint8_t>& blob) const {
        return backend_
                   ? backend_->importBlob(entity, entity_info, blob)
                   : MemFabricGvaApi::importBlob(entity, entity_info, blob);
    }
    int map(void* entity) const {
        return backend_ ? backend_->map(entity) : MemFabricGvaApi::map(entity);
    }
    bool supportsConnect() const {
        return backend_ ? backend_->supportsConnect()
                        : MemFabricGvaApi::supportsConnect();
    }
    int connect(void* entity, uint32_t rank) const {
        return backend_ ? backend_->connect(entity, rank)
                        : MemFabricGvaApi::connect(entity, rank);
    }
    int reach(void* entity, uint32_t rank, uint32_t& out) const {
        return backend_ ? backend_->reach(entity, rank, out)
                        : MemFabricGvaApi::reach(entity, rank, out);
    }
    int remove(void* entity, uint32_t rank) const {
        return backend_ ? backend_->remove(entity, rank)
                        : MemFabricGvaApi::remove(entity, rank);
    }
    int unmap(void* entity, uint32_t rank) const {
        return backend_ ? backend_->unmap(entity, rank)
                        : MemFabricGvaApi::unmap(entity, rank);
    }

   private:
    std::shared_ptr<MemFabricGvaBackend> backend_;
    bool available_;
};
class MemFabricSharedMemory {
   public:
    MemFabricSharedMemory(MemFabricGvaConfig config, std::string owner);
    MemFabricSharedMemory(MemFabricGvaConfig config, std::string owner,
                          MemFabricGvaCalls api);
    ~MemFabricSharedMemory();
    bool available() const { return config_.enabled && bool(api_); }
    // Initialize the entity and reserve windows before allocation or mapping.
    Status initialize();
    Status allocate(void**, size_t, uint32_t memory_type, uint64_t requested);
    Status allocate(void**, size_t, uint32_t memory_type, MemoryOptions&);
    Status free(void*, size_t);
    bool overlaps(uint64_t, size_t) const;
    Status publish(void*, size_t, std::string&);
    // Publish all complete allocations together; failure leaves publication
    // state and the caller's descriptors unchanged.
    Status publish(const std::vector<std::pair<void*, size_t>>& ranges,
                   std::vector<std::string>& descriptors);
    Status unpublish(void*, std::optional<size_t> size = std::nullopt);
    Status map(const std::string& owner, const std::string& descriptor,
               uint64_t address, size_t size);
    Status prepare(const std::string& owner, const std::string& descriptor,
                   uint64_t address, size_t size);
    Status unmap(const std::string& owner, uint64_t address);
    // Resolve only live local pools or successfully mapped, active peer pools.
    Status resolve(uint64_t address, size_t size, void** device_address);
    Status validateCopyTarget(const std::string& owner, uint64_t address,
                              const std::string& descriptor) const;
    Status beginCopy(uint64_t source, uint64_t destination, size_t size,
                     uint32_t& direction);
    Status runCopy(uint64_t source, uint64_t destination, size_t size,
                   uint32_t direction);
    bool supportsBatchCopy() const { return api_ && api_->supportsBatchCopy(); }
    // Each member has already been pinned by beginCopy; settles all those pins.
    Status runBatchCopy(const std::vector<MemFabricGvaCopy>&,
                        uint32_t direction);
    void cancelCopy(size_t count = 1);
    void failCopy(size_t count = 1);
    Status close();

   private:
    using Json = nlohmann::json;
    struct Pool {
        void* slice = nullptr;
        uint64_t length = 0;
        uint32_t memory_type = 0;
        bool published = false;
        Json descriptor;
    };
    struct RemotePool {
        Json descriptor;
        std::string publication;
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
    // Device binding belongs to each native caller.
    Status initializeContext();
    void destroyEntity();
    Status bindDevice();
    Status validateDescriptor(const Json&, const std::string&) const;
    RemotePool& recordPool(ImportedRank&, const Json&);
    Status importPeer(ImportedRank&, const Json&);
    Status verifyPeerMapping(uint32_t, const RemotePool&);
    Status cleanupPeer(uint32_t, ImportedRank&);
    Json makeIdentity() const;
    mutable std::mutex mutex_;
    MemFabricGvaConfig config_;
    Status config_status_ = Status::OK();
    std::string owner_, generation_;
    MemFabricGvaCalls api_;
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
