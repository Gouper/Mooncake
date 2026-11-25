// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_MEMFABRIC_GVA_ACCESS_H
#define TENT_MEMFABRIC_GVA_ACCESS_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {

class Config;
class MemFabricSharedMemory;

class MemFabricConfigScope {
   public:
    explicit MemFabricConfigScope(const Config &config);
    ~MemFabricConfigScope();

    MemFabricConfigScope(const MemFabricConfigScope &) = delete;
    MemFabricConfigScope &operator=(const MemFabricConfigScope &) = delete;

   private:
    friend class MemFabricTransport;

    static thread_local const MemFabricConfigScope *gCurrent;
    const Config &mConfig;
    std::shared_ptr<const Config> mSnapshot;
    const MemFabricConfigScope *mPrevious;
};

class MemFabricGvaAccess {
   public:
    static std::shared_ptr<MemFabricGvaAccess> Find(const std::string &segment);

    virtual ~MemFabricGvaAccess() = default;
    virtual Status GetGvaBases(uint64_t &host, uint64_t &device) const = 0;
    virtual MemFabricSharedMemory *GetGvaContext() const = 0;
    virtual Status GetGvaIdentity(std::string &identity) const = 0;
    virtual Status PrepareGvaPeer(const std::string &segment, const std::string &identity) = 0;
    virtual Status CloseGvaPeer(SegmentID id, uint32_t rank, const std::string &segment) = 0;
    virtual Status MapGvaRegion(SegmentID id, void *address, size_t size) = 0;
    virtual Status RevokeGvaRegion(SegmentID id, uint32_t rank, const std::string &segment, uint64_t address) = 0;
    virtual Status uninstall() = 0;
};

}
}

#endif
