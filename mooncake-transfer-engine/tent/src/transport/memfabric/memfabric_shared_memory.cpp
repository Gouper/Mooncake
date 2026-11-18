// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "tent/transport/memfabric/memfabric_shared_memory.h"
#include <algorithm>
#include <limits>
#include <random>
#include <sstream>
namespace mooncake {
namespace tent {
namespace {
constexpr size_t kMaxDescriptors = 4096;
constexpr size_t kMaxBlobSize = 1U << 18;
Status pending(const char* why) {
    return Status::TooManyRequests(
        std::string("MemFabric GVA cleanup pending: ") + why);
}
Status pending(const char* why, int code) {
    return pending(
        (std::string(why) + ", ret=" + std::to_string(code)).c_str());
}
Status nativeError(const char* op) {
    return Status::InternalError(std::string("HYBM GVA ") + op + " failed");
}
Status nativeError(const char* op, int code) {
    return Status::InternalError(std::string("HYBM GVA ") + op +
                                 " failed, ret=" + std::to_string(code));
}
bool blobValid(const nlohmann::json& descriptor) {
    if (!descriptor.is_array() || descriptor.empty() ||
        descriptor.size() > kMaxBlobSize)
        return false;
    for (const auto& byte : descriptor)
        if (!byte.is_number_unsigned() || byte.get<uint64_t>() > 255)
            return false;
    return true;
}
}  // namespace
MemFabricSharedMemory::MemFabricSharedMemory(MemFabricGvaConfig config,
                                             std::string owner)
    : MemFabricSharedMemory(std::move(config), std::move(owner),
                            MemFabricGvaCalls{}) {}
MemFabricSharedMemory::MemFabricSharedMemory(MemFabricGvaConfig config,
                                             std::string owner,
                                             MemFabricGvaCalls api)
    : config_(std::move(config)),
      owner_(std::move(owner)),
      api_(std::move(api)) {
    std::random_device random;
    std::ostringstream generation;
    for (int index = 0; index < 4; ++index)
        generation << std::hex << random() << '-';
    generation_ = generation.str();
    config_status_ = validateConfig();
}
MemFabricSharedMemory::~MemFabricSharedMemory() {
    if (!close().ok()) {
        // Native functions and initialized state must outlive unreleased maps.
        // A failed teardown is deliberately quarantined for process lifetime.
        static auto* retained = new std::vector<MemFabricGvaCalls>;
        static auto* lock = new std::mutex;
        std::lock_guard<std::mutex> guard(*lock);
        retained->push_back(std::move(api_));
    }
}
Status MemFabricSharedMemory::validateConfig() const {
    if (!available())
        return Status::NotImplemented(
            "MemFabric GVA requires compatible HYBM headers and library "
            "symbols");
    if (config_.group.empty() || config_.group.size() > 256)
        return Status::InvalidArgument("Invalid MemFabric GVA group identity");
    for (auto pair : {std::make_pair(config_.hbm_size, config_.hbm_window),
                      std::make_pair(config_.dram_size, config_.dram_window)}) {
        if (pair.first > pair.second ||
            (config_.rank_count &&
             pair.second > UINT64_MAX / config_.rank_count))
            return Status::InvalidArgument(
                "GVA budget/window arithmetic overflow");
    }
    return Status::OK();
}
Status MemFabricSharedMemory::bindDevice() {
    const auto ret = api_->bind(config_.device);
    if (ret) return nativeError("bind device", ret);
    return Status::OK();
}
Status MemFabricSharedMemory::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);
    return initializeContext();
}
Status MemFabricSharedMemory::initializeContext() {
    CHECK_STATUS(config_status_);
    CHECK_STATUS(bindDevice());
    if (entity_ && !entity_blob_.is_null()) return Status::OK();
    if (!initialized_) {
        const auto ret = api_->initialize(config_.device);
        if (ret) return nativeError("initialize", ret);
        initialized_ = true;
    }
    if (!entity_) {
        entity_ = api_->create(config_);
        if (!entity_) return nativeError("create entity");
        const auto ret = api_->reserve(entity_);
        if (ret) {
            api_->destroy(entity_);
            entity_ = nullptr;
            return nativeError("reserve windows", ret);
        }
    }
    for (size_t index = 0; index < window_bases_.size(); ++index) {
        const auto window =
            index == 0 ? config_.hbm_window : config_.dram_window;
        if (!window) continue;
        window_bases_[index] = api_->base(entity_, index + 1);
        if (!window_bases_[index] ||
            config_.rank_count > (UINT64_MAX - window_bases_[index]) / window)
            return Status::InvalidArgument(
                "Invalid native GVA window base/range");
    }
    std::vector<uint8_t> blob;
    const auto ret = api_->exportBlob(entity_, nullptr, blob);
    if (ret) return nativeError("export entity", ret);
    entity_blob_ = blob;
    return Status::OK();
}
MemFabricSharedMemory::Json MemFabricSharedMemory::makeIdentity() const {
    return Json{{"version", 4u},
                {"protocol", config_.protocol},
                {"address_mode", "identity"},
                {"group", config_.group},
                {"owner", owner_},
                {"generation", generation_},
                {"rank", config_.rank},
                {"ranks", config_.rank_count},
                {"entity", config_.entity},
                {"device", config_.device},
                {"hbm_window", config_.hbm_window},
                {"dram_window", config_.dram_window},
                {"hbm_size", config_.hbm_size},
                {"dram_size", config_.dram_size},
                {"entity_blob", entity_blob_}};
}
Status MemFabricSharedMemory::allocate(void** out, size_t size,
                                       uint32_t memory_type,
                                       uint64_t requested) {
    MemoryOptions options;
    options.global_address = requested;
    return allocate(out, size, memory_type, options);
}
Status MemFabricSharedMemory::allocate(void** out, size_t size,
                                       uint32_t memory_type,
                                       MemoryOptions& options) {
    if (!out) return Status::InvalidArgument("Null GVA output");
    *out = nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    if (memory_type != 1 && memory_type != 2)
        return Status::InvalidArgument("Unknown GVA memory type");
    if (pools_.size() >= kMaxDescriptors)
        return Status::InvalidArgument("Too many local GVA allocations");
    const auto index = memory_type - 1;
    const auto budget = index == 0 ? config_.hbm_size : config_.dram_size;
    const auto window = index == 0 ? config_.hbm_window : config_.dram_window;
    if (!size || consumed_[index] > budget || size > budget - consumed_[index])
        return Status::InvalidArgument(
            "GVA allocation exceeds remaining native capacity");
    CHECK_STATUS(config_status_);
    if (!entity_ || entity_blob_.is_null())
        return Status::InvalidArgument(
            "MemFabric GVA context is not initialized");
    CHECK_STATUS(bindDevice());
    void* slice = api_->allocate(entity_, memory_type, size);
    if (!slice) return nativeError("allocate slice (null handle)");
    const auto gva = api_->address(entity_, slice);
    const auto mapped = api_->size(entity_, slice);
    const auto rank_base = window_bases_[index] + config_.rank * window;
    bool range = gva >= rank_base && gva - rank_base <= budget && mapped &&
                 mapped <= budget - (gva - rank_base);
    // HYBM's bump cursor is not rewound by free, including failed fixed-address
    // validation. Account for actual offsets, rather than assuming offset zero.
    if (gva >= rank_base && mapped <= UINT64_MAX - (gva - rank_base))
        consumed_[index] = std::max(consumed_[index], gva - rank_base + mapped);
    else
        consumed_[index] = budget;
    // Network DRAM is accessed through its host VA; HYBM may assign a
    // different device alias when registering that host memory.
    const uint32_t address_type =
        memory_type == 2 && (config_.protocol == "device_rdma" ||
                             config_.protocol == "device_urma")
            ? 2u
            : 1u;
    uint64_t local_address = 0, end = 0;
    const auto first_ret =
        range ? api_->translate(gva, address_type, local_address) : 0;
    const auto last_ret =
        range ? api_->translate(gva + mapped - 1, address_type, end) : 0;
    bool valid = range && mapped == size && !first_ret && !last_ret &&
                 local_address == gva && end == gva + size - 1 &&
                 (!options.global_address || options.global_address == gva) &&
                 pools_.find(gva) == pools_.end();
    if (!valid) {
        const auto rollback_ret = api_->free(entity_, slice);
        if (rollback_ret) {
            rollback_.push_back(slice);
            return nativeError("allocation rollback (resources retained)",
                               rollback_ret);
        }
        if (first_ret)
            return nativeError("translate allocation start", first_ret);
        if (last_ret) return nativeError("translate allocation end", last_ret);
        return Status::InvalidArgument(
            "Native allocation must fit its rank window and requested "
            "identical VA");
    }
    Pool pool;
    pool.slice = slice;
    pool.memory_type = memory_type;
    pool.length = size;
    pool.descriptor = makeIdentity();
    pool.descriptor["type"] = memory_type;
    pool.descriptor["address"] = gva;
    pool.descriptor["size"] = size;
    pool.descriptor["mapped_size"] = mapped;
    pool.descriptor["window_base"] = window_bases_[index];
    pool.descriptor["offset"] = gva - rank_base;
    pool.descriptor["pool_generation"] = ++pool_generation_;
    pools_.emplace(gva, std::move(pool));
    options.global_address = gva;
    options.type = MEMFABRIC;
    options.location = "npu:" + std::to_string(config_.device);
    *out = reinterpret_cast<void*>(gva);
    return Status::OK();
}
bool MemFabricSharedMemory::overlaps(uint64_t address, size_t size) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto intersects = [address, size](uint64_t base, uint64_t length) {
        if (!base || !length || !size) return false;
        return address >= base ? address - base < length
                               : size > base - address;
    };
    for (const auto& [base, pool] : pools_)
        if (intersects(base, pool.length))
            return true;
    // Physical companion mappings and partial maps also own device aliases.
    // They must not fall through to CPU probing, ACL free or trans
    // registration, even when the logical pool is inactive or map has not
    // returned success.
    for (const auto& entry : imported_ranks_) {
        if (entry.second.cleaned) continue;
        for (const auto& [allocation, remote] : entry.second.pools) {
            const auto& descriptor = remote.descriptor;
            const auto base = descriptor.at("address").get<uint64_t>();
            const auto length = descriptor.at("size").get<uint64_t>();
            if (intersects(base, length)) return true;
            uint64_t device_address = 0;
            if (api_ && !api_->translate(base, 1, device_address) &&
                intersects(device_address, length))
                return true;
        }
    }
    return false;
}
Status MemFabricSharedMemory::publish(void* ptr, size_t size,
                                      std::string& out) {
    std::vector<std::string> descriptors;
    CHECK_STATUS(publish({{ptr, size}}, descriptors));
    out = std::move(descriptors.front());
    return Status::OK();
}
Status MemFabricSharedMemory::publish(
    const std::vector<std::pair<void*, size_t>>& ranges,
    std::vector<std::string>& descriptors) {
    if (ranges.empty()) {
        descriptors.clear();
        return Status::OK();
    }
    CHECK_STATUS(bindDevice());
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Pool*> selected;
    selected.reserve(ranges.size());
    for (const auto& [ptr, size] : ranges) {
        const auto found = pools_.find(reinterpret_cast<uint64_t>(ptr));
        if (found == pools_.end() || found->second.length != size)
            return Status::InvalidArgument(
                "Only a complete trusted GVA pool can be registered");
        selected.push_back(&found->second);
    }
    std::vector<Json> staged;
    std::vector<std::string> serialized;
    staged.reserve(selected.size());
    serialized.reserve(selected.size());
    for (const auto* pool : selected) {
        auto descriptor = pool->descriptor;
        if (!descriptor.contains("slice_blob")) {
            std::vector<uint8_t> blob;
            const auto ret = api_->exportBlob(entity_, pool->slice, blob);
            if (ret) return nativeError("export slice", ret);
            if (blob.empty() || blob.size() > kMaxBlobSize)
                return Status::InvalidArgument(
                    "HYBM exported an invalid slice descriptor");
            descriptor["slice_blob"] = blob;
        }
        serialized.push_back(descriptor.dump());
        staged.push_back(std::move(descriptor));
    }
    for (size_t index = 0; index < selected.size(); ++index) {
        selected[index]->descriptor.swap(staged[index]);
        selected[index]->published = true;
    }
    descriptors.swap(serialized);
    return Status::OK();
}
Status MemFabricSharedMemory::unpublish(void* ptr,
                                        std::optional<size_t> size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    auto pool = pools_.find(reinterpret_cast<uint64_t>(ptr));
    if (pool == pools_.end() || (size && pool->second.length != *size))
        return Status::InvalidArgument("Unknown GVA pool or size");
    pool->second.published = false;
    return Status::OK();
}
Status MemFabricSharedMemory::free(void* ptr, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    auto pool = pools_.find(reinterpret_cast<uint64_t>(ptr));
    if (pool == pools_.end() || pool->second.length != size)
        return Status::InvalidArgument("Unknown GVA pool or size");
    if (pool->second.published) return pending("pool is still registered");
    CHECK_STATUS(bindDevice());
    const auto ret = api_->free(entity_, pool->second.slice);
    if (ret) return nativeError("free slice", ret);
    pools_.erase(pool);
    return Status::OK();
}
Status MemFabricSharedMemory::validateDescriptor(
    const Json& descriptor, const std::string& owner) const {
    try {
        for (const char* key :
             {"version", "rank", "ranks", "entity", "device", "hbm_window",
              "dram_window", "hbm_size", "dram_size", "type", "address", "size",
              "mapped_size", "window_base", "offset", "pool_generation"})
            if (!descriptor.at(key).is_number_unsigned())
                return Status::InvalidArgument(
                    "GVA descriptor integers must be unsigned");
        if (descriptor.at("device").get<uint64_t>() > UINT16_MAX)
            return Status::InvalidArgument(
                "GVA descriptor device is not representable");
        if (descriptor.at("version").get<uint64_t>() != 4 ||
            descriptor.at("address_mode").get<std::string>() != "identity" ||
            descriptor.at("protocol").get<std::string>() != config_.protocol ||
            descriptor.at("owner").get<std::string>() != owner ||
            owner.empty() ||
            descriptor.at("group").get<std::string>() != config_.group ||
            descriptor.at("generation").get<std::string>().empty() ||
            descriptor.at("generation").get<std::string>().size() > 128 ||
            descriptor.at("ranks").get<uint64_t>() != config_.rank_count ||
            descriptor.at("rank").get<uint64_t>() >= config_.rank_count ||
            descriptor.at("rank").get<uint64_t>() == config_.rank ||
            descriptor.at("entity").get<uint64_t>() != config_.entity ||
            descriptor.at("hbm_window").get<uint64_t>() != config_.hbm_window ||
            descriptor.at("dram_window").get<uint64_t>() !=
                config_.dram_window ||
            descriptor.at("hbm_size").get<uint64_t>() != config_.hbm_size ||
            descriptor.at("dram_size").get<uint64_t>() != config_.dram_size ||
            !blobValid(descriptor.at("entity_blob")))
            return Status::InvalidArgument(
                "Incompatible GVA peer identity/layout");
        const auto type = descriptor.at("type").get<uint64_t>();
        if (type != 1 && type != 2)
            return Status::InvalidArgument("Invalid GVA memory type");
        const auto address = descriptor.at("address").get<uint64_t>();
        const auto size = descriptor.at("size").get<uint64_t>();
        const auto offset = descriptor.at("offset").get<uint64_t>();
        const auto base = descriptor.at("window_base").get<uint64_t>();
        const auto budget = type == 1 ? config_.hbm_size : config_.dram_size;
        const auto window =
            type == 1 ? config_.hbm_window : config_.dram_window;
        const auto rank = descriptor.at("rank").get<uint64_t>();
        if (!address || !size || size > UINT64_MAX - address ||
            size != descriptor.at("mapped_size").get<uint64_t>() ||
            offset > budget || size > budget - offset || !window ||
            base != window_bases_[type - 1] ||
            rank > (UINT64_MAX - base) / window ||
            offset > UINT64_MAX - (base + rank * window) ||
            address != base + rank * window + offset ||
            !descriptor.at("pool_generation").get<uint64_t>() ||
            !blobValid(descriptor.at("slice_blob")))
            return Status::InvalidArgument("Invalid GVA allocation descriptor");
        return Status::OK();
    } catch (const Json::exception&) {
        return Status::InvalidArgument("Malformed GVA descriptor");
    }
}
MemFabricSharedMemory::RemotePool& MemFabricSharedMemory::recordPool(
    ImportedRank& peer, const Json& descriptor) {
    const auto address = descriptor.at("address").get<uint64_t>();
    auto& pool = peer.pools[address];
    if (pool.descriptor.is_null()) pool.descriptor = descriptor;
    return pool;
}
Status MemFabricSharedMemory::importPeer(ImportedRank& peer,
                                         const Json& descriptor) {
    auto& pool = recordPool(peer, descriptor);
    if (pool.imported) return Status::OK();
    const auto ret = api_->importBlob(
        entity_, false,
        descriptor.at("slice_blob").get<std::vector<uint8_t>>());
    if (ret)
        return nativeError("import slice (rank retained for cleanup)", ret);
    pool.imported = true;
    return Status::OK();
}
Status MemFabricSharedMemory::verifyPeerMapping(uint32_t rank,
                                                const RemotePool& pool) {
    uint32_t reach = 0;
    const auto ret = api_->reach(entity_, rank, reach);
    if (ret) return nativeError("query direct reachability", ret);
    if (!(reach & 1u))
        return Status::NotImplemented(
            "Peer lacks HYBM MTE direct device access");
    const auto address = pool.descriptor.at("address").get<uint64_t>();
    const auto size = pool.descriptor.at("size").get<uint64_t>();
    uint64_t first = 0, last = 0;
    auto code = api_->translate(address, 1, first);
    if (code) return nativeError("translate peer start", code);
    code = api_->translate(address + size - 1, 1, last);
    if (code) return nativeError("translate peer end", code);
    if (first != address || last != address + size - 1)
        return Status::InvalidArgument(
            "Peer allocation has no identical contiguous device VA");
    return Status::OK();
}
Status MemFabricSharedMemory::map(const std::string& owner,
                                  const std::string& descriptor,
                                  uint64_t address, size_t size) {
    return mapOrPrepare(owner, descriptor, address, size, true);
}
Status MemFabricSharedMemory::prepare(const std::string& owner,
                                      const std::string& descriptor,
                                      uint64_t address, size_t size) {
    return mapOrPrepare(owner, descriptor, address, size, false);
}
Status MemFabricSharedMemory::mapOrPrepare(const std::string& owner,
                                           const std::string& descriptor,
                                           uint64_t address, size_t size,
                                           bool direct) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    if (direct && config_.protocol != "device_sdma")
        return Status::NotImplemented(
            "Network GVA does not provide a direct device mapping");
    if (config_.protocol != "device_sdma" &&
        (!api_ || !api_->supportsConnect()))
        return Status::NotImplemented(
            "HYBM network connect symbol unavailable");
    if (descriptor.size() > (16U << 20))
        return Status::InvalidArgument("GVA descriptor bundle exceeds 16 MiB");
    try {
        auto descriptors = Json::parse(descriptor);
        if (!descriptors.is_array()) descriptors = Json::array({descriptors});
        if (descriptors.empty() || descriptors.size() > kMaxDescriptors)
            return Status::InvalidArgument("Invalid GVA allocation bundle");
        const Json* requested = nullptr;
        std::map<uint64_t, uint64_t> ranges;
        const auto rank = descriptors.front().at("rank").get<uint32_t>();
        for (const auto& desc : descriptors) {
            CHECK_STATUS(validateDescriptor(desc, owner));
            if (desc.at("rank").get<uint32_t>() != rank ||
                desc.at("generation") != descriptors.front().at("generation") ||
                desc.at("entity_blob") !=
                    descriptors.front().at("entity_blob") ||
                desc.at("device") != descriptors.front().at("device"))
                return Status::InvalidArgument(
                    "Inconsistent GVA allocation bundle");
            const auto base = desc.at("address").get<uint64_t>();
            const auto length = desc.at("size").get<uint64_t>();
            if (!ranges.emplace(base, length).second)
                return Status::InvalidArgument("Duplicate GVA allocation");
            if (base == address && length == size) requested = &desc;
        }
        uint64_t end = 0;
        for (const auto& [base, length] : ranges) {
            if (base < end)
                return Status::InvalidArgument("Overlapping GVA allocations");
            end = base + length;
        }
        if (!requested)
            return Status::InvalidArgument(
                "Map requires a complete published allocation");
        CHECK_STATUS(config_status_);
        if (!entity_ || entity_blob_.is_null())
            return Status::InvalidArgument(
                "MemFabric GVA context is not initialized");
        CHECK_STATUS(bindDevice());
        // Validate the entire bundle against retained descriptors before any
        // native import, preserving other allocations' active state on failure.
        const auto existing = imported_ranks_.find(rank);
        if (existing != imported_ranks_.end() && !existing->second.cleaned) {
            const auto& peer = existing->second;
            if (peer.owner != owner ||
                peer.generation !=
                    requested->at("generation").get<std::string>() ||
                peer.entity_blob != requested->at("entity_blob"))
                return pending("old peer generation must exit first");
            size_t retained = peer.pools.size();
            for (const auto& desc : descriptors) {
                if (!peer.pools.count(desc.at("address").get<uint64_t>()) &&
                    ++retained > kMaxDescriptors)
                    return Status::InvalidArgument(
                        "Too many retained peer allocations");
                const auto base = desc.at("address").get<uint64_t>();
                const auto length = desc.at("size").get<uint64_t>();
                for (const auto& [old_base, old] : peer.pools) {
                    if (desc.at("device") != old.descriptor.at("device"))
                        return Status::InvalidArgument(
                            "Peer device changed within its generation");
                    if (base == old_base) {
                        if (desc != old.descriptor)
                            return pending(
                                "peer allocation descriptor changed");
                    } else {
                        const auto old_length =
                            old.descriptor.at("size").get<uint64_t>();
                        if (base >= old_base ? base - old_base < old_length
                                             : old_base - base < length)
                            return Status::InvalidArgument(
                                "New allocation overlaps retained peer "
                                "allocation");
                    }
                }
            }
        }
        auto& peer = imported_ranks_[rank];
        if (peer.cleaned) peer = ImportedRank{};
        if (peer.owner.empty()) {
            peer.owner = owner;
            peer.generation = requested->at("generation").get<std::string>();
            peer.entity_blob = requested->at("entity_blob");
        }
        // Record the requested allocation before HYBM can acquire resources.
        auto& pool = recordPool(peer, *requested);
        pool.active = pool.mapped = pool.copy_ready = false;
        const auto import_ret = api_->importBlob(
            entity_, true, peer.entity_blob.get<std::vector<uint8_t>>());
        if (import_ret) return nativeError("import entity", import_ret);
        for (const auto& desc : descriptors)
            CHECK_STATUS(importPeer(peer, desc));
        auto ret = api_->map(entity_);
        if (ret)
            return pending(
                "map peer allocations (retry when reciprocal imports are "
                "ready)",
                ret);
        if (config_.protocol == "device_sdma") {
            CHECK_STATUS(verifyPeerMapping(rank, pool));
            pool.mapped = true;
        } else {
            ret = api_->connect(entity_, rank);
            if (ret) return pending("connect peer (retry when ready)", ret);
        }
        pool.active = pool.copy_ready = true;
        return Status::OK();
    } catch (const Json::exception&) {
        return Status::InvalidArgument("Malformed GVA allocation bundle");
    }
}
Status MemFabricSharedMemory::cleanupPeer(uint32_t rank, ImportedRank& peer) {
    if (peer.cleaned) return Status::OK();
    for (const auto& [base, pool] : peer.pools)
        if (pool.active) return pending("another allocation is active");
    CHECK_STATUS(bindDevice());
    auto ret = api_->remove(entity_, rank);
    if (ret) return pending("remove imported rank", ret);
    ret = api_->unmap(entity_, rank);
    if (ret) return pending("unmap rank", ret);
    for (const auto& [base, pool] : peer.pools) {
        const auto size = pool.descriptor.at("size").get<uint64_t>();
        uint64_t va = 0;
        const auto first_ret = api_->translate(base, 1, va);
        if (first_ret != -12)
            return pending("GVA registry still contains imported mappings",
                           first_ret);
        const auto last_ret = api_->translate(base + size - 1, 1, va);
        if (last_ret != -12)
            return pending("GVA registry still contains imported mappings",
                           last_ret);
    }
    peer.cleaned = true;
    return Status::OK();
}
Status MemFabricSharedMemory::unmap(const std::string& owner,
                                    uint64_t address) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    for (auto& [rank, peer] : imported_ranks_) {
        if (peer.owner != owner) continue;
        auto pool = peer.pools.find(address);
        if (pool == peer.pools.end()) continue;
        pool->second.active = pool->second.mapped = pool->second.copy_ready =
            false;
        return cleanupPeer(rank, peer);
    }
    return Status::InvalidArgument("Unknown GVA mapping");
}
Status MemFabricSharedMemory::resolve(uint64_t address, size_t size,
                                      void** device_address) {
    if (!device_address)
        return Status::InvalidArgument("Null device address output");
    *device_address = nullptr;
    if (!address || !size || size > UINT64_MAX - address)
        return Status::InvalidArgument("Invalid GVA resolution range");
    std::lock_guard<std::mutex> lock(mutex_);
    auto contains = [address, size](uint64_t base, uint64_t length) {
        return address >= base && address - base < length &&
               size <= length - (address - base);
    };
    bool live = false;
    for (const auto& [base, pool] : pools_)
        if (contains(base, pool.length)) live = true;
    for (const auto& [rank, peer] : imported_ranks_) {
        if (peer.cleaned) continue;
        for (const auto& [base, pool] : peer.pools)
            if (pool.active && pool.mapped &&
                contains(base, pool.descriptor.at("size").get<uint64_t>()))
                live = true;
    }
    if (!live)
        return Status::InvalidArgument("GVA range is not locally accessible");
    CHECK_STATUS(bindDevice());
    uint64_t first = 0, last = 0;
    auto ret = api_->translate(address, 1, first);
    if (ret) return nativeError("resolve allocation start", ret);
    ret = api_->translate(address + size - 1, 1, last);
    if (ret) return nativeError("resolve allocation end", ret);
    if (!first || first != address || size > UINT64_MAX - first ||
        last != first + size - 1)
        return Status::InvalidArgument(
            "GVA range has no contiguous device mapping");
    *device_address = reinterpret_cast<void*>(first);
    return Status::OK();
}
Status MemFabricSharedMemory::validateCopyTarget(
    const std::string& owner, const std::string& descriptor) const {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        const auto published = Json::parse(descriptor);
        CHECK_STATUS(validateDescriptor(published, owner));
        auto iterator =
            imported_ranks_.find(published.at("rank").get<uint32_t>());
        if (iterator == imported_ranks_.end() || iterator->second.cleaned)
            return Status::InvalidArgument("Remote GVA rank is not prepared");
        const auto found = iterator->second.pools.find(
            published.at("address").get<uint64_t>());
        if (found == iterator->second.pools.end() || !found->second.active ||
            !found->second.copy_ready || found->second.descriptor != published)
            return Status::InvalidArgument(
                "Remote GVA publication differs from mapped allocation");
        return Status::OK();
    } catch (const Json::exception&) {
        return Status::InvalidArgument("Malformed GVA copy descriptor");
    }
}
Status MemFabricSharedMemory::beginCopy(uint64_t source, uint64_t destination,
                                        size_t size, uint32_t& direction) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!size || !source || !destination || size > UINT64_MAX - source ||
        size > UINT64_MAX - destination)
        return Status::InvalidArgument("Invalid GVA copy range");
    if (copy_failed_) return pending("native copy completion remains unknown");
    auto type = [&](uint64_t address) -> uint32_t {
        auto contains = [&](uint64_t base, uint64_t length) {
            return address >= base && address - base < length &&
                   size <= length - (address - base);
        };
        for (const auto& [base, pool] : pools_)
            if (pool.published && contains(base, pool.length))
                return pool.memory_type;
        for (const auto& [rank, peer] : imported_ranks_) {
            if (peer.cleaned) continue;
            for (const auto& [base, pool] : peer.pools)
                if (pool.active && pool.copy_ready &&
                    contains(base, pool.descriptor.at("size").get<uint64_t>()))
                    return pool.descriptor.at("type").get<uint32_t>();
        }
        return 0;
    };
    const auto src_type = type(source), dst_type = type(destination);
    if (!src_type || !dst_type)
        return Status::InvalidArgument(
            "GVA copy requires published local or active mapped logical pools");
    // HYBM_GLOBAL_{HOST,DEVICE}_TO_GLOBAL_{HOST,DEVICE}.
    direction = (src_type == 1 ? 8u : 4u) + (dst_type == 1 ? 1u : 0u);
    ++pending_copies_;
    return Status::OK();
}
Status MemFabricSharedMemory::runCopy(uint64_t source, uint64_t destination,
                                      size_t size, uint32_t direction) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (copy_failed_) {
            if (pending_copies_) --pending_copies_;
            return pending("previous native copy completion remains unknown");
        }
    }
    if (!api_->supportsCopy()) {
        cancelCopy();
        return Status::NotImplemented("HYBM copy symbols unavailable");
    }
    // beginCopy pins entity and all pools before queue admission. No native
    // blocking call holds mutex_, so status queries and teardown can progress.
    const auto bound = api_->bind(config_.device);
    const auto result =
        bound ? bound
              : api_->copy(entity_, source, destination, size, direction);
    const bool drained =
        bound || result == 0 ||
        (config_.protocol == "device_sdma" && api_->wait(entity_) == 0);
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_) --pending_copies_;
    if (!drained) copy_failed_ = true;
    return result == 0 ? Status::OK() : nativeError("synchronous copy", result);
}
Status MemFabricSharedMemory::runBatchCopy(
    const std::vector<MemFabricGvaCopy>& copies, uint32_t direction) {
    if (copies.empty()) return Status::OK();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (copy_failed_) {
            pending_copies_ -= std::min(pending_copies_, copies.size());
            return pending("previous native copy completion remains unknown");
        }
    }
    if (copies.size() > UINT32_MAX || !supportsBatchCopy()) {
        cancelCopy(copies.size());
        return Status::NotImplemented("HYBM batch copy capability unavailable");
    }
    const auto bound = api_->bind(config_.device);
    const auto result =
        bound ? bound : api_->batchCopy(entity_, copies, direction);
    const bool drained =
        bound || !result ||
        (config_.protocol == "device_sdma" && api_->wait(entity_) == 0);
    std::lock_guard<std::mutex> lock(mutex_);
    pending_copies_ -= std::min(pending_copies_, copies.size());
    if (!drained) copy_failed_ = true;
    return !result ? Status::OK()
                   : nativeError("synchronous batch copy", result);
}
void MemFabricSharedMemory::failCopy(size_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_copies_ -= std::min(pending_copies_, count);
    copy_failed_ = true;
}
void MemFabricSharedMemory::cancelCopy(size_t count) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_copies_ -= std::min(pending_copies_, count);
}
Status MemFabricSharedMemory::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    if (!pools_.empty()) return pending("local pools still allocated");
    for (const auto& entry : imported_ranks_)
        if (!entry.second.cleaned) return pending("peer mappings remain");
    if (!initialized_) return Status::OK();
    CHECK_STATUS(bindDevice());
    for (auto iterator = rollback_.begin(); iterator != rollback_.end();) {
        const auto ret = api_->free(entity_, *iterator);
        if (ret) return pending("allocation rollback failed", ret);
        iterator = rollback_.erase(iterator);
    }
    if (entity_) {
        const auto ret = api_->unreserve(entity_);
        if (ret) return pending("window release failed", ret);
        api_->destroy(entity_);
        entity_ = nullptr;
        entity_blob_ = Json{};
    }
    api_->uninitialize();
    initialized_ = false;
    consumed_ = {{0, 0}};
    window_bases_ = {{0, 0}};
    imported_ranks_.clear();
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
