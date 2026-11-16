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
constexpr uint64_t kPage = 2ULL << 20;
Status pending(const char* why) {
    return Status::TooManyRequests(
        std::string("MemFabric GVA cleanup pending: ") + why);
}
Status nativeError(const char* op) {
    return Status::InternalError(std::string("HYBM GVA ") + op + " failed");
}
bool blobValid(const nlohmann::json& j) {
    if (!j.is_array() || j.empty() || j.size() > (1U << 18)) return false;
    for (const auto& b : j)
        if (!b.is_number_unsigned() || b.get<uint64_t>() > 255) return false;
    return true;
}
}  // namespace
MemFabricSharedMemory::MemFabricSharedMemory(
    MemFabricGvaConfig c, std::string owner,
    std::shared_ptr<MemFabricGvaApi> api)
    : config_(std::move(c)), owner_(std::move(owner)), api_(std::move(api)) {
    std::random_device random;
    std::ostringstream id;
    for (int i = 0; i < 4; ++i) id << std::hex << random() << '-';
    generation_ = id.str();
}
MemFabricSharedMemory::~MemFabricSharedMemory() {
    if (!close().ok()) {
        // Native functions and initialized state must outlive unreleased maps.
        // A failed teardown is deliberately quarantined for process lifetime.
        static auto* retained =
            new std::vector<std::shared_ptr<MemFabricGvaApi>>;
        static auto* lock = new std::mutex;
        std::lock_guard<std::mutex> guard(*lock);
        retained->push_back(std::move(api_));
    }
}
Status MemFabricSharedMemory::ensureLocked() {
    if (!available())
        return Status::NotImplemented(
            "MemFabric GVA requires compatible HYBM headers and library "
            "symbols");
    if (config_.protocol != "device_sdma" &&
        config_.protocol != "device_rdma" && config_.protocol != "device_urma")
        return Status::InvalidArgument("Unsupported GVA protocol");
    if (config_.trans_url.size() >= 64)
        return Status::InvalidArgument("GVA transport URL too long");
    if (config_.group.empty() || config_.group.size() > 256 ||
        config_.rank_count == 0 || config_.rank >= config_.rank_count ||
        config_.entity < 64 || config_.entity >= 128 ||
        (!config_.hbm_window && !config_.dram_window))
        return Status::InvalidArgument(
            "Invalid MemFabric GVA rank/group/entity");
    for (auto pair : {std::make_pair(config_.hbm_size, config_.hbm_window),
                      std::make_pair(config_.dram_size, config_.dram_window)}) {
        if ((bool(pair.first) != bool(pair.second)) ||
            pair.first > pair.second || pair.first % kPage ||
            pair.second % kPage ||
            pair.second >
                std::numeric_limits<uint64_t>::max() / config_.rank_count)
            return Status::InvalidArgument(
                "GVA sizes must be aligned to 2 MiB and fit rank windows");
    }
    if (api_->bind(config_.device)) return nativeError("bind device");
    if (entity_ && !entity_blob_.is_null()) return Status::OK();
    if (!initialized_) {
        if (api_->initialize(config_.device)) return nativeError("initialize");
        initialized_ = true;
    }
    if (!entity_) {
        entity_ = api_->create(config_);
        if (!entity_) return nativeError("create entity");
        if (api_->reserve(entity_)) {
            api_->destroy(entity_);
            entity_ = nullptr;
            return nativeError("reserve windows");
        }
    }
    std::vector<uint8_t> blob;
    if (api_->exportBlob(entity_, nullptr, blob))
        return nativeError("export entity");
    entity_blob_ = blob;
    return Status::OK();
}
MemFabricSharedMemory::Json MemFabricSharedMemory::identityLocked() const {
    return Json{{"version", 2u},
                {"protocol", config_.protocol},
                {"address_mode", "logical56"},
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
Status MemFabricSharedMemory::allocate(void** out, size_t n, uint32_t t,
                                       uint64_t requested) {
    if (!out) return Status::InvalidArgument("Null GVA output");
    *out = nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    if (t != 1 && t != 2)
        return Status::InvalidArgument("Unknown GVA memory type");
    if (n == 0 || n % kPage ||
        n != (t == 1 ? config_.hbm_size : config_.dram_size))
        return Status::InvalidArgument(
            "Allocate exactly the configured complete GVA pool");
    auto& p = pools_[t - 1];
    if (p.slice)
        return Status::InvalidArgument("One pool per memory type is supported");
    // HYBM uses a bump allocator and free does not restore its cursor. Do not
    // repeatedly allocate to hunt for the requested VA, or promise pool reuse.
    if (consumed_[t - 1])
        return Status::InvalidArgument(
            "HYBM pool capacity already consumed; recreate the GVA context");
    for (const auto& kv : peers_)
        if (!kv.second.cleaned)
            return pending("allocate pools before peer preparation");
    CHECK_STATUS(ensureLocked());
    void* slice = api_->allocate(entity_, t, n);
    if (!slice) return nativeError("allocate slice");
    consumed_[t - 1] = n;
    auto gva = api_->address(entity_, slice);
    uint64_t dva = 0, end = 0;
    auto mapped = api_->size(entity_, slice);
    bool valid = gva && mapped == n &&
                 n <= std::numeric_limits<uint64_t>::max() - gva &&
                 !api_->translate(gva, 1, dva) && dva && dva != gva &&
                 n <= std::numeric_limits<uint64_t>::max() - dva &&
                 !api_->translate(gva + n - 1, 1, end) && end == dva + n - 1 &&
                 (!requested || requested == gva);
    if (!valid) {
        if (api_->free(entity_, slice)) rollback_.push_back(slice);
        return Status::InvalidArgument(
            "HYBM logical GVA must match the request and resolve to a distinct "
            "contiguous device VA; allocation retained for cleanup if needed");
    }
    p.slice = slice;
    p.address = gva;
    p.device_address = dva;
    p.length = n;
    if (t == 2) (void)api_->translate(gva, 2, p.host_address);
    p.descriptor = identityLocked();
    p.descriptor["type"] = t;
    p.descriptor["address"] = gva;
    p.descriptor["size"] = n;
    p.descriptor["mapped_size"] = mapped;
    p.descriptor["pool_generation"] = ++pool_generation_;
    *out = reinterpret_cast<void*>(gva);
    return Status::OK();
}
bool MemFabricSharedMemory::describe(void* ptr, size_t n,
                                     MemoryOptions& o) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& p : pools_)
        if (p.slice && p.address == reinterpret_cast<uint64_t>(ptr) &&
            p.length == n) {
            o.location = "npu:" + std::to_string(config_.device);
            o.type = MEMFABRIC;
            o.global_address = p.address;
            return true;
        }
    return false;
}
bool MemFabricSharedMemory::overlaps(uint64_t address, size_t n) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto intersects = [address, n](uint64_t base, uint64_t length) {
        if (!base || !length || !n) return false;
        return address >= base ? address - base < length : n > base - address;
    };
    for (const auto& p : pools_)
        if (p.slice && (intersects(p.address, p.length) ||
                        intersects(p.device_address, p.length)))
            return true;
    // Physical companion mappings and partial maps also own device aliases.
    // They must not fall through to CPU probing, ACL free or trans
    // registration, even when the logical pool is inactive or map has not
    // returned success.
    for (const auto& entry : peers_) {
        if (entry.second.cleaned) continue;
        for (const auto& descriptor : entry.second.descriptors) {
            if (descriptor.is_null()) continue;
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
Status MemFabricSharedMemory::publish(void* ptr, size_t n, std::string& out) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& p : pools_)
        if (p.slice && p.address == reinterpret_cast<uint64_t>(ptr) &&
            p.length == n) {
            if (!p.descriptor.contains("slice_blob")) {
                CHECK_STATUS(ensureLocked());
                std::vector<uint8_t> blob;
                if (api_->exportBlob(entity_, p.slice, blob) ||
                    !blobValid(Json(blob)))
                    return nativeError("export slice");
                p.descriptor["slice_blob"] = blob;
            }
            out = p.descriptor.dump();
            p.published = true;
            return Status::OK();
        }
    return Status::InvalidArgument(
        "Only a complete trusted GVA pool can be registered");
}
Status MemFabricSharedMemory::unpublish(void* ptr) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    for (auto& p : pools_)
        if (p.slice && p.address == reinterpret_cast<uint64_t>(ptr)) {
            p.published = false;
            return Status::OK();
        }
    return Status::InvalidArgument("Unknown GVA pool");
}
Status MemFabricSharedMemory::free(void* ptr, size_t n) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    for (auto& p : pools_)
        if (p.slice && p.address == reinterpret_cast<uint64_t>(ptr) &&
            p.length == n) {
            if (p.published) return pending("pool is still registered");
            for (const auto& kv : peers_)
                if (!kv.second.cleaned)
                    return pending("local context still holds peer imports");
            CHECK_STATUS(ensureLocked());
            if (api_->free(entity_, p.slice)) return nativeError("free slice");
            p = Pool{};
            return Status::OK();
        }
    return Status::InvalidArgument("Unknown GVA pool or size");
}
Status MemFabricSharedMemory::validateLocked(const Json& j,
                                             const std::string& owner) const {
    try {
        if (j.value("protocol", std::string("device_sdma")) != config_.protocol)
            return Status::InvalidArgument("GVA peer protocol differs");
        if (j.at("version").get<uint64_t>() != 2 ||
            j.at("address_mode").get<std::string>() != "logical56" ||
            j.at("owner").get<std::string>() != owner || owner.empty() ||
            j.at("group").get<std::string>() != config_.group ||
            j.at("generation").get<std::string>().empty() ||
            j.at("generation").get<std::string>().size() > 128 ||
            j.at("ranks").get<uint64_t>() != config_.rank_count ||
            j.at("rank").get<uint64_t>() >= config_.rank_count ||
            j.at("rank").get<uint64_t>() == config_.rank ||
            j.at("entity").get<uint64_t>() != config_.entity ||
            j.at("device").get<uint64_t>() > 65535 ||
            j.at("hbm_window").get<uint64_t>() != config_.hbm_window ||
            j.at("dram_window").get<uint64_t>() != config_.dram_window ||
            j.at("hbm_size").get<uint64_t>() != config_.hbm_size ||
            j.at("dram_size").get<uint64_t>() != config_.dram_size ||
            !blobValid(j.at("entity_blob")))
            return Status::InvalidArgument(
                "Incompatible GVA peer identity/layout");
        if (j.contains("type")) {
            auto t = j.at("type").get<uint64_t>();
            auto a = j.at("address").get<uint64_t>();
            auto n = j.at("size").get<uint64_t>();
            if ((t != 1 && t != 2) || !a || !n || a % kPage || n % kPage ||
                n > std::numeric_limits<uint64_t>::max() - a ||
                n != j.at("mapped_size").get<uint64_t>() ||
                n != (t == 1 ? config_.hbm_size : config_.dram_size) ||
                !j.at("pool_generation").get<uint64_t>() ||
                !blobValid(j.at("slice_blob")))
                return Status::InvalidArgument("Invalid GVA pool descriptor");
            auto rank = j.at("rank").get<uint64_t>();
            auto window = t == 1 ? config_.hbm_window : config_.dram_window;
            if (a < rank * window)
                return Status::InvalidArgument("Invalid GVA window address");
            const auto& local = pools_[t - 1];
            if (local.slice &&
                (local.address < config_.rank * window ||
                 a - rank * window != local.address - config_.rank * window))
                return Status::InvalidArgument(
                    "GVA window bases differ across ranks");
        }
        return Status::OK();
    } catch (const Json::exception&) {
        return Status::InvalidArgument("Malformed GVA descriptor");
    }
}
Status MemFabricSharedMemory::recordLocked(const Json& j,
                                           const std::string& owner) {
    CHECK_STATUS(validateLocked(j, owner));
    auto rank = j.at("rank").get<uint32_t>();
    auto generation = j.at("generation").get<std::string>();
    auto it = peers_.find(rank);
    if (it != peers_.end() && !it->second.cleaned &&
        (it->second.owner != owner || it->second.generation != generation))
        return pending("old peer generation must exit first");
    CHECK_STATUS(ensureLocked());
    auto& peer = peers_[rank];
    if (peer.cleaned) peer = Peer{};
    if (peer.owner.empty()) {
        peer.owner = owner;
        peer.generation = generation;
        peer.entity_blob = j.at("entity_blob");
    }
    if (peer.entity_blob != j.at("entity_blob"))
        return Status::InvalidArgument(
            "Peer entity blob changed within its generation");
    // Retain the requested address before either native import: entity import
    // may fail after changing state, and unmap must still be able to find it.
    if (j.contains("type")) {
        const auto index = j.at("type").get<size_t>() - 1;
        if (!peer.descriptors[index].is_null() && peer.descriptors[index] != j)
            return pending("peer pool descriptor changed");
        peer.descriptors[index] = j;
    }
    return Status::OK();
}
Status MemFabricSharedMemory::importLocked(const Json& j,
                                           const std::string& owner) {
    CHECK_STATUS(recordLocked(j, owner));
    auto& peer = peers_.at(j.at("rank").get<uint32_t>());
    if (api_->importBlob(entity_, true,
                         j.at("entity_blob").get<std::vector<uint8_t>>()))
        return nativeError("import entity");
    if (j.contains("type")) {
        size_t index = j.at("type").get<size_t>() - 1;
        if (peer.imported[index]) return Status::OK();
        if (api_->importBlob(entity_, false,
                             j.at("slice_blob").get<std::vector<uint8_t>>())) {
            return nativeError("import slice (rank retained for cleanup)");
        }
        peer.imported[index] = true;
    }
    return Status::OK();
}
Status MemFabricSharedMemory::verifyLocked(const Peer& p) {
    uint32_t reach = 0;
    uint32_t rank = 0;
    bool found = false;
    for (const auto& d : p.descriptors)
        if (!d.is_null()) {
            rank = d.at("rank").get<uint32_t>();
            found = true;
            break;
        }
    if (!found) return Status::OK();
    if (api_->reach(entity_, rank, reach) || !(reach & 1u))
        return Status::NotImplemented(
            "Peer lacks HYBM MTE direct device access");
    for (const auto& d : p.descriptors)
        if (!d.is_null()) {
            auto a = d.at("address").get<uint64_t>(),
                 n = d.at("size").get<uint64_t>();
            uint64_t first = 0, last = 0;
            if (api_->translate(a, 1, first) || !first || first == a ||
                n > std::numeric_limits<uint64_t>::max() - first ||
                api_->translate(a + n - 1, 1, last) || last != first + n - 1)
                return Status::InvalidArgument(
                    "Peer logical GVA does not resolve to a distinct "
                    "contiguous device VA");
        }
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
    if (config_.protocol != "device_sdma" &&
        (!api_ || !api_->supportsConnect()))
        return Status::NotImplemented(
            "HYBM network connect symbol unavailable");
    try {
        auto descriptors = Json::parse(descriptor);
        if (!descriptors.is_array()) descriptors = Json::array({descriptors});
        if (descriptors.empty() || descriptors.size() > 2)
            return Status::InvalidArgument("Invalid GVA pool bundle");
        const Json* requested = nullptr;
        std::set<uint32_t> types;
        uint32_t rank = descriptors.front().at("rank").get<uint32_t>();
        for (const auto& d : descriptors) {
            CHECK_STATUS(validateLocked(d, owner));
            if (d.at("rank").get<uint32_t>() != rank ||
                d.at("generation") != descriptors.front().at("generation") ||
                d.at("entity_blob") != descriptors.front().at("entity_blob") ||
                !types.insert(d.at("type").get<uint32_t>()).second)
                return Status::InvalidArgument("Inconsistent GVA pool bundle");
            if (d.at("address").get<uint64_t>() == address &&
                d.at("size").get<uint64_t>() == size)
                requested = &d;
        }
        if (!requested)
            return Status::InvalidArgument(
                "Map requires complete published pool");
        CHECK_STATUS(recordLocked(*requested, owner));
        auto& peer = peers_.at(rank);
        peer.mapped = false;
        peer.copy_ready = false;
        peer.active[requested->at("type").get<size_t>() - 1] = true;
        for (const auto& d : descriptors) CHECK_STATUS(importLocked(d, owner));
        // Reciprocal imports are driven by each application's map calls.
        if (api_->map(entity_))
            return pending(
                "native map failed; ensure both peers have imported and retry");
        if (config_.protocol != "device_sdma") {
            if (api_->connect(entity_, rank))
                return pending(
                    "network connection not ready; prepare both peers and "
                    "retry");
            uint32_t reach = 0;
            const uint32_t required =
                config_.protocol == "device_rdma" ? 4u : 256u;
            if (api_->reach(entity_, rank, reach) || !(reach & required))
                return Status::NotImplemented(
                    "Peer lacks configured network copy capability");
            peer.copy_ready = true;
        }
        if (direct || config_.protocol == "device_sdma") {
            CHECK_STATUS(verifyLocked(peer));
            peer.mapped = true;
            peer.copy_ready = true;
        }
        return Status::OK();
    } catch (const Json::exception&) {
        return Status::InvalidArgument("Malformed GVA pool bundle");
    }
}
Status MemFabricSharedMemory::cleanupLocked(uint32_t rank, Peer& p) {
    if (p.cleaned) return Status::OK();
    if (p.active[0] || p.active[1]) return pending("another pool is active");
    CHECK_STATUS(ensureLocked());
    // remove_imported propagates segment errors; unmap_rank alone swallows
    // them.
    if (api_->remove(entity_, rank) || api_->unmap(entity_, rank))
        return pending("native rank removal failed");
    for (const auto& d : p.descriptors)
        if (!d.is_null()) {
            auto a = d.at("address").get<uint64_t>(),
                 n = d.at("size").get<uint64_t>();
            uint64_t va = 0;
            if (api_->translate(a, 1, va) != -12 ||
                api_->translate(a + n - 1, 1, va) != -12)
                return pending("GVA registry still contains imported mappings");
        }
    // Native HAL errors are not observable through this ABI. This certifies
    // the native API and registry contract, not independent HAL revocation.
    p.mapped = false;
    p.copy_ready = false;
    p.cleaned = true;
    return Status::OK();
}
Status MemFabricSharedMemory::unmap(const std::string& owner,
                                    uint64_t address) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    for (auto& entry : peers_) {
        auto& peer = entry.second;
        if (peer.owner != owner) continue;
        for (size_t i = 0; i < 2; ++i) {
            if (!peer.descriptors[i].is_null() &&
                peer.descriptors[i].at("address").get<uint64_t>() == address) {
                peer.active[i] = false;
                return cleanupLocked(entry.first, peer);
            }
        }
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
    for (const auto& pool : pools_)
        if (pool.slice && contains(pool.address, pool.length)) live = true;
    for (const auto& entry : peers_) {
        const auto& peer = entry.second;
        if (peer.cleaned || !peer.mapped) continue;
        for (size_t i = 0; i < peer.descriptors.size(); ++i)
            if (peer.active[i] && !peer.descriptors[i].is_null() &&
                contains(peer.descriptors[i].at("address").get<uint64_t>(),
                         peer.descriptors[i].at("size").get<uint64_t>()))
                live = true;
    }
    if (!live)
        return Status::InvalidArgument("GVA range is not locally accessible");
    CHECK_STATUS(ensureLocked());
    uint64_t first = 0, last = 0;
    if (api_->translate(address, 1, first) || !first || first == address ||
        size > UINT64_MAX - first ||
        api_->translate(address + size - 1, 1, last) ||
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
        CHECK_STATUS(validateLocked(published, owner));
        auto it = peers_.find(published.at("rank").get<uint32_t>());
        auto type = published.at("type").get<uint32_t>();
        if (it == peers_.end() || type < 1 || type > 2 || it->second.cleaned ||
            !it->second.copy_ready || !it->second.active[type - 1] ||
            it->second.descriptors[type - 1] != published)
            return Status::InvalidArgument(
                "Remote GVA publication differs from mapped pool");
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
    if (!api_ || !api_->supportsCopy())
        return Status::NotImplemented("HYBM copy symbols unavailable");
    auto type = [&](uint64_t address) -> uint32_t {
        auto contains = [&](uint64_t base, uint64_t length) {
            return address >= base && address - base < length &&
                   size <= length - (address - base);
        };
        for (size_t i = 0; i < pools_.size(); ++i)
            if (pools_[i].slice && pools_[i].published &&
                contains(pools_[i].address, pools_[i].length))
                return i + 1;
        for (const auto& entry : peers_) {
            const auto& peer = entry.second;
            if (peer.cleaned || !peer.copy_ready) continue;
            for (size_t i = 0; i < peer.descriptors.size(); ++i)
                if (peer.active[i] && !peer.descriptors[i].is_null() &&
                    contains(peer.descriptors[i].at("address").get<uint64_t>(),
                             peer.descriptors[i].at("size").get<uint64_t>()))
                    return i + 1;
        }
        return 0;
    };
    const auto src_type = type(source), dst_type = type(destination);
    if (!src_type || !dst_type)
        return Status::InvalidArgument(
            "GVA copy requires published local or active mapped logical pools");
    // HYBM_GLOBAL_{HOST,DEVICE}_TO_GLOBAL_{HOST,DEVICE}.
    direction = (src_type == 1 ? 8u : 4u) + (dst_type == 1 ? 1u : 0u);
    CHECK_STATUS(ensureLocked());
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
    return result == 0 ? Status::OK() : nativeError("synchronous copy");
}
void MemFabricSharedMemory::failCopy() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_) --pending_copies_;
    copy_failed_ = true;
}
void MemFabricSharedMemory::cancelCopy() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_) --pending_copies_;
}
Status MemFabricSharedMemory::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_)
        return pending("GVA copies retain resources");
    for (const auto& p : pools_)
        if (p.slice) return pending("local pools still allocated");
    for (const auto& kv : peers_)
        if (!kv.second.cleaned) return pending("peer mappings remain");
    if (!initialized_) return Status::OK();
    if (api_->bind(config_.device)) return nativeError("bind for cleanup");
    for (auto it = rollback_.begin(); it != rollback_.end();) {
        if (api_->free(entity_, *it))
            return pending("allocation rollback failed");
        it = rollback_.erase(it);
    }
    if (entity_) {
        if (api_->unreserve(entity_)) return pending("window release failed");
        api_->destroy(entity_);
        entity_ = nullptr;
    }
    api_->uninitialize();
    initialized_ = false;
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
