// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "tent/transport/memfabric/memfabric_shared_memory.h"
#ifdef TENT_WITH_HYBM_GVA
#include <acl/acl.h>
#include "tent/transport/memfabric/memfabric_library.h"
#include <cstring>
#include <atomic>
#include <glog/logging.h>
#include <hybm_big_mem.h>
#include <hybm_data_op.h>
static_assert(BM_GVA_NOT_FOUND == -12,
              "MemFabric GVA requires distinct missing-registry errors");
namespace mooncake {
namespace tent {
namespace {
class NativeHybm final {
   public:
    bool loadSymbols() {
        library_ = MemFabricLibrary::load("libmf_hybm_core.so");
        if (!library_) return false;
#define LOAD(name)                                        \
    name##_ = library_->symbol<decltype(name##_)>(#name); \
    if (!name##_) return false
        LOAD(hybm_init);
        LOAD(hybm_uninit);
        LOAD(hybm_create_entity);
        LOAD(hybm_destroy_entity);
        LOAD(hybm_reserve_mem_space);
        LOAD(hybm_unreserve_mem_space);
        LOAD(hybm_alloc_local_memory);
        LOAD(hybm_free_local_memory);
        LOAD(hybm_get_slice_va);
        LOAD(hybm_get_memory_ptr);
        LOAD(hybm_get_slice_size);
        LOAD(hybm_gva_to_va);
        LOAD(hybm_export);
        LOAD(hybm_export_info_free);
        LOAD(hybm_import);
        LOAD(hybm_mmap);
        LOAD(hybm_entity_reach_types);
        LOAD(hybm_remove_imported);
        LOAD(hybm_unmap_rank);
        hybm_data_copy_ =
            library_->symbol<decltype(hybm_data_copy_)>("hybm_data_copy");
        hybm_transport_connect_ =
            library_->symbol<decltype(hybm_transport_connect_)>(
                "hybm_transport_connect");
        hybm_wait_ = library_->symbol<decltype(hybm_wait_)>("hybm_wait");
#undef LOAD
        return true;
    }
    bool supportsCopy() const { return hybm_data_copy_ && hybm_wait_; }
    int copy(void* entity, uint64_t source, uint64_t destination, size_t size,
             uint32_t direction) {
        if (!supportsCopy()) return -1;
        hybm_copy_params params{reinterpret_cast<void*>(source),
                                reinterpret_cast<void*>(destination), size};
        return hybm_data_copy_(entity, &params,
                               static_cast<hybm_data_copy_direction>(direction),
                               nullptr, 0);
    }
    int wait(void* entity) {
        return hybm_wait_ ? hybm_wait_(entity) : -1;
    }
    int bind(uint16_t device) { return aclrtSetDevice(device); }
    // HYBM itself reference-counts init/uninit, also used by smem_trans_init.
    int initialize(uint16_t device) {
        const auto result = hybm_init_(device, 0);
        if (result) return result;
        // Older libraries expose the same symbols but collapse registry misses
        // and arbitrary conversion failures into BM_ERROR. Refuse allocation
        // rather than discover at teardown that absence cannot be certified.
        uint64_t ignored = 0;
        if (hybm_gva_to_va_(UINT64_MAX, HYBM_MEM_TYPE_DEVICE, &ignored) !=
            BM_GVA_NOT_FOUND) {
            LOG(ERROR) << "HYBM GVA requires the BM_GVA_NOT_FOUND registry "
                          "contract; the loaded library is incompatible";
            hybm_uninit_();
            return BM_NOT_SUPPORTED;
        }
        return 0;
    }
    void uninitialize() { hybm_uninit_(); }
    void* create(const MemFabricGvaConfig& config) {
        hybm_options options{};
        options.bmType = HYBM_TYPE_HOST_INITIATE;
        options.memType = static_cast<hybm_mem_type>(
            (config.hbm_window ? HYBM_MEM_TYPE_DEVICE : 0) |
            (config.dram_window ? HYBM_MEM_TYPE_HOST : 0));
        options.bmDataOpType =
            config.protocol == "device_rdma"   ? HYBM_DOP_TYPE_DEVICE_RDMA
            : config.protocol == "device_urma" ? HYBM_DOP_TYPE_DEVICE_URMA
                                               : HYBM_DOP_TYPE_SDMA;
        if (config.protocol == "device_rdma") {
            // Match SmemTransEntry's default HYBM endpoint. HYBM discovers the
            // bound NPU's IP; this URL supplies a nonzero device port.
            const auto trans_url =
                "tcp://127.0.0.1:" + std::to_string(11000U + config.entity);
            std::memcpy(options.transUrl, trans_url.c_str(),
                        trans_url.size() + 1);
        }
        options.rankCount = config.rank_count;
        options.rankId = config.rank;
        options.devId = config.device;
        options.maxHBMSize = config.hbm_window;
        options.maxDRAMSize = config.dram_window;
        options.deviceVASpace = config.hbm_size;
        options.hostVASpace = config.dram_size;
        options.scene = HYBM_SCENE_DEFAULT;
        options.enable56BitsGva = false;
        options.dramShmFd = -1;
        return hybm_create_entity_(config.entity, &options, 0);
    }
    void destroy(void* entity_handle) {
        hybm_destroy_entity_(entity_handle, 0);
    }
    int reserve(void* entity_handle) {
        return hybm_reserve_mem_space_(entity_handle, 0);
    }
    int unreserve(void* entity_handle) {
        return hybm_unreserve_mem_space_(entity_handle, 0);
    }
    void* allocate(void* entity_handle, uint32_t memory_type,
                   uint64_t size_bytes) {
        return hybm_alloc_local_memory_(entity_handle,
                                        static_cast<hybm_mem_type>(memory_type),
                                        size_bytes, 0);
    }
    int free(void* entity_handle, void* slice) {
        return hybm_free_local_memory_(entity_handle, slice, 1, 0);
    }
    uint64_t address(void* entity_handle, void* slice) {
        return reinterpret_cast<uint64_t>(
            hybm_get_slice_va_(entity_handle, slice, HYBM_GET_SLICE_GVA));
    }
    uint64_t size(void* entity_handle, void* slice) {
        return hybm_get_slice_size_(entity_handle, slice);
    }
    uint64_t base(void* entity_handle, uint32_t memory_type) {
        return reinterpret_cast<uint64_t>(hybm_get_memory_ptr_(
            entity_handle, static_cast<hybm_mem_type>(memory_type)));
    }
    int translate(uint64_t address, uint32_t memory_type,
                  uint64_t& out) {
        return hybm_gva_to_va_(address, static_cast<hybm_mem_type>(memory_type),
                               &out);
    }
    int exportBlob(void* entity_handle, void* slice,
                   std::vector<uint8_t>& blob) {
        hybm_exchange_info exchange_info{};
        int result =
            hybm_export_(entity_handle, slice,
                         slice ? 0 : HYBM_FLAG_EXPORT_ENTITY, &exchange_info);
        if (result == 0 && exchange_info.desc && exchange_info.descLen &&
            exchange_info.descLen <= (1U << 20))
            blob.assign(exchange_info.desc,
                        exchange_info.desc + exchange_info.descLen);
        else if (result == 0)
            result = -1;
        hybm_export_info_free_(&exchange_info);
        return result;
    }
    int importBlob(void* entity_handle, bool entity,
                   const std::vector<uint8_t>& blob) {
        hybm_exchange_info exchange_info{const_cast<uint8_t*>(blob.data()),
                                         static_cast<uint32_t>(blob.size())};
        return hybm_import_(entity_handle, &exchange_info, 1, nullptr,
                            entity ? HYBM_FLAG_EXPORT_ENTITY : 0);
    }
    bool supportsConnect() const {
        return hybm_transport_connect_ != nullptr;
    }
    int connect(void* entity_handle, uint32_t rank) {
        return hybm_transport_connect_
                   ? hybm_transport_connect_(entity_handle, &rank, 1, 0)
                   : -1;
    }
    int map(void* entity_handle) {
        return hybm_mmap_(entity_handle, 0);
    }
    int reach(void* entity_handle, uint32_t rank, uint32_t& out) {
        hybm_data_op_type capabilities = HYBM_DOP_TYPE_DEFAULT;
        auto result =
            hybm_entity_reach_types_(entity_handle, rank, capabilities, 0);
        out = capabilities;
        return result;
    }
    int remove(void* entity_handle, uint32_t rank) {
        return hybm_remove_imported_(entity_handle, rank, 0);
    }
    int unmap(void* entity_handle, uint32_t rank) {
        return hybm_unmap_rank_(entity_handle, rank);
    }

   private:
    std::shared_ptr<MemFabricLibrary> library_;
#define FN(name) decltype(&name) name##_ = nullptr
    FN(hybm_data_copy);
    FN(hybm_wait);
    FN(hybm_transport_connect);
    FN(hybm_init);
    FN(hybm_uninit);
    FN(hybm_create_entity);
    FN(hybm_destroy_entity);
    FN(hybm_reserve_mem_space);
    FN(hybm_unreserve_mem_space);
    FN(hybm_alloc_local_memory);
    FN(hybm_free_local_memory);
    FN(hybm_get_slice_va);
    FN(hybm_get_memory_ptr);
    FN(hybm_get_slice_size);
    FN(hybm_gva_to_va);
    FN(hybm_export);
    FN(hybm_export_info_free);
    FN(hybm_import);
    FN(hybm_mmap);
    FN(hybm_entity_reach_types);
    FN(hybm_remove_imported);
    FN(hybm_unmap_rank);
#undef FN
};
// Publish only complete tables. Failed loads leave no state and can retry.
std::atomic<NativeHybm*>& nativeApi() {
    static std::atomic<NativeHybm*> api{nullptr};
    return api;
}
}  // namespace
bool MemFabricGvaApi::LoadLibrary() {
    static std::mutex mutex;
    std::lock_guard<std::mutex> lock(mutex);
    if (IsLoaded()) return true;
    auto api = std::make_unique<NativeHybm>();
    if (!api->loadSymbols()) return false;
    // Intentionally resident: entities can survive failed cleanup.
    nativeApi().store(api.release(), std::memory_order_release);
    return true;
}
bool MemFabricGvaApi::IsLoaded() {
    return nativeApi().load(std::memory_order_acquire) != nullptr;
}
bool MemFabricGvaApi::supportsCopy() {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->supportsCopy() : false;
}
int MemFabricGvaApi::copy(void* entity, uint64_t source, uint64_t destination,
                          size_t size, uint32_t direction) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->copy(entity, source, destination, size, direction) : -1;
}
int MemFabricGvaApi::wait(void* entity) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->wait(entity) : -1;
}
int MemFabricGvaApi::bind(uint16_t device) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->bind(device) : -1;
}
int MemFabricGvaApi::initialize(uint16_t device) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->initialize(device) : -1;
}
void MemFabricGvaApi::uninitialize() {
    auto* api = nativeApi().load(std::memory_order_acquire);
    if (api) api->uninitialize();
}
void* MemFabricGvaApi::create(const MemFabricGvaConfig& config) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->create(config) : nullptr;
}
void MemFabricGvaApi::destroy(void* entity) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    if (api) api->destroy(entity);
}
int MemFabricGvaApi::reserve(void* entity) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->reserve(entity) : -1;
}
int MemFabricGvaApi::unreserve(void* entity) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->unreserve(entity) : -1;
}
void* MemFabricGvaApi::allocate(void* entity, uint32_t memory_type,
                                uint64_t size) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->allocate(entity, memory_type, size) : nullptr;
}
int MemFabricGvaApi::free(void* entity, void* slice) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->free(entity, slice) : -1;
}
uint64_t MemFabricGvaApi::address(void* entity, void* slice) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->address(entity, slice) : 0;
}
uint64_t MemFabricGvaApi::size(void* entity, void* slice) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->size(entity, slice) : 0;
}
uint64_t MemFabricGvaApi::base(void* entity, uint32_t memory_type) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->base(entity, memory_type) : 0;
}
int MemFabricGvaApi::translate(uint64_t address, uint32_t memory_type,
                               uint64_t& out) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->translate(address, memory_type, out) : -1;
}
int MemFabricGvaApi::exportBlob(void* entity, void* slice,
                                std::vector<uint8_t>& blob) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->exportBlob(entity, slice, blob) : -1;
}
int MemFabricGvaApi::importBlob(void* entity, bool entity_info,
                                const std::vector<uint8_t>& blob) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->importBlob(entity, entity_info, blob) : -1;
}
int MemFabricGvaApi::map(void* entity) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->map(entity) : -1;
}
bool MemFabricGvaApi::supportsConnect() {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->supportsConnect() : false;
}
int MemFabricGvaApi::connect(void* entity, uint32_t rank) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->connect(entity, rank) : -1;
}
int MemFabricGvaApi::reach(void* entity, uint32_t rank, uint32_t& out) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->reach(entity, rank, out) : -1;
}
int MemFabricGvaApi::remove(void* entity, uint32_t rank) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->remove(entity, rank) : -1;
}
int MemFabricGvaApi::unmap(void* entity, uint32_t rank) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->unmap(entity, rank) : -1;
}
}  // namespace tent
}  // namespace mooncake
#else
namespace mooncake {
namespace tent {
bool MemFabricGvaApi::LoadLibrary() { return false; }
bool MemFabricGvaApi::IsLoaded() { return false; }
bool MemFabricGvaApi::supportsCopy() { return false; }
int MemFabricGvaApi::copy(void*, uint64_t, uint64_t, size_t, uint32_t) {
    return -1;
}
int MemFabricGvaApi::wait(void*) { return -1; }
int MemFabricGvaApi::bind(uint16_t) { return -1; }
int MemFabricGvaApi::initialize(uint16_t) { return -1; }
void MemFabricGvaApi::uninitialize() {}
void* MemFabricGvaApi::create(const MemFabricGvaConfig&) { return nullptr; }
void MemFabricGvaApi::destroy(void*) {}
int MemFabricGvaApi::reserve(void*) { return -1; }
int MemFabricGvaApi::unreserve(void*) { return -1; }
void* MemFabricGvaApi::allocate(void*, uint32_t, uint64_t) { return nullptr; }
int MemFabricGvaApi::free(void*, void*) { return -1; }
uint64_t MemFabricGvaApi::address(void*, void*) { return 0; }
uint64_t MemFabricGvaApi::size(void*, void*) { return 0; }
uint64_t MemFabricGvaApi::base(void*, uint32_t) { return 0; }
int MemFabricGvaApi::translate(uint64_t, uint32_t, uint64_t&) { return -1; }
int MemFabricGvaApi::exportBlob(void*, void*, std::vector<uint8_t>&) {
    return -1;
}
int MemFabricGvaApi::importBlob(void*, bool, const std::vector<uint8_t>&) {
    return -1;
}
int MemFabricGvaApi::map(void*) { return -1; }
bool MemFabricGvaApi::supportsConnect() { return false; }
int MemFabricGvaApi::connect(void*, uint32_t) { return -1; }
int MemFabricGvaApi::reach(void*, uint32_t, uint32_t&) { return -1; }
int MemFabricGvaApi::remove(void*, uint32_t) { return -1; }
int MemFabricGvaApi::unmap(void*, uint32_t) { return -1; }
}  // namespace tent
}  // namespace mooncake
#endif
