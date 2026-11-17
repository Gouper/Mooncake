// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "tent/transport/memfabric/memfabric_shared_memory.h"
#ifdef TENT_WITH_HYBM_GVA
#include <acl/acl.h>
#include <dlfcn.h>
#include <cstring>
#include <glog/logging.h>
#include <hybm_big_mem.h>
#include <hybm_data_op.h>
static_assert(BM_GVA_NOT_FOUND == -12,
              "MemFabric GVA requires distinct missing-registry errors");
namespace mooncake {
namespace tent {
namespace {
class NativeHybm final : public MemFabricGvaApi {
   public:
    ~NativeHybm() override {
        if (library_) dlclose(library_);
    }
    bool loadSymbols() {
        library_ = dlopen("libmf_hybm_core.so", RTLD_NOW | RTLD_LOCAL);
        if (!library_) return false;
#define LOAD(name)                                                         \
    name##_ = reinterpret_cast<decltype(name##_)>(dlsym(library_, #name)); \
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
        LOAD(hybm_get_slice_size);
        LOAD(hybm_gva_to_va);
        LOAD(hybm_export);
        LOAD(hybm_export_info_free);
        LOAD(hybm_import);
        LOAD(hybm_mmap);
        LOAD(hybm_entity_reach_types);
        LOAD(hybm_remove_imported);
        LOAD(hybm_unmap_rank);
        hybm_data_copy_ = reinterpret_cast<decltype(hybm_data_copy_)>(
            dlsym(library_, "hybm_data_copy"));
        hybm_transport_connect_ =
            reinterpret_cast<decltype(hybm_transport_connect_)>(
                dlsym(library_, "hybm_transport_connect"));
        hybm_wait_ = reinterpret_cast<decltype(hybm_wait_)>(
            dlsym(library_, "hybm_wait"));
#undef LOAD
        return true;
    }
    bool supportsCopy() const override { return hybm_data_copy_ && hybm_wait_; }
    int copy(void* entity, uint64_t source, uint64_t destination, size_t size,
             uint32_t direction) override {
        hybm_copy_params params{reinterpret_cast<void*>(source),
                                reinterpret_cast<void*>(destination), size};
        return hybm_data_copy_(entity, &params,
                               static_cast<hybm_data_copy_direction>(direction),
                               nullptr, 0);
    }
    int wait(void* entity) override {
        return hybm_wait_ ? hybm_wait_(entity) : -1;
    }
    int bind(uint16_t device) override { return aclrtSetDevice(device); }
    // HYBM itself reference-counts init/uninit, also used by smem_trans_init.
    int initialize(uint16_t device) override {
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
    void uninitialize() override { hybm_uninit_(); }
    void* create(const MemFabricGvaConfig& config) override {
        hybm_options options{};
        options.bmType = HYBM_TYPE_HOST_INITIATE;
        options.memType = static_cast<hybm_mem_type>(
            (config.hbm_window ? HYBM_MEM_TYPE_DEVICE : 0) |
            (config.dram_window ? HYBM_MEM_TYPE_HOST : 0));
        options.bmDataOpType =
            config.protocol == "device_rdma"   ? HYBM_DOP_TYPE_DEVICE_RDMA
            : config.protocol == "device_urma" ? HYBM_DOP_TYPE_DEVICE_URMA
                                               : HYBM_DOP_TYPE_SDMA;
        if (config.trans_url.size() >= sizeof(options.transUrl)) return nullptr;
        std::memcpy(options.transUrl, config.trans_url.c_str(),
                    config.trans_url.size() + 1);
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
    void destroy(void* entity_handle) override {
        hybm_destroy_entity_(entity_handle, 0);
    }
    int reserve(void* entity_handle) override {
        return hybm_reserve_mem_space_(entity_handle, 0);
    }
    int unreserve(void* entity_handle) override {
        return hybm_unreserve_mem_space_(entity_handle, 0);
    }
    void* allocate(void* entity_handle, uint32_t memory_type,
                   uint64_t size_bytes) override {
        return hybm_alloc_local_memory_(entity_handle,
                                        static_cast<hybm_mem_type>(memory_type),
                                        size_bytes, 0);
    }
    int free(void* entity_handle, void* slice) override {
        return hybm_free_local_memory_(entity_handle, slice, 1, 0);
    }
    uint64_t address(void* entity_handle, void* slice) override {
        return reinterpret_cast<uint64_t>(
            hybm_get_slice_va_(entity_handle, slice, HYBM_GET_SLICE_GVA));
    }
    uint64_t size(void* entity_handle, void* slice) override {
        return hybm_get_slice_size_(entity_handle, slice);
    }
    int translate(uint64_t address, uint32_t memory_type,
                  uint64_t& out) override {
        return hybm_gva_to_va_(address, static_cast<hybm_mem_type>(memory_type),
                               &out);
    }
    int exportBlob(void* entity_handle, void* slice,
                   std::vector<uint8_t>& blob) override {
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
                   const std::vector<uint8_t>& blob) override {
        hybm_exchange_info exchange_info{const_cast<uint8_t*>(blob.data()),
                                         static_cast<uint32_t>(blob.size())};
        return hybm_import_(entity_handle, &exchange_info, 1, nullptr,
                            entity ? HYBM_FLAG_EXPORT_ENTITY : 0);
    }
    bool supportsConnect() const override {
        return hybm_transport_connect_ != nullptr;
    }
    int connect(void* entity_handle, uint32_t rank) override {
        return hybm_transport_connect_
                   ? hybm_transport_connect_(entity_handle, &rank, 1, 0)
                   : -1;
    }
    int map(void* entity_handle) override {
        return hybm_mmap_(entity_handle, 0);
    }
    int reach(void* entity_handle, uint32_t rank, uint32_t& out) override {
        hybm_data_op_type capabilities = HYBM_DOP_TYPE_DEFAULT;
        auto result =
            hybm_entity_reach_types_(entity_handle, rank, capabilities, 0);
        out = capabilities;
        return result;
    }
    int remove(void* entity_handle, uint32_t rank) override {
        return hybm_remove_imported_(entity_handle, rank, 0);
    }
    int unmap(void* entity_handle, uint32_t rank) override {
        return hybm_unmap_rank_(entity_handle, rank);
    }

   private:
    void* library_ = nullptr;
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
}  // namespace
std::shared_ptr<MemFabricGvaApi> MemFabricGvaApi::load() {
    auto api = std::make_shared<NativeHybm>();
    return api->loadSymbols() ? api : nullptr;
}
}  // namespace tent
}  // namespace mooncake
#else
namespace mooncake {
namespace tent {
std::shared_ptr<MemFabricGvaApi> MemFabricGvaApi::load() { return nullptr; }
}  // namespace tent
}  // namespace mooncake
#endif
