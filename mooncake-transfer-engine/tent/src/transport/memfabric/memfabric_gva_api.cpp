// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "tent/transport/memfabric/memfabric_shared_memory.h"
#ifdef TENT_WITH_HYBM_GVA
#include <acl/acl.h>
#include <dlfcn.h>
#include <glog/logging.h>
#include <hybm_big_mem.h>
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
#undef LOAD
        return true;
    }
    int bind(uint16_t d) override { return aclrtSetDevice(d); }
    // HYBM itself reference-counts init/uninit, also used by smem_trans_init.
    int initialize(uint16_t d) override {
        const auto result = hybm_init_(d, 0);
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
    void* create(const MemFabricGvaConfig& c) override {
        hybm_options o{};
        o.bmType = HYBM_TYPE_HOST_INITIATE;
        o.memType = static_cast<hybm_mem_type>(
            (c.hbm_window ? HYBM_MEM_TYPE_DEVICE : 0) |
            (c.dram_window ? HYBM_MEM_TYPE_HOST : 0));
        o.bmDataOpType = HYBM_DOP_TYPE_SDMA;
        o.rankCount = c.rank_count;
        o.rankId = c.rank;
        o.devId = c.device;
        o.maxHBMSize = c.hbm_window;
        o.maxDRAMSize = c.dram_window;
        o.deviceVASpace = c.hbm_size;
        o.hostVASpace = c.dram_size;
        o.scene = HYBM_SCENE_DEFAULT;
        o.enable56BitsGva = true;
        o.dramShmFd = -1;
        return hybm_create_entity_(c.entity, &o, 0);
    }
    void destroy(void* e) override { hybm_destroy_entity_(e, 0); }
    int reserve(void* e) override { return hybm_reserve_mem_space_(e, 0); }
    int unreserve(void* e) override { return hybm_unreserve_mem_space_(e, 0); }
    void* allocate(void* e, uint32_t t, uint64_t n) override {
        return hybm_alloc_local_memory_(e, static_cast<hybm_mem_type>(t), n, 0);
    }
    int free(void* e, void* s) override {
        return hybm_free_local_memory_(e, s, 1, 0);
    }
    uint64_t address(void* e, void* s) override {
        return reinterpret_cast<uint64_t>(
            hybm_get_slice_va_(e, s, HYBM_GET_SLICE_GVA));
    }
    uint64_t size(void* e, void* s) override {
        return hybm_get_slice_size_(e, s);
    }
    int translate(uint64_t p, uint32_t t, uint64_t& out) override {
        return hybm_gva_to_va_(p, static_cast<hybm_mem_type>(t), &out);
    }
    int exportBlob(void* e, void* s, std::vector<uint8_t>& b) override {
        hybm_exchange_info x{};
        int rc = hybm_export_(e, s, s ? 0 : HYBM_FLAG_EXPORT_ENTITY, &x);
        if (rc == 0 && x.desc && x.descLen && x.descLen <= (1U << 20))
            b.assign(x.desc, x.desc + x.descLen);
        else if (rc == 0)
            rc = -1;
        hybm_export_info_free_(&x);
        return rc;
    }
    int importBlob(void* e, bool entity,
                   const std::vector<uint8_t>& b) override {
        hybm_exchange_info x{const_cast<uint8_t*>(b.data()),
                             static_cast<uint32_t>(b.size())};
        return hybm_import_(e, &x, 1, nullptr,
                            entity ? HYBM_FLAG_EXPORT_ENTITY : 0);
    }
    int map(void* e) override { return hybm_mmap_(e, 0); }
    int reach(void* e, uint32_t r, uint32_t& out) override {
        hybm_data_op_type t = HYBM_DOP_TYPE_DEFAULT;
        auto rc = hybm_entity_reach_types_(e, r, t, 0);
        out = t;
        return rc;
    }
    int remove(void* e, uint32_t r) override {
        return hybm_remove_imported_(e, r, 0);
    }
    int unmap(void* e, uint32_t r) override { return hybm_unmap_rank_(e, r); }

   private:
    void* library_ = nullptr;
#define FN(name) decltype(&name) name##_ = nullptr
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
    auto a = std::make_shared<NativeHybm>();
    return a->loadSymbols() ? a : nullptr;
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
