#include "tent/transport/memfabric/memfabric_under_api.h"

#include <dlfcn.h>

#include <glog/logging.h>

namespace mooncake {
namespace tent {

std::mutex MemFabricUnderApi::gMutex;
bool MemFabricUnderApi::gLoaded = false;
void* MemFabricUnderApi::gLibHandle = nullptr;
std::shared_ptr<const MemFabricUnderApi::MemoryApi>
    MemFabricUnderApi::gMemoryApi;
std::weak_ptr<const MemFabricUnderApi::MemoryApi>
    MemFabricUnderApi::gMemoryApiWeak;
std::unordered_map<void*, std::shared_ptr<const MemFabricUnderApi::MemoryApi>>
    MemFabricUnderApi::gAllocations;

smemTransInitFunc MemFabricUnderApi::pSmemTransInit = nullptr;
smemTransUninitFunc MemFabricUnderApi::pSmemTransUninit = nullptr;
smemTransConfigInitFunc MemFabricUnderApi::pSmemTransConfigInit = nullptr;
smemTransCreateFunc MemFabricUnderApi::pSmemTransCreate = nullptr;
smemTransDestroyFunc MemFabricUnderApi::pSmemTransDestroy = nullptr;
smemTransGetRpcPortFunc MemFabricUnderApi::pSmemTransGetRpcPort = nullptr;
smemTransRegisterMemFunc MemFabricUnderApi::pSmemTransRegisterMem = nullptr;
smemTransBatchRegisterMemFunc MemFabricUnderApi::pSmemTransBatchRegisterMem =
    nullptr;
smemTransDeregisterMemFunc MemFabricUnderApi::pSmemTransDeregisterMem = nullptr;
smemTransBatchCopyFunc MemFabricUnderApi::pSmemTransBatchCopy = nullptr;
smemTransBatchStatusQueryFunc MemFabricUnderApi::pSmemTransBatchStatusQuery =
    nullptr;
smemSetLogLevelFunc MemFabricUnderApi::pSmemSetLogLevel = nullptr;

#define DL_LOAD_SYM(field, name)                                           \
    do {                                                                   \
        field = (decltype(field))dlsym(gLibHandle, name);                  \
        if (!field) {                                                      \
            LOG(ERROR) << "Failed to dlsym " << name << ": " << dlerror(); \
            ResetSymbols();                                                \
            return false;                                                  \
        }                                                                  \
    } while (0)

bool MemFabricUnderApi::LoadLibrary() {
    std::lock_guard<std::mutex> guard(gMutex);
    return LoadLibraryLocked();
}

bool MemFabricUnderApi::LoadLibraryLocked() {
    if (gLoaded) return true;

    auto api = gMemoryApiWeak.lock();
    if (!api) {
        auto loaded = std::make_shared<MemoryApi>();
        void* library = dlopen("libmf_smem.so", RTLD_NOW | RTLD_GLOBAL);
        if (!library) {
            LOG(ERROR) << "Failed to dlopen libmf_smem.so: " << dlerror();
            return false;
        }
        loaded->library = std::shared_ptr<void>(
            library, [](void* handle) { (void)dlclose(handle); });
        gLibHandle = library;
        DL_LOAD_SYM(loaded->allocate, "smem_mem_alloc");
        DL_LOAD_SYM(loaded->free, "smem_mem_free");
        api = std::move(loaded);
    }
    gLibHandle = api->library.get();

    DL_LOAD_SYM(pSmemTransInit, "smem_trans_init");
    DL_LOAD_SYM(pSmemTransUninit, "smem_trans_uninit");
    DL_LOAD_SYM(pSmemTransConfigInit, "smem_trans_config_init");
    DL_LOAD_SYM(pSmemTransCreate, "smem_trans_create");
    DL_LOAD_SYM(pSmemTransDestroy, "smem_trans_destroy");
    DL_LOAD_SYM(pSmemTransGetRpcPort, "smem_trans_get_rpc_port");
    DL_LOAD_SYM(pSmemTransRegisterMem, "smem_trans_register_mem");
    DL_LOAD_SYM(pSmemTransBatchRegisterMem, "smem_trans_batch_register_mem");
    DL_LOAD_SYM(pSmemTransDeregisterMem, "smem_trans_deregister_mem");
    DL_LOAD_SYM(pSmemTransBatchCopy, "smem_trans_batch_copy");
    DL_LOAD_SYM(pSmemTransBatchStatusQuery, "smem_trans_batch_status_query");
    DL_LOAD_SYM(pSmemSetLogLevel, "smem_set_log_level");

    gMemoryApi = std::move(api);
    gMemoryApiWeak = gMemoryApi;
    gLoaded = true;
    return true;
}

int32_t MemFabricUnderApi::SmemMemAlloc(uint16_t deviceId, uint64_t size,
                                        uint32_t flags, void** address) {
    std::shared_ptr<const MemoryApi> api;
    {
        std::lock_guard<std::mutex> guard(gMutex);
        if (!LoadLibraryLocked()) return SM_ERROR;
        api = gMemoryApi;
    }
    const int32_t ret = api->allocate(deviceId, size, flags, address);
    if (ret != 0 || !address || !*address) return ret;
    try {
        std::lock_guard<std::mutex> guard(gMutex);
        gAllocations.emplace(*address, api);
    } catch (...) {
        (void)api->free(*address);
        throw;
    }
    return ret;
}

int32_t MemFabricUnderApi::SmemMemFree(void* address, bool& managed) {
    std::shared_ptr<const MemoryApi> api;
    {
        std::lock_guard<std::mutex> guard(gMutex);
        auto it = gAllocations.find(address);
        managed = it != gAllocations.end();
        if (!managed) return SM_OK;
        api = std::move(it->second);
        gAllocations.erase(it);
    }
    const int32_t ret = api->free(address);
    if (ret != 0) {
        std::lock_guard<std::mutex> guard(gMutex);
        gAllocations.emplace(address, std::move(api));
    }
    return ret;
}

void MemFabricUnderApi::CleanupLibrary() {
    std::shared_ptr<const MemoryApi> api;
    {
        std::lock_guard<std::mutex> guard(gMutex);
        ResetSymbols();
        api = std::move(gMemoryApi);
    }
}

void MemFabricUnderApi::ResetSymbols() {
    pSmemTransInit = nullptr;
    pSmemTransUninit = nullptr;
    pSmemTransConfigInit = nullptr;
    pSmemTransCreate = nullptr;
    pSmemTransDestroy = nullptr;
    pSmemTransGetRpcPort = nullptr;
    pSmemTransRegisterMem = nullptr;
    pSmemTransBatchRegisterMem = nullptr;
    pSmemTransDeregisterMem = nullptr;
    pSmemTransBatchCopy = nullptr;
    pSmemTransBatchStatusQuery = nullptr;
    pSmemSetLogLevel = nullptr;

    gLibHandle = nullptr;
    gLoaded = false;
}

}  // namespace tent
}  // namespace mooncake

#undef DL_LOAD_SYM

// HYBM GVA API. Keep its symbols and lifecycle independent of trans.
#ifdef TENT_WITH_HYBM_GVA
#include <acl/acl.h>
#include <cstring>
#include <atomic>
#include <list>
#include <algorithm>
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
        library_ = std::shared_ptr<void>(
            dlopen("libmf_hybm_core.so", RTLD_NOW | RTLD_LOCAL),
            [](void* handle) {
                if (handle) (void)dlclose(handle);
            });
        if (!library_) return false;
#define LOAD(name)                                                         \
    name##_ =                                                              \
        reinterpret_cast<decltype(name##_)>(dlsym(library_.get(), #name)); \
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
        hybm_data_copy_ = reinterpret_cast<decltype(hybm_data_copy_)>(
            dlsym(library_.get(), "hybm_data_copy"));
        hybm_data_batch_copy_ =
            reinterpret_cast<decltype(hybm_data_batch_copy_)>(
                dlsym(library_.get(), "hybm_data_batch_copy"));
        hybm_transport_connect_ =
            reinterpret_cast<decltype(hybm_transport_connect_)>(
                dlsym(library_.get(), "hybm_transport_connect"));
        hybm_wait_ = reinterpret_cast<decltype(hybm_wait_)>(
            dlsym(library_.get(), "hybm_wait"));
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
    bool supportsBatchCopy() const {
        return hybm_data_batch_copy_ && hybm_wait_;
    }
    int batchCopy(void* entity, const std::vector<MemFabricGvaCopy>& copies,
                  uint32_t direction) {
        if (!supportsBatchCopy() || copies.empty() ||
            copies.size() > UINT32_MAX) {
            return -1;
        }
        auto buffers = std::make_shared<BatchBuffers>();
        buffers->entity = entity;
        buffers->sources.reserve(copies.size());
        buffers->destinations.reserve(copies.size());
        buffers->sizes.reserve(copies.size());
        for (const auto& copy : copies) {
            buffers->sources.push_back(reinterpret_cast<void*>(copy.source));
            buffers->destinations.push_back(
                reinterpret_cast<void*>(copy.destination));
            buffers->sizes.push_back(copy.size);
        }
        // HYBM can rewrite the pointer arrays. Own them independently of the
        // validated requests, and retain them if native completion is unknown.
        std::list<std::shared_ptr<BatchBuffers>>::iterator retained;
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            pending_batches_.push_front(buffers);
            retained = pending_batches_.begin();
        }
        hybm_batch_copy_params params{
            buffers->sources.data(), buffers->destinations.data(),
            buffers->sizes.data(), static_cast<uint32_t>(copies.size())};
        const auto ret = hybm_data_batch_copy_(
            entity, &params, static_cast<hybm_data_copy_direction>(direction),
            nullptr, 0);
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            if (!ret) {
                pending_batches_.erase(retained);
            } else {
                buffers->failed = true;
            }
        }
        return ret;
    }
    int wait(void* entity) {
        std::vector<std::shared_ptr<BatchBuffers>> failed;
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            for (const auto& batch : pending_batches_) {
                if (batch->failed && batch->entity == entity) {
                    failed.push_back(batch);
                }
            }
        }
        const auto ret = hybm_wait_ ? hybm_wait_(entity) : -1;
        if (!ret) {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            // Only release failed calls present when this drain began. A
            // concurrently failing or still running call must keep its arrays.
            pending_batches_.remove_if([&failed](const auto& batch) {
                return std::find(failed.begin(), failed.end(), batch) !=
                       failed.end();
            });
        }
        return ret;
    }
    int bind(uint16_t device) { return aclrtSetDevice(device); }
    // HYBM itself reference-counts init/uninit, also used by smem_trans_init.
    int initialize(uint16_t device) { return hybm_init_(device, 0); }
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
    int translate(uint64_t address, uint32_t memory_type, uint64_t& out) {
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
            exchange_info.descLen <= (1U << 20)) {
            blob.assign(exchange_info.desc,
                        exchange_info.desc + exchange_info.descLen);
        } else if (result == 0) {
            result = -1;
        }
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
    bool supportsConnect() const { return hybm_transport_connect_ != nullptr; }
    int connect(void* entity_handle, uint32_t rank) {
        return hybm_transport_connect_
                   ? hybm_transport_connect_(entity_handle, &rank, 1, 0)
                   : -1;
    }
    int map(void* entity_handle) { return hybm_mmap_(entity_handle, 0); }
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
    struct BatchBuffers {
        void* entity = nullptr;
        bool failed = false;
        std::vector<void*> sources, destinations;
        std::vector<uint64_t> sizes;
    };
    std::mutex batch_mutex_;
    std::list<std::shared_ptr<BatchBuffers>> pending_batches_;
    std::shared_ptr<void> library_;
#define FN(name) decltype(&name) name##_ = nullptr
    FN(hybm_data_copy);
    FN(hybm_data_batch_copy);
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
bool MemFabricGvaApi::supportsBatchCopy() {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->supportsBatchCopy() : false;
}
int MemFabricGvaApi::batchCopy(void* entity,
                               const std::vector<MemFabricGvaCopy>& copies,
                               uint32_t direction) {
    auto* api = nativeApi().load(std::memory_order_acquire);
    return api ? api->batchCopy(entity, copies, direction) : -1;
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
bool MemFabricGvaApi::supportsBatchCopy() { return false; }
int MemFabricGvaApi::batchCopy(void*, const std::vector<MemFabricGvaCopy>&,
                               uint32_t) {
    return -1;
}
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
