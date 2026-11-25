// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <acl/acl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <unordered_map>
#include <unordered_set>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include "tent/common/config.h"
#include "memory_engine_client.h"
#include "memory_engine_server.h"
#include "tent/runtime/control_plane.h"
#include "tent/transfer_engine.h"
#include "tent/transport/memfabric/memfabric_shared_memory.h"
#include "tent/transport/memfabric/memfabric_transport.h"
#include "tent/transport/memfabric/memfabric_under_api.h"

namespace {

thread_local int g_device_id = 0;
thread_local aclrtContext g_context = reinterpret_cast<aclrtContext>(0x10000);
int g_device_count = 1;

void ResetAcl() {
    g_device_id = 0;
    g_device_count = 1;
    g_context = reinterpret_cast<aclrtContext>(0x10000);
}

}  // namespace

extern "C" {

aclError aclrtGetDeviceCount(uint32_t* count) {
    *count = static_cast<uint32_t>(g_device_count);
    return ACL_ERROR_NONE;
}

aclError aclrtGetDevice(int* deviceId) {
    *deviceId = g_device_id;
    return ACL_ERROR_NONE;
}

aclError aclrtSetDevice(int deviceId) {
    if (deviceId >= g_device_count) return 1;
    g_device_id = deviceId;
    return ACL_ERROR_NONE;
}

aclError aclrtGetCurrentContext(aclrtContext* context) {
    *context = g_context;
    return ACL_ERROR_NONE;
}

aclError aclrtSetCurrentContext(aclrtContext context) {
    g_context = context;
    return ACL_ERROR_NONE;
}

aclError aclrtPointerGetAttributes(const void* ptr,
                                   aclrtPtrAttributes* attributes) {
    (void)ptr;
    attributes->location.id = 0;
    attributes->location.type = ACL_MEM_LOCATION_TYPE_HOST;
    return ACL_ERROR_NONE;
}

aclError aclrtMalloc(void** devPtr, size_t size, aclrtMemMallocPolicy policy) {
    (void)policy;
    *devPtr = std::malloc(size);
    return *devPtr != nullptr ? ACL_ERROR_NONE : 1;
}

aclError aclrtFree(void* devPtr) {
    std::free(devPtr);
    return ACL_ERROR_NONE;
}

aclError aclrtMallocHost(void** hostPtr, size_t size) {
    *hostPtr = std::malloc(size);
    return *hostPtr != nullptr ? ACL_ERROR_NONE : 1;
}

aclError aclrtFreeHost(void* hostPtr) {
    std::free(hostPtr);
    return ACL_ERROR_NONE;
}

aclError aclrtGetPhyDevIdByLogicDevId(int32_t, int32_t* phyDevId) {
    *phyDevId = 0;
    return ACL_SUCCESS;
}

aclError aclrtMallocPhysical(aclrtDrvMemHandle*, size_t,
                             const aclrtPhysicalMemProp*, uint64_t) {
    return ACL_SUCCESS;
}

aclError aclrtFreePhysical(aclrtDrvMemHandle) { return ACL_SUCCESS; }

aclError aclrtReserveMemAddress(void** va, size_t, uint64_t, void*, size_t) {
    *va = nullptr;
    return ACL_ERROR_NONE;
}

aclError aclrtMapMem(void*, size_t, uint64_t, aclrtDrvMemHandle, uint64_t) {
    return ACL_ERROR_NONE;
}

aclError aclrtUnmapMem(void*) { return ACL_ERROR_NONE; }

aclError aclrtReleaseMemAddress(void*) { return ACL_ERROR_NONE; }

aclError aclrtMemcpy(void*, size_t, const void*, size_t, aclrtMemcpyKind) {
    return ACL_SUCCESS;
}

const char* aclGetRecentErrMsg() { return ""; }

}  // extern "C"

namespace mooncake {
namespace tent {

namespace {

struct FakeHybm {
    static constexpr uint64_t kSize = 2ULL << 20;
    bool loaded = false;
    bool blocked = false, entered = false, throw_batch = false;
    std::optional<uint64_t> blocked_source;
    bool reachable = true, stale_registry = false;
    bool fail_create = false;
    int initialize_result = 0, reserve_result = 0;
    int result = 0, wait_result = 0, waits = 0;
    int connect_result = 0, map_result = 0, remove_result = 0;
    int entity_export_result = 0, fail_slice_export_at = 0;
    int entity_import_result = 0;
    std::atomic<int> slice_import_result{0};
    int translate_result = 0;
    int initializes = 0, creates = 0, frees = 0, unmaps = 0;
    int entity_exports = 0, slice_exports = 0, imports = 0, slice_imports = 0;
    int connects = 0;
    uint64_t alias_offset = 0;
    std::vector<size_t> batch_sizes;
    std::vector<std::array<uint64_t, 4>> copies;

    std::mutex mutex;
    std::condition_variable cv;

    void release() {
        std::lock_guard lock(mutex);
        blocked = false;
        cv.notify_all();
    }

    bool awaitEntry() {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(2),
                           [&] { return entered; });
    }
};

struct HybmSlice {
    uint64_t size;
    bool imported;
};

struct HybmEntity {
    MemFabricGvaConfig config;
    uint64_t serial;
    std::array<uint64_t, 2> cursors{};
    std::map<uint64_t, HybmSlice> slices;
};

auto g_hybm = std::make_unique<FakeHybm>();
std::mutex gHybmMutex;
std::map<uint16_t, std::unique_ptr<HybmEntity>> g_hybm_entities;
thread_local uint16_t g_hybm_device;

HybmEntity& hybmEntity(void* handle) {
    return *static_cast<HybmEntity*>(handle);
}

FakeHybm& fakeHybm() { return *g_hybm; }

// Call after destroying contexts and draining their workers.
void resetFakeHybm() {
    g_hybm_entities.clear();
    g_hybm = std::make_unique<FakeHybm>();
    g_hybm_device = 0;
}

}  // namespace

bool MemFabricGvaApi::LoadLibrary() { return g_hybm->loaded; }

int MemFabricGvaApi::copy(void*, uint64_t, uint64_t, size_t, uint32_t) {
    return g_hybm->result;
}

int MemFabricGvaApi::batchCopy(void*,
                               const std::vector<MemFabricGvaCopy>& copies,
                               uint32_t direction) {
    std::unique_lock lock(g_hybm->mutex);
    g_hybm->batch_sizes.push_back(copies.size());
    for (const auto& copy : copies) {
        g_hybm->copies.push_back(
            {copy.source, copy.destination, copy.size, direction});
    }
    g_hybm->entered = true;
    g_hybm->cv.notify_all();
    g_hybm->cv.wait(lock, [&] {
        return !g_hybm->blocked ||
               (g_hybm->blocked_source &&
                copies.front().source != *g_hybm->blocked_source);
    });
    if (g_hybm->throw_batch) throw std::runtime_error("mock HYBM batch error");
    return g_hybm->result;
}

int MemFabricGvaApi::wait(void*) {
    std::lock_guard lock(g_hybm->mutex);
    ++g_hybm->waits;
    return g_hybm->wait_result;
}

int MemFabricGvaApi::bind(uint16_t next_device) {
    g_hybm_device = next_device;
    return 0;
}

int MemFabricGvaApi::initialize(uint16_t) {
    ++g_hybm->initializes;
    return g_hybm->initialize_result;
}

void MemFabricGvaApi::uninitialize() {}

void* MemFabricGvaApi::create(const MemFabricGvaConfig& config) {
    std::lock_guard lock(gHybmMutex);
    if (g_hybm->fail_create) {
        return nullptr;
    }
    auto created = std::make_unique<HybmEntity>();
    created->config = config;
    created->serial = ++g_hybm->creates;
    return g_hybm_entities.emplace(config.device, std::move(created))
        .first->second.get();
}

void MemFabricGvaApi::destroy(void* handle) {
    std::lock_guard lock(gHybmMutex);
    const auto id = hybmEntity(handle).config.device;
    g_hybm_entities.erase(id);
}

int MemFabricGvaApi::reserve(void*) { return g_hybm->reserve_result; }
int MemFabricGvaApi::unreserve(void*) { return 0; }

void* MemFabricGvaApi::allocate(void* handle, uint32_t type, uint64_t bytes) {
    if (type != kGvaMemoryTypeDevice && type != kGvaMemoryTypeHost) {
        return nullptr;
    }
    std::lock_guard lock(gHybmMutex);
    auto& local = hybmEntity(handle);
    const auto window = type == kGvaMemoryTypeDevice ? local.config.hbm_window : local.config.dram_window;
    auto& cursor = local.cursors.at(type - kGvaMemoryTypeDevice);
    const auto addr = base(handle, type) + local.config.rank * window + cursor;
    cursor += bytes;
    local.slices.emplace(addr, HybmSlice{bytes, false});
    return reinterpret_cast<void*>(addr);
}

int MemFabricGvaApi::free(void* handle, void* slice) {
    std::lock_guard lock(gHybmMutex);
    ++g_hybm->frees;
    hybmEntity(handle).slices.erase(reinterpret_cast<uint64_t>(slice));
    return 0;
}

uint64_t MemFabricGvaApi::address(void*, void* slice) {
    return reinterpret_cast<uint64_t>(slice);
}

uint64_t MemFabricGvaApi::size(void* handle, void* slice) {
    std::lock_guard lock(gHybmMutex);
    return hybmEntity(handle).slices.at(address(handle, slice)).size;
}

uint64_t MemFabricGvaApi::base(void*, uint32_t type) {
    return (1ULL << 56) + (uint64_t{type} << 40);
}

int MemFabricGvaApi::translate(uint64_t addr, uint32_t type, uint64_t& out) {
    std::lock_guard lock(gHybmMutex);
    if (g_hybm->translate_result) {
        return g_hybm->translate_result;
    }
    auto local = g_hybm_entities.find(g_hybm_device);
    if ((type != kGvaMemoryTypeDevice && type != kGvaMemoryTypeHost) ||
        local == g_hybm_entities.end()) {
        return -12;
    }
    const auto& slices = local->second->slices;
    auto slice = slices.upper_bound(addr);
    if (slice == slices.begin()) {
        return -12;
    }
    --slice;
    if (addr - slice->first >= slice->second.size) {
        return -12;
    }
    if (slice->second.imported &&
        local->second->config.protocol != "device_sdma") {
        return -12;
    }
    out = addr + g_hybm->alias_offset;
    return 0;
}

int MemFabricGvaApi::exportBlob(void* handle, void* slice,
                                std::vector<uint8_t>& blob) {
    if (!slice) {
        ++g_hybm->entity_exports;
        if (g_hybm->entity_export_result) return g_hybm->entity_export_result;
    } else {
        if (++g_hybm->slice_exports == g_hybm->fail_slice_export_at) return -1;
    }
    const std::array<uint64_t, 2> data =
        slice ? std::array<uint64_t, 2>{address(handle, slice),
                                        size(handle, slice)}
              : std::array<uint64_t, 2>{hybmEntity(handle).serial, 0};
    blob.resize(sizeof(data));
    std::memcpy(blob.data(), data.data(), sizeof(data));
    return 0;
}

int MemFabricGvaApi::importBlob(void* handle, bool entity_info,
                                const std::vector<uint8_t>& blob) {
    std::lock_guard lock(gHybmMutex);
    ++g_hybm->imports;
    if (entity_info) {
        return g_hybm->entity_import_result;
    }
    ++g_hybm->slice_imports;
    const auto result = g_hybm->slice_import_result.load();
    if (result) {
        return result;
    }
    std::array<uint64_t, 2> data;
    if (blob.size() != sizeof(data)) {
        return -1;
    }
    std::memcpy(data.data(), blob.data(), sizeof(data));
    hybmEntity(handle).slices.emplace(data[0], HybmSlice{data[1], true});
    return 0;
}

int MemFabricGvaApi::map(void*) { return g_hybm->map_result; }

int MemFabricGvaApi::connect(void*, uint32_t) {
    std::lock_guard lock(gHybmMutex);
    ++g_hybm->connects;
    return g_hybm->connect_result;
}

int MemFabricGvaApi::reach(void*, uint32_t, uint32_t& out) {
    out = g_hybm->reachable ? 1 : 0;
    return 0;
}

int MemFabricGvaApi::remove(void*, uint32_t) { return g_hybm->remove_result; }

int MemFabricGvaApi::unmap(void* handle, uint32_t rank) {
    std::lock_guard lock(gHybmMutex);
    ++g_hybm->unmaps;
    if (g_hybm->stale_registry) {
        return 0;
    }
    auto& local = hybmEntity(handle);
    const auto inWindow = [&](uint64_t address, uint32_t type, uint64_t window) {
        const auto start = base(handle, type) + rank * window;
        return address >= start && address - start < window;
    };
    std::erase_if(local.slices, [&](const auto& entry) {
        return entry.second.imported &&
               (inWindow(entry.first, kGvaMemoryTypeDevice, local.config.hbm_window) ||
                inWindow(entry.first, kGvaMemoryTypeHost, local.config.dram_window));
    });
    return 0;
}

namespace {

class MemFabricGva : public ::testing::Test {
   protected:
    void SetUp() override {
        resetFakeHybm();
        fakeHybm().loaded = true;
    }

    void TearDown() override { resetFakeHybm(); }
};

::testing::AssertionResult IsOk(const Status &status)
{
    if (status.ok()) {
        return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure() << status.ToString();
}

uint64_t MockSliceSize(uint16_t device, const void *address)
{
    std::lock_guard lock(gHybmMutex);
    const auto entity = g_hybm_entities.find(device);
    if (entity == g_hybm_entities.end()) {
        return 0;
    }
    const auto &slices = entity->second->slices;
    const auto slice = slices.find(reinterpret_cast<uint64_t>(address));
    return slice == slices.end() ? 0 : slice->second.size;
}

uint16_t PickLoopbackPort()
{
    asio::io_context context;
    asio::ip::tcp::acceptor acceptor(context, {asio::ip::address_v4::loopback(), 0});
    return acceptor.local_endpoint().port();
}

class MemoryEngineTest : public MemFabricGva {
protected:
    static constexpr uint32_t FIRST_RANK = 0;
    static constexpr uint32_t SECOND_RANK = 1;
    static constexpr uint32_t WORLD_SIZE = 2;
    static constexpr uint64_t WINDOW_SIZE = 1ULL << 30;
    static constexpr size_t REQUEST_SIZE = 4UL << 10;
    static constexpr int NATIVE_ERROR = -1;
    static constexpr unsigned RPC_THREADS = 2;
    static constexpr const char *LOOPBACK = "127.0.0.1";

    void SetUp() override
    {
        MemFabricGva::SetUp();
        ResetAcl();
        g_device_count = static_cast<int>(WORLD_SIZE);
        ASSERT_EQ(setenv("MF_PROTOCOL", "device_rdma", 1), 0);
        mServerUrl = std::string(LOOPBACK) + ":" + std::to_string(PickLoopbackPort());
        Config config;
        config.set("memory_engine/server_url", mServerUrl);
        config.set("memory_engine/server/world_size", WORLD_SIZE);
        config.set("memory_engine/server/host_mem_max", WINDOW_SIZE);
        config.set("memory_engine/server/device_mem_max", WINDOW_SIZE);
        config.set("memory_engine/server/rpc_server_threads", RPC_THREADS);
        ASSERT_TRUE(IsOk(MemoryEngineServer::Instance().Start(config)));
    }

    void TearDown() override
    {
        MemoryEngineServer::Instance().Stop();
        EXPECT_EQ(unsetenv("MF_PROTOCOL"), 0);
        ResetAcl();
        MemFabricGva::TearDown();
    }

    void ConfigureClient(Config &config, std::optional<uint32_t> rank = std::nullopt) const
    {
        config.set("memory_engine/server_url", mServerUrl);
        config.set("memory_engine/client_host", LOOPBACK);
        config.set("memory_engine/world_size", WORLD_SIZE);
        config.set("memory_engine/host_mem_max", WINDOW_SIZE);
        config.set("memory_engine/device_mem_max", WINDOW_SIZE);
        config.set("memory_engine/auto_ranking", !rank.has_value());
        if (rank) {
            config.set("memory_engine/local_rank", *rank);
        }
        config.set("rpc_server_hostname", LOOPBACK);
        config.set("rpc_server_threads", RPC_THREADS);
        config.set("log_level", "warning");
        config.set("transports/rdma/enable", false);
    }

    Status CreateClient(std::unique_ptr<MemoryEngine> &engine, uint16_t device,
                        std::optional<uint32_t> rank = std::nullopt) const
    {
        CHECK_ASCEND(aclrtSetDevice(static_cast<int>(device)));
        Config config;
        ConfigureClient(config, rank);
        return MemoryEngine::Create(config, &engine);
    }

private:
    std::string mServerUrl;
};

TEST_F(MemoryEngineTest, RejectsInvalidOutputsAndClientLayouts)
{
    Config config;
    EXPECT_TRUE(MemoryEngineServer::Instance().Start(config).IsInvalidArgument());

    std::unique_ptr<MemoryEngine> engine;
    ConfigureClient(config);
    EXPECT_TRUE(MemoryEngine::Create(config, nullptr).IsInvalidArgument());
    config.set("memory_engine/host_mem_max", REQUEST_SIZE);
    EXPECT_TRUE(MemoryEngine::Create(config, &engine).IsInvalidArgument());
    EXPECT_EQ(engine, nullptr);

    ConfigureClient(config, WORLD_SIZE);
    EXPECT_TRUE(MemoryEngine::Create(config, &engine).IsInvalidArgument());
    EXPECT_EQ(engine, nullptr);

    ConfigureClient(config);
    config.set("memory_engine/host_mem_max", 0U);
    EXPECT_TRUE(MemoryEngine::Create(config, &engine).IsInvalidArgument());
    EXPECT_EQ(engine, nullptr);
}

TEST_F(MemoryEngineTest, ReservesExplicitAndAutomaticRanksUntilLeave)
{
    std::unique_ptr<MemoryEngine> explicitRank, automaticRank, next;
    ASSERT_TRUE(IsOk(CreateClient(explicitRank, SECOND_RANK, SECOND_RANK)));
    EXPECT_EQ(explicitRank->GetLocalRank(), SECOND_RANK);
    EXPECT_TRUE(CreateClient(next, FIRST_RANK, SECOND_RANK).IsInvalidEntry());
    EXPECT_EQ(next, nullptr);

    ASSERT_TRUE(IsOk(CreateClient(automaticRank, FIRST_RANK)));
    EXPECT_EQ(automaticRank->GetLocalRank(), FIRST_RANK);
    EXPECT_TRUE(CreateClient(next, FIRST_RANK).IsTooManyRequests());
    EXPECT_EQ(next, nullptr);

    ASSERT_TRUE(IsOk(explicitRank->Leave()));
    ASSERT_TRUE(IsOk(CreateClient(next, SECOND_RANK)));
    EXPECT_EQ(next->GetLocalRank(), SECOND_RANK);
}

TEST_F(MemoryEngineTest, FailedCreateReleasesRank)
{
    std::unique_ptr<MemoryEngine> engine;
    fakeHybm().reserve_result = NATIVE_ERROR;
    EXPECT_FALSE(CreateClient(engine, FIRST_RANK, FIRST_RANK).ok());
    EXPECT_EQ(engine, nullptr);

    fakeHybm().reserve_result = 0;
    ASSERT_TRUE(IsOk(CreateClient(engine, FIRST_RANK, FIRST_RANK)));
    EXPECT_EQ(engine->GetLocalRank(), FIRST_RANK);
}

TEST_F(MemoryEngineTest, ValidatesAndAllocatesInHostAndDeviceWindows)
{
    std::unique_ptr<MemoryEngine> engine;
    ASSERT_TRUE(IsOk(CreateClient(engine, SECOND_RANK, SECOND_RANK)));
    void *address = nullptr;
    EXPECT_TRUE(engine->Allocate(REQUEST_SIZE, MemoryEngineMemType::Host, &address).IsInvalidEntry());
    EXPECT_TRUE(engine->Free(nullptr).IsInvalidEntry());
    ASSERT_TRUE(IsOk(engine->Join()));
    ASSERT_TRUE(IsOk(engine->Join()));

    for (auto type : {MemoryEngineMemType::Host, MemoryEngineMemType::Device}) {
        SCOPED_TRACE(static_cast<int>(type));
        void *base = nullptr;
        ASSERT_TRUE(IsOk(engine->GvaBase(type, SECOND_RANK, &base)));
        EXPECT_TRUE(engine->GvaBase(type, WORLD_SIZE, &address).IsInvalidArgument());
        EXPECT_TRUE(engine->Allocate(0, type, &address).IsInvalidArgument());
        EXPECT_TRUE(engine->Allocate(REQUEST_SIZE, type, nullptr).IsInvalidArgument());
        EXPECT_TRUE(engine->Allocate(WINDOW_SIZE + REQUEST_SIZE, type, &address).IsTooManyRequests());
        ASSERT_TRUE(IsOk(engine->Allocate(REQUEST_SIZE, type, &address)));
        EXPECT_EQ(address, base);
        EXPECT_EQ(MockSliceSize(SECOND_RANK, address), FakeHybm::kSize);

        uint32_t rank = FIRST_RANK;
        ASSERT_TRUE(IsOk(engine->RankOfGva(address, &rank)));
        EXPECT_EQ(rank, SECOND_RANK);
        const auto interior = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(address) + REQUEST_SIZE);
        EXPECT_TRUE(engine->Free(interior).IsInvalidArgument());
        ASSERT_TRUE(IsOk(engine->Free(address)));
        EXPECT_EQ(MockSliceSize(SECOND_RANK, address), 0U);
        EXPECT_TRUE(engine->Free(address).IsInvalidArgument());
    }
}

TEST_F(MemoryEngineTest, RetainsAssignedGvaLayoutWhenTentLoadsEnvironment)
{
    Config environment;
    environment.set("transports/memfabric/enable", true);
    environment.set("transports/memfabric/gva/rank", FIRST_RANK);
    environment.set("transports/memfabric/gva/rank_count", 1);
    environment.set("transports/memfabric/gva/dram_window", 0);
    environment.set("transports/memfabric/gva/hbm_window", 2);
    environment.set("transports/rdma/enable", false);

    const auto *previous = std::getenv("MC_TENT_CONF");
    const auto saved = previous ? std::optional<std::string>(previous) : std::nullopt;
    ASSERT_EQ(setenv("MC_TENT_CONF", environment.dump().c_str(), 1), 0);
    std::unique_ptr<MemoryEngine> engine;
    const auto status = CreateClient(engine, SECOND_RANK, SECOND_RANK);
    EXPECT_EQ(saved ? setenv("MC_TENT_CONF", saved->c_str(), 1) : unsetenv("MC_TENT_CONF"), 0);
    ASSERT_TRUE(IsOk(status));

    ASSERT_TRUE(IsOk(engine->Join()));
    for (const auto type : {MemoryEngineMemType::Host, MemoryEngineMemType::Device}) {
        void *base = nullptr;
        void *address = nullptr;
        ASSERT_TRUE(IsOk(engine->GvaBase(type, SECOND_RANK, &base)));
        ASSERT_TRUE(IsOk(engine->Allocate(REQUEST_SIZE, type, &address)));
        EXPECT_EQ(address, base);
        ASSERT_TRUE(IsOk(engine->Free(address)));
    }
    ASSERT_TRUE(IsOk(engine->Leave()));
}

TEST_F(MemoryEngineTest, SharesExistingAndNewAllocationsAndDrainsRetiredSlices)
{
    std::unique_ptr<MemoryEngine> owner, peer;
    ASSERT_TRUE(IsOk(CreateClient(owner, FIRST_RANK)));
    ASSERT_TRUE(IsOk(owner->Join()));
    void *early = nullptr;
    ASSERT_TRUE(IsOk(owner->Allocate(REQUEST_SIZE, MemoryEngineMemType::Device, &early)));
    ASSERT_TRUE(IsOk(CreateClient(peer, SECOND_RANK)));
    EXPECT_EQ(MockSliceSize(SECOND_RANK, early), 0U);
    ASSERT_TRUE(IsOk(peer->Join()));
    EXPECT_EQ(MockSliceSize(SECOND_RANK, early), FakeHybm::kSize);

    void *late = nullptr;
    ASSERT_TRUE(IsOk(owner->Allocate(REQUEST_SIZE, MemoryEngineMemType::Device, &late)));
    EXPECT_NE(late, early);
    EXPECT_EQ(MockSliceSize(SECOND_RANK, late), FakeHybm::kSize);
    EXPECT_TRUE(peer->Free(early).IsInvalidArgument());
    ASSERT_TRUE(IsOk(owner->Free(early)));
    EXPECT_EQ(MockSliceSize(FIRST_RANK, early), FakeHybm::kSize);
    EXPECT_EQ(MockSliceSize(SECOND_RANK, late), FakeHybm::kSize);

    ASSERT_TRUE(IsOk(owner->Free(late)));
    for (auto *address : {early, late}) {
        EXPECT_EQ(MockSliceSize(FIRST_RANK, address), 0U);
        EXPECT_EQ(MockSliceSize(SECOND_RANK, address), 0U);
    }
}

TEST_F(MemoryEngineTest, FailedJoinCanRetryWithTheSameRank)
{
    std::unique_ptr<MemoryEngine> owner, peer;
    ASSERT_TRUE(IsOk(CreateClient(owner, FIRST_RANK)));
    ASSERT_TRUE(IsOk(owner->Join()));
    void *address = nullptr;
    ASSERT_TRUE(IsOk(owner->Allocate(REQUEST_SIZE, MemoryEngineMemType::Device, &address)));
    ASSERT_TRUE(IsOk(CreateClient(peer, SECOND_RANK)));

    fakeHybm().slice_import_result = NATIVE_ERROR;
    EXPECT_TRUE(peer->Join().IsInternalError());
    EXPECT_EQ(MockSliceSize(SECOND_RANK, address), 0U);
    void *unallocated = nullptr;
    EXPECT_TRUE(peer->Allocate(REQUEST_SIZE, MemoryEngineMemType::Device, &unallocated).IsInvalidEntry());

    fakeHybm().slice_import_result = 0;
    ASSERT_TRUE(IsOk(peer->Join()));
    EXPECT_EQ(peer->GetLocalRank(), SECOND_RANK);
    EXPECT_EQ(MockSliceSize(SECOND_RANK, address), FakeHybm::kSize);
}

TEST_F(MemoryEngineTest, FailedPublishReleasesLocalAllocation)
{
    std::unique_ptr<MemoryEngine> owner, peer;
    ASSERT_TRUE(IsOk(CreateClient(owner, FIRST_RANK)));
    ASSERT_TRUE(IsOk(owner->Join()));
    ASSERT_TRUE(IsOk(CreateClient(peer, SECOND_RANK)));
    ASSERT_TRUE(IsOk(peer->Join()));
    void *base = nullptr;
    ASSERT_TRUE(IsOk(owner->GvaBase(MemoryEngineMemType::Device, FIRST_RANK, &base)));

    fakeHybm().slice_import_result = NATIVE_ERROR;
    void *address = nullptr;
    EXPECT_TRUE(owner->Allocate(REQUEST_SIZE, MemoryEngineMemType::Device, &address).IsInternalError());
    EXPECT_EQ(address, nullptr);
    EXPECT_EQ(MockSliceSize(FIRST_RANK, base), 0U);
    EXPECT_EQ(MockSliceSize(SECOND_RANK, base), 0U);

    fakeHybm().slice_import_result = 0;
    ASSERT_TRUE(IsOk(owner->Allocate(REQUEST_SIZE, MemoryEngineMemType::Device, &address)));
    EXPECT_EQ(MockSliceSize(SECOND_RANK, address), FakeHybm::kSize);
}

TEST_F(MemoryEngineTest, LeaveRevokesLiveAllocationsAndAllowsRejoin)
{
    std::unique_ptr<MemoryEngine> owner, peer;
    ASSERT_TRUE(IsOk(CreateClient(owner, FIRST_RANK, FIRST_RANK)));
    ASSERT_TRUE(IsOk(CreateClient(peer, SECOND_RANK, SECOND_RANK)));
    ASSERT_TRUE(IsOk(owner->Join()));
    ASSERT_TRUE(IsOk(peer->Join()));
    void *address = nullptr;
    ASSERT_TRUE(IsOk(owner->Allocate(REQUEST_SIZE, MemoryEngineMemType::Device, &address)));
    EXPECT_EQ(MockSliceSize(SECOND_RANK, address), FakeHybm::kSize);

    ASSERT_TRUE(IsOk(owner->Leave()));
    ASSERT_TRUE(IsOk(owner->Leave()));
    EXPECT_TRUE(owner->Free(address).IsInvalidEntry());
    EXPECT_EQ(MockSliceSize(FIRST_RANK, address), 0U);
    EXPECT_EQ(MockSliceSize(SECOND_RANK, address), 0U);

    void *peerAddress = nullptr;
    ASSERT_TRUE(IsOk(peer->Allocate(REQUEST_SIZE, MemoryEngineMemType::Device, &peerAddress)));
    ASSERT_EQ(aclrtSetDevice(static_cast<int>(FIRST_RANK)), ACL_SUCCESS);
    ASSERT_TRUE(IsOk(owner->Join()));
    EXPECT_EQ(owner->GetLocalRank(), FIRST_RANK);
    EXPECT_EQ(MockSliceSize(FIRST_RANK, peerAddress), FakeHybm::kSize);
    ASSERT_TRUE(IsOk(peer->Free(peerAddress)));
    EXPECT_EQ(MockSliceSize(FIRST_RANK, peerAddress), 0U);
}

}  // namespace

}
}
