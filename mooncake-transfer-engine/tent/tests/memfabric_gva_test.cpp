// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "memfabric_gva_test_api.h"
#include "tent/transport/memfabric/memfabric_shared_memory.h"

namespace mooncake::tent {
namespace {

using test::fakeHybm;
using test::resetFakeHybm;
using Json = nlohmann::json;
constexpr auto kPoolSize = test::FakeHybm::kSize;
constexpr auto kHbm = kGvaMemoryTypeDevice;

MemFabricGvaConfig config(uint16_t rank = 0,
                          const char* protocol = "device_sdma") {
    MemFabricGvaConfig options;
    options.enabled = true;
    options.group = "hbm-test";
    options.protocol = protocol;
    options.rank_count = 2;
    options.rank = options.device = rank;
    options.hbm_window = options.hbm_size = 4 * kPoolSize;
    return options;
}

uint64_t addr(void* pointer) { return reinterpret_cast<uint64_t>(pointer); }

void freePool(MemFabricSharedMemory& memory, void* pointer) {
    ASSERT_TRUE(memory.unpublish(pointer).ok());
    ASSERT_TRUE(memory.free(pointer, kPoolSize).ok());
}

class MemFabricGva : public ::testing::Test {
   protected:
    void SetUp() override {
        resetFakeHybm();
        fakeHybm().loaded = true;
    }
};

struct HbmPair {
    explicit HbmPair(const char* protocol = "device_sdma")
        : local(config(0, protocol), "local"),
          remote(config(1, protocol), "remote") {}

    void initialize() {
        ASSERT_TRUE(local.initialize().ok());
        ASSERT_TRUE(remote.initialize().ok());
        ASSERT_TRUE(local.allocate(&source, kPoolSize, kHbm, 0).ok());
        ASSERT_TRUE(remote.allocate(&target, kPoolSize, kHbm, 0).ok());
        std::string descriptor;
        ASSERT_TRUE(local.publish(source, kPoolSize, descriptor).ok());
        ASSERT_TRUE(remote.publish(target, kPoolSize, publication).ok());
    }

    void release() {
        ASSERT_TRUE(unmap(target).ok());
        freePool(local, source);
        freePool(remote, target);
    }

    Status map() { return map(publication, addr(target)); }
    Status map(const std::string& desc, uint64_t base,
               size_t size = kPoolSize) {
        return local.map("remote", desc, base, size);
    }
    Status unmap(void* pointer) { return local.unmap("remote", addr(pointer)); }
    Status prepare() {
        return local.prepare("remote", publication, addr(target), kPoolSize);
    }
    Status beginCopy(const MemFabricGvaCopy& copy, uint32_t& direction) {
        return local.beginCopy(copy.source, copy.destination, copy.size,
                               direction);
    }

    MemFabricSharedMemory local, remote;
    void* source = nullptr;
    void* target = nullptr;
    std::string publication;
};

TEST_F(MemFabricGva, InitializesExplicitlyAndRetriesNativeFailures) {
    auto& native = fakeHybm();
    native.loaded = false;
    MemFabricSharedMemory unavailable(config(), "segment");
    EXPECT_TRUE(unavailable.initialize().IsNotImplemented());
    native.loaded = true;
    MemFabricSharedMemory memory(config(), "segment");
    void* pointer = nullptr;
    EXPECT_TRUE(
        memory.allocate(&pointer, kPoolSize, kHbm, 0).IsInvalidArgument());
    native.initialize_result = -1;
    EXPECT_FALSE(memory.initialize().ok());
    native.initialize_result = 0;
    native.fail_create = true;
    EXPECT_FALSE(memory.initialize().ok());
    native.fail_create = false;
    native.reserve_result = -1;
    EXPECT_FALSE(memory.initialize().ok());
    native.reserve_result = 0;
    native.entity_export_result = -1;
    EXPECT_FALSE(memory.initialize().ok());
    native.entity_export_result = 0;
    ASSERT_TRUE(memory.initialize().ok());
    EXPECT_EQ(native.creates, 2);
    EXPECT_EQ(native.initializes, 2);
    ASSERT_TRUE(memory.initialize().ok());
    EXPECT_EQ(native.entity_exports, 2);
    ASSERT_TRUE(memory.close().ok());
    ASSERT_TRUE(memory.initialize().ok());
    EXPECT_EQ(native.creates, 3);
}

TEST_F(MemFabricGva, HbmAllocationUsesIdentityAndAccountsForNativeCursor) {
    auto settings = config();
    settings.hbm_window = settings.hbm_size = 5 * kPoolSize;
    MemFabricSharedMemory memory(settings, "segment");
    ASSERT_TRUE(memory.initialize().ok());
    auto& native = fakeHybm();
    const auto base = MemFabricGvaApi::base(nullptr, kHbm);
    void* pointer = nullptr;
    EXPECT_FALSE(memory.allocate(&pointer, kPoolSize, kHbm, base + 1).ok());
    EXPECT_EQ(pointer, nullptr);
    native.alias_offset = 128;
    EXPECT_FALSE(memory.allocate(&pointer, kPoolSize, kHbm, 0).ok());
    native.alias_offset = 0;
    native.translate_result = -23;
    EXPECT_FALSE(memory.allocate(&pointer, kPoolSize, kHbm, 0).ok());
    native.translate_result = 0;
    EXPECT_EQ(native.frees, 3);
    MemoryOptions options;
    options.global_address = base + 3 * kPoolSize;
    ASSERT_TRUE(memory.allocate(&pointer, kPoolSize, kHbm, options).ok());
    EXPECT_EQ(addr(pointer), options.global_address);
    EXPECT_EQ(options.type, MEMFABRIC);
    EXPECT_EQ(options.location, "npu:0");
    void* output = nullptr;
    EXPECT_FALSE(memory.allocate(&output, 2 * kPoolSize, kHbm, 0).ok());
    EXPECT_TRUE(memory.close().IsTooManyRequests());
    EXPECT_FALSE(memory.free(pointer, kPoolSize - 1).ok());
    ASSERT_TRUE(memory.free(pointer, kPoolSize).ok());
    ASSERT_TRUE(memory.allocate(&output, kPoolSize, kHbm, 0).ok());
    EXPECT_EQ(addr(output), base + 4 * kPoolSize);
    ASSERT_TRUE(memory.free(output, kPoolSize).ok());
    EXPECT_FALSE(memory.allocate(&output, kPoolSize, kHbm, 0).ok());
}

TEST_F(MemFabricGva, BatchPublicationValidatesAndCommitsAllHbmSlices) {
    MemFabricSharedMemory memory(config(), "segment");
    ASSERT_TRUE(memory.initialize().ok());
    void *first = nullptr, *second = nullptr;
    ASSERT_TRUE(memory.allocate(&first, kPoolSize, kHbm, 0).ok());
    ASSERT_TRUE(memory.allocate(&second, kPoolSize, kHbm, 0).ok());
    const std::vector<std::pair<void*, size_t>> ranges{{first, kPoolSize},
                                                       {second, kPoolSize}};
    auto& native = fakeHybm();
    std::vector<std::string> descriptors{"sentinel"};
    EXPECT_FALSE(
        memory.publish({ranges.front(), {second, 32}}, descriptors).ok());
    EXPECT_EQ(native.slice_exports, 0);
    native.empty_slice_export = true;
    EXPECT_FALSE(memory.publish(ranges, descriptors).ok());
    native.empty_slice_export = false;
    native.fail_slice_export_at = 3;
    EXPECT_FALSE(memory.publish(ranges, descriptors).ok());
    EXPECT_EQ(descriptors, std::vector<std::string>{"sentinel"});
    uint32_t direction = 0;
    EXPECT_FALSE(
        memory.beginCopy(addr(first), addr(second), 32, direction).ok());
    ASSERT_TRUE(memory.publish(ranges, descriptors).ok());
    ASSERT_EQ(descriptors.size(), 2u);
    const auto descriptor = Json::parse(descriptors.front());
    EXPECT_EQ(descriptor["address_mode"], "identity");
    EXPECT_EQ(descriptor["type"], kHbm);
    EXPECT_EQ(descriptor["address"], addr(first));
    EXPECT_TRUE(memory.free(first, kPoolSize).IsTooManyRequests());
    EXPECT_FALSE(memory.unpublish(first, 32).ok());
    ASSERT_TRUE(memory.unpublish(first).ok());
    const auto exports = native.slice_exports;
    std::string repeated;
    ASSERT_TRUE(memory.publish(first, kPoolSize, repeated).ok());
    EXPECT_EQ(repeated, descriptors.front());
    EXPECT_EQ(native.slice_exports, exports);
    freePool(memory, first);
    freePool(memory, second);
}

TEST_F(MemFabricGva, InvalidBundlesNeverReachNativeImports) {
    HbmPair pair;
    ASSERT_NO_FATAL_FAILURE(pair.initialize());
    const auto desc = Json::parse(pair.publication);
    const auto target = addr(pair.target);
    for (const auto& [key, value] : std::vector<std::pair<std::string, Json>>{
             {"address_mode", "logical56"},
             {"segment_name", "other"},
             {"generation", 1u},
             {"rank", 0u},
             {"ranks", "2"},
             {"offset", UINT64_MAX},
             {"slice_blob", Json::array()},
             {"slice_blob", Json::array({"invalid"})},
             {"slice_blob", Json::array({256u})}}) {
        SCOPED_TRACE(key);
        auto invalid = desc;
        invalid[key] = value;
        const auto status = pair.map(invalid.dump(), target);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.message().find("'" + key + "'"), std::string::npos);
    }
    EXPECT_FALSE(pair.map("{", target).ok());
    EXPECT_FALSE(pair.map(Json::array({desc, desc}).dump(), target).ok());
    auto shifted = desc;
    shifted["address"] = target + 32;
    shifted["offset"] = 32u;
    auto inconsistent = shifted;
    inconsistent["generation"] = "restart";
    for (const auto& second : {shifted, inconsistent}) {
        EXPECT_FALSE(pair.map(Json::array({desc, second}).dump(), target).ok());
    }
    EXPECT_FALSE(pair.map(pair.publication, target + 1, 32).ok());
    EXPECT_EQ(fakeHybm().imports, 0);
    ASSERT_TRUE(pair.map().ok());
    EXPECT_FALSE(pair.map(shifted.dump(), target + 32).ok());
    auto changed = desc;
    changed["generation"] = "restart";
    EXPECT_TRUE(pair.map(changed.dump(), target).IsTooManyRequests());
    EXPECT_FALSE(
        pair.local.validateCopyTarget("remote", target, changed.dump()).ok());
    EXPECT_TRUE(
        pair.local.validateCopyTarget("remote", target, pair.publication).ok());
    ASSERT_NO_FATAL_FAILURE(pair.release());
}

TEST_F(MemFabricGva, SdmaRetriesImportsAndRequiresLiveIdentityMapping) {
    HbmPair pair;
    ASSERT_NO_FATAL_FAILURE(pair.initialize());
    auto& native = fakeHybm();
    native.entity_import_result = -1;
    EXPECT_FALSE(pair.map().ok());
    EXPECT_EQ(native.slice_imports, 0);
    native.entity_import_result = 0;
    native.slice_import_result = -1;
    EXPECT_FALSE(pair.map().ok());
    native.slice_import_result = 0;
    native.map_result = -1;
    EXPECT_TRUE(pair.map().IsTooManyRequests());
    native.map_result = 0;
    native.reachable = false;
    EXPECT_TRUE(pair.map().IsNotImplemented());
    native.reachable = true;
    const auto imports = native.slice_imports;
    native.alias_offset = 128;
    EXPECT_FALSE(pair.map().ok());
    native.alias_offset = 0;
    ASSERT_TRUE(pair.map().ok());
    ASSERT_TRUE(pair.map().ok());
    EXPECT_EQ(native.slice_imports, imports);
    native.remove_result = -1;
    EXPECT_TRUE(pair.unmap(pair.target).IsTooManyRequests());
    native.remove_result = 0;
    native.stale_registry = true;
    EXPECT_TRUE(pair.unmap(pair.target).IsTooManyRequests());
    native.stale_registry = false;
    ASSERT_NO_FATAL_FAILURE(pair.release());
    EXPECT_TRUE(
        pair.local.unmap("other", addr(pair.target)).IsInvalidArgument());
}

TEST_F(MemFabricGva, HbmSlicesShareRankCleanupAndSdmaFailureReleasesPins) {
    HbmPair pair;
    ASSERT_NO_FATAL_FAILURE(pair.initialize());
    ASSERT_TRUE(pair.map().ok());
    void* second = nullptr;
    std::string publication;
    ASSERT_TRUE(pair.remote.allocate(&second, kPoolSize, kHbm, 0).ok());
    ASSERT_TRUE(pair.remote.publish(second, kPoolSize, publication).ok());
    const auto bundle =
        Json::array({Json::parse(pair.publication), Json::parse(publication)})
            .dump();
    ASSERT_TRUE(pair.map(bundle, addr(second)).ok());
    auto& native = fakeHybm();
    EXPECT_TRUE(pair.unmap(second).IsTooManyRequests());
    EXPECT_EQ(native.unmaps, 0);
    uint32_t direction = 0;
    const std::vector<MemFabricGvaCopy> copies{
        {addr(pair.source), addr(pair.target), 32},
        {addr(pair.source) + 32, addr(pair.target) + 32, 32}};
    for (const auto& copy : copies) {
        ASSERT_TRUE(pair.beginCopy(copy, direction).ok());
    }
    EXPECT_EQ(direction, 9u);
    EXPECT_TRUE(pair.local.unpublish(pair.source).IsTooManyRequests());
    EXPECT_TRUE(pair.unmap(pair.target).IsTooManyRequests());
    native.result = -23;
    EXPECT_FALSE(pair.local.runBatchCopy(copies, direction).ok());
    EXPECT_EQ(native.batch_sizes, std::vector<size_t>{2});
    EXPECT_EQ(native.waits, 1);
    ASSERT_TRUE(pair.unmap(pair.target).ok());
    EXPECT_EQ(native.unmaps, 1);
    ASSERT_TRUE(pair.map(publication, addr(second)).ok());
    ASSERT_TRUE(pair.unmap(second).ok());
    freePool(pair.remote, second);
    freePool(pair.local, pair.source);
    freePool(pair.remote, pair.target);
}

class MemFabricNetworkGva : public MemFabricGva,
                            public ::testing::WithParamInterface<const char*> {
};

TEST_P(MemFabricNetworkGva,
       PreparesHbmWithoutDirectMappingAndQuarantinesFailure) {
    HbmPair pair(GetParam());
    ASSERT_NO_FATAL_FAILURE(pair.initialize());
    auto& native = fakeHybm();
    EXPECT_TRUE(pair.map().IsNotImplemented());
    EXPECT_EQ(native.imports, 0);
    native.connect_result = -1;
    EXPECT_TRUE(pair.prepare().IsTooManyRequests());
    uint32_t direction = 0;
    const MemFabricGvaCopy copy{addr(pair.source), addr(pair.target), 32};
    EXPECT_FALSE(pair.beginCopy(copy, direction).ok());
    native.connect_result = 0;
    ASSERT_TRUE(pair.prepare().ok());
    EXPECT_EQ(native.connects, 2);
    EXPECT_EQ(native.slice_imports, 1);
    ASSERT_TRUE(pair.beginCopy(copy, direction).ok());
    ASSERT_TRUE(pair.beginCopy(copy, direction).ok());
    native.result = -23;
    EXPECT_FALSE(pair.local.runBatchCopy({copy}, direction).ok());
    EXPECT_TRUE(pair.local.runBatchCopy({copy}, direction).IsTooManyRequests());
    EXPECT_EQ(native.waits, 0);
    EXPECT_EQ(native.batch_sizes, std::vector<size_t>{1});
    EXPECT_TRUE(pair.local.unpublish(pair.source).IsTooManyRequests());
    EXPECT_TRUE(pair.local.close().IsTooManyRequests());
    freePool(pair.remote, pair.target);
}

INSTANTIATE_TEST_SUITE_P(Protocols, MemFabricNetworkGva,
                         ::testing::Values("device_rdma", "device_urma"));

}  // namespace
}  // namespace mooncake::tent
