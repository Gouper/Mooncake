#include <gtest/gtest.h>
#include "tent/transport/memfabric/memfabric_shared_memory.h"
using namespace mooncake::tent;
namespace {
constexpr uint64_t M = 2ULL << 20;
class FakeHybm : public MemFabricGvaApi {
   public:
    int creates = 0, frees = 0, imports = 0, unmaps = 0, slice_imports = 0;
    int slice_exports = 0;
    int initializes = 0, binds = 0, entity_exports = 0;
    bool fail_bind = false;
    bool fail_slice_export = false, empty_slice_export = false;
    uint32_t rank = 0;
    bool mismatch = false, fail_unmap = false, stale = false,
         fail_import = false, reachable = true;
    bool fail_entity_export = false, fail_entity_import = false;
    bool require_hbm = false;
    std::set<uint64_t> imported;
    std::map<uint64_t, uint64_t> addresses;
    int initialize(uint16_t) override {
        ++initializes;
        return 0;
    }
    void uninitialize() override {}
    int bind(uint16_t) override {
        ++binds;
        return fail_bind ? -1 : 0;
    }
    void* create(const MemFabricGvaConfig& c) override {
        rank = c.rank;
        ++creates;
        return this;
    }
    void destroy(void*) override {}
    int reserve(void*) override { return 0; }
    int unreserve(void*) override { return 0; }
    void* allocate(void*, uint32_t t, uint64_t) override {
        auto p = 0x10000000ULL + t * 0x1000000 + rank * M;
        addresses[p] = mismatch ? p : p + 0x100000000ULL;
        return reinterpret_cast<void*>(p);
    }
    int free(void*, void* p) override {
        ++frees;
        addresses.erase(reinterpret_cast<uint64_t>(p));
        return 0;
    }
    uint64_t address(void*, void* p) override {
        return reinterpret_cast<uint64_t>(p);
    }
    uint64_t size(void*, void*) override { return M; }
    int translate(uint64_t p, uint32_t, uint64_t& out) override {
        auto it = addresses.upper_bound(p);
        if (it == addresses.begin()) return -12;
        --it;
        if (p - it->first >= M) return -12;
        out = it->second + (p - it->first);
        return 0;
    }
    int exportBlob(void*, void* p, std::vector<uint8_t>& b) override {
        if (!p) ++entity_exports;
        if (!p && fail_entity_export) return -1;
        if (p) {
            ++slice_exports;
            if (fail_slice_export) return -1;
            if (empty_slice_export) return 0;
        }
        auto a = reinterpret_cast<uint64_t>(p);
        b.resize(8);
        std::memcpy(b.data(), &a, 8);
        return 0;
    }
    int importBlob(void*, bool entity, const std::vector<uint8_t>& b) override {
        ++imports;
        if (entity && fail_entity_import) return -1;
        if (!entity) {
            ++slice_imports;
            if (fail_import) return -1;
            uint64_t p;
            std::memcpy(&p, b.data(), 8);
            addresses[p] = p + 0x200000000ULL + rank * 0x100000000ULL;
            imported.insert(p);
        }
        return 0;
    }
    int map(void*) override { return 0; }
    int reach(void*, uint32_t peer, uint32_t& r) override {
        r = reachable &&
                    (!require_hbm || imported.count(0x11000000ULL + peer * M))
                ? 1
                : 4;
        return 0;
    }
    int remove(void*, uint32_t) override { return fail_unmap ? -1 : 0; }
    int unmap(void*, uint32_t) override {
        ++unmaps;
        if (fail_unmap) return -1;
        if (!stale) {
            for (auto p : imported) addresses.erase(p);
            imported.clear();
        }
        return 0;
    }
};
MemFabricGvaConfig config(uint32_t rank = 0) {
    MemFabricGvaConfig c;
    c.enabled = true;
    c.group = "test";
    c.rank_count = 2;
    c.rank = rank;
    c.device = 0;
    c.entity = 64;
    c.hbm_window = c.dram_window = c.hbm_size = c.dram_size = M;
    return c;
}
TEST(MemFabricGva, MissingApiRejectsAllocation) {
    MemFabricSharedMemory m(config(), "owner", nullptr);
    void* p = nullptr;
    EXPECT_FALSE(m.allocate(&p, M, 1, 0).ok());
    EXPECT_EQ(p, nullptr);
}
TEST(MemFabricGva, ExplicitlyDisabledDramAllowsHbmOnlyContext) {
    auto api = std::make_shared<FakeHybm>();
    auto options = config();
    options.dram_size = options.dram_window = 0;
    MemFabricSharedMemory memory(options, "owner", api);
    void* address = nullptr;
    EXPECT_TRUE(memory.allocate(&address, M, 1, 0).ok());
    void* dram = nullptr;
    EXPECT_FALSE(memory.allocate(&dram, M, 2, 0).ok());
    EXPECT_TRUE(memory.free(address, M).ok());
}
TEST(MemFabricGva, DisabledTypeMustHaveZeroWindowAndZeroLocalSize) {
    auto api = std::make_shared<FakeHybm>();
    auto options = config();
    options.dram_size = 0;
    MemFabricSharedMemory memory(options, "owner", api);
    void* address = nullptr;
    EXPECT_FALSE(memory.allocate(&address, M, 1, 0).ok());
    EXPECT_EQ(api->creates, 0);
}
TEST(MemFabricGva, IdentityDeviceAddressAndWrongLogicalRequestAreRejected) {
    auto a = std::make_shared<FakeHybm>();
    MemFabricSharedMemory m(config(), "owner", a);
    void* p = nullptr;
    a->mismatch = true;
    EXPECT_FALSE(m.allocate(&p, M, 1, 0).ok());
    EXPECT_EQ(a->frees, 1);
    a->mismatch = false;
    EXPECT_FALSE(m.allocate(&p, M, 2, 42).ok());
    EXPECT_EQ(a->frees, 2);
    EXPECT_EQ(p, nullptr);
}
TEST(MemFabricGva, BothPoolsShareEntityAndFailedSecondPreservesFirst) {
    auto a = std::make_shared<FakeHybm>();
    MemFabricSharedMemory m(config(), "owner", a);
    void* p = nullptr;
    ASSERT_TRUE(m.allocate(&p, M, 1, 0).ok());
    void* q = nullptr;
    a->mismatch = true;
    EXPECT_FALSE(m.allocate(&q, M, 2, 0).ok());
    EXPECT_EQ(a->creates, 1);
    MemoryOptions o;
    EXPECT_TRUE(m.describe(p, M, o));
    EXPECT_EQ(o.global_address, reinterpret_cast<uint64_t>(p));
    EXPECT_TRUE(m.free(p, M).ok());
}
TEST(MemFabricGva, PublicationPreventsFreeAndDescriptorIsVersioned) {
    auto a = std::make_shared<FakeHybm>();
    MemFabricSharedMemory m(config(), "owner", a);
    void* p = nullptr;
    ASSERT_TRUE(m.allocate(&p, M, 1, 0).ok());
    std::string desc;
    ASSERT_TRUE(m.publish(p, M, desc).ok());
    EXPECT_NE(desc.find("\"version\":2"), std::string::npos);
    EXPECT_FALSE(m.free(p, M).ok());
    EXPECT_TRUE(m.unpublish(p).ok());
    EXPECT_TRUE(m.free(p, M).ok());
}

TEST(MemFabricGva, MalformedDescriptorDoesNotImport) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    EXPECT_FALSE(memory.map("peer", "{}", 0x11000000, M).ok());
    EXPECT_EQ(api->imports, 0);
}
struct Pair {
    std::shared_ptr<FakeHybm> aa = std::make_shared<FakeHybm>(),
                              bb = std::make_shared<FakeHybm>();
    MemFabricSharedMemory a{config(0), "a", aa}, b{config(1), "b", bb};
    void *ah = nullptr, *ad = nullptr, *bh = nullptr, *bd = nullptr;
    std::string h, d;
    Pair() {
        EXPECT_TRUE(a.allocate(&ah, M, 1, 0).ok());
        EXPECT_TRUE(a.allocate(&ad, M, 2, 0).ok());
        EXPECT_TRUE(b.allocate(&bh, M, 1, 0).ok());
        EXPECT_TRUE(b.allocate(&bd, M, 2, 0).ok());
        EXPECT_TRUE(b.publish(bh, M, h).ok());
        EXPECT_TRUE(b.publish(bd, M, d).ok());
    }
    ~Pair() {
        // Even unsuccessful map attempts may own native imports.
        aa->fail_unmap = aa->stale = false;
        bb->fail_unmap = bb->stale = false;
        a.unmap("b", reinterpret_cast<uint64_t>(bh));
        a.unmap("b", reinterpret_cast<uint64_t>(bd));
        b.unmap("a", reinterpret_cast<uint64_t>(ah));
        b.unmap("a", reinterpret_cast<uint64_t>(ad));
        a.unpublish(ah);
        a.unpublish(ad);
        b.unpublish(bh);
        b.unpublish(bd);
        a.free(ah, M);
        a.free(ad, M);
        b.free(bh, M);
        b.free(bd, M);
    }
    std::string bundle() const {
        return nlohmann::json::array(
                   {nlohmann::json::parse(h), nlohmann::json::parse(d)})
            .dump();
    }
    Status mapH() {
        return a.map("b", bundle(), reinterpret_cast<uint64_t>(bh), M);
    }
    Status unmapH() { return a.unmap("b", reinterpret_cast<uint64_t>(bh)); }
};
TEST(MemFabricGva, DualPoolMapIsIdempotentAndRankUnmapIsDeferred) {
    Pair p;
    ASSERT_TRUE(p.mapH().ok());
    auto count = p.aa->slice_imports;
    ASSERT_TRUE(p.mapH().ok());
    EXPECT_EQ(p.aa->slice_imports, count);
    ASSERT_TRUE(
        p.a.map("b", p.bundle(), reinterpret_cast<uint64_t>(p.bd), M).ok());
    EXPECT_TRUE(p.unmapH().IsTooManyRequests());
    EXPECT_EQ(p.aa->unmaps, 0);
    EXPECT_TRUE(p.mapH().ok());
    EXPECT_TRUE(p.unmapH().IsTooManyRequests());
    EXPECT_TRUE(p.a.unmap("b", reinterpret_cast<uint64_t>(p.bd)).ok());
    EXPECT_EQ(p.aa->unmaps, 1);
    EXPECT_TRUE(p.unmapH().ok());
    EXPECT_EQ(p.aa->unmaps, 1);
}
TEST(MemFabricGva, NativeCleanupFailureRetainsLocalResourcesUntilRetry) {
    Pair p;
    ASSERT_TRUE(p.mapH().ok());
    p.aa->fail_unmap = true;
    EXPECT_TRUE(p.unmapH().IsTooManyRequests());
    EXPECT_FALSE(p.a.free(p.ah, M).ok());
    p.aa->fail_unmap = false;
    EXPECT_TRUE(p.unmapH().ok());
    EXPECT_TRUE(p.a.free(p.ah, M).ok());
}
TEST(MemFabricGva, RegistryRemainingAfterUnmapIsPending) {
    Pair p;
    ASSERT_TRUE(p.mapH().ok());
    p.aa->stale = true;
    EXPECT_TRUE(p.unmapH().IsTooManyRequests());
    p.aa->stale = false;
    EXPECT_TRUE(p.unmapH().ok());
}
TEST(MemFabricGva, CopyReachabilityDoesNotAuthorizeDirectMap) {
    Pair p;
    p.aa->reachable = false;
    EXPECT_FALSE(p.mapH().ok());
}
TEST(MemFabricGva, PeerBecomingReadyCanRetryWithoutReimportingSlices) {
    Pair p;
    p.aa->reachable = false;
    EXPECT_FALSE(p.mapH().ok());
    EXPECT_EQ(p.aa->slice_imports, 2);
    p.aa->reachable = true;
    ASSERT_TRUE(p.mapH().ok());
    EXPECT_EQ(p.aa->slice_imports, 2);
}
TEST(MemFabricGva, FailedImportCanBeRetriedWithoutPretendingItSucceeded) {
    Pair p;
    p.aa->fail_import = true;
    EXPECT_FALSE(p.mapH().ok());
    p.aa->fail_import = false;
    EXPECT_TRUE(p.mapH().ok());
    EXPECT_EQ(p.aa->slice_imports, 3);
}
TEST(MemFabricGva, BidirectionalMappingsAndCleanupAreLocalOperations) {
    Pair p;
    ASSERT_TRUE(p.mapH().ok());
    EXPECT_EQ(p.bb->imports, 0);
    std::string ah, ad;
    ASSERT_TRUE(p.a.publish(p.ah, M, ah).ok());
    ASSERT_TRUE(p.a.publish(p.ad, M, ad).ok());
    auto bundle = nlohmann::json::array(
                      {nlohmann::json::parse(ah), nlohmann::json::parse(ad)})
                      .dump();
    ASSERT_TRUE(p.b.map("a", bundle, reinterpret_cast<uint64_t>(p.ah), M).ok());
    // The application stops both kernels before either local unmap.
    EXPECT_TRUE(p.unmapH().ok());
    EXPECT_EQ(p.bb->unmaps, 0);
    EXPECT_TRUE(p.b.unmap("a", reinterpret_cast<uint64_t>(p.ah)).ok());
    // Application barrier precedes unpublish/free, no owner RPC acknowledgment.
    EXPECT_TRUE(p.a.unpublish(p.ah).ok());
    EXPECT_TRUE(p.b.unpublish(p.bh).ok());
    EXPECT_TRUE(p.a.free(p.ah, M).ok());
    EXPECT_TRUE(p.b.free(p.bh, M).ok());
}
TEST(MemFabricGva, EntityExportFailureCanBeRetriedOnTheSameEntity) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    void* address = nullptr;
    api->fail_entity_export = true;
    EXPECT_FALSE(memory.allocate(&address, M, 1, 0).ok());
    api->fail_entity_export = false;
    EXPECT_TRUE(memory.allocate(&address, M, 1, 0).ok());
    EXPECT_EQ(api->creates, 1);
    EXPECT_TRUE(memory.free(address, M).ok());
}
TEST(MemFabricGva, FailedFixedAddressDoesNotKeepConsumingNativeCapacity) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    void* address = nullptr;
    EXPECT_FALSE(memory.allocate(&address, M, 1, 42).ok());
    EXPECT_FALSE(memory.allocate(&address, M, 1, 0).ok());
    EXPECT_EQ(api->frees, 1);
}
TEST(MemFabricGva, MalformedIdentityAndBinaryDataNeverReachNativeImport) {
    Pair p;
    auto descriptor = nlohmann::json::parse(p.h);
    descriptor["slice_blob"] = {256u};
    EXPECT_FALSE(
        p.a.map("b", descriptor.dump(), reinterpret_cast<uint64_t>(p.bh), M)
            .ok());
    descriptor = nlohmann::json::parse(p.h);
    descriptor["rank"] = 9u;
    EXPECT_FALSE(
        p.a.map("b", descriptor.dump(), reinterpret_cast<uint64_t>(p.bh), M)
            .ok());
    EXPECT_EQ(p.aa->imports, 0);
}
TEST(MemFabricGva, InvalidCompanionPoolIsRejectedBeforeAnyImport) {
    Pair p;
    auto bundle = nlohmann::json::parse(p.bundle());
    bundle[1]["group"] = "different-group";
    EXPECT_FALSE(
        p.a.map("b", bundle.dump(), reinterpret_cast<uint64_t>(p.bh), M).ok());
    EXPECT_EQ(p.aa->imports, 0);
}
TEST(MemFabricGva, EntityImportFailureStillHasAnAddressableCleanupRecord) {
    Pair p;
    p.aa->fail_entity_import = true;
    EXPECT_FALSE(p.mapH().ok());
    p.aa->fail_entity_import = false;
    EXPECT_TRUE(p.unmapH().ok());
    EXPECT_TRUE(p.a.free(p.ah, M).ok());
}
TEST(MemFabricGva, DramFirstImportsBothPoolsButOnlyActivatesRequestedPool) {
    Pair p;
    p.aa->require_hbm = true;
    ASSERT_TRUE(
        p.a.map("b", p.bundle(), reinterpret_cast<uint64_t>(p.bd), M).ok());
    EXPECT_EQ(p.aa->slice_imports, 2);
    EXPECT_EQ(p.bb->imports, 0);
    EXPECT_TRUE(p.a.unmap("b", reinterpret_cast<uint64_t>(p.bd)).ok());
    EXPECT_EQ(p.aa->unmaps, 1);
}
}  // namespace

TEST(MemFabricGva, RegistrationExportsSliceLazilyAndCachesSuccess) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    void* address = nullptr;
    api->fail_slice_export = true;
    ASSERT_TRUE(memory.allocate(&address, M, 1, 0).ok());
    EXPECT_EQ(api->slice_exports, 0);
    std::string descriptor;
    EXPECT_FALSE(memory.publish(address, M, descriptor).ok());
    EXPECT_TRUE(descriptor.empty());
    EXPECT_EQ(api->frees, 0);
    api->fail_slice_export = false;
    ASSERT_TRUE(memory.publish(address, M, descriptor).ok());
    EXPECT_FALSE(nlohmann::json::parse(descriptor).at("slice_blob").empty());
    EXPECT_EQ(api->slice_exports, 2);
    ASSERT_TRUE(memory.unpublish(address).ok());
    std::string repeated;
    ASSERT_TRUE(memory.publish(address, M, repeated).ok());
    EXPECT_EQ(repeated, descriptor);
    EXPECT_EQ(api->slice_exports, 2);
    ASSERT_TRUE(memory.unpublish(address).ok());
    EXPECT_TRUE(memory.free(address, M).ok());
}
TEST(MemFabricGva, FailedRegistrationLeavesAllocationFreeable) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    void* address = nullptr;
    ASSERT_TRUE(memory.allocate(&address, M, 1, 0).ok());
    api->fail_slice_export = true;
    std::string descriptor;
    EXPECT_FALSE(memory.publish(address, M, descriptor).ok());
    EXPECT_TRUE(memory.free(address, M).ok());
    EXPECT_EQ(api->frees, 1);
}
TEST(MemFabricGva, EmptyExportCannotPublishAndCanBeRetried) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    void* address = nullptr;
    ASSERT_TRUE(memory.allocate(&address, M, 1, 0).ok());
    api->empty_slice_export = true;
    std::string descriptor;
    EXPECT_FALSE(memory.publish(address, M, descriptor).ok());
    api->empty_slice_export = false;
    ASSERT_TRUE(memory.publish(address, M, descriptor).ok());
    EXPECT_EQ(api->slice_exports, 2);
    ASSERT_TRUE(memory.unpublish(address).ok());
    EXPECT_TRUE(memory.free(address, M).ok());
}

TEST(MemFabricGva, FixedLogicalAddressResolvesToDistinctDeviceAddress) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    void* gva = nullptr;
    constexpr uint64_t requested = 0x11000000ULL;
    ASSERT_TRUE(memory.allocate(&gva, M, 1, requested).ok());
    EXPECT_EQ(reinterpret_cast<uint64_t>(gva), requested);
    void* dva = nullptr;
    ASSERT_TRUE(memory.resolve(requested, M, &dva).ok());
    EXPECT_EQ(reinterpret_cast<uint64_t>(dva), requested + 0x100000000ULL);
    ASSERT_TRUE(memory.resolve(requested + 64, 32, &dva).ok());
    EXPECT_EQ(reinterpret_cast<uint64_t>(dva), requested + 0x100000040ULL);
    EXPECT_FALSE(memory.resolve(requested + M - 1, 2, &dva).ok());
    EXPECT_EQ(dva, nullptr);
    EXPECT_FALSE(memory.resolve(requested, 0, &dva).ok());
    EXPECT_FALSE(memory.resolve(UINT64_MAX - 3, 8, &dva).ok());
    EXPECT_FALSE(memory.resolve(requested, 1, nullptr).ok());
    ASSERT_TRUE(memory.free(gva, M).ok());
    EXPECT_FALSE(memory.resolve(requested, 1, &dva).ok());
}
TEST(MemFabricGva, RemoteResolutionUsesCallerDeviceVaAndRequiresActiveMap) {
    Pair p;
    const auto gva = reinterpret_cast<uint64_t>(p.bh);
    void *owner_dva = nullptr, *remote_dva = nullptr;
    ASSERT_TRUE(p.b.resolve(gva, M, &owner_dva).ok());
    EXPECT_FALSE(p.a.resolve(gva, M, &remote_dva).ok());
    ASSERT_TRUE(p.mapH().ok());
    ASSERT_TRUE(p.a.resolve(gva, M, &remote_dva).ok());
    EXPECT_NE(remote_dva, p.bh);
    EXPECT_NE(remote_dva, owner_dva);
    EXPECT_EQ(reinterpret_cast<uint64_t>(remote_dva), gva + 0x200000000ULL);
    ASSERT_TRUE(p.a.resolve(gva + 123, 50, &remote_dva).ok());
    EXPECT_EQ(reinterpret_cast<uint64_t>(remote_dva),
              gva + 0x200000000ULL + 123);
    // Importing a companion pool does not grant logical access to it.
    EXPECT_FALSE(
        p.a.resolve(reinterpret_cast<uint64_t>(p.bd), M, &remote_dva).ok());
    ASSERT_TRUE(p.unmapH().ok());
    EXPECT_FALSE(p.a.resolve(gva, M, &remote_dva).ok());
    EXPECT_EQ(remote_dva, nullptr);
}
TEST(MemFabricGva, FailedMappingNeverExposesAnImportedDeviceAddress) {
    Pair p;
    p.aa->reachable = false;
    EXPECT_FALSE(p.mapH().ok());
    void* dva = nullptr;
    EXPECT_FALSE(p.a.resolve(reinterpret_cast<uint64_t>(p.bh), M, &dva).ok());
    EXPECT_EQ(dva, nullptr);
    p.aa->reachable = true;
    ASSERT_TRUE(p.mapH().ok());
    EXPECT_TRUE(p.a.resolve(reinterpret_cast<uint64_t>(p.bh), M, &dva).ok());
}
TEST(MemFabricGva, LogicalUnmapRevokesResolutionBeforeRankCleanup) {
    Pair p;
    ASSERT_TRUE(p.mapH().ok());
    ASSERT_TRUE(
        p.a.map("b", p.bundle(), reinterpret_cast<uint64_t>(p.bd), M).ok());
    EXPECT_TRUE(p.unmapH().IsTooManyRequests());
    void* dva = nullptr;
    EXPECT_FALSE(p.a.resolve(reinterpret_cast<uint64_t>(p.bh), M, &dva).ok());
    EXPECT_TRUE(p.a.resolve(reinterpret_cast<uint64_t>(p.bd), M, &dva).ok());
}
TEST(MemFabricGva, LegacyIdentityAddressMetadataIsRejectedBeforeImport) {
    Pair p;
    auto descriptors = nlohmann::json::parse(p.bundle());
    descriptors[0]["version"] = 1u;
    EXPECT_FALSE(
        p.a.map("b", descriptors.dump(), reinterpret_cast<uint64_t>(p.bh), M)
            .ok());
    EXPECT_EQ(p.aa->imports, 0);
    descriptors = nlohmann::json::parse(p.bundle());
    descriptors[0]["address_mode"] = "identity";
    EXPECT_FALSE(
        p.a.map("b", descriptors.dump(), reinterpret_cast<uint64_t>(p.bh), M)
            .ok());
    EXPECT_EQ(p.aa->imports, 0);
}

TEST(MemFabricGva, DeviceAliasesRemainProtectedUntilPhysicalCleanup) {
    Pair p;
    void* local_dva = nullptr;
    ASSERT_TRUE(
        p.a.resolve(reinterpret_cast<uint64_t>(p.ah), M, &local_dva).ok());
    EXPECT_TRUE(p.a.overlaps(reinterpret_cast<uint64_t>(local_dva), M));
    ASSERT_TRUE(p.mapH().ok());
    void* remote_dva = nullptr;
    ASSERT_TRUE(
        p.a.resolve(reinterpret_cast<uint64_t>(p.bh), M, &remote_dva).ok());
    EXPECT_TRUE(p.a.overlaps(reinterpret_cast<uint64_t>(remote_dva), M));
    ASSERT_TRUE(
        p.a.map("b", p.bundle(), reinterpret_cast<uint64_t>(p.bd), M).ok());
    EXPECT_TRUE(p.unmapH().IsTooManyRequests());
    EXPECT_TRUE(p.a.overlaps(reinterpret_cast<uint64_t>(remote_dva), M));
    ASSERT_TRUE(p.a.unmap("b", reinterpret_cast<uint64_t>(p.bd)).ok());
    EXPECT_FALSE(p.a.overlaps(reinterpret_cast<uint64_t>(remote_dva), M));
}

TEST(MemFabricGva, QueuedCopyPinsPublishedPoolsAndMappedPeerUntilCancelled) {
    Pair p;
    std::string local;
    ASSERT_TRUE(p.a.publish(p.ah, M, local).ok());
    ASSERT_TRUE(p.mapH().ok());
    uint32_t direction = 0;
    ASSERT_TRUE(p.a.beginCopy(reinterpret_cast<uint64_t>(p.ah),
                              reinterpret_cast<uint64_t>(p.bh), 64, direction)
                    .ok());
    EXPECT_EQ(direction, 9u);
    EXPECT_FALSE(p.a.unpublish(p.ah).ok());
    EXPECT_FALSE(p.unmapH().ok());
    p.a.cancelCopy();
    EXPECT_TRUE(p.a.unpublish(p.ah).ok());
    EXPECT_TRUE(p.unmapH().ok());
}
TEST(MemFabricGva, CopyRejectsUnpublishedLogicalPoolAndDeviceAlias) {
    Pair p;
    ASSERT_TRUE(p.mapH().ok());
    uint32_t direction = 0;
    EXPECT_FALSE(p.a.beginCopy(reinterpret_cast<uint64_t>(p.ah),
                               reinterpret_cast<uint64_t>(p.bh), 64, direction)
                     .ok());
    std::string local;
    ASSERT_TRUE(p.a.publish(p.ah, M, local).ok());
    void* dva = nullptr;
    ASSERT_TRUE(p.a.resolve(reinterpret_cast<uint64_t>(p.ah), 64, &dva).ok());
    EXPECT_FALSE(p.a.beginCopy(reinterpret_cast<uint64_t>(dva),
                               reinterpret_cast<uint64_t>(p.bh), 64, direction)
                     .ok());
    EXPECT_FALSE(p.a.beginCopy(reinterpret_cast<uint64_t>(p.ah) + M - 1,
                               reinterpret_cast<uint64_t>(p.bh), 64, direction)
                     .ok());
    EXPECT_TRUE(p.unmapH().ok());
}

TEST(MemFabricGva, CopyTargetRejectsChangedGenerationAtReusedLogicalAddress) {
    Pair p;
    ASSERT_TRUE(p.mapH().ok());
    EXPECT_TRUE(p.a.validateCopyTarget("b", p.h).ok());
    auto changed = nlohmann::json::parse(p.h);
    changed["generation"] = "restarted-owner";
    EXPECT_FALSE(p.a.validateCopyTarget("b", changed.dump()).ok());
    EXPECT_FALSE(p.a.validateCopyTarget("different-owner", p.h).ok());
    ASSERT_TRUE(p.unmapH().ok());
    EXPECT_FALSE(p.a.validateCopyTarget("b", p.h).ok());
}

namespace {
class NetworkHybm : public FakeHybm {
   public:
    uint32_t protocol_bit = 4;
    int connect_calls = 0, copy_calls = 0, wait_calls = 0;
    int connect_result = 0, copy_result = 0;
    int translate(uint64_t address, uint32_t type, uint64_t& out) override {
        for (auto base : imported)
            if (address >= base && address - base < M) return -1;
        return FakeHybm::translate(address, type, out);
    }
    bool connect_available = true;
    bool supportsConnect() const override { return connect_available; }
    int connect(void*, uint32_t) override {
        ++connect_calls;
        return connect_result;
    }
    int reach(void*, uint32_t, uint32_t& result) override {
        result = protocol_bit;
        return 0;
    }
    int copy(void*, uint64_t, uint64_t, size_t, uint32_t) override {
        ++copy_calls;
        return copy_result;
    }
    int wait(void*) override {
        ++wait_calls;
        return 0;
    }
};
class MemFabricNetworkGva : public ::testing::TestWithParam<const char*> {
   protected:
    void SetUp() override {
        auto local_config = config();
        local_config.protocol = GetParam();
        local_config.trans_url = "tcp://127.0.0.1:19001";
        auto remote_config = local_config;
        remote_config.rank = 1;
        remote_config.trans_url = "tcp://127.0.0.1:19002";
        local_api = std::make_shared<NetworkHybm>();
        remote_api = std::make_shared<NetworkHybm>();
        local_api->protocol_bit =
            std::string(GetParam()) == "device_urma" ? 256 : 4;
        remote_api->protocol_bit = local_api->protocol_bit;
        local = std::make_unique<MemFabricSharedMemory>(local_config, "local",
                                                        local_api);
        remote = std::make_unique<MemFabricSharedMemory>(remote_config,
                                                         "remote", remote_api);
        ASSERT_TRUE(local->allocate(&source, M, 1, 0).ok());
        ASSERT_TRUE(remote->allocate(&target, M, 1, 0).ok());
        ASSERT_TRUE(local->publish(source, M, local_desc).ok());
        ASSERT_TRUE(remote->publish(target, M, remote_desc).ok());
    }
    void TearDown() override {
        local->unmap("remote", reinterpret_cast<uint64_t>(target));
        local->unpublish(source);
        remote->unpublish(target);
        local->free(source, M);
        remote->free(target, M);
    }
    Status prepare() {
        return local->prepare("remote", remote_desc,
                              reinterpret_cast<uint64_t>(target), M);
    }
    Status begin(uint32_t& direction) {
        return local->beginCopy(reinterpret_cast<uint64_t>(source),
                                reinterpret_cast<uint64_t>(target), M,
                                direction);
    }
    std::shared_ptr<NetworkHybm> local_api, remote_api;
    std::unique_ptr<MemFabricSharedMemory> local, remote;
    void *source = nullptr, *target = nullptr;
    std::string local_desc, remote_desc;
};
TEST_P(MemFabricNetworkGva, MissingConnectSymbolFailsBeforeImport) {
    local_api->connect_available = false;
    EXPECT_TRUE(prepare().IsNotImplemented());
    EXPECT_EQ(local_api->imports, 0);
    EXPECT_EQ(local_api->connect_calls, 0);
}
TEST_P(MemFabricNetworkGva, NetworkCopyDoesNotRequireRemoteDeviceAddress) {
    uint32_t direction = 0;
    EXPECT_FALSE(begin(direction).ok());
    ASSERT_TRUE(prepare().ok());
    EXPECT_GE(local_api->connect_calls, 1);
    void* device_address = reinterpret_cast<void*>(1);
    EXPECT_FALSE(
        local->resolve(reinterpret_cast<uint64_t>(target), M, &device_address)
            .ok());
    EXPECT_EQ(device_address, nullptr);
    ASSERT_TRUE(local->validateCopyTarget("remote", remote_desc).ok());
    ASSERT_TRUE(begin(direction).ok());
    EXPECT_EQ(direction, 9u);
    ASSERT_TRUE(local
                    ->runCopy(reinterpret_cast<uint64_t>(source),
                              reinterpret_cast<uint64_t>(target), M, direction)
                    .ok());
    EXPECT_EQ(local_api->copy_calls, 1);
    EXPECT_FALSE(
        local->map("remote", remote_desc, reinterpret_cast<uint64_t>(target), M)
            .ok());
}
TEST_P(MemFabricNetworkGva, FailedConnectCannotAuthorizeCopyAndCanRetry) {
    local_api->connect_result = -1;
    EXPECT_FALSE(prepare().ok());
    uint32_t direction = 0;
    EXPECT_FALSE(begin(direction).ok());
    local_api->connect_result = 0;
    ASSERT_TRUE(prepare().ok());
    ASSERT_TRUE(begin(direction).ok());
    local->cancelCopy();
}
TEST_P(MemFabricNetworkGva, WrongReachabilityProtocolCannotAuthorizeCopy) {
    local_api->protocol_bit = 1;
    EXPECT_FALSE(prepare().ok());
    uint32_t direction = 0;
    EXPECT_FALSE(begin(direction).ok());
}
TEST_P(MemFabricNetworkGva, FailedNetworkCopyQuarantinesDespiteWaitSuccess) {
    ASSERT_TRUE(prepare().ok());
    uint32_t direction = 0;
    ASSERT_TRUE(begin(direction).ok());
    local_api->copy_result = -1;
    EXPECT_FALSE(local
                     ->runCopy(reinterpret_cast<uint64_t>(source),
                               reinterpret_cast<uint64_t>(target), M, direction)
                     .ok());
    EXPECT_FALSE(begin(direction).ok());
    EXPECT_FALSE(
        local->unmap("remote", reinterpret_cast<uint64_t>(target)).ok());
    EXPECT_FALSE(local->unpublish(source).ok());
    EXPECT_FALSE(local->close().ok());
}
TEST_P(MemFabricNetworkGva, GenerationChangeInvalidatesPreparedCopyTarget) {
    ASSERT_TRUE(prepare().ok());
    auto descriptor = nlohmann::json::parse(remote_desc);
    descriptor["generation"] = "new-owner-instance";
    EXPECT_FALSE(local->validateCopyTarget("remote", descriptor.dump()).ok());
}
INSTANTIATE_TEST_SUITE_P(Protocols, MemFabricNetworkGva,
                         ::testing::Values("device_rdma", "device_urma"));
}  // namespace

TEST(MemFabricGva, UnsupportedNetworkProtocolRejectsAllocation) {
    auto options = config();
    options.protocol = "unknown_protocol";
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(options, "owner", api);
    void* address = nullptr;
    EXPECT_FALSE(memory.allocate(&address, M, 1, 0).ok());
    EXPECT_EQ(address, nullptr);
    EXPECT_EQ(api->creates, 0);
}

TEST(MemFabricGva, ExistingContextOnlyRebindsDeviceAndRetriesBindFailure) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    void* address = nullptr;
    ASSERT_TRUE(memory.allocate(&address, M, 1, 0).ok());
    EXPECT_EQ(api->initializes, 1);
    EXPECT_EQ(api->entity_exports, 1);
    api->fail_bind = true;
    std::string descriptor;
    EXPECT_FALSE(memory.publish(address, M, descriptor).ok());
    EXPECT_EQ(api->slice_exports, 0);
    api->fail_bind = false;
    ASSERT_TRUE(memory.publish(address, M, descriptor).ok());
    EXPECT_EQ(api->binds, 3);
    EXPECT_EQ(api->initializes, 1);
    EXPECT_EQ(api->entity_exports, 1);
    ASSERT_TRUE(memory.unpublish(address).ok());
    ASSERT_TRUE(memory.free(address, M).ok());
    EXPECT_EQ(api->initializes, 1);
    EXPECT_EQ(api->entity_exports, 1);
    ASSERT_TRUE(memory.close().ok());
}
TEST(MemFabricGva, ClosedContextRecreatesEntityAndExportsFreshIdentity) {
    auto api = std::make_shared<FakeHybm>();
    MemFabricSharedMemory memory(config(), "owner", api);
    void* address = nullptr;
    ASSERT_TRUE(memory.allocate(&address, M, 1, 0).ok());
    ASSERT_TRUE(memory.free(address, M).ok());
    ASSERT_TRUE(memory.close().ok());
    ASSERT_TRUE(memory.allocate(&address, M, 2, 0).ok());
    EXPECT_EQ(api->initializes, 2);
    EXPECT_EQ(api->creates, 2);
    EXPECT_EQ(api->entity_exports, 2);
    ASSERT_TRUE(memory.free(address, M).ok());
    ASSERT_TRUE(memory.close().ok());
}
