// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "tent/transport/memfabric/memfabric_library.h"

using mooncake::tent::MemFabricLibrary;
namespace {
void countClose(void* context) { ++*static_cast<int*>(context); }
using Value = int (*)();
using OnClose = void (*)(void (*)(void*), void*);

TEST(MemFabricLibrary, SharesOwnershipUntilLastFunctionTableReleases) {
    int closed = 0;
    auto trans = MemFabricLibrary::load(MEMFABRIC_TEST_LIBRARY_PATH);
    ASSERT_NE(trans, nullptr);
    auto gva = MemFabricLibrary::load(MEMFABRIC_TEST_LIBRARY_PATH);
    ASSERT_EQ(gva, trans);
    auto on_close = trans->symbol<OnClose>("memfabric_fixture_on_close");
    ASSERT_NE(on_close, nullptr);
    on_close(countClose, &closed);
    auto value = trans->symbol<Value>("memfabric_fixture_value");
    ASSERT_NE(value, nullptr);
    // MemoryApi retains this alias across transport CleanupLibrary/uninstall.
    std::shared_ptr<void> allocation(trans, trans->handle());
    std::weak_ptr<MemFabricLibrary> weak = trans;
    trans.reset();
    gva.reset();
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(closed, 0);
    EXPECT_EQ(value(), 42);
    allocation.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(closed, 1);
    // An expired weak cache entry must reopen the library, not revive a stale
    // function table. The fixture's static state is freshly initialized.
    auto reopened = MemFabricLibrary::load(MEMFABRIC_TEST_LIBRARY_PATH);
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(reopened->symbol<Value>("memfabric_fixture_value")(), 42);
    reopened.reset();
    EXPECT_EQ(closed, 1);
}

TEST(MemFabricLibrary, MissingOptionalSymbolsAndOtherLibrariesAreIndependent) {
    auto trans = MemFabricLibrary::load(MEMFABRIC_TEST_LIBRARY_PATH,
                                        RTLD_NOW | RTLD_GLOBAL);
    ASSERT_NE(trans, nullptr);
    EXPECT_EQ(trans->symbol<Value>("hybm_optional_symbol_not_present"),
              nullptr);
    std::string error;
    EXPECT_EQ(MemFabricLibrary::load("libmooncake_missing_fixture.so",
                                     RTLD_NOW | RTLD_LOCAL, &error),
              nullptr);
    EXPECT_FALSE(error.empty());
    EXPECT_EQ(trans->symbol<Value>("memfabric_fixture_value")(), 42);
    EXPECT_EQ(trans, MemFabricLibrary::load(MEMFABRIC_TEST_LIBRARY_PATH,
                                            RTLD_NOW | RTLD_GLOBAL));
}

TEST(MemFabricLibrary, ConcurrentLoadsReuseTheSameOwner) {
    constexpr size_t count = 12;
    std::vector<std::shared_ptr<MemFabricLibrary>> owners(count);
    std::vector<std::thread> threads;
    std::atomic<bool> start{false};
    for (size_t index = 0; index < count; ++index) {
        threads.emplace_back([&, index] {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            owners[index] = MemFabricLibrary::load(MEMFABRIC_TEST_LIBRARY_PATH);
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();
    ASSERT_NE(owners.front(), nullptr);
    for (const auto& owner : owners) EXPECT_EQ(owner, owners.front());
}
}  // namespace
