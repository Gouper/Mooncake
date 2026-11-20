// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_MEMFABRIC_GVA_TEST_API_H
#define TENT_MEMFABRIC_GVA_TEST_API_H

#include <array>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

namespace mooncake::tent::test {

struct FakeHybm {
    static constexpr uint64_t kSize = 2ULL << 20;
    bool loaded = false;
    bool blocked = false, entered = false, throw_batch = false;
    std::optional<uint64_t> blocked_source;
    bool reachable = true, stale_registry = false, empty_slice_export = false;
    bool fail_create = false;
    int bind_result = 0, initialize_result = 0, reserve_result = 0;
    int result = 0, wait_result = 0, waits = 0;
    int connect_result = 0, map_result = 0, remove_result = 0;
    int entity_export_result = 0, fail_slice_export_at = 0;
    int entity_import_result = 0, slice_import_result = 0;
    int translate_result = 0;
    int initializes = 0, creates = 0, frees = 0, unmaps = 0;
    int entity_exports = 0, slice_exports = 0, imports = 0, slice_imports = 0;
    int connects = 0;
    uint64_t alias_offset = 0;
    std::vector<size_t> batch_sizes;
    std::vector<std::array<uint64_t, 4>> copies;

    std::mutex mutex;
    std::condition_variable cv;
    void release();
    bool awaitEntry();
};

FakeHybm& fakeHybm();
// Call after destroying contexts and draining their workers.
void resetFakeHybm();

}  // namespace mooncake::tent::test

#endif  // TENT_MEMFABRIC_GVA_TEST_API_H
