// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "memfabric_gva_test_api.h"

#include <chrono>
#include <cstring>
#include <map>
#include <memory>
#include <stdexcept>

#include "tent/transport/memfabric/memfabric_under_api.h"

namespace mooncake::tent {
namespace {

struct Slice {
    uint64_t size;
    bool imported;
};

struct Entity {
    MemFabricGvaConfig config;
    uint64_t serial;
    uint64_t cursor = 0;
    std::map<uint64_t, Slice> slices;
};

auto state = std::make_unique<test::FakeHybm>();
std::map<uint16_t, std::unique_ptr<Entity>> entities;
thread_local uint16_t device;

Entity& entity(void* handle) { return *static_cast<Entity*>(handle); }

}  // namespace

namespace test {

FakeHybm& fakeHybm() { return *state; }

void resetFakeHybm() {
    entities.clear();
    state = std::make_unique<FakeHybm>();
    device = 0;
}

void FakeHybm::release() {
    std::lock_guard lock(mutex);
    blocked = false;
    cv.notify_all();
}

bool FakeHybm::awaitEntry() {
    std::unique_lock lock(mutex);
    return cv.wait_for(lock, std::chrono::seconds(2), [&] { return entered; });
}

}  // namespace test

bool MemFabricGvaApi::LoadLibrary() { return state->loaded; }

bool MemFabricGvaApi::supportsCopy() { return true; }
int MemFabricGvaApi::copy(void*, uint64_t, uint64_t, size_t, uint32_t) {
    return state->result;
}

bool MemFabricGvaApi::supportsBatchCopy() { return state->batch_available; }

int MemFabricGvaApi::batchCopy(void*,
                               const std::vector<MemFabricGvaCopy>& copies,
                               uint32_t direction) {
    std::unique_lock lock(state->mutex);
    state->batch_sizes.push_back(copies.size());
    for (const auto& copy : copies) {
        state->copies.push_back(
            {copy.source, copy.destination, copy.size, direction});
    }
    state->entered = true;
    state->cv.notify_all();
    state->cv.wait(lock, [] { return !state->blocked; });
    if (state->throw_batch) throw std::runtime_error("mock HYBM batch error");
    return state->result;
}

int MemFabricGvaApi::wait(void*) {
    std::lock_guard lock(state->mutex);
    ++state->waits;
    return state->wait_result;
}

int MemFabricGvaApi::bind(uint16_t next_device) {
    ++state->binds;
    device = next_device;
    return state->bind_result;
}

int MemFabricGvaApi::initialize(uint16_t) {
    ++state->initializes;
    return state->initialize_result;
}

void MemFabricGvaApi::uninitialize() {}

void* MemFabricGvaApi::create(const MemFabricGvaConfig& config) {
    if (state->fail_create) return nullptr;
    auto created = std::make_unique<Entity>();
    created->config = config;
    created->serial = ++state->creates;
    return entities.emplace(config.device, std::move(created))
        .first->second.get();
}

void MemFabricGvaApi::destroy(void* handle) {
    const auto id = entity(handle).config.device;
    entities.erase(id);
}

int MemFabricGvaApi::reserve(void*) { return state->reserve_result; }
int MemFabricGvaApi::unreserve(void*) { return 0; }

void* MemFabricGvaApi::allocate(void* handle, uint32_t type, uint64_t bytes) {
    if (type != kGvaMemoryTypeDevice) return nullptr;
    auto& local = entity(handle);
    const auto addr = base(handle, type) +
                      local.config.rank * local.config.hbm_window +
                      local.cursor;
    local.cursor += bytes;
    local.slices.emplace(addr, Slice{bytes, false});
    return reinterpret_cast<void*>(addr);
}

int MemFabricGvaApi::free(void* handle, void* slice) {
    ++state->frees;
    entity(handle).slices.erase(reinterpret_cast<uint64_t>(slice));
    return 0;
}

uint64_t MemFabricGvaApi::address(void*, void* slice) {
    return reinterpret_cast<uint64_t>(slice);
}

uint64_t MemFabricGvaApi::size(void* handle, void* slice) {
    return entity(handle).slices.at(address(handle, slice)).size;
}

uint64_t MemFabricGvaApi::base(void*, uint32_t type) {
    return (1ULL << 56) + (uint64_t{type} << 40);
}

int MemFabricGvaApi::translate(uint64_t addr, uint32_t type, uint64_t& out) {
    if (state->translate_result) return state->translate_result;
    auto local = entities.find(device);
    if (type != kGvaMemoryTypeDevice || local == entities.end()) return -12;
    const auto& slices = local->second->slices;
    auto slice = slices.upper_bound(addr);
    if (slice == slices.begin()) return -12;
    --slice;
    if (addr - slice->first >= slice->second.size) return -12;
    if (slice->second.imported &&
        local->second->config.protocol != "device_sdma")
        return -12;
    out = addr + state->alias_offset;
    return 0;
}

int MemFabricGvaApi::exportBlob(void* handle, void* slice,
                                std::vector<uint8_t>& blob) {
    if (!slice) {
        ++state->entity_exports;
        if (state->entity_export_result) return state->entity_export_result;
    } else {
        if (++state->slice_exports == state->fail_slice_export_at) return -1;
        if (state->empty_slice_export) return 0;
    }
    const std::array<uint64_t, 2> data =
        slice ? std::array<uint64_t, 2>{address(handle, slice),
                                        size(handle, slice)}
              : std::array<uint64_t, 2>{entity(handle).serial, 0};
    blob.resize(sizeof(data));
    std::memcpy(blob.data(), data.data(), sizeof(data));
    return 0;
}

int MemFabricGvaApi::importBlob(void* handle, bool entity_info,
                                const std::vector<uint8_t>& blob) {
    ++state->imports;
    if (entity_info) return state->entity_import_result;
    ++state->slice_imports;
    if (state->slice_import_result) return state->slice_import_result;
    std::array<uint64_t, 2> data;
    if (blob.size() != sizeof(data)) return -1;
    std::memcpy(data.data(), blob.data(), sizeof(data));
    entity(handle).slices.emplace(data[0], Slice{data[1], true});
    return 0;
}

int MemFabricGvaApi::map(void*) { return state->map_result; }
bool MemFabricGvaApi::supportsConnect() { return state->connect_available; }

int MemFabricGvaApi::connect(void*, uint32_t) {
    ++state->connects;
    return state->connect_result;
}

int MemFabricGvaApi::reach(void*, uint32_t, uint32_t& out) {
    out = state->reachable ? 1 : 0;
    return 0;
}

int MemFabricGvaApi::remove(void*, uint32_t) { return state->remove_result; }

int MemFabricGvaApi::unmap(void* handle, uint32_t rank) {
    ++state->unmaps;
    if (state->stale_registry) return 0;
    auto& local = entity(handle);
    const auto window = local.config.hbm_window;
    const auto start = base(handle, kGvaMemoryTypeDevice) + rank * window;
    const auto end = start + window;
    std::erase_if(local.slices, [&](const auto& entry) {
        return entry.second.imported && entry.first >= start &&
               entry.first < end;
    });
    return 0;
}

}  // namespace mooncake::tent
