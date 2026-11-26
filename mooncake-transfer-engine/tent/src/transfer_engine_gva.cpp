// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <limits>
#include <mutex>
#include <new>
#include <utility>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"
#ifdef USE_MEMFABRIC
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/memfabric/memfabric_transport.h"
#endif
#include "tent/transfer_engine.h"

namespace mooncake {
namespace tent {
namespace {

constexpr char GVA_UNAVAILABLE[] = "GVA transport is unavailable";

bool ValidCopyRange(uint64_t source, uint64_t destination, size_t size)
{
    const auto first = std::min(source, destination);
    const auto last = std::max(source, destination);
    return first != 0 && size != 0 && size <= std::numeric_limits<uint64_t>::max() - last &&
           (first == last || size <= last - first);
}

#ifdef USE_MEMFABRIC
constexpr uint64_t GIB = 1ULL << 30;
constexpr uint64_t MAX_GVA_WINDOW = 1ULL << 50;
constexpr size_t GVA_COPY_BATCH_PIN_COUNT = 1;
constexpr char CONFIG_METADATA_TYPE[] = "metadata_type";
constexpr char CONFIG_METADATA_SERVERS[] = "metadata_servers";
constexpr char CONFIG_MEMFABRIC_ENABLE[] = "transports/memfabric/enable";
constexpr char CONFIG_GVA_ENABLED[] = "transports/memfabric/gva/enabled";
constexpr char CONFIG_GVA_RANK[] = "transports/memfabric/gva/rank";
constexpr char CONFIG_GVA_RANK_COUNT[] = "transports/memfabric/gva/rank_count";
constexpr char CONFIG_GVA_HOST_WINDOW[] = "transports/memfabric/gva/dram_window";
constexpr char CONFIG_GVA_DEVICE_WINDOW[] = "transports/memfabric/gva/hbm_window";

bool ValidGvaWindow(uint64_t window, uint32_t rankCount)
{
    return window < MAX_GVA_WINDOW && window % GIB == 0 &&
           window <= std::numeric_limits<uint64_t>::max() / static_cast<uint64_t>(rankCount);
}
#endif

}

Status TransferEngine::CreateWithGva(const Config &config, const GvaLayout &layout,
                                    std::unique_ptr<TransferEngine> &engine)
{
#ifdef USE_MEMFABRIC
    if (engine || layout.rank >= layout.rankCount ||
        (layout.hostWindow == 0 && layout.deviceWindow == 0)) [[unlikely]] {
        return Status::InvalidArgument("Invalid GVA engine output or rank layout");
    }
    if (!ValidGvaWindow(layout.hostWindow, layout.rankCount) ||
        !ValidGvaWindow(layout.deviceWindow, layout.rankCount)) [[unlikely]] {
        return Status::InvalidArgument("Invalid GVA window sizes");
    }

    auto native = std::make_shared<Config>();
    CHECK_STATUS(native->load(config.dump()));
    native->set(CONFIG_METADATA_TYPE, "p2p");
    native->set(CONFIG_METADATA_SERVERS, "");
    native->set(CONFIG_MEMFABRIC_ENABLE, true);
    native->set(CONFIG_GVA_ENABLED, true);
    native->set(CONFIG_GVA_RANK, layout.rank);
    native->set(CONFIG_GVA_RANK_COUNT, layout.rankCount);
    native->set(CONFIG_GVA_HOST_WINDOW, layout.hostWindow / GIB);
    native->set(CONFIG_GVA_DEVICE_WINDOW, layout.deviceWindow / GIB);

    MemFabricConfigScope scope(*native);
    auto result = std::unique_ptr<TransferEngine>(new (std::nothrow) TransferEngine(native));
    if (!result || !result->available()) [[unlikely]] {
        return Status::InternalError("Could not initialize GVA transfer engine");
    }
    CHECK_STATUS(result->WithGvaTransport([](auto &) { return Status::OK(); }));
    engine = std::move(result);
    return Status::OK();
#else
    static_cast<void>(config);
    static_cast<void>(layout);
    static_cast<void>(engine);
    return Status::NotImplemented(GVA_UNAVAILABLE);
#endif
}

template <typename Operation>
Status TransferEngine::WithGvaTransport([[maybe_unused]] Operation operation) const
{
#ifdef USE_MEMFABRIC
    const auto &transportSlot = impl_->transport_list_[static_cast<size_t>(MEMFABRIC)];
    auto *transport = dynamic_cast<MemFabricTransport *>(transportSlot.get());
    if (transport && transport->supportsGlobalAddressing()) {
        return operation(*transport);
    }
#endif
    return Status::NotImplemented(GVA_UNAVAILABLE);
}

Status TransferEngine::GetGvaBases(uint64_t &host, uint64_t &device) const
{
    return WithGvaTransport([&](auto &transport) { return transport.GetGvaBases(host, device); });
}

Status TransferEngine::GetGvaIdentity(std::string &identity) const
{
    return WithGvaTransport([&](auto &transport) { return transport.GetGvaIdentity(identity); });
}

Status TransferEngine::PrepareGvaPeer(const std::string &segment, const std::string &identity)
{
    if (segment.empty() || segment == getSegmentName() || identity.empty()) [[unlikely]] {
        return Status::InvalidArgument("Invalid GVA peer identity");
    }
    return WithGvaTransport([&](auto &transport) { return transport.PrepareGvaPeer(segment, identity); });
}

Status TransferEngine::CloseGvaPeer(SegmentID peer, uint32_t rank, const std::string &segment)
{
    if (peer == LOCAL_SEGMENT_ID || segment.empty()) [[unlikely]] {
        return Status::InvalidArgument("Invalid GVA peer segment");
    }
    return WithGvaTransport([&](auto &transport) { return transport.CloseGvaPeer(peer, rank, segment); });
}

Status TransferEngine::MapGvaRegion(SegmentID peer, void *address, size_t size)
{
    if (peer == LOCAL_SEGMENT_ID || !address || !size) [[unlikely]] {
        return Status::InvalidArgument("Invalid GVA region");
    }
    return WithGvaTransport([&](auto &transport) { return transport.MapGvaRegion(peer, address, size); });
}

Status TransferEngine::RevokeGvaRegion(SegmentID peer, uint32_t rank, const std::string &segment, uint64_t address)
{
    if (peer == LOCAL_SEGMENT_ID || segment.empty() || !address) [[unlikely]] {
        return Status::InvalidArgument("Invalid GVA region");
    }
    return WithGvaTransport([&](auto &transport) { return transport.RevokeGvaRegion(peer, rank, segment, address); });
}

Status TransferEngine::AllocateGvaMemory(void **address, size_t size, MemoryOptions &options)
{
    if (!address || !size) [[unlikely]] {
        return Status::InvalidArgument("Invalid GVA allocation output or size");
    }
    return WithGvaTransport([&](auto &) {
        options.type = MEMFABRIC;
        return allocateLocalMemory(address, size, options);
    });
}

Status TransferEngine::CopyLocalMemory(void *destination, const void *source, size_t size)
{
    const auto src = reinterpret_cast<uint64_t>(source);
    const auto dst = reinterpret_cast<uint64_t>(destination);
    if (!ValidCopyRange(src, dst, size)) [[unlikely]] {
        return Status::InvalidArgument("Invalid local copy range");
    }
    if (destination == source) {
        return Status::OK();
    }

    static auto &platform = Platform::getLoader();
    return platform.copy(destination, const_cast<void *>(source), size);
}

Status TransferEngine::PinGvaCopyBatch(GvaCopyBatch &batch) const
{
    if (batch.mContext) [[unlikely]] {
        return Status::InvalidArgument("GVA copy batch is already pinned");
    }
#ifdef USE_MEMFABRIC
    return WithGvaTransport([&](auto &transport) {
        std::shared_ptr<MemFabricSharedMemory> context;
        CHECK_STATUS(transport.PinGvaCopyBatch(context));
        batch.mContext = std::move(context);
        return Status::OK();
    });
#else
    return Status::NotImplemented(GVA_UNAVAILABLE);
#endif
}

TransferEngine::GvaCopyBatch::~GvaCopyBatch()
{
#ifdef USE_MEMFABRIC
    if (mContext) {
        static_cast<MemFabricSharedMemory *>(mContext.get())->releaseCopy(GVA_COPY_BATCH_PIN_COUNT);
    }
#endif
}

Status TransferEngine::GvaCopyBatch::Copy(const std::vector<GvaCopy> &copies) const
{
    if (copies.empty() || copies.size() > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
        return Status::InvalidArgument("Invalid GVA copy batch size");
    }
#ifdef USE_MEMFABRIC
    if (!mContext) [[unlikely]] {
        return Status::InvalidEntry("GVA copy batch is not pinned");
    }

    std::vector<MemFabricGvaCopy> native;
    native.reserve(copies.size());
    for (const auto &copy : copies) {
        if (!ValidCopyRange(copy.source, copy.destination, copy.size)) [[unlikely]] {
            return Status::InvalidArgument("Invalid GVA copy range");
        }
        native.push_back({copy.source, copy.destination, copy.size});
    }
    return static_cast<MemFabricSharedMemory *>(mContext.get())->RunPinnedBatchCopy(native, kGvaCopyAuto);
#else
    return Status::NotImplemented(GVA_UNAVAILABLE);
#endif
}

Status TransferEngine::CloseGva()
{
    return WithGvaTransport([](auto &transport) { return transport.uninstall(); });
}

}
}
