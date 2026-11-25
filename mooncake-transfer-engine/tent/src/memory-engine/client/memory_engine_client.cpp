// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "tent/common/utils/ip.h"
#include "tent/runtime/transfer_engine_impl.h"
#ifdef USE_MEMFABRIC
#include "tent/runtime/platform.h"
#include "tent/transport/memfabric/memfabric_shared_memory.h"
#endif
#include "../common/memory_engine_common.h"
#include "tent/memory-engine/memory_engine_client.h"

namespace mooncake {
namespace tent {
namespace {

constexpr auto DEFAULT_HEARTBEAT = Milliseconds(3000);
constexpr auto DEFAULT_BATCH_COPY_TIMEOUT = Milliseconds(30000);
constexpr size_t ALLOCATION_ALIGNMENT = 2UL << 20;
constexpr int REGISTRATION_ID_WORDS = 4;

struct RemoteRegion {
    size_t size{};
    bool mapped = false;
    bool revoked = false;
};

struct PeerMapping {
    uint64_t generation{};
    SegmentID segment{LOCAL_SEGMENT_ID};
    bool revoked = false;
    std::string segmentName;
    std::string identity;
    std::map<uint64_t, RemoteRegion> regions;
};

enum class RegionState { Pending, Published, Revoking, Retired };

struct LocalRegion {
    size_t size{};
    bool registered = false;
    RegionState state{RegionState::Pending};
};

struct ClientOptions {
    Endpoint server;
    std::string callbackHost;
    Layout layout;
    std::optional<uint32_t> requestedRank;
    Milliseconds heartbeat{DEFAULT_HEARTBEAT};
};

#ifdef USE_MEMFABRIC
constexpr uint32_t LOCAL_VA_RANK = UINT32_MAX;
constexpr int32_t INVALID_COPY = static_cast<int32_t>(MemoryEngineCopyResult::InvalidArgument);
constexpr uint64_t COPY_GROUP_DIRECTIONS = 2;

struct CopyEndpoint {
    uint32_t rank = LOCAL_VA_RANK;
    bool valid = true;
};

struct CopyGroup {
    std::vector<MemFabricGvaCopy> copies;
    std::vector<size_t> indices;
};

struct CopyPlan {
    std::vector<size_t> local;
    std::unordered_map<uint64_t, CopyGroup> remote;
};

CopyEndpoint ClassifyWindow(uint64_t address, uint64_t size, uint64_t base, uint64_t window, uint32_t worldSize)
{
    if (window == 0) {
        return {};
    }
    if (address < base) {
        return {LOCAL_VA_RANK, size <= base - address};
    }
    const auto offset = address - base;
    if (offset >= window * static_cast<uint64_t>(worldSize)) {
        return {};
    }
    return {static_cast<uint32_t>(offset / window), size <= window - offset % window};
}

template <typename Regions>
const typename Regions::mapped_type *FindCopyRegion(const Regions &regions, uint64_t address, uint64_t size)
{
    const auto next = regions.upper_bound(address);
    if (next == regions.begin()) {
        return nullptr;
    }
    const auto region = std::prev(next);
    const auto offset = address - region->first;
    return offset < region->second.size && size <= region->second.size - offset ? &region->second : nullptr;
}

bool ValidCopyRange(uint64_t src, uint64_t dst, uint64_t size)
{
    if (src == 0 || dst == 0 || size == 0 || size > UINT64_MAX - src || size > UINT64_MAX - dst) [[unlikely]] {
        return false;
    }
    return src == dst || size <= (src < dst ? dst - src : src - dst);
}

bool CopyExpired(Clock::time_point deadline)
{
    return deadline != Clock::time_point::max() && Clock::now() >= deadline;
}

MemoryEngineCopyResult CopyResult(const Status &status, Clock::time_point deadline)
{
    if (CopyExpired(deadline)) {
        return MemoryEngineCopyResult::TimedOut;
    }
    return status.ok() ? MemoryEngineCopyResult::Ok : MemoryEngineCopyResult::TransferFailed;
}

struct CopyLogContext {
    std::string_view id;
    uint32_t rank;
};

void ExecuteLocalCopies(const std::vector<MemoryEngineCopyEntry> &entries, const CopyPlan &plan,
                        std::vector<int32_t> &results, Clock::time_point deadline, const CopyLogContext &context)
{
    auto &platform = Platform::getLoader();
    for (const auto index : plan.local) {
        auto outcome = MemoryEngineCopyResult::TimedOut;
        if (!CopyExpired(deadline)) {
            const auto &entry = entries[index];
            const auto status = entry.src == entry.dst ? Status::OK() :
                platform.copy(entry.dst, const_cast<void *>(entry.src), static_cast<size_t>(entry.size));
            outcome = CopyResult(status, deadline);
            if (!status.ok()) [[unlikely]] {
                LOG(ERROR) << "MemoryEngine local copy failed, registration ID:" << context.id
                           << ", rank:" << context.rank << ", index:" << index << ", bytes:" << entry.size
                           << ", status:" << static_cast<uint16_t>(status.code()) << ".";
            }
        }
        results[index] = static_cast<int32_t>(outcome);
    }
}

void ExecuteRemoteCopies(MemFabricSharedMemory &gva, const CopyPlan &plan, std::vector<int32_t> &results,
                         Clock::time_point deadline, const CopyLogContext &context)
{
    for (const auto &[key, group] : plan.remote) {
        auto outcome = MemoryEngineCopyResult::TimedOut;
        if (!CopyExpired(deadline)) {
            const auto status = gva.RunPinnedBatchCopy(group.copies, kGvaCopyAuto);
            outcome = CopyResult(status, deadline);
            if (!status.ok()) [[unlikely]] {
                LOG(ERROR) << "MemoryEngine remote copy failed, registration ID:" << context.id
                           << ", rank:" << context.rank << ", peer rank:" << (key / COPY_GROUP_DIRECTIONS)
                           << ", first index:" << group.indices.front() << ", count:" << group.copies.size()
                           << ", status:" << static_cast<uint16_t>(status.code()) << ".";
            }
        }
        for (const auto index : group.indices) {
            results[index] = static_cast<int32_t>(outcome);
        }
    }
}
#endif

Status ReadClientOptions(const Config &config, ClientOptions &options)
{
    if (!config.get(config_keys::CONFIG_ENABLED, true)) {
        return Status::NotImplemented("MemoryEngine is disabled");
    }
    CHECK_STATUS(ParseEndpoint(config.get(config_keys::CONFIG_SERVER_URL, ""), options.server));
    CHECK_STATUS(ReadLayout(config, config_keys::CONFIG_WORLD_SIZE, config_keys::CONFIG_HOST_MEM_MAX,
                            config_keys::CONFIG_DEVICE_MEM_MAX, options.layout));
    CHECK_STATUS(ValidateLayout(options.layout));
    if (!config.get(config_keys::CONFIG_AUTO_RANKING, true)) {
        const auto rank =
            ReadUnsigned(config.get(config_keys::CONFIG_LOCAL_RANK, json()), options.layout.worldSize - 1);
        if (!rank) {
            return Status::InvalidArgument("Explicit local_rank is required and must be in range");
        }
        options.requestedRank = static_cast<uint32_t>(*rank);
    }
    options.callbackHost = config.get(config_keys::CONFIG_CLIENT_HOST, "");
    options.heartbeat = Milliseconds(config.get(config_keys::CONFIG_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT.count()));
    if (options.heartbeat <= Milliseconds::zero() || options.heartbeat >= MAX_TIMEOUT) {
        return Status::InvalidArgument("Invalid MemoryEngine heartbeat interval");
    }
    return Status::OK();
}

std::string NewRegistrationId()
{
    std::random_device random;
    std::ostringstream id;
    id << std::hex;
    for (int index = 0; index < REGISTRATION_ID_WORDS; ++index) {
        id << random() << '-';
    }
    return id.str();
}

Status CallControl(CoroRpcAgent &rpc, MemoryRpc op, const PeerCall &server, Clock::time_point deadline)
{
    do {
        auto status = CallAsync(rpc, op, server, deadline).get();
        if (!Retryable(status)) {
            return status;
        }
        std::this_thread::sleep_for(RETRY_INTERVAL);
    } while (Clock::now() < deadline);
    return Status::TimedOut("MemoryEngine control request timed out");
}

}

struct MemoryEngine::Impl {
    ~Impl();
    Status Start(std::shared_ptr<const Config> config);
    Status StartCallbacks(std::string host);
    Status Register();
    Status InitializeRegistration();
    Status ConfirmRegistration();
    Status CancelRegistration();
    Status CreateTransferEngine(const Config &config);
    Status Join();
    Status Leave();
    Status Allocate(size_t size, MemoryEngineMemType type, void **address);
    Status Free(void *address);
#ifdef USE_MEMFABRIC
    Status BatchCopy(const std::vector<MemoryEngineCopyEntry> &entries, std::vector<int32_t> &results);
    CopyEndpoint ResolveCopyEndpoint(uint64_t address, uint64_t size) const;
    bool IsPublishedCopyRange(uint32_t rank, uint64_t address, uint64_t size) const;
    void PlanCopy(const MemoryEngineCopyEntry &entry, size_t index, CopyPlan &plan, int32_t &result,
                  std::unique_lock<std::mutex> &peerLock) const;
#endif
    Status DestroyTent();
    Status SharePeer(const json &payload);
    Status SharePeers(const json &payload);
    Status UnsharePeer(const json &payload);
    Status ValidatePeerRequest(const std::string &id, uint32_t rank) const;
    Status UnmapPeer(uint32_t rank, PeerMapping &peer);
    Status PreparePeerGeneration(uint32_t rank, uint64_t generation, PeerMapping &peer);
    Status ImportSharedPeer(PeerMapping &peer, SharedPeer &request);
    Status MapSharedPeerRegions(PeerMapping &peer, const SharedPeer &request);
    Status MapPeerRegion(uint32_t rank, PeerMapping &peer, const GvaRegion &region);
    Status UpdatePeerRegion(const json &payload, bool map);
    Status PrepareNativePeers();
    Status AllocateLocal(size_t size, MemoryEngineMemType type, uint64_t &address);
    Status UpdateLocalRegion(uint64_t address, size_t size, bool publish);
    Status ReleaseLocal(uint64_t address, LocalRegion &local, bool releaseMemory);
    Status ReconcileAllocation();
    Status RetireLocal(uint64_t address, LocalRegion &local);
    Status DrainRetired(uint64_t requested);
    void StartHeartbeat(Milliseconds interval);

    mutable std::shared_mutex mApiMutex;
    std::mutex mPeerMutex;
    std::shared_ptr<const Config> mConfig;
    ClientOptions mOptions;
    Layout mLayout;
    Bases mBases;
    uint32_t mRank{};
    std::string mServer;
    std::string mId;
    std::string mCallbackAddress;
    bool mJoined = false;
    bool mLeaving = false;
    bool mReady = false;
    Milliseconds mJoinTimeout{DEFAULT_REGISTRATION};
    Milliseconds mBatchCopyTimeout{DEFAULT_BATCH_COPY_TIMEOUT};
    bool mRegistrationAttempted = false;
    std::unordered_map<uint32_t, PeerMapping> mPeers;
    std::map<uint64_t, LocalRegion> mLocalRegions;
    std::unordered_set<uint64_t> mRetired;
    std::optional<uint64_t> mFailedAllocation;
    size_t mLiveRegions{};
    CoroRpcAgent mRpc;
    std::unique_ptr<TransferEngineImpl> mTent;
    std::jthread mHeartbeat;
};

MemoryEngine::Impl::~Impl()
{
    if (mReady || mLeaving) {
        const auto status = Leave();
        if (!status.ok()) {
            LOG(ERROR) << "MemoryEngine destructor could not finish leaving, registration ID:" << mId
                       << ", rank:" << mRank << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        }
    }
    mHeartbeat = std::jthread();
    const auto stopped = mRpc.stop();
    if (!stopped.ok()) {
        LOG(ERROR) << "Failed to stop MemoryEngine client RPC, registration ID:" << mId
                   << ", status:" << static_cast<uint16_t>(stopped.code()) << ".";
    }
    mTent.reset();
    const auto status = CancelRegistration();
    if (!status.ok()) {
        LOG(ERROR) << "MemoryEngine destructor could not cancel registration, registration ID:" << mId
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
    }
}

Status MemoryEngine::Impl::StartCallbacks(std::string host)
{
    bool ipv6 = false;
    if (host.empty()) {
        CHECK_STATUS(discoverLocalIpAddress(host, ipv6));
    } else {
        CHECK_STATUS(checkLocalIpAddress(host, ipv6));
    }
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::SharePeer,
                                 [this](const json &request, json &) { return SharePeer(request); }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::SharePeers,
                                 [this](const json &request, json &) { return SharePeers(request); }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::UnsharePeer,
                                 [this](const json &request, json &) { return UnsharePeer(request); }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::MapRegion,
                                 [this](const json &request, json &) { return UpdatePeerRegion(request, true); },
                                 true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::UnmapRegion, [this](const json &request, json &) {
        return UpdatePeerRegion(request, false);
    }, true));
    uint16_t port{};
    CHECK_STATUS(mRpc.start(port, ipv6));
    mCallbackAddress = buildIpAddrWithPort(host, port, ipv6);
    return Status::OK();
}

Status MemoryEngine::Impl::Register()
{
    {
        std::lock_guard lock(mPeerMutex);
        mId = NewRegistrationId();
        mPeers.clear();
    }

    const RegisterRequest request{mId, mLayout, mCallbackAddress, mOptions.requestedRank};
    const auto payload = EncodeRegisterRequest(request);
    mRegistrationAttempted = true;
    json reply;
    auto status = Call(mRpc, mServer, MemoryRpc::Register, payload, reply);
    if (!status.ok()) {
        LOG(ERROR) << "MemoryEngine Register RPC failed, registration ID:" << mId << ", requested rank:"
                   << (request.requestedRank ? std::to_string(*request.requestedRank) : "auto")
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }

    RegisterResponse response;
    status = DecodeRegisterResponse(reply, mLayout.worldSize, response);
    if (!status.ok()) {
        LOG(ERROR) << "Invalid MemoryEngine Register response, registration ID:" << mId
                   << ", world size:" << mLayout.worldSize << ", status:"
                   << static_cast<uint16_t>(status.code()) << ", reason:" << status.message() << ".";
        return status;
    }
    if (static_cast<uint64_t>(mOptions.heartbeat.count()) >= response.leaseMs) {
        LOG(ERROR) << "MemoryEngine heartbeat interval exceeds or equals rank lease, registration ID:" << mId
                   << ", rank:" << response.rank << ", heartbeat interval ms:" << mOptions.heartbeat.count()
                   << ", rank lease ms:" << response.leaseMs << ".";
        return Status::InvalidArgument("MemoryEngine heartbeat interval must be shorter than the server lease");
    }

    std::lock_guard lock(mPeerMutex);
    mRank = response.rank;
    return Status::OK();
}

Status MemoryEngine::Impl::ValidatePeerRequest(const std::string &id, uint32_t rank) const
{
    if (rank >= mLayout.worldSize || rank == mRank || id != mId) [[unlikely]] {
        LOG(ERROR) << "Invalid MemoryEngine peer callback, request ID:" << id
                   << ", registration ID:" << mId << ", rank:" << mRank << ", peer rank:" << rank
                   << ", world size:" << mLayout.worldSize << ".";
        return Status::InvalidArgument("Invalid MemoryEngine peer callback");
    }
    return Status::OK();
}

Status MemoryEngine::Impl::PreparePeerGeneration(uint32_t rank, uint64_t generation, PeerMapping &peer)
{
    if (peer.generation > generation || (peer.generation == generation && peer.revoked)) [[unlikely]] {
        LOG(WARNING) << "MemoryEngine SharePeer generation was replaced or revoked, registration ID:" << mId
                     << ", rank:" << mRank << ", peer rank:" << rank << ", generation:" << generation
                     << ", current generation:" << peer.generation << ".";
        return Status::InvalidEntry("MemoryEngine peer generation was replaced or revoked");
    }
    if (peer.generation < generation) {
        CHECK_STATUS(UnmapPeer(rank, peer));
        peer.generation = generation;
        peer.revoked = false;
    }
    return Status::OK();
}

Status MemoryEngine::Impl::ImportSharedPeer(PeerMapping &peer, SharedPeer &request)
{
    const auto rank = request.rank;
    if (request.segment == mTent->getSegmentName()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine SharePeer named the local segment, registration ID:" << mId
                   << ", rank:" << mRank << ", peer rank:" << rank << ".";
        return Status::InvalidArgument("Invalid MemoryEngine peer description");
    }

    if (peer.segment == LOCAL_SEGMENT_ID) {
        const auto openRet = mTent->openSegment(peer.segment, request.segment);
        if (!openRet.ok()) [[unlikely]] {
            LOG(ERROR) << "Failed to open shared MemoryEngine segment, registration ID:" << mId << ", rank:" << mRank
                       << ", peer rank:" << rank << ", status:" << static_cast<uint16_t>(openRet.code()) << ".";
            return openRet;
        }
        peer.segmentName = std::move(request.segment);
        peer.identity = std::move(request.identity);
    }

    const auto prepareRet = mTent->PrepareMemFabricPeer(peer.segmentName, peer.identity);
    if (!prepareRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to prepare shared MemoryEngine peer, registration ID:" << mId << ", rank:" << mRank
                   << ", peer rank:" << rank << ", status:" << static_cast<uint16_t>(prepareRet.code()) << ".";
        return prepareRet;
    }
    return Status::OK();
}

Status MemoryEngine::Impl::MapSharedPeerRegions(PeerMapping &peer, const SharedPeer &request)
{
    for (const auto &region : request.regions) {
        CHECK_STATUS(MapPeerRegion(request.rank, peer, region));
    }
    return Status::OK();
}

Status MemoryEngine::Impl::MapPeerRegion(uint32_t rank, PeerMapping &peer, const GvaRegion &region)
{
    auto &mapped = peer.regions.try_emplace(region.address, RemoteRegion{region.size}).first->second;
    if (mapped.revoked || mapped.size != region.size) [[unlikely]] {
        LOG(WARNING) << "MemoryEngine region cannot be mapped, registration ID:" << mId << ", rank:" << mRank
                     << ", peer rank:" << rank << ", generation:" << peer.generation << ", revoked:" << mapped.revoked
                     << ", requested bytes:" << region.size << ", mapped bytes:" << mapped.size << ".";
        return Status::InvalidEntry("MemoryEngine region was revoked or has a conflicting size");
    }
    if (mapped.mapped) {
        return Status::OK();
    }

    const auto mapRet = mTent->MapMemFabricRegion(peer.segment, reinterpret_cast<void *>(region.address), region.size);
    if (!mapRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to map MemoryEngine peer region, registration ID:" << mId << ", rank:" << mRank
                   << ", peer rank:" << rank << ", bytes:" << region.size
                   << ", status:" << static_cast<uint16_t>(mapRet.code()) << ".";
        return mapRet;
    }
    mapped.mapped = true;
    return Status::OK();
}

Status MemoryEngine::Impl::UpdatePeerRegion(const json &payload, bool map)
{
    std::lock_guard lock(mPeerMutex);
    PeerRegionRequest request;
    const auto decodeRet = DecodePeerRequest(payload, request.peer);
    if (!decodeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode MemoryEngine peer region request, error:" << decodeRet.message()
                   << ", registration ID:" << mId << ", local rank:"
                   << (mTent ? std::to_string(mRank) : "unknown") << ".";
        return decodeRet;
    }

    CHECK_STATUS(ValidatePeerRequest(request.peer.id, request.peer.rank));
    const auto rank = request.peer.rank;
    const auto found = mPeers.find(rank);
    if (!mTent || found == mPeers.end() || found->second.revoked ||
        found->second.generation != request.peer.generation) {
        if (map) {
            LOG(WARNING) << "MemoryEngine map request has no current peer, registration ID:" << request.peer.id
                         << ", rank:" << mRank << ", peer rank:" << rank
                         << ", generation:" << request.peer.generation << ".";
            return Status::InvalidEntry("MemoryEngine peer relationship is not current");
        }
        return Status::OK();
    }

    const auto regionRet = DecodePeerRegion(payload, request.region);
    if (!regionRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode MemoryEngine peer region, error:" << regionRet.message()
                   << ", registration ID:" << request.peer.id << ", rank:" << mRank << ", peer rank:" << rank << ".";
        return regionRet;
    }

    const auto &region = request.region;
    auto &peer = found->second;
    if (map) {
        return MapPeerRegion(rank, peer, region);
    }
    auto &mapped = peer.regions.try_emplace(region.address, RemoteRegion{region.size}).first->second;
    if (mapped.size != region.size) {
        LOG(ERROR) << "MemoryEngine UnmapRegion size mismatch, registration ID:" << request.peer.id
                   << ", rank:" << mRank << ", peer rank:" << rank << ", bytes:" << region.size
                   << ", mapped bytes:" << mapped.size << ".";
        return Status::InvalidArgument("Conflicting MemoryEngine region size");
    }
    mapped.revoked = true;
    const auto revokeRet = mTent->RevokeMemFabricRegion(peer.segment, rank, peer.segmentName, region.address);
    if (!revokeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to revoke MemoryEngine peer region, registration ID:" << request.peer.id
                   << ", rank:" << mRank << ", peer rank:" << rank << ", bytes:" << region.size
                   << ", status:" << static_cast<uint16_t>(revokeRet.code()) << ".";
    }
    return revokeRet;
}

Status MemoryEngine::Impl::UnmapPeer(uint32_t rank, PeerMapping &peer)
{
    if (peer.segment != LOCAL_SEGMENT_ID) {
        const auto unmapRet = mTent->CloseMemFabricPeer(peer.segment, rank, peer.segmentName);
        if (!unmapRet.ok()) [[unlikely]] {
            LOG(ERROR) << "Failed to unmap MemoryEngine peer, registration ID:" << mId << ", rank:" << mRank
                       << ", peer rank:" << rank << ", status:" << static_cast<uint16_t>(unmapRet.code()) << ".";
            return unmapRet;
        }

        const auto closeRet = mTent->closeSegment(peer.segment);
        if (!closeRet.ok()) [[unlikely]] {
            LOG(ERROR) << "Failed to close MemoryEngine peer segment, registration ID:" << mId << ", rank:" << mRank
                       << ", peer rank:" << rank << ", status:" << static_cast<uint16_t>(closeRet.code()) << ".";
            return closeRet;
        }
        peer.segment = LOCAL_SEGMENT_ID;
    }
    peer.regions.clear();
    peer.segmentName.clear();
    peer.identity.clear();
    return Status::OK();
}

Status MemoryEngine::Impl::SharePeer(const json &payload)
{
    std::lock_guard lock(mPeerMutex);
    if (!mTent) {
        LOG(WARNING) << "MemoryEngine SharePeer cannot access TENT, registration ID:" << mId << ".";
        return Status::InvalidEntry("MemoryEngine TENT is unavailable");
    }
    PeerRequest header;
    const auto decodeRet = DecodePeerRequest(payload, header);
    if (!decodeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode MemoryEngine SharePeer request, error:" << decodeRet.message()
                   << ", registration ID:" << mId << ", rank:" << mRank << ".";
        return decodeRet;
    }

    const auto rank = header.rank;
    CHECK_STATUS(ValidatePeerRequest(header.id, rank));
    auto &peer = mPeers[rank];
    CHECK_STATUS(PreparePeerGeneration(rank, header.generation, peer));

    SharedPeer request;
    request.rank = rank;
    const auto detailsRet = DecodeSharePeerDetails(payload, request);
    if (!detailsRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode shared MemoryEngine peer details, error:" << detailsRet.message()
                   << ", registration ID:" << mId << ", rank:" << mRank << ", peer rank:" << rank << ".";
        return detailsRet;
    }

    CHECK_STATUS(ImportSharedPeer(peer, request));
    return MapSharedPeerRegions(peer, request);
}

Status MemoryEngine::Impl::SharePeers(const json &payload)
{
    SharePeersRequest request;
    const auto decodeRet = DecodeSharePeersRequest(payload, mLayout.worldSize, request);

    std::lock_guard lock(mPeerMutex);
    if (!decodeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode MemoryEngine SharePeers request, error:" << decodeRet.message()
                   << ", request ID:" << request.id << ", registration ID:" << mId << ".";
        return decodeRet;
    }
    if (!mTent) [[unlikely]] {
        LOG(WARNING) << "MemoryEngine SharePeers cannot access TENT, request ID:" << request.id << ".";
        return Status::InvalidEntry("MemoryEngine TENT is unavailable");
    }

    for (auto &sharedPeer : request.peers) {
        const auto rank = sharedPeer.rank;
        CHECK_STATUS(ValidatePeerRequest(request.id, rank));
        auto &peer = mPeers[rank];
        CHECK_STATUS(PreparePeerGeneration(rank, sharedPeer.generation, peer));
        CHECK_STATUS(ImportSharedPeer(peer, sharedPeer));
    }

    // Region mapping starts after all reciprocal identity imports have completed.
    for (const auto &sharedPeer : request.peers) {
        CHECK_STATUS(MapSharedPeerRegions(mPeers.at(sharedPeer.rank), sharedPeer));
    }
    return Status::OK();
}

Status MemoryEngine::Impl::UnsharePeer(const json &payload)
{
    PeerRequest request;
    const auto decodeRet = DecodePeerRequest(payload, request);

    std::lock_guard lock(mPeerMutex);
    if (!decodeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode MemoryEngine UnsharePeer request, error:" << decodeRet.message()
                   << ", registration ID:" << mId << ".";
        return decodeRet;
    }

    CHECK_STATUS(ValidatePeerRequest(request.id, request.rank));
    const auto rank = request.rank;
    const auto generation = request.generation;
    auto &peer = mPeers[rank];
    if (peer.generation > generation) {
        return Status::OK();
    }
    peer.generation = generation;
    peer.revoked = true;
    return UnmapPeer(rank, peer);
}

Status MemoryEngine::Impl::Join()
{
    std::lock_guard lock(mApiMutex);
    if (mLeaving) {
        return Status::TooManyRequests("Retry MemoryEngine leave before rejoining");
    }
    if (mJoined) {
        return Status::OK();
    }
    if (!mReady) {
        const auto initRet = InitializeRegistration();
        if (!initRet.ok()) [[unlikely]] {
            LOG(ERROR) << "Failed to initialize registration, status:" << static_cast<uint16_t>(initRet.code()) << ".";
            return initRet;
        }
    }

    const auto deadline = Clock::now() + mJoinTimeout + RPC_TIMEOUT;
    std::string identity;
    const auto identityRet = mTent->GetMemFabricGvaIdentity(identity);
    if (!identityRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to get MemoryEngine GVA identity for Join, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(identityRet.code()) << ".";
        return identityRet;
    }

    const PeerCall server{mServer, EncodeJoinRequest({mId, mTent->getSegmentName(), std::move(identity)})};
    const auto joinRet = CallControl(mRpc, MemoryRpc::Join, server, deadline);
    if (!joinRet.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine Join request failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(joinRet.code()) << ".";
        return joinRet;
    }
    mJoined = true;
    return Status::OK();
}

Status MemoryEngine::Impl::PrepareNativePeers()
{
    std::lock_guard lock(mPeerMutex);
    for (const auto &[rank, peer] : mPeers) {
        if (!peer.revoked && peer.segment != LOCAL_SEGMENT_ID) {
            const auto status = mTent->PrepareMemFabricPeer(peer.segmentName, peer.identity);
            if (!status.ok()) [[unlikely]] {
                LOG(ERROR) << "Failed to prepare MemoryEngine peer for allocation, registration ID:" << mId
                           << ", rank:" << mRank << ", peer rank:" << rank
                           << ", status:" << static_cast<uint16_t>(status.code()) << ".";
                return status;
            }
        }
    }
    return Status::OK();
}

Status MemoryEngine::Impl::AllocateLocal(size_t size, MemoryEngineMemType type, uint64_t &address)
{
    MemoryOptions options;
    options.type = MEMFABRIC;
    options.location = type == MemoryEngineMemType::Host ? "cpu" : "npu";
    void *memory = nullptr;
    auto status = mTent->allocateLocalMemory(&memory, size, options);
    if (!status.ok()) {
        LOG(ERROR) << "MemoryEngine local allocation failed, registration ID:" << mId << ", rank:" << mRank
                   << ", location:" << options.location << ", bytes:" << size
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }

    address = reinterpret_cast<uint64_t>(memory);
    auto &local = mLocalRegions.try_emplace(address, LocalRegion{size}).first->second;
    // Registration can have published metadata before reporting an error.
    local.registered = true;
    status = mTent->registerLocalMemory({memory}, {size}, options);
    if (!status.ok()) {
        LOG(ERROR) << "MemoryEngine local allocation registration failed, registration ID:" << mId
                   << ", rank:" << mRank << ", location:" << options.location << ", bytes:" << size
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        if (ReleaseLocal(address, local, true).ok()) {
            mLocalRegions.erase(address);
        } else {
            mFailedAllocation = address;
        }
    }
    return status;
}

Status MemoryEngine::Impl::UpdateLocalRegion(uint64_t address, size_t size, bool publish)
{
    const auto op = publish ? MemoryRpc::PublishRegion : MemoryRpc::RevokeRegion;
    const PeerCall server{mServer, EncodeRegionRequest({mId, {address, size}})};
    const auto status = CallControl(mRpc, op, server, Clock::now() + mJoinTimeout + RPC_TIMEOUT);
    if (!status.ok()) {
        LOG(ERROR) << "MemoryEngine region update failed, registration ID:" << mId << ", rank:" << mRank
                   << ", bytes:" << size << ", status:" << static_cast<uint16_t>(status.code()) << ".";
    }
    return status;
}

Status MemoryEngine::Impl::ReleaseLocal(uint64_t address, LocalRegion &local, bool releaseMemory)
{
    auto *memory = reinterpret_cast<void *>(address);
    if (local.registered) {
        const auto unregisterRet = mTent->unregisterLocalMemory(memory, local.size);
        if (!unregisterRet.ok()) [[unlikely]] {
            LOG(ERROR) << "Failed to unregister MemoryEngine allocation, registration ID:" << mId << ", rank:" << mRank
                       << ", bytes:" << local.size << ", status:" << static_cast<uint16_t>(unregisterRet.code()) << ".";
            return unregisterRet;
        }
        local.registered = false;
    }

    if (releaseMemory) {
        const auto freeRet = mTent->freeLocalMemory(memory);
        if (!freeRet.ok()) [[unlikely]] {
            LOG(ERROR) << "Failed to free MemoryEngine allocation, registration ID:" << mId << ", rank:" << mRank
                       << ", bytes:" << local.size << ", status:" << static_cast<uint16_t>(freeRet.code()) << ".";
        }
        return freeRet;
    }
    return Status::OK();
}

Status MemoryEngine::Impl::ReconcileAllocation()
{
    if (!mFailedAllocation) {
        return Status::OK();
    }
    auto &local = mLocalRegions.at(*mFailedAllocation);
    const bool exported = local.state == RegionState::Revoking;
    if (exported) {
        CHECK_STATUS(UpdateLocalRegion(*mFailedAllocation, local.size, false));
    }
    const bool release = !exported || !mLiveRegions;
    CHECK_STATUS(ReleaseLocal(*mFailedAllocation, local, release));
    if (release) {
        mLocalRegions.erase(*mFailedAllocation);
    } else {
        local.state = RegionState::Retired;
        mRetired.insert(*mFailedAllocation);
    }
    mFailedAllocation.reset();
    return Status::OK();
}

Status MemoryEngine::Impl::Allocate(size_t size, MemoryEngineMemType type, void **address)
{
    std::lock_guard lock(mApiMutex);
    if (!mJoined) {
        return Status::InvalidEntry("Join MemoryEngine before allocating");
    }
    if (!address || !size || (type != MemoryEngineMemType::Host && type != MemoryEngineMemType::Device)) {
        return Status::InvalidArgument("Invalid MemoryEngine allocation request");
    }
    const auto window = type == MemoryEngineMemType::Host ? mLayout.hostWindow : mLayout.deviceWindow;
    if (size > window) {
        LOG(WARNING) << "MemoryEngine allocation exceeds the rank window, registration ID:" << mId
                     << ", rank:" << mRank << ", location:" << (type == MemoryEngineMemType::Host ? "cpu" : "npu")
                     << ", bytes:" << size << ", window bytes:" << window << ".";
        return Status::TooManyRequests("MemoryEngine allocation exceeds the rank window");
    }
    const auto aligned = ((size - 1) / ALLOCATION_ALIGNMENT + 1) * ALLOCATION_ALIGNMENT;
    CHECK_STATUS(ReconcileAllocation());
    CHECK_STATUS(DrainRetired(0));
    CHECK_STATUS(PrepareNativePeers());

    uint64_t allocated{};
    CHECK_STATUS(AllocateLocal(aligned, type, allocated));
    auto status = UpdateLocalRegion(allocated, aligned, true);
    auto &local = mLocalRegions.at(allocated);
    if (!status.ok()) {
        local.state = RegionState::Revoking;
        mFailedAllocation = allocated;
        const auto cleanup = ReconcileAllocation();
        if (!cleanup.ok()) {
            LOG(ERROR) << "Failed to roll back MemoryEngine allocation, registration ID:" << mId << ", rank:" << mRank
                       << ", bytes:" << aligned << ", status:" << static_cast<uint16_t>(cleanup.code()) << ".";
        }
        return status;
    }
    local.state = RegionState::Published;
    ++mLiveRegions;
    *address = reinterpret_cast<void *>(allocated);
    return Status::OK();
}

Status MemoryEngine::Impl::RetireLocal(uint64_t address, LocalRegion &local)
{
    local.state = RegionState::Revoking;
    CHECK_STATUS(UpdateLocalRegion(address, local.size, false));
    CHECK_STATUS(ReleaseLocal(address, local, false));
    local.state = RegionState::Retired;
    mRetired.insert(address);
    --mLiveRegions;
    return Status::OK();
}

Status MemoryEngine::Impl::DrainRetired(uint64_t requested)
{
    // HYBM unmaps an imported rank as a unit. Its revoked slices can still
    // have native mappings until peers revoke the last live slice.
    if (mLiveRegions || mFailedAllocation) {
        return Status::OK();
    }
    Status result;
    for (auto it = mRetired.begin(); it != mRetired.end();) {
        auto local = mLocalRegions.find(*it);
        auto status = ReleaseLocal(local->first, local->second, true);
        if (status.ok()) {
            mLocalRegions.erase(local);
            it = mRetired.erase(it);
        } else {
            if (*it == requested) {
                result = status;
            }
            ++it;
        }
    }
    return result;
}

Status MemoryEngine::Impl::Free(void *address)
{
    std::lock_guard lock(mApiMutex);
    if (!mJoined) {
        return Status::InvalidEntry("Join MemoryEngine before freeing");
    }
    const auto key = reinterpret_cast<uint64_t>(address);
    const auto found = mLocalRegions.find(key);
    if (found == mLocalRegions.end() || mFailedAllocation == key) {
        LOG(WARNING) << "MemoryEngine Free rejected an allocation, registration ID:" << mId << ", rank:" << mRank
                     << ", reason:" << (found == mLocalRegions.end() ? "base not owned" : "rollback pending") << ".";
        return Status::InvalidArgument("MemoryEngine requires an owned allocation base");
    }
    auto &local = found->second;
    if (local.state != RegionState::Retired) {
        CHECK_STATUS(RetireLocal(key, local));
    }
    return DrainRetired(key);
}

#ifdef USE_MEMFABRIC
CopyEndpoint MemoryEngine::Impl::ResolveCopyEndpoint(uint64_t address, uint64_t size) const
{
    const auto host = ClassifyWindow(address, size, mBases.host, mLayout.hostWindow, mLayout.worldSize);
    if (!host.valid || host.rank != LOCAL_VA_RANK) {
        return host;
    }
    return ClassifyWindow(address, size, mBases.device, mLayout.deviceWindow, mLayout.worldSize);
}

bool MemoryEngine::Impl::IsPublishedCopyRange(uint32_t rank, uint64_t address, uint64_t size) const
{
    if (rank == LOCAL_VA_RANK) {
        return true;
    }
    if (rank == mRank) {
        const auto *region = FindCopyRegion(mLocalRegions, address, size);
        return region && region->state == RegionState::Published;
    }
    const auto peer = mPeers.find(rank);
    if (peer == mPeers.end() || peer->second.revoked) {
        return false;
    }
    const auto *region = FindCopyRegion(peer->second.regions, address, size);
    return region && region->mapped && !region->revoked;
}

void MemoryEngine::Impl::PlanCopy(const MemoryEngineCopyEntry &entry, size_t index, CopyPlan &plan,
                                  int32_t &result, std::unique_lock<std::mutex> &peerLock) const
{
    const auto src = reinterpret_cast<uint64_t>(entry.src);
    const auto dst = reinterpret_cast<uint64_t>(entry.dst);
    if (!ValidCopyRange(src, dst, entry.size)) [[unlikely]] {
        return;
    }
    const auto source = ResolveCopyEndpoint(src, entry.size);
    const auto destination = ResolveCopyEndpoint(dst, entry.size);
    if (!source.valid || !destination.valid) {
        return;
    }

    const bool srcGva = source.rank != LOCAL_VA_RANK;
    const bool dstGva = destination.rank != LOCAL_VA_RANK;
    const bool remote = (srcGva && source.rank != mRank) || (dstGva && destination.rank != mRank);
    if (remote && !peerLock.owns_lock()) {
        peerLock.lock();
    }
    if (!IsPublishedCopyRange(source.rank, src, entry.size) ||
        !IsPublishedCopyRange(destination.rank, dst, entry.size)) {
        return;
    }
    if (srcGva && dstGva && remote) {
        result = static_cast<int32_t>(MemoryEngineCopyResult::Unsupported);
        return;
    }
    if (!remote) {
        plan.local.push_back(index);
        return;
    }

    const auto rank = srcGva ? source.rank : destination.rank;
    const auto key = static_cast<uint64_t>(rank) * COPY_GROUP_DIRECTIONS + static_cast<uint64_t>(dstGva);
    auto &group = plan.remote[key];
    group.copies.push_back({src, dst, static_cast<size_t>(entry.size)});
    group.indices.push_back(index);
}

Status MemoryEngine::Impl::BatchCopy(const std::vector<MemoryEngineCopyEntry> &entries, std::vector<int32_t> &results)
{
    std::shared_lock lock(mApiMutex);
    if (!mJoined) {
        return Status::InvalidEntry("Join MemoryEngine before copying");
    }
    const auto deadline = mBatchCopyTimeout == Milliseconds::zero() ? Clock::time_point::max() :
        Clock::now() + mBatchCopyTimeout;
    results.assign(entries.size(), INVALID_COPY);

    CopyPlan plan;
    const auto release = [](MemFabricSharedMemory *gva) { gva->releaseCopy(1); };
    std::unique_ptr<MemFabricSharedMemory, decltype(release)> pin(nullptr, release);
    {
        std::unique_lock peerLock(mPeerMutex, std::defer_lock);
        for (size_t index = 0; index < entries.size(); ++index) {
            PlanCopy(entries[index], index, plan, results[index], peerLock);
        }
        if (!plan.remote.empty()) {
            // Pin before releasing the peer lock so revoke cannot invalidate a planned range.
            auto *gva = mTent->GetMemFabricGva();
            const auto status = gva->PinCopyBatch();
            if (!status.ok()) [[unlikely]] {
                std::fill(results.begin(), results.end(), INVALID_COPY);
                LOG(ERROR) << "MemoryEngine BatchCopy could not pin remote mappings, registration ID:" << mId
                           << ", rank:" << mRank << ", entries:" << entries.size()
                           << ", status:" << static_cast<uint16_t>(status.code()) << ".";
                return status;
            }
            pin.reset(gva);
        }
    }

    const CopyLogContext context{mId, mRank};
    if (!plan.local.empty()) {
        ExecuteLocalCopies(entries, plan, results, deadline, context);
    }
    if (pin) {
        ExecuteRemoteCopies(*pin, plan, results, deadline, context);
    }
    return Status::OK();
}
#endif

Status MemoryEngine::Impl::DestroyTent()
{
    std::lock_guard lock(mPeerMutex);
    if (!mTent) {
        return Status::OK();
    }
    for (auto &[rank, peer] : mPeers) {
        peer.revoked = true;
        CHECK_STATUS(UnmapPeer(rank, peer));
    }
    for (auto it = mLocalRegions.begin(); it != mLocalRegions.end();) {
        CHECK_STATUS(ReleaseLocal(it->first, it->second, true));
        it = mLocalRegions.erase(it);
    }
    mFailedAllocation.reset();
    mRetired.clear();
    mLiveRegions = 0;
    const auto closeRet = mTent->CloseMemFabricGva();
    if (!closeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to close MemoryEngine GVA layout, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(closeRet.code()) << ".";
        return closeRet;
    }
    mTent.reset();
    return Status::OK();
}

Status MemoryEngine::Impl::Leave()
{
    std::lock_guard lock(mApiMutex);
    if (!mReady && !mLeaving) {
        return CancelRegistration();
    }
    mLeaving = true;
    mJoined = false;
    const PeerCall server{mServer, EncodeRegistrationIdRequest({mId})};
    const auto deadline = Clock::now() + mJoinTimeout;
    const auto beginRet = CallControl(mRpc, MemoryRpc::BeginLeave, server, deadline);
    if (!beginRet.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine BeginLeave request failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(beginRet.code()) << ".";
        return beginRet;
    }
    CHECK_STATUS(DestroyTent());

    const auto finishRet = CallAsync(mRpc, MemoryRpc::FinishLeave, server, Clock::now() + RPC_TIMEOUT).get();
    if (!finishRet.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine FinishLeave request failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(finishRet.code()) << ".";
        return finishRet;
    }
    mHeartbeat = std::jthread();
    mRegistrationAttempted = false;
    mReady = false;
    mLeaving = false;
    return Status::OK();
}

Status MemoryEngine::Impl::CancelRegistration()
{
    if (!mRegistrationAttempted) {
        return Status::OK();
    }
    const auto request = EncodeCancelRequest({mId, mCallbackAddress});
    const auto status = Call(mRpc, mServer, MemoryRpc::Cancel, request);
    if (!status.ok()) {
        LOG(ERROR) << "MemoryEngine Cancel RPC failed, registration ID:" << mId
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    mRegistrationAttempted = false;
    return Status::OK();
}

Status MemoryEngine::Impl::CreateTransferEngine(const Config &config)
{
    auto native = std::make_shared<Config>();
    CHECK_STATUS(native->load(config.dump()));
    native->set(config_keys::TENT_CONFIG_METADATA_TYPE, "p2p");
    native->set(config_keys::TENT_CONFIG_METADATA_SERVERS, "");
    native->set(config_keys::TENT_CONFIG_MEMFABRIC_ENABLE, true);
    native->set(config_keys::TENT_CONFIG_GVA_ENABLED, true);
    native->set(config_keys::TENT_CONFIG_GVA_RANK, mRank);
    native->set(config_keys::TENT_CONFIG_GVA_RANK_COUNT, mLayout.worldSize);
    native->set(config_keys::TENT_CONFIG_GVA_DRAM_WINDOW, mLayout.hostWindow / GIB);
    native->set(config_keys::TENT_CONFIG_GVA_HBM_WINDOW, mLayout.deviceWindow / GIB);
    auto tent = std::unique_ptr<TransferEngineImpl>(new (std::nothrow) TransferEngineImpl(native, true, false));
    if (!tent || !tent->available()) [[unlikely]] {
        LOG(ERROR) << "Failed to initialize MemoryEngine TENT instance, registration ID:" << mId << ", rank:" << mRank
                   << ", world size:" << mLayout.worldSize << ".";
        return Status::InternalError("Could not initialize MemoryEngine TENT instance");
    }

    const auto basesRet = tent->GetMemFabricGvaBases(mBases.host, mBases.device);
    if (!basesRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to get MemoryEngine GVA bases, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(basesRet.code()) << ".";
        return basesRet;
    }
    const auto validateRet = ValidateBases(mLayout, mBases);
    if (!validateRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Invalid MemoryEngine GVA layout, error:" << validateRet.message()
                   << ", registration ID:" << mId << ", rank:" << mRank << ", world size:" << mLayout.worldSize
                   << ", host window bytes:" << mLayout.hostWindow << ", device window bytes:" << mLayout.deviceWindow
                   << ".";
        return validateRet;
    }
    std::lock_guard lock(mPeerMutex);
    mTent = std::move(tent);
    return Status::OK();
}

void MemoryEngine::Impl::StartHeartbeat(Milliseconds interval)
{
    mHeartbeat = std::jthread([this, interval](std::stop_token stop) {
        auto lastCode = Status::Code::kOk;
        const auto request = EncodeRegistrationIdRequest({mId});
        RunPeriodic(stop, interval, [&] {
            const auto status = Call(mRpc, mServer, MemoryRpc::Heartbeat, request);
            const auto code = status.code();
            if (!status.ok() && code != lastCode) {
                if (status.IsInvalidEntry()) {
                    LOG(ERROR) << "MemoryEngine server rejected the heartbeat registration, registration ID:" << mId
                               << ", rank:" << mRank << ", status:" << static_cast<uint16_t>(code) << ".";
                } else {
                    LOG(WARNING) << "MemoryEngine Heartbeat RPC failed, registration ID:" << mId << ", rank:" << mRank
                                 << ", status:" << static_cast<uint16_t>(code) << ".";
                }
            }
            lastCode = code;
        });
    });
}

Status MemoryEngine::Impl::InitializeRegistration()
{
    // A timed-out registration may still exist on the server.
    CHECK_STATUS(DestroyTent());
    CHECK_STATUS(CancelRegistration());
    auto status = ConfirmRegistration();
    if (!status.ok()) {
        auto cleanup = DestroyTent();
        if (cleanup.ok()) {
            cleanup = CancelRegistration();
        }
        if (!cleanup.ok()) {
            LOG(ERROR) << "Failed to clean up MemoryEngine registration, registration ID:" << mId
                       << ", create status:" << static_cast<uint16_t>(status.code())
                       << ", cleanup status:" << static_cast<uint16_t>(cleanup.code()) << ".";
            return cleanup;
        }
        return status;
    }
    StartHeartbeat(mOptions.heartbeat);
    mReady = true;
    return Status::OK();
}

Status MemoryEngine::Impl::ConfirmRegistration()
{
    CHECK_STATUS(Register());
    CHECK_STATUS(CreateTransferEngine(*mConfig));
    const auto request = EncodeConfirmRequest({mId, mRank, mBases});
    const auto confirmRet = Call(mRpc, mServer, MemoryRpc::Confirm, request);
    if (!confirmRet.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine Confirm request failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(confirmRet.code()) << ".";
    }
    return confirmRet;
}

Status MemoryEngine::Impl::Start(std::shared_ptr<const Config> config)
{
    const auto status = ReadClientOptions(*config, mOptions);
    if (!status.ok()) {
        LOG(ERROR) << "MemoryEngine could not read client options, status:" << static_cast<uint16_t>(status.code())
                   << ", reason:" << status.message() << ".";
        return status;
    }
    mJoinTimeout = Milliseconds(config->get(config_keys::CONFIG_JOIN_TIMEOUT_MS, DEFAULT_REGISTRATION.count()));
    if (mJoinTimeout <= Milliseconds::zero() || mJoinTimeout >= MAX_TIMEOUT) {
        LOG(ERROR) << "Invalid MemoryEngine client timeout, config:" << config_keys::CONFIG_JOIN_TIMEOUT_MS
                   << ", value ms:" << mJoinTimeout.count() << ".";
        return Status::InvalidArgument("Invalid MemoryEngine join timeout");
    }
    mBatchCopyTimeout = Milliseconds(config->get(config_keys::CONFIG_BATCH_COPY_TIMEOUT_MS,
                                                DEFAULT_BATCH_COPY_TIMEOUT.count()));
    if (mBatchCopyTimeout < Milliseconds::zero() || mBatchCopyTimeout >= MAX_TIMEOUT) {
        LOG(ERROR) << "Invalid MemoryEngine client timeout, config:" << config_keys::CONFIG_BATCH_COPY_TIMEOUT_MS
                   << ", value ms:" << mBatchCopyTimeout.count() << ".";
        return Status::InvalidArgument("Invalid MemoryEngine batch copy timeout");
    }
    mConfig = std::move(config);
    mLayout = mOptions.layout;
    const auto &server = mOptions.server;
    mServer = buildIpAddrWithPort(server.host, server.port, server.ipv6);
    const auto callbacks = StartCallbacks(mOptions.callbackHost);
    if (!callbacks.ok()) {
        LOG(ERROR) << "Could not start MemoryEngine callback RPC, status:"
                   << static_cast<uint16_t>(callbacks.code()) << ".";
        return callbacks;
    }
    return InitializeRegistration();
}

MemoryEngine::MemoryEngine(std::unique_ptr<Impl> impl) : mImpl(std::move(impl))
{
}

MemoryEngine::~MemoryEngine() = default;

Status MemoryEngine::Create(const Config &config, std::unique_ptr<MemoryEngine> *engine)
{
    if (!engine || *engine) {
        return Status::InvalidArgument("MemoryEngine requires an empty output");
    }
    auto impl = std::unique_ptr<Impl>(new (std::nothrow) Impl);
    if (!impl) {
        return Status::InternalError("Could not allocate MemoryEngine state");
    }
    auto result = std::unique_ptr<MemoryEngine>(new (std::nothrow) MemoryEngine(std::move(impl)));
    if (!result) {
        return Status::InternalError("Could not allocate MemoryEngine");
    }
    auto status = result->mImpl->Start(config.freeze());
    if (!status.ok()) {
        LOG(ERROR) << "Failed to create MemoryEngine, status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    *engine = std::move(result);
    return Status::OK();
}

Status MemoryEngine::Create(std::unique_ptr<MemoryEngine> *engine)
{
    Config config;
    CHECK_STATUS(ConfigHelper().loadFromEnv(config));
    return Create(config, engine);
}

uint32_t MemoryEngine::GetLocalRank() const
{
    std::shared_lock lock(mImpl->mApiMutex);
    return mImpl->mRank;
}

Status MemoryEngine::Join()
{
    return mImpl->Join();
}

Status MemoryEngine::Leave()
{
    return mImpl->Leave();
}

Status MemoryEngine::Allocate(size_t size, MemoryEngineMemType type, void **address)
{
    return mImpl->Allocate(size, type, address);
}

Status MemoryEngine::Free(void *address)
{
    return mImpl->Free(address);
}

Status MemoryEngine::BatchCopy(const std::vector<MemoryEngineCopyEntry> &entries, std::vector<int32_t> *results)
{
    if (results == nullptr || entries.empty() || entries.size() > UINT32_MAX) [[unlikely]] {
        return Status::InvalidArgument("Invalid MemoryEngine copy batch");
    }
#ifdef USE_MEMFABRIC
    return mImpl->BatchCopy(entries, *results);
#else
    return Status::NotImplemented("MemoryEngine batch copy requires MemFabric GVA");
#endif
}

Status MemoryEngine::GvaBase(MemoryEngineMemType type, uint32_t rank, void **address) const
{
    std::shared_lock lock(mImpl->mApiMutex);
    if (!address || rank >= mImpl->mLayout.worldSize) {
        return Status::InvalidArgument("Invalid MemoryEngine rank or address output");
    }
    if (type != MemoryEngineMemType::Host && type != MemoryEngineMemType::Device) {
        return Status::InvalidArgument("Unknown MemoryEngine memory type");
    }
    const bool host = type == MemoryEngineMemType::Host;
    const auto window = host ? mImpl->mLayout.hostWindow : mImpl->mLayout.deviceWindow;
    if (!window) {
        return Status::InvalidArgument("Requested MemoryEngine GVA window is disabled");
    }
    const auto base = host ? mImpl->mBases.host : mImpl->mBases.device;
    *address = reinterpret_cast<void *>(base + rank * window);
    return Status::OK();
}

Status MemoryEngine::RankOfGva(const void *address, uint32_t *rank) const
{
    std::shared_lock lock(mImpl->mApiMutex);
    if (!rank || !address) {
        return Status::InvalidArgument("Missing MemoryEngine GVA or rank output");
    }
    const auto value = reinterpret_cast<uint64_t>(address);
    const auto &layout = mImpl->mLayout;
    const auto &bases = mImpl->mBases;
    const auto owner = [&](uint64_t base, uint64_t window) -> std::optional<uint32_t> {
        if (!window || value < base || value - base >= layout.worldSize * window) {
            return std::nullopt;
        }
        return static_cast<uint32_t>((value - base) / window);
    };
    auto result = owner(bases.host, layout.hostWindow);
    if (!result) {
        result = owner(bases.device, layout.deviceWindow);
    }
    if (!result) {
        return Status::InvalidArgument("Address is outside the MemoryEngine GVA layout");
    }
    *rank = *result;
    return Status::OK();
}

}
}
