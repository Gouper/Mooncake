// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "tent/common/utils/ip.h"
#include "memory_engine_common.h"
#include "memory_engine_server.h"

namespace mooncake {
namespace tent {
namespace {

constexpr auto DEFAULT_LEASE = Milliseconds(20000);
constexpr auto REAP_INTERVAL = Milliseconds(1000);
constexpr size_t DEFAULT_RPC_THREADS = 4;
constexpr int HEARTBEAT_LOG_EVERY = 100;

enum class MemberState { Pending, Registered, Joining, Joined, Leaving };

struct Registration {
    uint32_t rank{};
    std::string id;
    std::string callback;
    Clock::time_point expires;
    MemberState state{MemberState::Pending};
    std::string segment{};
    std::string identity{};
    uint64_t generation{};
    std::map<uint64_t, GvaRegion> regions{};
    std::unordered_set<uint64_t> revokedRegions{};
    bool leavePrepared = false;
};

struct PeerCallback {
    PeerCall call;
    uint32_t targetRank{};
    uint32_t ownerRank{};
};

class Mutation {
   public:
    explicit Mutation(std::atomic<uint64_t> &active, bool exclusive = true)
        : mActive(active), mWeight(exclusive ? EXCLUSIVE : SHARED), mAcquired(Acquire(exclusive))
    {
    }

    ~Mutation()
    {
        if (mAcquired) {
            mActive.fetch_sub(mWeight, std::memory_order_release);
        }
    }

    bool Acquired() const
    {
        return mAcquired;
    }

   private:
    bool Acquire(bool exclusive)
    {
        uint64_t active = 0;
        if (exclusive) {
            return mActive.compare_exchange_strong(active, EXCLUSIVE, std::memory_order_acquire);
        }
        active = mActive.load(std::memory_order_relaxed);
        while (active < EXCLUSIVE - SHARED) {
            if (mActive.compare_exchange_weak(active, active + SHARED, std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    static constexpr uint64_t EXCLUSIVE = 1ULL << 63;
    static constexpr uint64_t SHARED = 1;
    std::atomic<uint64_t> &mActive;
    const uint64_t mWeight;
    const bool mAcquired;
};

class OwnerMutation {
   public:
    OwnerMutation(std::mutex &mutex, std::unordered_set<std::string> &active, const std::string &id)
        : mMutex(mutex), mActive(active), mId(id)
    {
        std::lock_guard lock(mMutex);
        mAcquired = mActive.insert(id).second;
    }

    ~OwnerMutation()
    {
        if (mAcquired) {
            std::lock_guard lock(mMutex);
            mActive.erase(mId);
        }
    }

    bool Acquired() const
    {
        return mAcquired;
    }

   private:
    std::mutex &mMutex;
    std::unordered_set<std::string> &mActive;
    const std::string &mId;
    bool mAcquired{};
};

Status SendToPeersInParallel(MemoryEngineRpc &rpc, MemoryRpc op, const std::vector<PeerCallback> &peers,
                             Clock::time_point deadline)
{
    std::vector<size_t> pending(peers.size());
    std::iota(pending.begin(), pending.end(), size_t{0});
    while (!pending.empty()) {
        const auto rpcDeadline = std::min(deadline, Clock::now() + RPC_TIMEOUT);
        std::vector<std::future<Status>> calls;
        calls.reserve(pending.size());
        for (const auto peerIndex : pending) {
            calls.emplace_back(CallAsync(rpc, op, peers[peerIndex].call, rpcDeadline));
        }

        Status failure;
        size_t retryCount = 0;
        for (size_t index = 0; index < calls.size(); ++index) {
            const auto peerIndex = pending[index];
            auto status = calls[index].get();
            if (Retryable(status)) {
                pending[retryCount++] = peerIndex;
            } else if (!status.ok() && failure.ok()) {
                const auto &peer = peers[peerIndex];
                LOG(ERROR) << "MemoryEngine peer callback failed, target rank:" << peer.targetRank
                           << ", owner rank:" << peer.ownerRank
                           << ", status:" << static_cast<uint16_t>(status.code()) << ".";
                failure = std::move(status);
            }
        }

        if (!failure.ok()) {
            return failure;
        }
        pending.resize(retryCount);
        if (retryCount && Clock::now() >= deadline) {
            const auto status = Status::TimedOut("MemoryEngine callbacks did not settle");
            const auto &peer = peers[pending.front()];
            LOG(ERROR) << "MemoryEngine peer callbacks did not settle, pending count:" << retryCount
                       << ", first pending rank:" << peer.targetRank << ", owner rank:" << peer.ownerRank
                       << ", status:" << static_cast<uint16_t>(status.code()) << ".";
            return status;
        }
        if (retryCount) {
            std::this_thread::sleep_for(RETRY_INTERVAL);
        }
    }
    return Status::OK();
}

SharedPeer BuildSharedPeer(const Registration &target, const Registration &owner, bool includeRegions)
{
    SharedPeer peer{owner.rank, std::max(target.generation, owner.generation), owner.segment, owner.identity, {}};
    if (includeRegions) {
        peer.regions.reserve(owner.regions.size());
        for (const auto &entry : owner.regions) {
            const auto &region = entry.second;
            if (!region.revoking) {
                peer.regions.push_back(region);
            }
        }
    }
    return peer;
}

PeerCallback UnshareRequest(const Registration &target, const Registration &owner)
{
    const auto generation = std::max(target.generation, owner.generation);
    return {{target.callback, EncodePeerRequest({target.id, owner.rank, generation})}, target.rank, owner.rank};
}

bool InRankWindow(uint64_t base, uint64_t window, uint32_t rank, const GvaRegion &region)
{
    const auto start = base + rank * window;
    return window && region.address >= start && region.address - start < window &&
           region.size <= window - (region.address - start);
}

bool RegionOverlaps(const std::map<uint64_t, GvaRegion> &regions, const GvaRegion &region)
{
    const auto next = regions.lower_bound(region.address);
    if (next != regions.end() && next->first - region.address < region.size) {
        return true;
    }
    if (next == regions.begin()) {
        return false;
    }
    const auto previous = std::prev(next);
    return region.address - previous->first < previous->second.size;
}

}

struct MemoryEngineServer::Impl {
    ~Impl();
    Status Start(const Config &config, const Endpoint &endpoint);
    Status CheckEndpoint(const Endpoint &endpoint) const;
    Status RegisterRpcHandlers();
    Status HandleRegister(const json &payload, json &reply);
    Status HandleBeginLeave(const json &payload);
    Status Register(const RegisterRequest &request, RegisterResponse &response);
    Status Confirm(const ConfirmRequest &request);
    Status Heartbeat(const RegistrationIdRequest &request);
    Status Cancel(const CancelRequest &request);
    Status Join(const json &payload);
    Status PrepareJoin(const JoinRequest &request, Registration &member, std::vector<Registration> &peers);
    Status SharePeersForJoin(const Registration &member, const std::vector<Registration> &peers);
    Status SendPeersInfoToJoiner(const Registration &member, const std::vector<Registration> &peers,
                                 Clock::time_point deadline);
    Status RollbackJoin(const Registration &member, const std::vector<Registration> &peers);
    Status BeginLeave(const std::string &id);
    Status FinishLeave(const std::string &id);
    Status PrepareLeave(const std::string &id, std::vector<PeerCallback> &calls);
    Status UpdateRegion(const json &payload, bool publish);
    Status PrepareRegion(const std::string &id, const GvaRegion &region, bool publish,
                         std::vector<PeerCallback> &calls);
    void CommitRegion(const std::string &id, GvaRegion &&region, bool publish);
    Status ClaimRank(std::optional<uint32_t> requested, uint32_t &rank);
    void ReleaseRank(uint32_t rank);
    void ResetUnconfirmedLayout();
    void Reap();

    std::mutex mMutex;
    std::atomic<uint64_t> mMutating{0};
    std::unordered_set<std::string> mUpdatingOwners;
    uint64_t mGeneration{};
    Layout mLayout;
    Layout mConfiguredLayout;
    Endpoint mEndpoint;
    Bases mExpectedBases;
    std::optional<Bases> mBases;
    std::unordered_map<std::string, Registration> mRegistrations;
    std::set<uint32_t> mFreeRanks;
    Milliseconds mLease{DEFAULT_LEASE};
    Milliseconds mRegistrationTimeout{DEFAULT_REGISTRATION};
    MemoryEngineRpc mRpc;
    std::jthread mReaper;
};

MemoryEngineServer::Impl::~Impl()
{
    mReaper = std::jthread();
    const auto status = mRpc.Stop();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to stop MemoryEngine server RPC, port:" << mEndpoint.port
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
    }
}

Status MemoryEngineServer::Impl::Start(const Config &config, const Endpoint &endpoint)
{
    CHECK_STATUS(ReadLayout(config, config_keys::CONFIG_SERVER_WORLD_SIZE, config_keys::CONFIG_SERVER_HOST_MEM_MAX,
                            config_keys::CONFIG_SERVER_DEVICE_MEM_MAX, mConfiguredLayout));
    if (!ValidWindow(mConfiguredLayout.hostWindow) || !ValidWindow(mConfiguredLayout.deviceWindow)) {
        LOG(ERROR) << "Invalid MemoryEngine server GVA windows, host window bytes:" << mConfiguredLayout.hostWindow
                   << ", device window bytes:" << mConfiguredLayout.deviceWindow << ".";
        return Status::InvalidArgument("Invalid MemoryEngine server GVA windows");
    }
    const auto host = ReadUnsigned(config.get(config_keys::CONFIG_SERVER_GVA_BASE_HOST, json(0)), UINT64_MAX);
    const auto device = ReadUnsigned(config.get(config_keys::CONFIG_SERVER_GVA_BASE_DEVICE, json(0)), UINT64_MAX);
    if (!host || !device) {
        return Status::InvalidArgument("Invalid MemoryEngine server GVA bases");
    }
    mExpectedBases = {*host, *device};

    mLease = Milliseconds(config.get(config_keys::CONFIG_SERVER_RANK_LEASE_MS, DEFAULT_LEASE.count()));
    mRegistrationTimeout = Milliseconds(config.get(config_keys::CONFIG_JOIN_TIMEOUT_MS, DEFAULT_REGISTRATION.count()));
    if (mLease <= Milliseconds::zero() || mRegistrationTimeout <= Milliseconds::zero() ||
        mLease >= MAX_TIMEOUT || mRegistrationTimeout >= MAX_TIMEOUT) {
        LOG(ERROR) << "Invalid MemoryEngine server timeouts, rank lease ms:" << mLease.count()
                   << ", registration timeout ms:" << mRegistrationTimeout.count() << ".";
        return Status::InvalidArgument("Invalid MemoryEngine server timeouts");
    }
    const auto threads =
        ReadUnsigned(config.get(config_keys::CONFIG_SERVER_RPC_SERVER_THREADS, json(DEFAULT_RPC_THREADS)),
                     std::numeric_limits<size_t>::max());
    if (!threads || !*threads) {
        return Status::InvalidArgument("Invalid MemoryEngine server RPC thread count");
    }

    CHECK_STATUS(RegisterRpcHandlers());
    auto listener = endpoint;
    CHECK_STATUS(checkLocalIpAddress(listener.host, listener.ipv6));
    CHECK_STATUS(mRpc.StartAt(listener.host, listener.port, static_cast<size_t>(*threads)));
    mReaper = std::jthread([this](std::stop_token stop) { RunPeriodic(stop, REAP_INTERVAL, [this] { Reap(); }); });
    return Status::OK();
}

Status MemoryEngineServer::Impl::CheckEndpoint(const Endpoint &endpoint) const
{
    if (mEndpoint.host == endpoint.host && mEndpoint.port == endpoint.port) {
        return Status::OK();
    }
    return Status::InvalidEntry("MemoryEngine server is already listening on another endpoint");
}

Status MemoryEngineServer::Impl::RegisterRpcHandlers()
{
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::Register,
                                 [this](const json &payload, json &reply) { return HandleRegister(payload, reply); }));
    CHECK_STATUS(RegisterRequestHandler(mRpc, MemoryRpc::Confirm, DecodeConfirmRequest,
                                        [this](const ConfirmRequest &request) { return Confirm(request); }));
    CHECK_STATUS(RegisterRequestHandler(mRpc, MemoryRpc::Heartbeat, DecodeRegistrationIdRequest,
                                        [this](const RegistrationIdRequest &request) { return Heartbeat(request); }));
    CHECK_STATUS(RegisterRequestHandler(mRpc, MemoryRpc::Cancel, DecodeCancelRequest,
                                        [this](const CancelRequest &request) { return Cancel(request); }));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::Join,
                                 [this](const json &request, json &) { return Join(request); }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::BeginLeave,
                                 [this](const json &request, json &) { return HandleBeginLeave(request); }, true));
    CHECK_STATUS(RegisterRequestHandler(mRpc, MemoryRpc::FinishLeave, DecodeRegistrationIdRequest,
                                        [this](const RegistrationIdRequest &request) {
        return FinishLeave(request.id);
    }));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::PublishRegion, [this](const json &request, json &) {
        return UpdateRegion(request, true);
    }, true));
    return RegisterHandler(mRpc, MemoryRpc::RevokeRegion, [this](const json &request, json &) {
        return UpdateRegion(request, false);
    }, true);
}

Status MemoryEngineServer::Impl::HandleRegister(const json &payload, json &reply)
{
    RegisterRequest request;
    const auto decodeRet = DecodeRegisterRequest(payload, request);
    if (!decodeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode MemoryEngine Register request, error:" << decodeRet.message() << ".";
        return decodeRet;
    }

    RegisterResponse response;
    const auto status = Register(request, response);
    reply = EncodeRegisterResponse(response, status);
    return status;
}

Status MemoryEngineServer::Impl::HandleBeginLeave(const json &payload)
{
    Mutation mutation(mMutating);
    if (!mutation.Acquired()) {
        return Status::TooManyRequests("MemoryEngine membership operation in progress");
    }
    RegistrationIdRequest request;
    CHECK_STATUS(DecodeRegistrationIdRequest(payload, request));
    return BeginLeave(request.id);
}

Status MemoryEngineServer::Impl::ClaimRank(std::optional<uint32_t> requested, uint32_t &rank)
{
    if (mFreeRanks.empty()) {
        return Status::TooManyRequests("No free MemoryEngine rank");
    }
    const auto freeRank = requested ? mFreeRanks.find(*requested) : mFreeRanks.begin();
    if (freeRank == mFreeRanks.end()) {
        return Status::InvalidEntry("Requested MemoryEngine rank is already in use");
    }
    rank = *freeRank;
    mFreeRanks.erase(freeRank);
    return Status::OK();
}

void MemoryEngineServer::Impl::ReleaseRank(uint32_t rank)
{
    mFreeRanks.emplace(rank);
}

void MemoryEngineServer::Impl::ResetUnconfirmedLayout()
{
    if (!mBases && mRegistrations.empty()) {
        mLayout = {};
        mFreeRanks.clear();
    }
}

Status MemoryEngineServer::Impl::Register(const RegisterRequest &request, RegisterResponse &response)
{
    const auto &layout = request.layout;
    std::lock_guard lock(mMutex);
    if ((mConfiguredLayout.worldSize && mConfiguredLayout.worldSize != layout.worldSize) ||
        (mConfiguredLayout.hostWindow && mConfiguredLayout.hostWindow != layout.hostWindow) ||
        (mConfiguredLayout.deviceWindow && mConfiguredLayout.deviceWindow != layout.deviceWindow)) {
        LOG(ERROR) << "MemoryEngine Register layout differs from server configuration, registration ID:" << request.id
                   << ", client world size:" << layout.worldSize
                   << ", server world size:" << mConfiguredLayout.worldSize
                   << ", client host window bytes:" << layout.hostWindow
                   << ", server host window bytes:" << mConfiguredLayout.hostWindow
                   << ", client device window bytes:" << layout.deviceWindow
                   << ", server device window bytes:" << mConfiguredLayout.deviceWindow << ".";
        return Status::InvalidArgument("MemoryEngine rank layout differs from configured server layout");
    }
    if (mLayout == Layout{}) {
        std::set<uint32_t> freeRanks;
        for (uint32_t rank = 0; rank < layout.worldSize; ++rank) {
            freeRanks.emplace_hint(freeRanks.end(), rank);
        }
        mFreeRanks = std::move(freeRanks);
        mLayout = layout;
    }
    if (layout != mLayout) {
        LOG(ERROR) << "MemoryEngine Register layout differs from cluster, registration ID:" << request.id
                   << ", client world size:" << layout.worldSize << ", cluster world size:" << mLayout.worldSize
                   << ", client host window bytes:" << layout.hostWindow
                   << ", cluster host window bytes:" << mLayout.hostWindow
                   << ", client device window bytes:" << layout.deviceWindow
                   << ", cluster device window bytes:" << mLayout.deviceWindow << ".";
        return Status::InvalidArgument("MemoryEngine rank layout differs from registered server layout");
    }
    response.leaseMs = static_cast<uint64_t>(mLease.count());
    auto found = mRegistrations.find(request.id);
    if (found != mRegistrations.end()) {
        if (found->second.expires <= Clock::now() || found->second.callback != request.callbackAddress ||
            (request.requestedRank && *request.requestedRank != found->second.rank)) {
            LOG(WARNING) << "MemoryEngine Register ID is already in use, registration ID:" << request.id
                         << ", registered rank:" << found->second.rank << ", requested rank:"
                         << (request.requestedRank ? std::to_string(*request.requestedRank) : "auto") << ".";
            return Status::InvalidEntry("MemoryEngine registration ID already in use");
        }
        response.rank = found->second.rank;
        return Status::OK();
    }

    uint32_t rank{};
    const auto status = ClaimRank(request.requestedRank, rank);
    if (!status.ok()) {
        LOG(WARNING) << "MemoryEngine Register could not claim a rank, registration ID:" << request.id
                     << ", requested rank:" << (request.requestedRank ? std::to_string(*request.requestedRank) : "auto")
                     << ", free rank count:" << mFreeRanks.size()
                     << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    const auto expires = Clock::now() + mRegistrationTimeout;
    mRegistrations.emplace(request.id, Registration{rank, request.id, request.callbackAddress, expires});
    response.rank = rank;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Confirm(const ConfirmRequest &request)
{
    const auto &bases = request.bases;
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(request.id);
    if (found == mRegistrations.end() || found->second.rank != request.rank ||
        found->second.expires <= Clock::now()) {
        LOG(WARNING) << "MemoryEngine Confirm registration expired, unknown or mismatched, registration ID:"
                     << request.id << ", reported rank:" << request.rank << ", registered rank:"
                     << (found != mRegistrations.end() ? std::to_string(found->second.rank) : "unknown") << ".";
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    }
    const auto status = ValidateBases(mLayout, bases);
    if (!status.ok()) {
        LOG(ERROR) << "MemoryEngine Confirm has invalid GVA ranges, registration ID:" << request.id
                   << ", rank:" << request.rank << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    if ((mExpectedBases.host && bases.host != mExpectedBases.host) ||
        (mExpectedBases.device && bases.device != mExpectedBases.device)) {
        LOG(ERROR) << "MemoryEngine Confirm GVA bases differ from server configuration, registration ID:"
                   << request.id << ", rank:" << request.rank << ".";
        return Status::InvalidArgument("MemFabric GVA bases differ from configured server bases");
    }
    if (mBases && bases != *mBases) {
        LOG(ERROR) << "MemoryEngine Confirm GVA bases differ from cluster, registration ID:" << request.id
                   << ", rank:" << request.rank << ".";
        return Status::InvalidArgument("MemFabric GVA bases differ from the cluster bases");
    }
    mBases = bases;
    if (found->second.state == MemberState::Pending) {
        found->second.state = MemberState::Registered;
    }
    found->second.expires = Clock::now() + mLease;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Heartbeat(const RegistrationIdRequest &request)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(request.id);
    const auto now = Clock::now();
    if (found == mRegistrations.end()) {
        LOG_EVERY_N(WARNING, HEARTBEAT_LOG_EVERY)
            << "MemoryEngine Heartbeat registration unknown, registration ID:" << request.id << ", rank:unknown.";
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    }
    const auto &member = found->second;
    if (member.state == MemberState::Pending || member.expires <= now) {
        LOG_EVERY_N(WARNING, HEARTBEAT_LOG_EVERY)
            << "MemoryEngine Heartbeat rejected, registration ID:" << request.id << ", rank:" << member.rank
            << ", reason:" << (member.expires <= now ? "lease expired" : "registration pending") << ".";
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    }
    found->second.expires = now + mLease;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Cancel(const CancelRequest &request)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(request.id);
    if (found != mRegistrations.end() && found->second.callback == request.callbackAddress) {
        if (found->second.state != MemberState::Pending && found->second.state != MemberState::Registered) {
            LOG(WARNING) << "MemoryEngine Cancel requires unsharing, registration ID:" << request.id
                         << ", rank:" << found->second.rank << ".";
            return Status::InvalidEntry("Unshare joined MemoryEngine rank before canceling");
        }
        ReleaseRank(found->second.rank);
        mRegistrations.erase(found);
        ResetUnconfirmedLayout();
    }
    return Status::OK();
}

void MemoryEngineServer::Impl::Reap()
{
    Mutation mutation(mMutating);
    if (!mutation.Acquired()) {
        return;
    }
    const auto now = Clock::now();
    std::vector<std::pair<std::string, uint32_t>> expired;
    {
        std::lock_guard lock(mMutex);
        for (const auto &[id, member] : mRegistrations) {
            if (member.expires <= now) {
                expired.emplace_back(id, member.rank);
            }
        }
    }
    for (const auto &[id, rank] : expired) {
        auto status = BeginLeave(id);
        if (status.ok()) {
            status = FinishLeave(id);
        }
        if (!status.ok()) {
            LOG(ERROR) << "Could not reclaim expired MemoryEngine registration, registration ID:" << id
                       << ", rank:" << rank << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        }
    }
}

Status MemoryEngineServer::Impl::PrepareLeave(const std::string &id, std::vector<PeerCallback> &calls)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found == mRegistrations.end() || found->second.leavePrepared) {
        return Status::OK();
    }
    auto &member = found->second;
    member.state = MemberState::Leaving;
    if (!member.generation) {
        return Status::OK();
    }

    const auto now = Clock::now();
    for (const auto &[peerId, peer] : mRegistrations) {
        if (peerId == id || !peer.generation || peer.expires <= now) {
            continue;
        }
        calls.push_back(UnshareRequest(peer, member));
    }
    return Status::OK();
}

Status MemoryEngineServer::Impl::BeginLeave(const std::string &id)
{
    std::vector<PeerCallback> calls;
    CHECK_STATUS(PrepareLeave(id, calls));
    const auto deadline = Clock::now() + RPC_TIMEOUT;
    CHECK_STATUS(SendToPeersInParallel(mRpc, MemoryRpc::UnsharePeer, calls, deadline));
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found != mRegistrations.end()) {
        found->second.leavePrepared = true;
    }
    return Status::OK();
}

Status MemoryEngineServer::Impl::FinishLeave(const std::string &id)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found == mRegistrations.end()) {
        return Status::OK();
    }
    if (!found->second.leavePrepared) {
        LOG(WARNING) << "MemoryEngine FinishLeave requires unsharing, registration ID:" << id
                     << ", rank:" << found->second.rank << ".";
        return Status::InvalidEntry("MemoryEngine rank has not finished unsharing");
    }
    ReleaseRank(found->second.rank);
    mRegistrations.erase(found);
    ResetUnconfirmedLayout();
    return Status::OK();
}

Status MemoryEngineServer::Impl::PrepareJoin(const JoinRequest &request, Registration &member,
                                             std::vector<Registration> &peers)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(request.id);
    if (found == mRegistrations.end() || found->second.state == MemberState::Pending ||
        found->second.state == MemberState::Leaving ||
        found->second.expires <= Clock::now()) {
        LOG(WARNING) << "MemoryEngine Join registration unavailable, registration ID:" << request.id << ", rank:"
                     << (found != mRegistrations.end() ? std::to_string(found->second.rank) : "unknown") << ".";
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    }

    auto &current = found->second;
    if (current.state == MemberState::Joined) {
        if (current.segment != request.segment || current.identity != request.identity) {
            LOG(WARNING) << "MemoryEngine Join rank already uses another segment, registration ID:" << request.id
                         << ", rank:" << current.rank << ", generation:" << current.generation << ".";
            return Status::InvalidEntry("MemoryEngine rank already joined with another segment");
        }
        member = current;
        return Status::OK();
    }

    for (const auto &[peerId, peer] : mRegistrations) {
        if (peerId != request.id && peer.state == MemberState::Joined) {
            peers.push_back(peer);
        }
    }
    if (current.state == MemberState::Registered) {
        current.segment = request.segment;
        current.identity = request.identity;
        current.generation = ++mGeneration;
    }
    member = current;
    current.state = MemberState::Joining;
    return Status::OK();
}

Status MemoryEngineServer::Impl::SharePeersForJoin(const Registration &member, const std::vector<Registration> &peers)
{
    if (peers.empty()) {
        return Status::OK();
    }

    const auto deadline = Clock::now() + mRegistrationTimeout;
    std::vector<PeerCallback> calls;
    calls.reserve(peers.size());
    for (const auto &peer : peers) {
        auto request = EncodeSharePeerRequest({peer.id, BuildSharedPeer(peer, member, false)});
        calls.push_back({{peer.callback, std::move(request)}, peer.rank, member.rank});
    }

    const auto status = SendToPeersInParallel(mRpc, MemoryRpc::SharePeer, calls, deadline);
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to notify MemoryEngine peers of Join, registration ID:" << member.id
                   << ", rank:" << member.rank << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    return SendPeersInfoToJoiner(member, peers, deadline);
}

Status MemoryEngineServer::Impl::SendPeersInfoToJoiner(const Registration &member,
                                                       const std::vector<Registration> &peers,
                                                       Clock::time_point deadline)
{
    PeerCall call{member.callback, {}};
    {
        SharePeersRequest request{member.id, {}};
        request.peers.reserve(peers.size());
        for (const auto &peer : peers) {
            request.peers.push_back(BuildSharedPeer(member, peer, true));
        }
        call.request = EncodeSharePeersRequest(request);
    }

    while (Clock::now() < deadline) {
        const auto rpcDeadline = std::min(deadline, Clock::now() + RPC_TIMEOUT);
        auto status = CallAsync(mRpc, MemoryRpc::SharePeers, call, rpcDeadline).get();
        if (!Retryable(status)) {
            if (!status.ok()) [[unlikely]] {
                LOG(ERROR) << "Failed to share existing MemoryEngine peers with joining member, registration ID:"
                           << member.id << ", rank:" << member.rank << ", peer count:" << peers.size()
                           << ", status:" << static_cast<uint16_t>(status.code()) << ".";
            }
            return status;
        }
        std::this_thread::sleep_until(std::min(deadline, Clock::now() + RETRY_INTERVAL));
    }

    LOG(ERROR) << "Timed out sharing existing MemoryEngine peers with joining member, registration ID:" << member.id
               << ", rank:" << member.rank << ", peer count:" << peers.size() << ".";
    return Status::TimedOut("MemoryEngine callbacks did not settle");
}

Status MemoryEngineServer::Impl::RollbackJoin(const Registration &member, const std::vector<Registration> &peers)
{
    std::vector<PeerCallback> calls;
    calls.reserve(peers.size() + peers.size());
    for (const auto &peer : peers) {
        calls.push_back(UnshareRequest(peer, member));
        calls.push_back(UnshareRequest(member, peer));
    }
    const auto deadline = Clock::now() + RPC_TIMEOUT;
    auto status = SendToPeersInParallel(mRpc, MemoryRpc::UnsharePeer, calls, deadline);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to roll back MemoryEngine join, registration ID:" << member.id
                   << ", rank:" << member.rank << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    std::lock_guard lock(mMutex);
    mRegistrations.at(member.id).state = MemberState::Registered;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Join(const json &payload)
{
    Mutation mutation(mMutating);
    if (!mutation.Acquired()) {
        return Status::TooManyRequests("MemoryEngine membership operation in progress");
    }
    JoinRequest request;
    auto status = DecodeJoinRequest(payload, request);
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode MemoryEngine Join request, error:" << status.message() << ".";
        return status;
    }

    Registration member;
    std::vector<Registration> peers;
    status = PrepareJoin(request, member, peers);
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to prepare Join, request ID:" << request.id << ", segment:" << request.segment
                   << ", status:" << status.message() << ".";
        return status;
    }

    if (member.state == MemberState::Joined) {
        return Status::OK();
    }
    if (member.state == MemberState::Joining) {
        CHECK_STATUS(RollbackJoin(member, peers));
        peers.clear();
        CHECK_STATUS(PrepareJoin(request, member, peers));
    }

    status = SharePeersForJoin(member, peers);
    if (status.ok()) {
        std::lock_guard lock(mMutex);
        auto &current = mRegistrations.at(request.id);
        if (current.expires > Clock::now()) {
            current.state = MemberState::Joined;
            return status;
        }
        LOG(WARNING) << "MemoryEngine lease expired during Join, registration ID:" << request.id
                     << ", rank:" << current.rank << ".";
        status = Status::InvalidEntry("MemoryEngine registration expired during join");
    }
    const auto cleanup = RollbackJoin(member, peers);
    return cleanup.ok() ? status : cleanup;
}

Status MemoryEngineServer::Impl::PrepareRegion(const std::string &id, const GvaRegion &region, bool publish,
                                               std::vector<PeerCallback> &calls)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    const auto now = Clock::now();
    if (found == mRegistrations.end() || found->second.state != MemberState::Joined || found->second.expires <= now) {
        LOG(WARNING) << "MemoryEngine region owner is not joined or expired, registration ID:" << id << ", rank:"
                     << (found != mRegistrations.end() ? std::to_string(found->second.rank) : "unknown")
                     << ", bytes:" << region.size << ".";
        return Status::InvalidEntry("MemoryEngine owner is not joined");
    }
    auto &owner = found->second;
    if (!InRankWindow(mBases->host, mLayout.hostWindow, owner.rank, region) &&
        !InRankWindow(mBases->device, mLayout.deviceWindow, owner.rank, region)) {
        LOG(ERROR) << "Region lies outside the rank window, registration ID:" << id
                   << ", rank:" << owner.rank << ", bytes:" << region.size << ".";
        return Status::InvalidArgument("MemoryEngine region lies outside the owner rank window");
    }

    const auto existing = owner.regions.find(region.address);
    if (publish && owner.revokedRegions.contains(region.address)) {
        LOG(WARNING) << "Region was already revoked, registration ID:" << id
                     << ", rank:" << owner.rank << ", bytes:" << region.size << ".";
        return Status::InvalidEntry("MemoryEngine region was already revoked");
    }
    if (existing != owner.regions.end() &&
        (existing->second.size != region.size || (publish && existing->second.descriptor != region.descriptor))) {
        LOG(ERROR) << "Region metadata conflicts with existing allocation, registration ID:" << id
                   << ", rank:" << owner.rank << ", bytes:" << region.size
                   << ", registered bytes:" << existing->second.size << ".";
        return Status::InvalidArgument("Conflicting MemoryEngine region metadata");
    }
    if (publish && existing != owner.regions.end()) {
        return Status::OK();
    }
    if (publish && RegionOverlaps(owner.regions, region)) {
        LOG(ERROR) << "Region overlaps an existing allocation, registration ID:" << id
                   << ", rank:" << owner.rank << ", bytes:" << region.size << ".";
        return Status::InvalidArgument("MemoryEngine region overlaps an existing allocation");
    }
    if (!publish) {
        owner.revokedRegions.insert(region.address);
        if (existing != owner.regions.end()) {
            existing->second.revoking = true;
        }
    }

    for (const auto &[peerId, peer] : mRegistrations) {
        const bool available = publish ? peer.state == MemberState::Joined : peer.generation && peer.expires > now;
        if (peerId == id || !available) {
            continue;
        }
        const PeerRequest request{peerId, owner.rank, std::max(peer.generation, owner.generation)};
        calls.push_back({{peer.callback, EncodePeerRegionRequest(request, region)}, peer.rank, owner.rank});
    }
    return Status::OK();
}

void MemoryEngineServer::Impl::CommitRegion(const std::string &id, GvaRegion &&region, bool publish)
{
    std::lock_guard lock(mMutex);
    auto &regions = mRegistrations.at(id).regions;
    if (publish) {
        regions.try_emplace(region.address, std::move(region));
    } else {
        regions.erase(region.address);
    }
}

Status MemoryEngineServer::Impl::UpdateRegion(const json &payload, bool publish)
{
    Mutation mutation(mMutating, false);
    if (!mutation.Acquired()) {
        return Status::TooManyRequests("MemoryEngine region operation in progress");
    }
    RegionRequest request;
    const auto decodeRet = DecodeRegionRequest(payload, publish, request);
    if (!decodeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to decode MemoryEngine region update request, error:" << decodeRet.message() << ".";
        return decodeRet;
    }

    const auto &id = request.id;
    const auto &region = request.region;
    OwnerMutation owner(mMutex, mUpdatingOwners, id);
    if (!owner.Acquired()) {
        return Status::TooManyRequests("MemoryEngine owner region operation in progress");
    }
    std::vector<PeerCallback> peers;
    CHECK_STATUS(PrepareRegion(id, region, publish, peers));
    const auto op = publish ? MemoryRpc::MapRegion : MemoryRpc::UnmapRegion;
    auto status = SendToPeersInParallel(mRpc, op, peers, Clock::now() + RPC_TIMEOUT);
    if (!status.ok()) {
        if (publish) {
            for (auto &peer : peers) {
                ClearRegionDescriptor(peer.call.request);
            }
            const auto deadline = Clock::now() + RPC_TIMEOUT;
            const auto cleanup = SendToPeersInParallel(mRpc, MemoryRpc::UnmapRegion, peers, deadline);
            if (!cleanup.ok()) {
                return cleanup;
            }
        }
        return status;
    }
    CommitRegion(id, std::move(request.region), publish);
    return Status::OK();
}

MemoryEngineServer &MemoryEngineServer::Instance()
{
    static MemoryEngineServer instance;
    return instance;
}

MemoryEngineServer::MemoryEngineServer() = default;

MemoryEngineServer::~MemoryEngineServer() = default;

Status MemoryEngineServer::Start(const Config &config)
{
    const auto snapshot = config.freeze();
    Endpoint endpoint;
    const auto parsed = ParseEndpoint(snapshot->get(config_keys::CONFIG_SERVER_URL, ""), endpoint);
    if (!parsed.ok()) {
        LOG(ERROR) << "Invalid MemoryEngine server endpoint, config:" << config_keys::CONFIG_SERVER_URL
                   << ", status:" << static_cast<uint16_t>(parsed.code()) << ".";
        return parsed;
    }

    std::lock_guard lock(mLifecycleMutex);
    if (mImpl) {
        return mImpl->CheckEndpoint(endpoint);
    }

    auto impl = std::unique_ptr<Impl>(new (std::nothrow) Impl);
    if (!impl) [[unlikely]] {
        return Status::InternalError("Could not allocate MemoryEngineServer state");
    }
    const auto status = impl->Start(*snapshot, endpoint);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start MemoryEngine server, port:" << endpoint.port
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }

    impl->mEndpoint = std::move(endpoint);
    mImpl = std::move(impl);
    return Status::OK();
}

Status MemoryEngineServer::Start()
{
    Config config;
    CHECK_STATUS(LoadMemoryEngineConfig(config));
    return Start(config);
}

void MemoryEngineServer::Stop()
{
    std::lock_guard lock(mLifecycleMutex);
    mImpl.reset();
}

}
}
