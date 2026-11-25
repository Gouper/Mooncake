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
#include "../common/memory_engine_common.h"
#include "tent/memory-engine/memory_engine_server.h"

namespace mooncake {
namespace tent {
namespace {

constexpr auto DEFAULT_LEASE = Milliseconds(20000);
constexpr auto REAP_INTERVAL = Milliseconds(1000);
constexpr size_t DEFAULT_RPC_THREADS = 4;

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

Status Fanout(CoroRpcAgent &rpc, MemoryRpc op, std::vector<PeerCall> peers, Clock::time_point deadline)
{
    while (!peers.empty()) {
        const auto rpcDeadline = std::min(deadline, Clock::now() + RPC_TIMEOUT);
        std::vector<std::future<Status>> calls;
        calls.reserve(peers.size());
        for (const auto &peer : peers) {
            calls.emplace_back(CallAsync(rpc, op, peer, rpcDeadline));
        }

        Status failure;
        size_t pending = 0;
        for (size_t index = 0; index < calls.size(); ++index) {
            auto status = calls[index].get();
            if (Retryable(status)) {
                if (pending != index) {
                    peers[pending] = std::move(peers[index]);
                }
                ++pending;
            } else if (!status.ok() && failure.ok()) {
                failure = std::move(status);
            }
        }
        if (!failure.ok()) {
            return failure;
        }
        peers.resize(pending);
        if (pending && Clock::now() >= deadline) {
            return Status::TimedOut("MemoryEngine callbacks did not settle");
        }
        if (pending) {
            std::this_thread::sleep_for(RETRY_INTERVAL);
        }
    }
    return Status::OK();
}

json ShareRequest(const Registration &target, const Registration &owner, bool mapRegions)
{
    SharePeerRequest request{{target.id, owner.rank, std::max(target.generation, owner.generation)},
                             owner.segment, owner.identity, {}};
    if (mapRegions) {
        request.regions.reserve(owner.regions.size());
        for (const auto &[address, region] : owner.regions) {
            if (!region.revoking) {
                request.regions.push_back({address, region.size});
            }
        }
    }
    return EncodeSharePeerRequest(request);
}

PeerCall UnshareRequest(const Registration &target, const Registration &owner)
{
    return {target.callback, EncodePeerRequest({target.id, owner.rank, std::max(target.generation, owner.generation)})};
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
    Status BeginJoin(const JoinRequest &request, Registration &member, std::vector<Registration> &peers);
    Status Share(const Registration &member, const std::vector<Registration> &peers);
    Status RollbackJoin(const Registration &member, const std::vector<Registration> &peers);
    Status BeginLeave(const std::string &id);
    Status FinishLeave(const std::string &id);
    Status PrepareLeave(const std::string &id, std::vector<PeerCall> &calls);
    Status UpdateRegion(const json &payload, bool publish);
    Status PrepareRegion(const std::string &id, const GvaRegion &region, bool publish, std::vector<PeerCall> &calls);
    void CommitRegion(const std::string &id, const GvaRegion &region, bool publish);
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
    CoroRpcAgent mRpc;
    std::jthread mReaper;
};

MemoryEngineServer::Impl::~Impl()
{
    mReaper = std::jthread();
    const auto status = mRpc.stop();
    if (!status.ok()) {
        LOG(WARNING) << "Failed to stop MemoryEngine server RPC.";
    }
}

Status MemoryEngineServer::Impl::Start(const Config &config, const Endpoint &endpoint)
{
    CHECK_STATUS(ReadLayout(config, config_keys::CONFIG_SERVER_WORLD_SIZE, config_keys::CONFIG_SERVER_HOST_MEM_MAX,
                            config_keys::CONFIG_SERVER_DEVICE_MEM_MAX, mConfiguredLayout));
    if (!ValidWindow(mConfiguredLayout.hostWindow) || !ValidWindow(mConfiguredLayout.deviceWindow)) {
        return Status::InvalidArgument("Invalid MemoryEngine server GVA windows");
    }
    const auto host = ReadUnsigned(config.get(config_keys::CONFIG_SERVER_GVA_BASE_HOST, json(0)), UINT64_MAX);
    const auto device = ReadUnsigned(config.get(config_keys::CONFIG_SERVER_GVA_BASE_DEVICE, json(0)), UINT64_MAX);
    if (!host || !device) {
        return Status::InvalidArgument("Invalid MemoryEngine server GVA bases");
    }
    mExpectedBases = {*host, *device};

    mLease = Milliseconds(config.get(config_keys::CONFIG_SERVER_HEARTBEAT_TIMEOUT_MS, DEFAULT_LEASE.count()));
    mRegistrationTimeout = Milliseconds(config.get(config_keys::CONFIG_JOIN_TIMEOUT_MS, DEFAULT_REGISTRATION.count()));
    if (mLease <= Milliseconds::zero() || mRegistrationTimeout <= Milliseconds::zero() ||
        mLease >= MAX_TIMEOUT || mRegistrationTimeout >= MAX_TIMEOUT) {
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
    CHECK_STATUS(DecodeRegisterRequest(payload, request));

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
        return Status::InvalidArgument("MemoryEngine rank layout differs from registered server layout");
    }
    response.leaseMs = static_cast<uint64_t>(mLease.count());
    auto found = mRegistrations.find(request.id);
    if (found != mRegistrations.end()) {
        if (found->second.expires <= Clock::now() || found->second.callback != request.callbackAddress ||
            (request.requestedRank && *request.requestedRank != found->second.rank)) {
            return Status::InvalidEntry("MemoryEngine registration ID already in use");
        }
        response.rank = found->second.rank;
        return Status::OK();
    }

    uint32_t rank{};
    CHECK_STATUS(ClaimRank(request.requestedRank, rank));
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
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    }
    CHECK_STATUS(ValidateBases(mLayout, bases));
    if ((mExpectedBases.host && bases.host != mExpectedBases.host) ||
        (mExpectedBases.device && bases.device != mExpectedBases.device)) {
        return Status::InvalidArgument("MemFabric GVA bases differ from configured server bases");
    }
    if (mBases && bases != *mBases) {
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
    if (found == mRegistrations.end() || found->second.state == MemberState::Pending || found->second.expires <= now) {
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
    std::vector<std::string> expired;
    {
        std::lock_guard lock(mMutex);
        for (const auto &[id, member] : mRegistrations) {
            if (member.expires <= now) {
                expired.push_back(id);
            }
        }
    }
    for (const auto &id : expired) {
        auto status = BeginLeave(id);
        if (status.ok()) {
            status = FinishLeave(id);
        }
        if (!status.ok()) {
            LOG(WARNING) << "Could not reclaim expired MemoryEngine registration, status:"
                         << static_cast<uint16_t>(status.code()) << ".";
        }
    }
}

Status MemoryEngineServer::Impl::PrepareLeave(const std::string &id, std::vector<PeerCall> &calls)
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
    std::vector<PeerCall> calls;
    CHECK_STATUS(PrepareLeave(id, calls));
    CHECK_STATUS(Fanout(mRpc, MemoryRpc::UnsharePeer, std::move(calls), Clock::now() + RPC_TIMEOUT));
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
        return Status::InvalidEntry("MemoryEngine rank has not finished unsharing");
    }
    ReleaseRank(found->second.rank);
    mRegistrations.erase(found);
    ResetUnconfirmedLayout();
    return Status::OK();
}

Status MemoryEngineServer::Impl::BeginJoin(const JoinRequest &request, Registration &member,
                                           std::vector<Registration> &peers)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(request.id);
    if (found == mRegistrations.end() || found->second.state == MemberState::Pending ||
        found->second.state == MemberState::Leaving ||
        found->second.expires <= Clock::now()) {
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    }
    auto &current = found->second;
    if (current.state == MemberState::Joined) {
        if (current.segment != request.segment || current.identity != request.identity) {
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

Status MemoryEngineServer::Impl::Share(const Registration &member, const std::vector<Registration> &peers)
{
    std::vector<PeerCall> calls;
    calls.reserve(peers.size() + peers.size());
    const auto deadline = Clock::now() + mRegistrationTimeout;
    for (const auto &peer : peers) {
        calls.push_back({peer.callback, ShareRequest(peer, member, false)});
        calls.push_back({member.callback, ShareRequest(member, peer, false)});
    }
    CHECK_STATUS(Fanout(mRpc, MemoryRpc::SharePeer, std::move(calls), deadline));
    calls.clear();
    for (const auto &peer : peers) {
        if (!peer.regions.empty()) {
            calls.push_back({member.callback, ShareRequest(member, peer, true)});
        }
    }
    return Fanout(mRpc, MemoryRpc::SharePeer, std::move(calls), deadline);
}

Status MemoryEngineServer::Impl::RollbackJoin(const Registration &member, const std::vector<Registration> &peers)
{
    std::vector<PeerCall> calls;
    calls.reserve(peers.size() + peers.size());
    for (const auto &peer : peers) {
        calls.push_back(UnshareRequest(peer, member));
        calls.push_back(UnshareRequest(member, peer));
    }
    auto status = Fanout(mRpc, MemoryRpc::UnsharePeer, std::move(calls), Clock::now() + RPC_TIMEOUT);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to roll back MemoryEngine join, rank:" << member.rank << ".";
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
    CHECK_STATUS(DecodeJoinRequest(payload, request));
    Registration member;
    std::vector<Registration> peers;
    CHECK_STATUS(BeginJoin(request, member, peers));
    if (member.state == MemberState::Joined) {
        return Status::OK();
    }
    if (member.state == MemberState::Joining) {
        CHECK_STATUS(RollbackJoin(member, peers));
        peers.clear();
        CHECK_STATUS(BeginJoin(request, member, peers));
    }

    auto status = Share(member, peers);
    if (status.ok()) {
        std::lock_guard lock(mMutex);
        auto &current = mRegistrations.at(request.id);
        if (current.expires > Clock::now()) {
            current.state = MemberState::Joined;
            return status;
        }
        status = Status::InvalidEntry("MemoryEngine registration expired during join");
    }
    const auto cleanup = RollbackJoin(member, peers);
    return cleanup.ok() ? status : cleanup;
}

Status MemoryEngineServer::Impl::PrepareRegion(const std::string &id, const GvaRegion &region, bool publish,
                                               std::vector<PeerCall> &calls)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    const auto now = Clock::now();
    if (found == mRegistrations.end() || found->second.state != MemberState::Joined || found->second.expires <= now) {
        return Status::InvalidEntry("MemoryEngine owner is not joined");
    }
    auto &owner = found->second;
    if (!InRankWindow(mBases->host, mLayout.hostWindow, owner.rank, region) &&
        !InRankWindow(mBases->device, mLayout.deviceWindow, owner.rank, region)) {
        return Status::InvalidArgument("MemoryEngine region lies outside the owner rank window");
    }

    const auto existing = owner.regions.find(region.address);
    if (publish && owner.revokedRegions.contains(region.address)) {
        return Status::InvalidEntry("MemoryEngine region was already revoked");
    }
    if (existing != owner.regions.end() && existing->second.size != region.size) {
        return Status::InvalidArgument("Conflicting MemoryEngine region size");
    }
    if (publish && existing != owner.regions.end()) {
        return Status::OK();
    }
    if (publish && RegionOverlaps(owner.regions, region)) {
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
        const PeerRegionRequest request{{peerId, owner.rank, std::max(peer.generation, owner.generation)}, region};
        calls.push_back({peer.callback, EncodePeerRegionRequest(request)});
    }
    return Status::OK();
}

void MemoryEngineServer::Impl::CommitRegion(const std::string &id, const GvaRegion &region, bool publish)
{
    std::lock_guard lock(mMutex);
    auto &regions = mRegistrations.at(id).regions;
    if (publish) {
        regions.try_emplace(region.address, region);
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
    CHECK_STATUS(DecodeRegionRequest(payload, request));
    const auto &id = request.id;
    const auto &region = request.region;
    OwnerMutation owner(mMutex, mUpdatingOwners, id);
    if (!owner.Acquired()) {
        return Status::TooManyRequests("MemoryEngine owner region operation in progress");
    }
    std::vector<PeerCall> peers;
    CHECK_STATUS(PrepareRegion(id, region, publish, peers));
    const auto op = publish ? MemoryRpc::MapRegion : MemoryRpc::UnmapRegion;
    auto status = Fanout(mRpc, op, peers, Clock::now() + RPC_TIMEOUT);
    if (!status.ok()) {
        if (publish) {
            const auto cleanup = Fanout(mRpc, MemoryRpc::UnmapRegion, std::move(peers), Clock::now() + RPC_TIMEOUT);
            if (!cleanup.ok()) {
                return cleanup;
            }
        }
        return status;
    }
    CommitRegion(id, region, publish);
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
    CHECK_STATUS(ParseEndpoint(snapshot->get(config_keys::CONFIG_SERVER_URL, ""), endpoint));

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
        LOG(ERROR) << "Failed to start MemoryEngine server, status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }

    impl->mEndpoint = std::move(endpoint);
    mImpl = std::move(impl);
    return Status::OK();
}

Status MemoryEngineServer::Start()
{
    Config config;
    CHECK_STATUS(ConfigHelper().loadFromEnv(config));
    return Start(config);
}

void MemoryEngineServer::Stop()
{
    std::lock_guard lock(mLifecycleMutex);
    mImpl.reset();
}

}
}
