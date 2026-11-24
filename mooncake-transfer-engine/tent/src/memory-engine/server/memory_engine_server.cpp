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

namespace mooncake::tent {
namespace {

using memory_engine::Bases;
using memory_engine::CallAsync;
using memory_engine::Clock;
using memory_engine::DEFAULT_REGISTRATION;
using memory_engine::Endpoint;
using memory_engine::GvaRegion;
using memory_engine::Layout;
using memory_engine::MAX_RANKS;
using memory_engine::MAX_TIMEOUT;
using memory_engine::MemoryRpc;
using memory_engine::Milliseconds;
using memory_engine::ParseEndpoint;
using memory_engine::PeerCall;
using memory_engine::ReadLayout;
using memory_engine::ReadRegion;
using memory_engine::ReadUnsigned;
using memory_engine::RegisterHandler;
using memory_engine::RETRY_INTERVAL;
using memory_engine::RPC_TIMEOUT;
using memory_engine::Retryable;
using memory_engine::RunPeriodic;
using memory_engine::ValidWindow;
using memory_engine::ValidateBases;
using memory_engine::ValidateLayout;

namespace config_keys = memory_engine::config_keys;

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

struct JoinDescription {
    std::string id;
    std::string segment;
    std::string identity;
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
    auto regions = json::array();
    if (mapRegions) {
        for (const auto &[address, region] : owner.regions) {
            if (!region.revoking) {
                regions.push_back({{"address", address}, {"size", region.size}});
            }
        }
    }
    return {{"id", target.id}, {"rank", owner.rank}, {"segment", owner.segment}, {"regions", std::move(regions)},
            {"identity", owner.identity},
            {"generation", std::max(target.generation, owner.generation)}};
}

PeerCall UnshareRequest(const Registration &target, const Registration &owner)
{
    return {target.callback, {{"id", target.id}, {"rank", owner.rank},
                              {"generation", std::max(target.generation, owner.generation)}}};
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
    Status Start(const Config &config);
    Status RegisterRpcHandlers();
    Status Register(const json &request, json &response);
    Status Confirm(const json &request);
    Status Heartbeat(const json &request);
    Status Cancel(const json &request);
    Status Join(const json &request);
    Status BeginJoin(const JoinDescription &request, Registration &member, std::vector<Registration> &peers);
    Status Share(const Registration &member, const std::vector<Registration> &peers);
    Status RollbackJoin(const Registration &member, const std::vector<Registration> &peers);
    Status BeginLeave(const std::string &id);
    Status FinishLeave(const std::string &id);
    Status PrepareLeave(const std::string &id, std::vector<PeerCall> &calls);
    Status UpdateRegion(const json &request, bool publish);
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
    Bases mExpectedBases;
    std::optional<Bases> mBases;
    std::unordered_map<std::string, Registration> mRegistrations;
    // Free rank intervals, with exclusive upper bounds.
    std::map<uint32_t, uint32_t> mFreeRanks;
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

Status MemoryEngineServer::Impl::Start(const Config &config)
{
    Endpoint endpoint;
    CHECK_STATUS(ParseEndpoint(config.get(config_keys::CONFIG_SERVER_URL, ""), endpoint));
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
    CHECK_STATUS(checkLocalIpAddress(endpoint.host, endpoint.ipv6));
    CHECK_STATUS(mRpc.StartAt(endpoint.host, endpoint.port, static_cast<size_t>(*threads)));
    mReaper = std::jthread([this](std::stop_token stop) { RunPeriodic(stop, REAP_INTERVAL, [this] { Reap(); }); });
    return Status::OK();
}

Status MemoryEngineServer::Impl::RegisterRpcHandlers()
{
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::Register,
                                 [this](const json &request, json &reply) { return Register(request, reply); }));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::Confirm,
                                 [this](const json &request, json &) { return Confirm(request); }));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::Heartbeat,
                                 [this](const json &request, json &) { return Heartbeat(request); }));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::Cancel,
                                 [this](const json &request, json &) { return Cancel(request); }));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::Join,
                                 [this](const json &request, json &) { return Join(request); }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::BeginLeave, [this](const json &request, json &) {
        Mutation mutation(mMutating);
        if (!mutation.Acquired()) {
            return Status::TooManyRequests("MemoryEngine membership operation in progress");
        }
        return BeginLeave(request.at("id").get<std::string>());
    }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::FinishLeave, [this](const json &request, json &) {
        return FinishLeave(request.at("id").get<std::string>());
    }));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::PublishRegion, [this](const json &request, json &) {
        return UpdateRegion(request, true);
    }, true));
    return RegisterHandler(mRpc, MemoryRpc::RevokeRegion, [this](const json &request, json &) {
        return UpdateRegion(request, false);
    }, true);
}

Status MemoryEngineServer::Impl::ClaimRank(std::optional<uint32_t> requested, uint32_t &rank)
{
    if (mFreeRanks.empty()) {
        return Status::TooManyRequests("No free MemoryEngine rank");
    }
    auto interval = mFreeRanks.begin();
    rank = interval->first;
    if (requested) {
        rank = *requested;
        auto next = mFreeRanks.upper_bound(rank);
        if (next == mFreeRanks.begin() || rank >= std::prev(next)->second) {
            return Status::InvalidEntry("Requested MemoryEngine rank is already in use");
        }
        interval = std::prev(next);
    }
    const auto [begin, end] = *interval;
    if (rank + 1 < end) {
        mFreeRanks.emplace(rank + 1, end);
    }
    if (begin < rank) {
        interval->second = rank;
    } else {
        mFreeRanks.erase(interval);
    }
    return Status::OK();
}

void MemoryEngineServer::Impl::ReleaseRank(uint32_t rank)
{
    auto begin = rank;
    auto end = rank + 1;
    auto next = mFreeRanks.upper_bound(rank);
    if (next != mFreeRanks.begin()) {
        auto previous = std::prev(next);
        if (previous->second == rank) {
            begin = previous->first;
            mFreeRanks.erase(previous);
        }
    }
    if (next != mFreeRanks.end() && end == next->first) {
        end = next->second;
        mFreeRanks.erase(next);
    }
    mFreeRanks.emplace(begin, end);
}

void MemoryEngineServer::Impl::ResetUnconfirmedLayout()
{
    if (!mBases && mRegistrations.empty()) {
        mLayout = {};
        mFreeRanks.clear();
    }
}

Status MemoryEngineServer::Impl::Register(const json &request, json &response)
{
    const auto id = request.at("id").get<std::string>();
    const auto callbackAddress = request.at("callback").get<std::string>();
    Layout layout;
    CHECK_STATUS(ReadLayout(request.at("worldSize"), request.at("hostWindow"), request.at("deviceWindow"), layout));
    CHECK_STATUS(ValidateLayout(layout));
    Endpoint callback;
    CHECK_STATUS(ParseEndpoint(callbackAddress, callback));
    if (id.empty()) {
        return Status::InvalidArgument("Missing MemoryEngine registration ID");
    }
    std::optional<uint32_t> requested;
    if (request.contains("rank")) {
        const auto rank = ReadUnsigned(request.at("rank"), layout.worldSize - 1);
        if (!rank) {
            return Status::InvalidArgument("Requested MemoryEngine rank is out of range");
        }
        requested = static_cast<uint32_t>(*rank);
    }

    std::lock_guard lock(mMutex);
    if ((mConfiguredLayout.worldSize && mConfiguredLayout.worldSize != layout.worldSize) ||
        (mConfiguredLayout.hostWindow && mConfiguredLayout.hostWindow != layout.hostWindow) ||
        (mConfiguredLayout.deviceWindow && mConfiguredLayout.deviceWindow != layout.deviceWindow)) {
        return Status::InvalidArgument("MemoryEngine rank layout differs from configured server layout");
    }
    if (mLayout == Layout{}) {
        mLayout = layout;
        mFreeRanks.emplace(0, layout.worldSize);
    }
    if (layout != mLayout) {
        return Status::InvalidArgument("MemoryEngine rank layout differs from registered server layout");
    }
    response["leaseMs"] = mLease.count();
    auto found = mRegistrations.find(id);
    if (found != mRegistrations.end()) {
        if (found->second.expires <= Clock::now() || found->second.callback != callbackAddress ||
            (requested && *requested != found->second.rank)) {
            return Status::InvalidEntry("MemoryEngine registration ID already in use");
        }
        response["rank"] = found->second.rank;
        return Status::OK();
    }

    uint32_t rank{};
    CHECK_STATUS(ClaimRank(requested, rank));
    mRegistrations.emplace(id, Registration{rank, id, callbackAddress, Clock::now() + mRegistrationTimeout});
    response["rank"] = rank;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Confirm(const json &request)
{
    const auto id = request.at("id").get<std::string>();
    const auto host = ReadUnsigned(request.at("hostBase"), UINT64_MAX);
    const auto device = ReadUnsigned(request.at("deviceBase"), UINT64_MAX);
    const auto rank = ReadUnsigned(request.at("rank"), MAX_RANKS);
    if (!host || !device || !rank) {
        return Status::InvalidArgument("Invalid MemoryEngine confirmation");
    }
    const Bases bases{*host, *device};
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found == mRegistrations.end() || static_cast<uint64_t>(found->second.rank) != *rank ||
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

Status MemoryEngineServer::Impl::Heartbeat(const json &request)
{
    const auto id = request.at("id").get<std::string>();
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    const auto now = Clock::now();
    if (found == mRegistrations.end() || found->second.state == MemberState::Pending || found->second.expires <= now) {
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    }
    found->second.expires = now + mLease;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Cancel(const json &request)
{
    const auto id = request.at("id").get<std::string>();
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found != mRegistrations.end()) {
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

Status MemoryEngineServer::Impl::BeginJoin(const JoinDescription &request, Registration &member,
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

Status MemoryEngineServer::Impl::Join(const json &request)
{
    Mutation mutation(mMutating);
    if (!mutation.Acquired()) {
        return Status::TooManyRequests("MemoryEngine membership operation in progress");
    }
    const JoinDescription description{request.at("id").get<std::string>(), request.at("segment").get<std::string>(),
                                      request.at("identity").get<std::string>()};
    if (description.segment.empty() || description.identity.empty()) {
        return Status::InvalidArgument("Missing MemoryEngine TENT segment or identity");
    }
    Registration member;
    std::vector<Registration> peers;
    CHECK_STATUS(BeginJoin(description, member, peers));
    if (member.state == MemberState::Joined) {
        return Status::OK();
    }
    if (member.state == MemberState::Joining) {
        CHECK_STATUS(RollbackJoin(member, peers));
        peers.clear();
        CHECK_STATUS(BeginJoin(description, member, peers));
    }

    auto status = Share(member, peers);
    if (status.ok()) {
        std::lock_guard lock(mMutex);
        auto &current = mRegistrations.at(description.id);
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
        calls.push_back({peer.callback, {{"id", peerId}, {"rank", owner.rank}, {"address", region.address},
                                         {"size", region.size}, {"generation", std::max(peer.generation,
                                                                                        owner.generation)}}});
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

Status MemoryEngineServer::Impl::UpdateRegion(const json &request, bool publish)
{
    Mutation mutation(mMutating, false);
    if (!mutation.Acquired()) {
        return Status::TooManyRequests("MemoryEngine region operation in progress");
    }
    GvaRegion region;
    if (!ReadRegion(request, region)) {
        return Status::InvalidArgument("Invalid MemoryEngine region");
    }
    const auto id = request.at("id").get<std::string>();
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

MemoryEngineServer::MemoryEngineServer(std::unique_ptr<Impl> impl)
    : mImpl(std::move(impl))
{
}

MemoryEngineServer::~MemoryEngineServer() = default;

Status MemoryEngineServer::Start(const Config &config, std::unique_ptr<MemoryEngineServer> *server)
{
    if (!server || *server) {
        return Status::InvalidArgument("MemoryEngineServer requires an empty output");
    }
    auto impl = std::unique_ptr<Impl>(new (std::nothrow) Impl);
    if (!impl) {
        return Status::InternalError("Could not allocate MemoryEngineServer state");
    }
    auto result = std::unique_ptr<MemoryEngineServer>(new (std::nothrow) MemoryEngineServer(std::move(impl)));
    if (!result) {
        return Status::InternalError("Could not allocate MemoryEngineServer");
    }
    const auto snapshot = config.freeze();
    auto status = result->mImpl->Start(*snapshot);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to start MemoryEngine server, status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    *server = std::move(result);
    return Status::OK();
}

Status MemoryEngineServer::Start(std::unique_ptr<MemoryEngineServer> *server)
{
    Config config;
    CHECK_STATUS(ConfigHelper().loadFromEnv(config));
    return Start(config, server);
}

}
