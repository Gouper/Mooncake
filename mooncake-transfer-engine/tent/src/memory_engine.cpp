// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "tent/common/config.h"
#include "tent/common/utils/ip.h"
#include "tent/rpc/rpc.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/memory_engine.h"

namespace mooncake {
namespace tent {
namespace {

using Clock = std::chrono::steady_clock;
using Milliseconds = std::chrono::milliseconds;

constexpr uint64_t GIB = 1ULL << 30;
constexpr uint64_t MAX_WINDOW = 1ULL << 50;
constexpr auto DEFAULT_HEARTBEAT = Milliseconds(3000);
constexpr auto DEFAULT_LEASE = Milliseconds(20000);
constexpr auto DEFAULT_REGISTRATION = Milliseconds(120000);
constexpr auto REAP_INTERVAL = Milliseconds(1000);
constexpr auto RPC_TIMEOUT = Milliseconds(5000);
constexpr auto RETRY_INTERVAL = Milliseconds(20);
constexpr size_t ALLOCATION_ALIGNMENT = 2UL << 20;
constexpr auto MAX_TIMEOUT = std::chrono::hours(24);
constexpr auto MAX_TIMEOUT_MS = static_cast<uint64_t>(std::chrono::duration_cast<Milliseconds>(MAX_TIMEOUT).count());
constexpr size_t DEFAULT_RPC_THREADS = 4;
constexpr int REGISTRATION_ID_WORDS = 4;
constexpr uint64_t MAX_RANKS = std::numeric_limits<uint32_t>::max();

enum class MemoryRpc {
    Register = 100,
    Confirm,
    Heartbeat,
    Cancel,
    Join,
    SharePeer,
    UnsharePeer,
    BeginLeave,
    FinishLeave,
    PublishRegion,
    RevokeRegion,
    MapRegion,
    UnmapRegion,
};

struct Endpoint {
    std::string host;
    uint16_t port{};
    bool ipv6{};
};

struct Layout {
    uint32_t worldSize{};
    uint64_t hostWindow{};
    uint64_t deviceWindow{};
    bool operator==(const Layout &) const = default;
};

struct Bases {
    uint64_t host{};
    uint64_t device{};
    bool operator==(const Bases &) const = default;
};

struct Region {
    uint64_t address{};
    size_t size{};
    bool revoking = false;
};

enum class MemberState { Pending, Registered, Joining, Joined, Leaving };

struct Registration {
    uint32_t rank{};
    std::string id;
    std::string callback;
    Clock::time_point expires;
    MemberState state{MemberState::Pending};
    std::string segment;
    std::string identity;
    uint64_t generation{};
    std::map<uint64_t, Region> regions;
    std::unordered_set<uint64_t> revokedRegions;
    bool leavePrepared = false;
};

struct PeerCall {
    std::string endpoint;
    json request;
};

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

struct JoinDescription {
    std::string id;
    std::string segment;
    std::string identity;
};

enum class RegionState { Pending, Published, Revoking, Retired };

struct LocalRegion {
    size_t size{};
    bool registered = false;
    RegionState state{RegionState::Pending};
};

class Mutation {
   public:
    explicit Mutation(std::atomic<uint64_t> &active, bool exclusive = true)
        : mActive(active), mWeight(exclusive ? EXCLUSIVE : SHARED), mAcquired(Acquire(exclusive))
    {
    }

    ~Mutation()
    {
        if (mAcquired)
            mActive.fetch_sub(mWeight, std::memory_order_release);
    }

    bool Acquired() const { return mAcquired; }

   private:
    bool Acquire(bool exclusive)
    {
        uint64_t active = 0;
        if (exclusive)
            return mActive.compare_exchange_strong(active, EXCLUSIVE, std::memory_order_acquire);
        active = mActive.load(std::memory_order_relaxed);
        while (active < EXCLUSIVE - SHARED) {
            if (mActive.compare_exchange_weak(active, active + SHARED, std::memory_order_acquire))
                return true;
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

    bool Acquired() const { return mAcquired; }

   private:
    std::mutex &mMutex;
    std::unordered_set<std::string> &mActive;
    const std::string &mId;
    bool mAcquired{};
};

struct ClientOptions {
    Endpoint server;
    std::string callbackHost;
    Layout layout;
    std::optional<uint32_t> requestedRank;
    Milliseconds heartbeat{DEFAULT_HEARTBEAT};
};

std::optional<uint64_t> ReadUnsigned(const json &value, uint64_t max)
{
    if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<int64_t>() < 0))
        return std::nullopt;
    const auto number = value.get<uint64_t>();
    if (number > max)
        return std::nullopt;
    return number;
}

Status ParseEndpoint(std::string url, Endpoint &endpoint)
{
    constexpr std::string_view SCHEME = "rpc://";
    if (url.starts_with(SCHEME))
        url.erase(0, SCHEME.size());
    auto [host, port] = parseHostNameWithPort(url, 0);
    if (host.empty() || !port || host.find_first_of("/ \t\r\n") != std::string::npos)
        return Status::InvalidArgument("Invalid MemoryEngine RPC endpoint");
    endpoint = {host, port, host.find(':') != std::string::npos};
    return Status::OK();
}

bool ValidWindow(uint64_t window)
{
    return window < MAX_WINDOW && window % GIB == 0;
}

Status ValidateLayout(const Layout &layout)
{
    if (!layout.worldSize || !(layout.hostWindow || layout.deviceWindow))
        return Status::InvalidArgument("MemoryEngine requires ranks and at least one GVA window");
    if (!ValidWindow(layout.hostWindow) || !ValidWindow(layout.deviceWindow))
        return Status::InvalidArgument("MemFabric GVA windows must be in whole GiB and below 1 PiB");
    const auto maxWindow = UINT64_MAX / layout.worldSize;
    if (layout.hostWindow > maxWindow || layout.deviceWindow > maxWindow)
        return Status::InvalidArgument("MemoryEngine GVA window size overflows");
    return Status::OK();
}

bool ValidRange(uint64_t base, uint64_t window, uint32_t worldSize)
{
    return window ? base && window <= (UINT64_MAX - base) / worldSize : base == 0;
}

Status ValidateBases(const Layout &layout, const Bases &bases)
{
    if (!ValidRange(bases.host, layout.hostWindow, layout.worldSize) ||
        !ValidRange(bases.device, layout.deviceWindow, layout.worldSize))
        return Status::InvalidArgument("Invalid MemFabric GVA range");
    const auto hostEnd = bases.host + layout.hostWindow * layout.worldSize;
    const auto deviceEnd = bases.device + layout.deviceWindow * layout.worldSize;
    if (bases.host && bases.device && bases.host < deviceEnd && bases.device < hostEnd)
        return Status::InvalidArgument("Overlapping Host and Device GVA windows");
    return Status::OK();
}

Status ReadLayout(const json &worldSize, const json &hostWindow, const json &deviceWindow, Layout &layout)
{
    const auto ranks = ReadUnsigned(worldSize, MAX_RANKS);
    const auto host = ReadUnsigned(hostWindow, MAX_WINDOW);
    const auto device = ReadUnsigned(deviceWindow, MAX_WINDOW);
    if (!ranks || !host || !device)
        return Status::InvalidArgument("Invalid MemoryEngine layout values");
    layout = {static_cast<uint32_t>(*ranks), *host, *device};
    return Status::OK();
}

Status ReadLayout(const Config &config, const std::string &prefix, Layout &layout)
{
    return ReadLayout(config.get(prefix + "world_size", json(0)), config.get(prefix + "host_mem_max", json(0)),
                      config.get(prefix + "device_mem_max", json(0)), layout);
}

Status ReadClientOptions(const Config &config, ClientOptions &options)
{
    if (!config.get("memory_engine/enabled", true))
        return Status::NotImplemented("MemoryEngine is disabled");
    CHECK_STATUS(ParseEndpoint(config.get("memory_engine/server_url", ""), options.server));
    CHECK_STATUS(ReadLayout(config, "memory_engine/", options.layout));
    CHECK_STATUS(ValidateLayout(options.layout));
    if (!config.get("memory_engine/auto_ranking", true)) {
        const auto rank = ReadUnsigned(config.get("memory_engine/local_rank", json()), options.layout.worldSize - 1);
        if (!rank)
            return Status::InvalidArgument("Explicit local_rank is required and must be in range");
        options.requestedRank = static_cast<uint32_t>(*rank);
    }
    options.callbackHost = config.get("memory_engine/client_host", "");
    options.heartbeat = Milliseconds(config.get("memory_engine/heartbeat_interval_ms", DEFAULT_HEARTBEAT.count()));
    if (options.heartbeat <= Milliseconds::zero() || options.heartbeat >= MAX_TIMEOUT)
        return Status::InvalidArgument("Invalid MemoryEngine heartbeat interval");
    return Status::OK();
}

std::string NewRegistrationId()
{
    std::random_device random;
    std::ostringstream id;
    id << std::hex;
    for (int index = 0; index < REGISTRATION_ID_WORDS; ++index)
        id << random() << '-';
    return id.str();
}

Status DecodeReply(const std::string &raw, json &response)
{
    try {
        response = json::parse(raw);
        const auto code = ReadUnsigned(response.at("code"), static_cast<uint16_t>(Status::Code::kMaxCode) - 1);
        if (!code)
            return Status::RpcServiceError("Invalid MemoryEngine RPC status");
        return Status(static_cast<Status::Code>(*code), response.at("message").get<std::string>());
    } catch (const json::exception &) {
        return Status::MalformedJson("Malformed MemoryEngine RPC response");
    }
}

Status Call(CoroRpcAgent &rpc, const std::string &endpoint, MemoryRpc op, const json &request, json &response)
{
    std::string raw;
    CHECK_STATUS(rpc.callOwned(endpoint, static_cast<int>(op), request.dump(), raw));
    return DecodeReply(raw, response);
}

std::future<Status> CallAsync(CoroRpcAgent &rpc, MemoryRpc op, const PeerCall &peer, Clock::time_point deadline)
{
    std::promise<Status> completion;
    auto result = completion.get_future();
    rpc.CallBefore(peer.endpoint, static_cast<int>(op), peer.request.dump(), deadline)
        .start([completion = std::move(completion)](auto &&reply) mutable {
            if (reply.hasError()) {
                completion.set_value(Status::RpcServiceError("MemoryEngine callback failed"));
                return;
            }
            auto [status, wire] = std::move(reply).value();
            json response;
            completion.set_value(status.ok() ? DecodeReply(wire, response) : std::move(status));
        });
    return result;
}

bool Retryable(const Status &status)
{
    return status.IsTooManyRequests() || status.IsNeedsRefreshCache();
}

Status Fanout(CoroRpcAgent &rpc, MemoryRpc op, std::vector<PeerCall> peers, Clock::time_point deadline)
{
    while (!peers.empty()) {
        const auto rpcDeadline = std::min(deadline, Clock::now() + RPC_TIMEOUT);
        std::vector<std::future<Status>> calls;
        calls.reserve(peers.size());
        for (const auto &peer : peers)
            calls.emplace_back(CallAsync(rpc, op, peer, rpcDeadline));

        Status failure;
        size_t pending = 0;
        for (size_t index = 0; index < calls.size(); ++index) {
            auto status = calls[index].get();
            if (Retryable(status)) {
                if (pending != index)
                    peers[pending] = std::move(peers[index]);
                ++pending;
            } else if (!status.ok() && failure.ok()) {
                failure = std::move(status);
            }
        }
        if (!failure.ok())
            return failure;
        peers.resize(pending);
        if (pending && Clock::now() >= deadline)
            return Status::TimedOut("MemoryEngine callbacks did not settle");
        if (pending)
            std::this_thread::sleep_for(RETRY_INTERVAL);
    }
    return Status::OK();
}

Status CallControl(CoroRpcAgent &rpc, MemoryRpc op, const PeerCall &server, Clock::time_point deadline)
{
    do {
        auto status = CallAsync(rpc, op, server, deadline).get();
        if (!Retryable(status))
            return status;
        std::this_thread::sleep_for(RETRY_INTERVAL);
    } while (Clock::now() < deadline);
    return Status::TimedOut("MemoryEngine control request timed out");
}

template <typename Handler>
Status RegisterHandler(CoroRpcAgent &rpc, MemoryRpc op, Handler handler, bool offload = false)
{
    return rpc.registerFunction(static_cast<int>(op), [handler](std::string_view data, std::string &output) {
        json response = json::object();
        Status status;
        try {
            status = handler(json::parse(data), response);
        } catch (const json::exception &) {
            status = Status::MalformedJson("Malformed MemoryEngine RPC request");
        }
        response["code"] = static_cast<uint16_t>(status.code());
        response["message"] = std::string(status.message());
        output = response.dump();
    }, offload);
}

json ShareRequest(const Registration &target, const Registration &owner, bool mapRegions)
{
    auto regions = json::array();
    if (mapRegions) {
        for (const auto &[address, region] : owner.regions) {
            if (!region.revoking)
                regions.push_back({{"address", address}, {"size", region.size}});
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

bool ReadRegion(const json &payload, Region &region)
{
    const auto address = ReadUnsigned(payload.at("address"), UINT64_MAX);
    const auto size = ReadUnsigned(payload.at("size"), std::numeric_limits<size_t>::max());
    if (!address || !*address || !size || !*size || *size > UINT64_MAX - *address)
        return false;
    region = {*address, static_cast<size_t>(*size)};
    return true;
}

bool InRankWindow(uint64_t base, uint64_t window, uint32_t rank, const Region &region)
{
    const auto start = base + rank * window;
    return window && region.address >= start && region.address - start < window &&
           region.size <= window - (region.address - start);
}

bool RegionOverlaps(const std::map<uint64_t, Region> &regions, const Region &region)
{
    const auto next = regions.lower_bound(region.address);
    if (next != regions.end() && next->first - region.address < region.size)
        return true;
    if (next == regions.begin())
        return false;
    const auto previous = std::prev(next);
    return region.address - previous->first < previous->second.size;
}

template <typename Tick>
void RunPeriodic(std::stop_token stop, Milliseconds interval, Tick tick)
{
    std::mutex mutex;
    std::condition_variable_any wake;
    std::unique_lock lock(mutex);
    while (!stop.stop_requested()) {
        wake.wait_for(lock, stop, interval, [] { return false; });
        if (!stop.stop_requested())
            tick();
    }
}

}

struct MemoryEngineServer::Impl {
    ~Impl();
    Status Start(const Config &config);
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
    Status PrepareRegion(const std::string &id, const Region &region, bool publish, std::vector<PeerCall> &calls);
    void CommitRegion(const std::string &id, const Region &region, bool publish);
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
    if (!status.ok())
        LOG(WARNING) << "Failed to stop MemoryEngine server RPC.";
}

Status MemoryEngineServer::Impl::Start(const Config &config)
{
    Endpoint endpoint;
    CHECK_STATUS(ParseEndpoint(config.get("memory_engine/server_url", ""), endpoint));
    CHECK_STATUS(ReadLayout(config, "memory_engine/server/", mConfiguredLayout));
    if (!ValidWindow(mConfiguredLayout.hostWindow) || !ValidWindow(mConfiguredLayout.deviceWindow))
        return Status::InvalidArgument("Invalid MemoryEngine server GVA windows");
    const auto host = ReadUnsigned(config.get("memory_engine/server/gva_base_host", json(0)), UINT64_MAX);
    const auto device = ReadUnsigned(config.get("memory_engine/server/gva_base_device", json(0)), UINT64_MAX);
    if (!host || !device)
        return Status::InvalidArgument("Invalid MemoryEngine server GVA bases");
    mExpectedBases = {*host, *device};

    mLease = Milliseconds(config.get("memory_engine/server/heartbeat_timeout_ms", DEFAULT_LEASE.count()));
    mRegistrationTimeout = Milliseconds(config.get("memory_engine/join_timeout_ms", DEFAULT_REGISTRATION.count()));
    if (mLease <= Milliseconds::zero() || mRegistrationTimeout <= Milliseconds::zero() ||
        mLease >= MAX_TIMEOUT || mRegistrationTimeout >= MAX_TIMEOUT)
        return Status::InvalidArgument("Invalid MemoryEngine server timeouts");
    const auto threads = ReadUnsigned(config.get("memory_engine/server/rpc_server_threads", json(DEFAULT_RPC_THREADS)),
                                      std::numeric_limits<size_t>::max());
    if (!threads || !*threads)
        return Status::InvalidArgument("Invalid MemoryEngine server RPC thread count");

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
        if (!mutation.Acquired())
            return Status::TooManyRequests("MemoryEngine membership operation in progress");
        return BeginLeave(request.at("id").get<std::string>());
    }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::FinishLeave, [this](const json &request, json &) {
        return FinishLeave(request.at("id").get<std::string>());
    }));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::PublishRegion, [this](const json &request, json &) {
        return UpdateRegion(request, true);
    }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::RevokeRegion, [this](const json &request, json &) {
        return UpdateRegion(request, false);
    }, true));
    CHECK_STATUS(checkLocalIpAddress(endpoint.host, endpoint.ipv6));
    CHECK_STATUS(mRpc.StartAt(endpoint.host, endpoint.port, static_cast<size_t>(*threads)));
    mReaper = std::jthread([this](std::stop_token stop) { RunPeriodic(stop, REAP_INTERVAL, [this] { Reap(); }); });
    return Status::OK();
}

Status MemoryEngineServer::Impl::ClaimRank(std::optional<uint32_t> requested, uint32_t &rank)
{
    if (mFreeRanks.empty())
        return Status::TooManyRequests("No free MemoryEngine rank");
    auto interval = mFreeRanks.begin();
    rank = interval->first;
    if (requested) {
        rank = *requested;
        auto next = mFreeRanks.upper_bound(rank);
        if (next == mFreeRanks.begin() || rank >= std::prev(next)->second)
            return Status::InvalidEntry("Requested MemoryEngine rank is already in use");
        interval = std::prev(next);
    }
    const auto [begin, end] = *interval;
    if (rank + 1 < end)
        mFreeRanks.emplace(rank + 1, end);
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
    if (id.empty())
        return Status::InvalidArgument("Missing MemoryEngine registration ID");
    std::optional<uint32_t> requested;
    if (request.contains("rank")) {
        const auto rank = ReadUnsigned(request.at("rank"), layout.worldSize - 1);
        if (!rank)
            return Status::InvalidArgument("Requested MemoryEngine rank is out of range");
        requested = static_cast<uint32_t>(*rank);
    }

    std::lock_guard lock(mMutex);
    if ((mConfiguredLayout.worldSize && mConfiguredLayout.worldSize != layout.worldSize) ||
        (mConfiguredLayout.hostWindow && mConfiguredLayout.hostWindow != layout.hostWindow) ||
        (mConfiguredLayout.deviceWindow && mConfiguredLayout.deviceWindow != layout.deviceWindow))
        return Status::InvalidArgument("MemoryEngine rank layout differs from configured server layout");
    if (mLayout == Layout{}) {
        mLayout = layout;
        mFreeRanks.emplace(0, layout.worldSize);
    }
    if (layout != mLayout)
        return Status::InvalidArgument("MemoryEngine rank layout differs from registered server layout");
    response["leaseMs"] = mLease.count();
    auto found = mRegistrations.find(id);
    if (found != mRegistrations.end()) {
        if (found->second.expires <= Clock::now() || found->second.callback != callbackAddress ||
            (requested && *requested != found->second.rank))
            return Status::InvalidEntry("MemoryEngine registration ID already in use");
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
    if (!host || !device || !rank)
        return Status::InvalidArgument("Invalid MemoryEngine confirmation");
    const Bases bases{*host, *device};
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found == mRegistrations.end() || static_cast<uint64_t>(found->second.rank) != *rank ||
        found->second.expires <= Clock::now())
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    CHECK_STATUS(ValidateBases(mLayout, bases));
    if ((mExpectedBases.host && bases.host != mExpectedBases.host) ||
        (mExpectedBases.device && bases.device != mExpectedBases.device))
        return Status::InvalidArgument("MemFabric GVA bases differ from configured server bases");
    if (mBases && bases != *mBases)
        return Status::InvalidArgument("MemFabric GVA bases differ from the cluster bases");
    mBases = bases;
    if (found->second.state == MemberState::Pending)
        found->second.state = MemberState::Registered;
    found->second.expires = Clock::now() + mLease;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Heartbeat(const json &request)
{
    const auto id = request.at("id").get<std::string>();
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    const auto now = Clock::now();
    if (found == mRegistrations.end() || found->second.state == MemberState::Pending || found->second.expires <= now)
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    found->second.expires = now + mLease;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Cancel(const json &request)
{
    const auto id = request.at("id").get<std::string>();
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found != mRegistrations.end()) {
        if (found->second.state != MemberState::Pending && found->second.state != MemberState::Registered)
            return Status::InvalidEntry("Unshare joined MemoryEngine rank before canceling");
        ReleaseRank(found->second.rank);
        mRegistrations.erase(found);
        ResetUnconfirmedLayout();
    }
    return Status::OK();
}

void MemoryEngineServer::Impl::Reap()
{
    Mutation mutation(mMutating);
    if (!mutation.Acquired())
        return;
    const auto now = Clock::now();
    std::vector<std::string> expired;
    {
        std::lock_guard lock(mMutex);
        for (const auto &[id, member] : mRegistrations) {
            if (member.expires <= now)
                expired.push_back(id);
        }
    }
    for (const auto &id : expired) {
        auto status = BeginLeave(id);
        if (status.ok())
            status = FinishLeave(id);
        if (!status.ok())
            LOG(WARNING) << "Could not reclaim expired MemoryEngine registration, status:"
                         << static_cast<uint16_t>(status.code()) << ".";
    }
}

Status MemoryEngineServer::Impl::PrepareLeave(const std::string &id, std::vector<PeerCall> &calls)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found == mRegistrations.end() || found->second.leavePrepared)
        return Status::OK();
    auto &member = found->second;
    member.state = MemberState::Leaving;
    if (!member.generation)
        return Status::OK();

    const auto now = Clock::now();
    for (const auto &[peerId, peer] : mRegistrations) {
        if (peerId == id || !peer.generation || peer.expires <= now)
            continue;
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
    if (found != mRegistrations.end())
        found->second.leavePrepared = true;
    return Status::OK();
}

Status MemoryEngineServer::Impl::FinishLeave(const std::string &id)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    if (found == mRegistrations.end())
        return Status::OK();
    if (!found->second.leavePrepared)
        return Status::InvalidEntry("MemoryEngine rank has not finished unsharing");
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
        found->second.expires <= Clock::now())
        return Status::InvalidEntry("MemoryEngine registration expired or unknown");
    auto &current = found->second;
    if (current.state == MemberState::Joined) {
        if (current.segment != request.segment || current.identity != request.identity)
            return Status::InvalidEntry("MemoryEngine rank already joined with another segment");
        member = current;
        return Status::OK();
    }

    for (const auto &[peerId, peer] : mRegistrations) {
        if (peerId != request.id && peer.state == MemberState::Joined)
            peers.push_back(peer);
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
        if (!peer.regions.empty())
            calls.push_back({member.callback, ShareRequest(member, peer, true)});
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
    if (!mutation.Acquired())
        return Status::TooManyRequests("MemoryEngine membership operation in progress");
    const JoinDescription description{request.at("id").get<std::string>(), request.at("segment").get<std::string>(),
                                      request.at("identity").get<std::string>()};
    if (description.segment.empty() || description.identity.empty())
        return Status::InvalidArgument("Missing MemoryEngine TENT segment or identity");
    Registration member;
    std::vector<Registration> peers;
    CHECK_STATUS(BeginJoin(description, member, peers));
    if (member.state == MemberState::Joined)
        return Status::OK();
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

Status MemoryEngineServer::Impl::PrepareRegion(const std::string &id, const Region &region, bool publish,
                                               std::vector<PeerCall> &calls)
{
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    const auto now = Clock::now();
    if (found == mRegistrations.end() || found->second.state != MemberState::Joined || found->second.expires <= now)
        return Status::InvalidEntry("MemoryEngine owner is not joined");
    auto &owner = found->second;
    if (!InRankWindow(mBases->host, mLayout.hostWindow, owner.rank, region) &&
        !InRankWindow(mBases->device, mLayout.deviceWindow, owner.rank, region))
        return Status::InvalidArgument("MemoryEngine region lies outside the owner rank window");

    const auto existing = owner.regions.find(region.address);
    if (publish && owner.revokedRegions.contains(region.address))
        return Status::InvalidEntry("MemoryEngine region was already revoked");
    if (existing != owner.regions.end() && existing->second.size != region.size)
        return Status::InvalidArgument("Conflicting MemoryEngine region size");
    if (publish && existing != owner.regions.end())
        return Status::OK();
    if (publish && RegionOverlaps(owner.regions, region))
        return Status::InvalidArgument("MemoryEngine region overlaps an existing allocation");
    if (!publish) {
        owner.revokedRegions.insert(region.address);
        if (existing != owner.regions.end())
            existing->second.revoking = true;
    }

    for (const auto &[peerId, peer] : mRegistrations) {
        const bool available = publish ? peer.state == MemberState::Joined : peer.generation && peer.expires > now;
        if (peerId == id || !available)
            continue;
        calls.push_back({peer.callback, {{"id", peerId}, {"rank", owner.rank}, {"address", region.address},
                                         {"size", region.size}, {"generation", std::max(peer.generation,
                                                                                        owner.generation)}}});
    }
    return Status::OK();
}

void MemoryEngineServer::Impl::CommitRegion(const std::string &id, const Region &region, bool publish)
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
    if (!mutation.Acquired())
        return Status::TooManyRequests("MemoryEngine region operation in progress");
    Region region;
    if (!ReadRegion(request, region))
        return Status::InvalidArgument("Invalid MemoryEngine region");
    const auto id = request.at("id").get<std::string>();
    OwnerMutation owner(mMutex, mUpdatingOwners, id);
    if (!owner.Acquired())
        return Status::TooManyRequests("MemoryEngine owner region operation in progress");
    std::vector<PeerCall> peers;
    CHECK_STATUS(PrepareRegion(id, region, publish, peers));
    const auto op = publish ? MemoryRpc::MapRegion : MemoryRpc::UnmapRegion;
    auto status = Fanout(mRpc, op, peers, Clock::now() + RPC_TIMEOUT);
    if (!status.ok()) {
        if (publish) {
            const auto cleanup = Fanout(mRpc, MemoryRpc::UnmapRegion, std::move(peers), Clock::now() + RPC_TIMEOUT);
            if (!cleanup.ok())
                return cleanup;
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
    if (!server || *server)
        return Status::InvalidArgument("MemoryEngineServer requires an empty output");
    auto impl = std::unique_ptr<Impl>(new (std::nothrow) Impl);
    if (!impl)
        return Status::InternalError("Could not allocate MemoryEngineServer state");
    auto result = std::unique_ptr<MemoryEngineServer>(new (std::nothrow) MemoryEngineServer(std::move(impl)));
    if (!result)
        return Status::InternalError("Could not allocate MemoryEngineServer");
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
    Status DestroyTent();
    Status SharePeer(const json &request);
    Status UnsharePeer(const json &request);
    Status PeerIdentity(const json &request, uint32_t &rank, uint64_t &generation) const;
    Status UnmapPeer(uint32_t rank, PeerMapping &peer);
    Status MapPeerRegions(PeerMapping &peer, const json &regions);
    Status MapPeerRegion(PeerMapping &peer, const Region &region);
    Status UpdatePeerRegion(const json &request, bool map);
    Status PrepareNativePeers();
    Status AllocateLocal(size_t size, MemoryEngineMemType type, uint64_t &address);
    Status UpdateLocalRegion(uint64_t address, size_t size, bool publish);
    Status ReleaseLocal(uint64_t address, LocalRegion &local, bool releaseMemory);
    Status ReconcileAllocation();
    Status RetireLocal(uint64_t address, LocalRegion &local);
    Status DrainRetired(uint64_t requested);
    void StartHeartbeat(Milliseconds interval);

    mutable std::mutex mApiMutex;
    std::mutex mPeerMutex;
    std::shared_ptr<const Config> mConfig;
    ClientOptions mOptions;
    Layout mLayout;
    Bases mBases;
    uint32_t mRank{};
    std::string mServer;
    std::string mId;
    std::string mCallback;
    bool mJoined = false;
    bool mLeaving = false;
    bool mReady = false;
    Milliseconds mJoinTimeout{DEFAULT_REGISTRATION};
    bool mRegistrationAttempted = false;
    std::unordered_map<uint32_t, PeerMapping> mPeers;
    std::unordered_map<uint64_t, LocalRegion> mLocalRegions;
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
        if (!status.ok())
            LOG(WARNING) << "Failed to leave MemoryEngine cluster, rank:" << mRank << ".";
    }
    mHeartbeat = std::jthread();
    const auto stopped = mRpc.stop();
    if (!stopped.ok())
        LOG(WARNING) << "Failed to stop MemoryEngine client RPC, rank:" << mRank << ".";
    mTent.reset();
    const auto status = CancelRegistration();
    if (!status.ok())
        LOG(WARNING) << "Failed to release MemoryEngine registration, rank:" << mRank << ".";
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
    mCallback = buildIpAddrWithPort(host, port, ipv6);
    return Status::OK();
}

Status MemoryEngine::Impl::Register()
{
    {
        std::lock_guard lock(mPeerMutex);
        mId = NewRegistrationId();
        mPeers.clear();
    }
    json request{{"id", mId}, {"worldSize", mLayout.worldSize}, {"hostWindow", mLayout.hostWindow},
                 {"deviceWindow", mLayout.deviceWindow}, {"callback", mCallback}};
    if (mOptions.requestedRank)
        request["rank"] = *mOptions.requestedRank;

    mRegistrationAttempted = true;
    json response;
    CHECK_STATUS(Call(mRpc, mServer, MemoryRpc::Register, request, response));
    try {
        const auto rank = ReadUnsigned(response.at("rank"), mLayout.worldSize - 1);
        if (!rank)
            return Status::RpcServiceError("MemoryEngine server returned a rank outside the layout");
        const auto lease = ReadUnsigned(response.at("leaseMs"), MAX_TIMEOUT_MS);
        if (!lease || static_cast<uint64_t>(mOptions.heartbeat.count()) >= *lease)
            return Status::InvalidArgument("MemoryEngine heartbeat interval must be shorter than the server lease");
        std::lock_guard lock(mPeerMutex);
        mRank = static_cast<uint32_t>(*rank);
    } catch (const json::exception &) {
        return Status::MalformedJson("Invalid MemoryEngine registration reply");
    }
    return Status::OK();
}

Status MemoryEngine::Impl::PeerIdentity(const json &request, uint32_t &rank, uint64_t &generation) const
{
    const auto peer = ReadUnsigned(request.at("rank"), mLayout.worldSize - 1);
    const auto version = ReadUnsigned(request.at("generation"), UINT64_MAX);
    if (!peer || *peer == mRank || !version || !*version || request.at("id") != mId)
        return Status::InvalidArgument("Invalid MemoryEngine peer callback");
    rank = static_cast<uint32_t>(*peer);
    generation = *version;
    return Status::OK();
}

Status MemoryEngine::Impl::MapPeerRegions(PeerMapping &peer, const json &regions)
{
    for (const auto &entry : regions) {
        Region region;
        if (!ReadRegion(entry, region))
            return Status::InvalidArgument("Invalid MemoryEngine mapped region");
        CHECK_STATUS(MapPeerRegion(peer, region));
    }
    return Status::OK();
}

Status MemoryEngine::Impl::MapPeerRegion(PeerMapping &peer, const Region &region)
{
    auto &mapped = peer.regions.try_emplace(region.address, RemoteRegion{region.size}).first->second;
    if (mapped.revoked || mapped.size != region.size)
        return Status::InvalidEntry("MemoryEngine region was revoked or has a conflicting size");
    if (mapped.mapped)
        return Status::OK();
    CHECK_STATUS(mTent->MapMemFabricRegion(peer.segment, reinterpret_cast<void *>(region.address), region.size));
    mapped.mapped = true;
    return Status::OK();
}

Status MemoryEngine::Impl::UpdatePeerRegion(const json &request, bool map)
{
    std::lock_guard lock(mPeerMutex);
    uint32_t rank{};
    uint64_t generation{};
    CHECK_STATUS(PeerIdentity(request, rank, generation));
    const auto found = mPeers.find(rank);
    if (!mTent || found == mPeers.end() || found->second.revoked || found->second.generation != generation)
        return map ? Status::InvalidEntry("MemoryEngine peer relationship is not current") : Status::OK();

    Region region;
    if (!ReadRegion(request, region))
        return Status::InvalidArgument("Invalid MemoryEngine peer region");
    auto &peer = found->second;
    if (map)
        return MapPeerRegion(peer, region);
    auto &mapped = peer.regions.try_emplace(region.address, RemoteRegion{region.size}).first->second;
    if (mapped.size != region.size)
        return Status::InvalidArgument("Conflicting MemoryEngine region size");
    mapped.revoked = true;
    return mTent->RevokeMemFabricRegion(peer.segment, rank, peer.segmentName, region.address);
}

Status MemoryEngine::Impl::UnmapPeer(uint32_t rank, PeerMapping &peer)
{
    if (peer.segment != LOCAL_SEGMENT_ID) {
        CHECK_STATUS(mTent->CloseMemFabricPeer(peer.segment, rank, peer.segmentName));
        CHECK_STATUS(mTent->closeSegment(peer.segment));
        peer.segment = LOCAL_SEGMENT_ID;
    }
    peer.regions.clear();
    peer.segmentName.clear();
    peer.identity.clear();
    return Status::OK();
}

Status MemoryEngine::Impl::SharePeer(const json &request)
{
    std::lock_guard lock(mPeerMutex);
    if (!mTent)
        return Status::InvalidEntry("MemoryEngine TENT is unavailable");
    uint32_t rank{};
    uint64_t generation{};
    CHECK_STATUS(PeerIdentity(request, rank, generation));
    auto &peer = mPeers[rank];
    if (peer.generation > generation || (peer.generation == generation && peer.revoked))
        return Status::InvalidEntry("MemoryEngine peer generation was replaced or revoked");
    if (peer.generation < generation) {
        CHECK_STATUS(UnmapPeer(rank, peer));
        peer.generation = generation;
        peer.revoked = false;
    }
    const auto segment = request.at("segment").get<std::string>();
    const auto identity = request.at("identity").get<std::string>();
    const auto &regions = request.at("regions");
    if (segment.empty() || segment == mTent->getSegmentName() || identity.empty() || !regions.is_array())
        return Status::InvalidArgument("Invalid MemoryEngine peer description");
    if (peer.segment == LOCAL_SEGMENT_ID) {
        CHECK_STATUS(mTent->openSegment(peer.segment, segment));
        peer.segmentName = segment;
        peer.identity = identity;
    }
    CHECK_STATUS(mTent->PrepareMemFabricPeer(peer.segmentName, peer.identity));
    return MapPeerRegions(peer, regions);
}

Status MemoryEngine::Impl::UnsharePeer(const json &request)
{
    std::lock_guard lock(mPeerMutex);
    uint32_t rank{};
    uint64_t generation{};
    CHECK_STATUS(PeerIdentity(request, rank, generation));
    auto &peer = mPeers[rank];
    if (peer.generation > generation)
        return Status::OK();
    peer.generation = generation;
    peer.revoked = true;
    return UnmapPeer(rank, peer);
}

Status MemoryEngine::Impl::Join()
{
    std::lock_guard lock(mApiMutex);
    if (mLeaving)
        return Status::TooManyRequests("Retry MemoryEngine leave before rejoining");
    if (mJoined)
        return Status::OK();
    if (!mReady)
        CHECK_STATUS(InitializeRegistration());
    const auto deadline = Clock::now() + mJoinTimeout + RPC_TIMEOUT;
    std::string identity;
    CHECK_STATUS(mTent->GetMemFabricGvaIdentity(identity));
    const PeerCall server{mServer, {{"id", mId}, {"segment", mTent->getSegmentName()},
                                   {"identity", std::move(identity)}}};
    CHECK_STATUS(CallControl(mRpc, MemoryRpc::Join, server, deadline));
    mJoined = true;
    return Status::OK();
}

Status MemoryEngine::Impl::PrepareNativePeers()
{
    std::lock_guard lock(mPeerMutex);
    for (const auto &[rank, peer] : mPeers) {
        if (!peer.revoked && peer.segment != LOCAL_SEGMENT_ID)
            CHECK_STATUS(mTent->PrepareMemFabricPeer(peer.segmentName, peer.identity));
    }
    return Status::OK();
}

Status MemoryEngine::Impl::AllocateLocal(size_t size, MemoryEngineMemType type, uint64_t &address)
{
    MemoryOptions options;
    options.type = MEMFABRIC;
    options.location = type == MemoryEngineMemType::Host ? "cpu" : "npu";
    void *memory = nullptr;
    CHECK_STATUS(mTent->allocateLocalMemory(&memory, size, options));
    address = reinterpret_cast<uint64_t>(memory);
    auto &local = mLocalRegions.try_emplace(address, LocalRegion{size}).first->second;
    // Registration can have published metadata before reporting an error.
    local.registered = true;
    auto status = mTent->registerLocalMemory({memory}, {size}, options);
    if (!status.ok()) {
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
    const PeerCall server{mServer, {{"id", mId}, {"address", address}, {"size", size}}};
    return CallControl(mRpc, op, server, Clock::now() + mJoinTimeout + RPC_TIMEOUT);
}

Status MemoryEngine::Impl::ReleaseLocal(uint64_t address, LocalRegion &local, bool releaseMemory)
{
    auto *memory = reinterpret_cast<void *>(address);
    if (local.registered) {
        CHECK_STATUS(mTent->unregisterLocalMemory(memory, local.size));
        local.registered = false;
    }
    if (releaseMemory)
        return mTent->freeLocalMemory(memory);
    return Status::OK();
}

Status MemoryEngine::Impl::ReconcileAllocation()
{
    if (!mFailedAllocation)
        return Status::OK();
    auto &local = mLocalRegions.at(*mFailedAllocation);
    const bool exported = local.state == RegionState::Revoking;
    if (exported)
        CHECK_STATUS(UpdateLocalRegion(*mFailedAllocation, local.size, false));
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
    if (!mJoined)
        return Status::InvalidEntry("Join MemoryEngine before allocating");
    if (!address || !size || (type != MemoryEngineMemType::Host && type != MemoryEngineMemType::Device))
        return Status::InvalidArgument("Invalid MemoryEngine allocation request");
    const auto window = type == MemoryEngineMemType::Host ? mLayout.hostWindow : mLayout.deviceWindow;
    if (size > window)
        return Status::TooManyRequests("MemoryEngine allocation exceeds the rank window");
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
        if (!cleanup.ok())
            LOG(WARNING) << "Failed to roll back MemoryEngine allocation, rank:" << mRank << ".";
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
    if (mLiveRegions || mFailedAllocation)
        return Status::OK();
    Status result;
    for (auto it = mRetired.begin(); it != mRetired.end();) {
        auto local = mLocalRegions.find(*it);
        auto status = ReleaseLocal(local->first, local->second, true);
        if (status.ok()) {
            mLocalRegions.erase(local);
            it = mRetired.erase(it);
        } else {
            if (*it == requested)
                result = status;
            LOG(WARNING) << "Failed to release retired MemoryEngine allocation, rank:" << mRank << ".";
            ++it;
        }
    }
    return result;
}

Status MemoryEngine::Impl::Free(void *address)
{
    std::lock_guard lock(mApiMutex);
    if (!mJoined)
        return Status::InvalidEntry("Join MemoryEngine before freeing");
    const auto key = reinterpret_cast<uint64_t>(address);
    const auto found = mLocalRegions.find(key);
    if (found == mLocalRegions.end() || mFailedAllocation == key)
        return Status::InvalidArgument("MemoryEngine requires an owned allocation base");
    auto &local = found->second;
    if (local.state != RegionState::Retired)
        CHECK_STATUS(RetireLocal(key, local));
    return DrainRetired(key);
}

Status MemoryEngine::Impl::DestroyTent()
{
    std::lock_guard lock(mPeerMutex);
    if (!mTent)
        return Status::OK();
    for (auto &[rank, peer] : mPeers) {
        peer.revoked = true;
        auto status = UnmapPeer(rank, peer);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to remove MemoryEngine peer during leave, rank:" << rank << ".";
            return status;
        }
    }
    for (auto it = mLocalRegions.begin(); it != mLocalRegions.end();) {
        CHECK_STATUS(ReleaseLocal(it->first, it->second, true));
        it = mLocalRegions.erase(it);
    }
    mFailedAllocation.reset();
    mRetired.clear();
    mLiveRegions = 0;
    CHECK_STATUS(mTent->CloseMemFabricGva());
    mTent.reset();
    return Status::OK();
}

Status MemoryEngine::Impl::Leave()
{
    std::lock_guard lock(mApiMutex);
    if (!mReady && !mLeaving)
        return CancelRegistration();
    mLeaving = true;
    mJoined = false;
    const PeerCall server{mServer, {{"id", mId}}};
    const auto deadline = Clock::now() + mJoinTimeout;
    CHECK_STATUS(CallControl(mRpc, MemoryRpc::BeginLeave, server, deadline));
    CHECK_STATUS(DestroyTent());
    CHECK_STATUS(CallAsync(mRpc, MemoryRpc::FinishLeave, server, Clock::now() + RPC_TIMEOUT).get());
    mHeartbeat = std::jthread();
    mRegistrationAttempted = false;
    mReady = false;
    mLeaving = false;
    return Status::OK();
}

Status MemoryEngine::Impl::CancelRegistration()
{
    if (!mRegistrationAttempted)
        return Status::OK();
    json response;
    CHECK_STATUS(Call(mRpc, mServer, MemoryRpc::Cancel, {{"id", mId}}, response));
    mRegistrationAttempted = false;
    return Status::OK();
}

Status MemoryEngine::Impl::CreateTransferEngine(const Config &config)
{
    auto native = std::make_shared<Config>();
    CHECK_STATUS(native->load(config.dump()));
    native->set("metadata_type", "p2p");
    native->set("metadata_servers", "");
    native->set("transports/memfabric/enable", true);
    native->set("transports/memfabric/gva/enabled", true);
    native->set("transports/memfabric/gva/rank", mRank);
    native->set("transports/memfabric/gva/rank_count", mLayout.worldSize);
    native->set("transports/memfabric/gva/dram_window", mLayout.hostWindow / GIB);
    native->set("transports/memfabric/gva/hbm_window", mLayout.deviceWindow / GIB);
    auto tent = std::unique_ptr<TransferEngineImpl>(new (std::nothrow) TransferEngineImpl(native, true, false));
    if (!tent || !tent->available())
        return Status::InternalError("Could not initialize MemoryEngine TENT instance");
    CHECK_STATUS(tent->GetMemFabricGvaBases(mBases.host, mBases.device));
    CHECK_STATUS(ValidateBases(mLayout, mBases));
    std::lock_guard lock(mPeerMutex);
    mTent = std::move(tent);
    return Status::OK();
}

void MemoryEngine::Impl::StartHeartbeat(Milliseconds interval)
{
    mHeartbeat = std::jthread([this, interval](std::stop_token stop) {
        bool failed = false;
        const json request{{"id", mId}};
        RunPeriodic(stop, interval, [&] {
            json response;
            const auto status = Call(mRpc, mServer, MemoryRpc::Heartbeat, request, response);
            if (!status.ok() && !failed)
                LOG(WARNING) << "MemoryEngine heartbeat failed, rank:" << mRank << ".";
            failed = !status.ok();
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
        if (cleanup.ok())
            cleanup = CancelRegistration();
        if (!cleanup.ok()) {
            LOG(WARNING) << "Failed to release MemoryEngine GVA layout, rank:" << mRank << ".";
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
    const json request{{"id", mId}, {"rank", mRank}, {"hostBase", mBases.host}, {"deviceBase", mBases.device}};
    json response;
    return Call(mRpc, mServer, MemoryRpc::Confirm, request, response);
}

Status MemoryEngine::Impl::Start(std::shared_ptr<const Config> config)
{
    CHECK_STATUS(ReadClientOptions(*config, mOptions));
    mJoinTimeout = Milliseconds(config->get("memory_engine/join_timeout_ms", DEFAULT_REGISTRATION.count()));
    if (mJoinTimeout <= Milliseconds::zero() || mJoinTimeout >= MAX_TIMEOUT)
        return Status::InvalidArgument("Invalid MemoryEngine join timeout");
    mConfig = std::move(config);
    mLayout = mOptions.layout;
    const auto &server = mOptions.server;
    mServer = buildIpAddrWithPort(server.host, server.port, server.ipv6);
    CHECK_STATUS(StartCallbacks(mOptions.callbackHost));
    return InitializeRegistration();
}

MemoryEngine::MemoryEngine(std::unique_ptr<Impl> impl) : mImpl(std::move(impl))
{
}

MemoryEngine::~MemoryEngine() = default;

Status MemoryEngine::Create(const Config &config, std::unique_ptr<MemoryEngine> *engine)
{
    if (!engine || *engine)
        return Status::InvalidArgument("MemoryEngine requires an empty output");
    auto impl = std::unique_ptr<Impl>(new (std::nothrow) Impl);
    if (!impl)
        return Status::InternalError("Could not allocate MemoryEngine state");
    auto result = std::unique_ptr<MemoryEngine>(new (std::nothrow) MemoryEngine(std::move(impl)));
    if (!result)
        return Status::InternalError("Could not allocate MemoryEngine");
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
    std::lock_guard lock(mImpl->mApiMutex);
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

Status MemoryEngine::GvaBase(MemoryEngineMemType type, uint32_t rank, void **address) const
{
    std::lock_guard lock(mImpl->mApiMutex);
    if (!address || rank >= mImpl->mLayout.worldSize)
        return Status::InvalidArgument("Invalid MemoryEngine rank or address output");
    if (type != MemoryEngineMemType::Host && type != MemoryEngineMemType::Device)
        return Status::InvalidArgument("Unknown MemoryEngine memory type");
    const bool host = type == MemoryEngineMemType::Host;
    const auto window = host ? mImpl->mLayout.hostWindow : mImpl->mLayout.deviceWindow;
    if (!window)
        return Status::InvalidArgument("Requested MemoryEngine GVA window is disabled");
    const auto base = host ? mImpl->mBases.host : mImpl->mBases.device;
    *address = reinterpret_cast<void *>(base + rank * window);
    return Status::OK();
}

Status MemoryEngine::RankOfGva(const void *address, uint32_t *rank) const
{
    std::lock_guard lock(mImpl->mApiMutex);
    if (!rank || !address)
        return Status::InvalidArgument("Missing MemoryEngine GVA or rank output");
    const auto value = reinterpret_cast<uint64_t>(address);
    const auto &layout = mImpl->mLayout;
    const auto &bases = mImpl->mBases;
    const auto owner = [&](uint64_t base, uint64_t window) -> std::optional<uint32_t> {
        if (!window || value < base || value - base >= layout.worldSize * window)
            return std::nullopt;
        return static_cast<uint32_t>((value - base) / window);
    };
    auto result = owner(bases.host, layout.hostWindow);
    if (!result)
        result = owner(bases.device, layout.deviceWindow);
    if (!result)
        return Status::InvalidArgument("Address is outside the MemoryEngine GVA layout");
    *rank = *result;
    return Status::OK();
}

}
}
