// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <chrono>
#include <condition_variable>
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
#include <utility>

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

struct Registration {
    uint32_t rank{};
    std::string callback;
    bool confirmed = false;
    Clock::time_point expires;
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

Status Call(CoroRpcAgent &rpc, const std::string &endpoint, MemoryRpc op, const json &request, json &response)
{
    std::string raw;
    CHECK_STATUS(rpc.callOwned(endpoint, static_cast<int>(op), request.dump(), raw));
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

template <typename Handler>
Status RegisterHandler(CoroRpcAgent &rpc, MemoryRpc op, Handler handler)
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
    });
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
    Status ClaimRank(std::optional<uint32_t> requested, uint32_t &rank);
    void ReleaseRank(uint32_t rank);
    void ResetUnconfirmedLayout();
    void Reap();

    std::mutex mMutex;
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
    mRegistrations.emplace(id, Registration{rank, callbackAddress, false, Clock::now() + mRegistrationTimeout});
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
    found->second.confirmed = true;
    found->second.expires = Clock::now() + mLease;
    return Status::OK();
}

Status MemoryEngineServer::Impl::Heartbeat(const json &request)
{
    const auto id = request.at("id").get<std::string>();
    std::lock_guard lock(mMutex);
    auto found = mRegistrations.find(id);
    const auto now = Clock::now();
    if (found == mRegistrations.end() || !found->second.confirmed || found->second.expires <= now)
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
        ReleaseRank(found->second.rank);
        mRegistrations.erase(found);
        ResetUnconfirmedLayout();
    }
    return Status::OK();
}

void MemoryEngineServer::Impl::Reap()
{
    const auto now = Clock::now();
    std::lock_guard lock(mMutex);
    for (auto it = mRegistrations.begin(); it != mRegistrations.end();) {
        if (it->second.expires > now) {
            ++it;
        } else {
            ReleaseRank(it->second.rank);
            it = mRegistrations.erase(it);
        }
    }
    ResetUnconfirmedLayout();
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
    Status Start(const Config &config);
    Status Register(const ClientOptions &options);
    Status CreateTransferEngine(const Config &config);
    void StartHeartbeat(Milliseconds interval);

    Layout mLayout;
    Bases mBases;
    uint32_t mRank{};
    std::string mServer;
    std::string mId;
    bool mRegistrationAttempted = false;
    CoroRpcAgent mRpc;
    std::unique_ptr<TransferEngineImpl> mTent;
    std::jthread mHeartbeat;
};

MemoryEngine::Impl::~Impl()
{
    mHeartbeat = std::jthread();
    mTent.reset();
    if (mRegistrationAttempted) {
        json response;
        const auto status = Call(mRpc, mServer, MemoryRpc::Cancel, {{"id", mId}}, response);
        if (!status.ok())
            LOG(WARNING) << "Failed to release MemoryEngine registration, rank:" << mRank << ".";
    }
    const auto status = mRpc.stop();
    if (!status.ok())
        LOG(WARNING) << "Failed to stop MemoryEngine client RPC, rank:" << mRank << ".";
}

Status MemoryEngine::Impl::Register(const ClientOptions &options)
{
    std::string host = options.callbackHost;
    bool ipv6 = false;
    if (host.empty()) {
        CHECK_STATUS(discoverLocalIpAddress(host, ipv6));
    } else {
        CHECK_STATUS(checkLocalIpAddress(host, ipv6));
    }
    uint16_t port{};
    CHECK_STATUS(mRpc.start(port, ipv6));
    mId = NewRegistrationId();
    json request{{"id", mId}, {"worldSize", mLayout.worldSize}, {"hostWindow", mLayout.hostWindow},
                 {"deviceWindow", mLayout.deviceWindow}, {"callback", buildIpAddrWithPort(host, port, ipv6)}};
    if (options.requestedRank)
        request["rank"] = *options.requestedRank;

    mRegistrationAttempted = true;
    json response;
    CHECK_STATUS(Call(mRpc, mServer, MemoryRpc::Register, request, response));
    try {
        const auto rank = ReadUnsigned(response.at("rank"), mLayout.worldSize - 1);
        if (!rank)
            return Status::RpcServiceError("MemoryEngine server returned a rank outside the layout");
        mRank = static_cast<uint32_t>(*rank);
        const auto lease = ReadUnsigned(response.at("leaseMs"), MAX_TIMEOUT_MS);
        if (!lease || static_cast<uint64_t>(options.heartbeat.count()) >= *lease)
            return Status::InvalidArgument("MemoryEngine heartbeat interval must be shorter than the server lease");
    } catch (const json::exception &) {
        return Status::MalformedJson("Invalid MemoryEngine registration reply");
    }
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
    mTent.reset(new (std::nothrow) TransferEngineImpl(native, true, false));
    if (!mTent || !mTent->available())
        return Status::InternalError("Could not initialize MemoryEngine TENT instance");
    CHECK_STATUS(mTent->GetMemFabricGvaBases(mBases.host, mBases.device));
    return ValidateBases(mLayout, mBases);
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

Status MemoryEngine::Impl::Start(const Config &config)
{
    ClientOptions options;
    CHECK_STATUS(ReadClientOptions(config, options));
    mLayout = options.layout;
    mServer = buildIpAddrWithPort(options.server.host, options.server.port, options.server.ipv6);
    CHECK_STATUS(Register(options));
    CHECK_STATUS(CreateTransferEngine(config));
    const json request{{"id", mId}, {"rank", mRank}, {"hostBase", mBases.host}, {"deviceBase", mBases.device}};
    json response;
    CHECK_STATUS(Call(mRpc, mServer, MemoryRpc::Confirm, request, response));
    StartHeartbeat(options.heartbeat);
    return Status::OK();
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
    const auto snapshot = config.freeze();
    auto status = result->mImpl->Start(*snapshot);
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
    return mImpl->mRank;
}

Status MemoryEngine::GvaBase(MemoryEngineMemType type, uint32_t rank, void **address) const
{
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
