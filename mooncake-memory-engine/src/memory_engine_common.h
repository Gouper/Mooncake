// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_SRC_MEMORY_ENGINE_COMMON_H
#define TENT_SRC_MEMORY_ENGINE_COMMON_H

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <future>
#include <limits>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include <glog/logging.h>

#include "tent/common/config.h"
#include "memory_engine_rpc.h"

namespace mooncake {
namespace tent {

using Clock = std::chrono::steady_clock;
using Milliseconds = std::chrono::milliseconds;

inline constexpr uint64_t GIB = 1ULL << 30;
inline constexpr auto DEFAULT_REGISTRATION = Milliseconds(120000);
inline constexpr auto RPC_TIMEOUT = Milliseconds(5000);
inline constexpr auto RETRY_INTERVAL = Milliseconds(20);
inline constexpr auto MAX_TIMEOUT = std::chrono::hours(24);
inline constexpr uint64_t MAX_RANKS = std::numeric_limits<uint32_t>::max();

namespace config_keys {

inline constexpr char CONFIG_ENABLED[] = "memory_engine/enabled";
inline constexpr char CONFIG_SERVER_URL[] = "memory_engine/server_url";
inline constexpr char CONFIG_WORLD_SIZE[] = "memory_engine/world_size";
inline constexpr char CONFIG_HOST_MEM_MAX[] = "memory_engine/host_mem_max";
inline constexpr char CONFIG_DEVICE_MEM_MAX[] = "memory_engine/device_mem_max";
inline constexpr char CONFIG_AUTO_RANKING[] = "memory_engine/auto_ranking";
inline constexpr char CONFIG_LOCAL_RANK[] = "memory_engine/local_rank";
inline constexpr char CONFIG_CLIENT_HOST[] = "memory_engine/client_host";
inline constexpr char CONFIG_HEARTBEAT_INTERVAL_MS[] = "memory_engine/heartbeat_interval_ms";
inline constexpr char CONFIG_JOIN_TIMEOUT_MS[] = "memory_engine/join_timeout_ms";
inline constexpr char CONFIG_BATCH_COPY_TIMEOUT_MS[] = "memory_engine/batch_copy_timeout_ms";

inline constexpr char CONFIG_SERVER_WORLD_SIZE[] = "memory_engine/server/world_size";
inline constexpr char CONFIG_SERVER_HOST_MEM_MAX[] = "memory_engine/server/host_mem_max";
inline constexpr char CONFIG_SERVER_DEVICE_MEM_MAX[] = "memory_engine/server/device_mem_max";
inline constexpr char CONFIG_SERVER_GVA_BASE_HOST[] = "memory_engine/server/gva_base_host";
inline constexpr char CONFIG_SERVER_GVA_BASE_DEVICE[] = "memory_engine/server/gva_base_device";
inline constexpr char CONFIG_SERVER_RANK_LEASE_MS[] = "memory_engine/server/rank_lease_ms";
inline constexpr char CONFIG_SERVER_RPC_SERVER_THREADS[] = "memory_engine/server/rpc_server_threads";

inline constexpr char TENT_CONFIG_METADATA_TYPE[] = "metadata_type";
inline constexpr char TENT_CONFIG_METADATA_SERVERS[] = "metadata_servers";
inline constexpr char TENT_CONFIG_MEMFABRIC_ENABLE[] = "transports/memfabric/enable";
inline constexpr char TENT_CONFIG_GVA_ENABLED[] = "transports/memfabric/gva/enabled";
inline constexpr char TENT_CONFIG_GVA_RANK[] = "transports/memfabric/gva/rank";
inline constexpr char TENT_CONFIG_GVA_RANK_COUNT[] = "transports/memfabric/gva/rank_count";
inline constexpr char TENT_CONFIG_GVA_DRAM_WINDOW[] = "transports/memfabric/gva/dram_window";
inline constexpr char TENT_CONFIG_GVA_HBM_WINDOW[] = "transports/memfabric/gva/hbm_window";

}

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
    SharePeers,
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

struct RegisterRequest {
    std::string id;
    Layout layout;
    std::string callbackAddress;
    std::optional<uint32_t> requestedRank;
};

struct RegisterResponse {
    uint32_t rank{};
    uint64_t leaseMs{};
};

struct Bases {
    uint64_t host{};
    uint64_t device{};
    bool operator==(const Bases &) const = default;
};

struct GvaRegion {
    uint64_t address{};
    size_t size{};
    bool revoking = false;
};

struct RegistrationIdRequest {
    std::string id;
};

struct ConfirmRequest {
    std::string id;
    uint32_t rank{};
    Bases bases;
};

struct CancelRequest {
    std::string id;
    std::string callbackAddress;
};

struct JoinRequest {
    std::string id;
    std::string segment;
    std::string identity;
};

struct RegionRequest {
    std::string id;
    GvaRegion region;
};

struct PeerRequest {
    std::string id;
    uint32_t rank{};
    uint64_t generation{};
};

struct SharedPeer {
    uint32_t rank{};
    uint64_t generation{};
    std::string segment;
    std::string identity;
    std::vector<GvaRegion> regions;
};

struct SharePeerRequest {
    std::string id;
    SharedPeer peer;
};

struct SharePeersRequest {
    std::string id;
    std::vector<SharedPeer> peers;
};

struct PeerRegionRequest {
    PeerRequest peer;
    GvaRegion region;
};

struct PeerCall {
    std::string endpoint;
    json request;
};

std::optional<uint64_t> ReadUnsigned(const json &value, uint64_t max);
Status ParseEndpoint(std::string url, Endpoint &endpoint);
bool ValidWindow(uint64_t window);
Status ValidateLayout(const Layout &layout);
Status ValidateBases(const Layout &layout, const Bases &bases);
Status ReadLayout(const json &worldSize, const json &hostWindow, const json &deviceWindow, Layout &layout);
Status ReadLayout(const Config &config, const char *worldSizeKey, const char *hostWindowKey,
                  const char *deviceWindowKey, Layout &layout);

json EncodeRegisterRequest(const RegisterRequest &request);
Status DecodeRegisterRequest(const json &payload, RegisterRequest &request);

json EncodeRegisterResponse(const RegisterResponse &response, const Status &status);
Status DecodeRegisterResponse(const json &payload, uint32_t worldSize, RegisterResponse &response);

json EncodeRegistrationIdRequest(const RegistrationIdRequest &request);
Status DecodeRegistrationIdRequest(const json &payload, RegistrationIdRequest &request);

json EncodeConfirmRequest(const ConfirmRequest &request);
Status DecodeConfirmRequest(const json &payload, ConfirmRequest &request);

json EncodeCancelRequest(const CancelRequest &request);
Status DecodeCancelRequest(const json &payload, CancelRequest &request);

json EncodeJoinRequest(const JoinRequest &request);
Status DecodeJoinRequest(const json &payload, JoinRequest &request);

json EncodeRegionRequest(const RegionRequest &request);
Status DecodeRegionRequest(const json &payload, RegionRequest &request);

json EncodePeerRequest(const PeerRequest &request);
Status DecodePeerRequest(const json &payload, PeerRequest &request);

json EncodeSharePeerRequest(const SharePeerRequest &request);
// Decode the rest of the callback after the recipient and generation have been checked.
Status DecodeSharePeerDetails(const json &payload, SharedPeer &peer);

json EncodeSharePeersRequest(const SharePeersRequest &request);
Status DecodeSharePeersRequest(const json &payload, uint32_t worldSize, SharePeersRequest &request);

json EncodePeerRegionRequest(const PeerRegionRequest &request);
Status DecodePeerRegion(const json &payload, GvaRegion &region);

std::string EncodeReply(const Status &status, json &response);

Status Call(MemoryEngineRpc &rpc, const std::string &endpoint, MemoryRpc op, const json &request, json &response);
Status Call(MemoryEngineRpc &rpc, const std::string &endpoint, MemoryRpc op, const json &request);
std::future<Status> CallAsync(MemoryEngineRpc &rpc, MemoryRpc op, const PeerCall &peer, Clock::time_point deadline);
bool Retryable(const Status &status);

template <typename Handler>
Status RegisterHandler(MemoryEngineRpc &rpc, MemoryRpc op, Handler handler, bool offload = false)
{
    return rpc.RegisterFunction(static_cast<int>(op), [handler](std::string_view data, std::string &output) {
        json response = json::object();
        Status status;
        try {
            status = handler(json::parse(data), response);
        } catch (const json::exception &) {
            LOG(ERROR) << "Malformed MemoryEngine RPC request, bytes:" << data.size() << ".";
            status = Status::MalformedJson("Malformed MemoryEngine RPC request");
        }
        output = EncodeReply(status, response);
    }, offload);
}

template <typename Request, typename Handler>
Status RegisterRequestHandler(MemoryEngineRpc &rpc, MemoryRpc op, Status (*decode)(const json &, Request &),
                              Handler handler, bool offload = false)
{
    return RegisterHandler(rpc, op, [decode, handler](const json &payload, json &) {
        Request request;
        CHECK_STATUS(decode(payload, request));
        return handler(request);
    }, offload);
}

template <typename Tick>
void RunPeriodic(std::stop_token stop, Milliseconds interval, Tick tick)
{
    std::mutex mutex;
    std::condition_variable_any wake;
    std::unique_lock lock(mutex);
    while (!stop.stop_requested()) {
        wake.wait_for(lock, stop, interval, [] { return false; });
        if (!stop.stop_requested()) {
            tick();
        }
    }
}

}
}

#endif
