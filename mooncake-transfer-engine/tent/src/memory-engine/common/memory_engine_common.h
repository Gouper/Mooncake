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

#include "tent/common/config.h"
#include "tent/rpc/rpc.h"

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
inline constexpr char CONFIG_SERVER_HEARTBEAT_TIMEOUT_MS[] = "memory_engine/server/heartbeat_timeout_ms";
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

struct GvaRegion {
    uint64_t address{};
    size_t size{};
    bool revoking = false;
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
bool ReadRegion(const json &payload, GvaRegion &region);
Status Call(CoroRpcAgent &rpc, const std::string &endpoint, MemoryRpc op, const json &request, json &response);
std::future<Status> CallAsync(CoroRpcAgent &rpc, MemoryRpc op, const PeerCall &peer, Clock::time_point deadline);
bool Retryable(const Status &status);

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
