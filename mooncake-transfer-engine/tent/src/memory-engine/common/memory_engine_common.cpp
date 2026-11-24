// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <limits>
#include <string_view>
#include <utility>

#include "tent/common/utils/ip.h"
#include "memory_engine_common.h"

namespace mooncake::tent::memory_engine {
namespace {

constexpr uint64_t MAX_WINDOW = 1ULL << 50;
constexpr std::string_view RPC_SCHEME = "rpc://";

bool ValidRange(uint64_t base, uint64_t window, uint32_t worldSize)
{
    return window ? base && window <= (UINT64_MAX - base) / worldSize : base == 0;
}

Status DecodeReply(const std::string &raw, json &response)
{
    try {
        response = json::parse(raw);
        const auto code = ReadUnsigned(response.at("code"), static_cast<uint16_t>(Status::Code::kMaxCode) - 1);
        if (!code) {
            return Status::RpcServiceError("Invalid MemoryEngine RPC status");
        }
        return Status(static_cast<Status::Code>(*code), response.at("message").get<std::string>());
    } catch (const json::exception &) {
        return Status::MalformedJson("Malformed MemoryEngine RPC response");
    }
}

}

std::optional<uint64_t> ReadUnsigned(const json &value, uint64_t max)
{
    if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<int64_t>() < 0)) {
        return std::nullopt;
    }
    const auto number = value.get<uint64_t>();
    if (number > max) {
        return std::nullopt;
    }
    return number;
}

Status ParseEndpoint(std::string url, Endpoint &endpoint)
{
    if (url.starts_with(RPC_SCHEME)) {
        url.erase(0, RPC_SCHEME.size());
    }
    auto [host, port] = parseHostNameWithPort(url, 0);
    if (host.empty() || !port || host.find_first_of("/ \t\r\n") != std::string::npos) {
        return Status::InvalidArgument("Invalid MemoryEngine RPC endpoint");
    }
    endpoint = {host, port, host.find(':') != std::string::npos};
    return Status::OK();
}

bool ValidWindow(uint64_t window)
{
    return window < MAX_WINDOW && window % GIB == 0;
}

Status ValidateLayout(const Layout &layout)
{
    if (!layout.worldSize || !(layout.hostWindow || layout.deviceWindow)) {
        return Status::InvalidArgument("MemoryEngine requires ranks and at least one GVA window");
    }
    if (!ValidWindow(layout.hostWindow) || !ValidWindow(layout.deviceWindow)) {
        return Status::InvalidArgument("MemFabric GVA windows must be in whole GiB and below 1 PiB");
    }
    const auto maxWindow = UINT64_MAX / layout.worldSize;
    if (layout.hostWindow > maxWindow || layout.deviceWindow > maxWindow) {
        return Status::InvalidArgument("MemoryEngine GVA window size overflows");
    }
    return Status::OK();
}

Status ValidateBases(const Layout &layout, const Bases &bases)
{
    if (!ValidRange(bases.host, layout.hostWindow, layout.worldSize) ||
        !ValidRange(bases.device, layout.deviceWindow, layout.worldSize)) {
        return Status::InvalidArgument("Invalid MemFabric GVA range");
    }
    const auto hostEnd = bases.host + layout.hostWindow * layout.worldSize;
    const auto deviceEnd = bases.device + layout.deviceWindow * layout.worldSize;
    if (bases.host && bases.device && bases.host < deviceEnd && bases.device < hostEnd) {
        return Status::InvalidArgument("Overlapping Host and Device GVA windows");
    }
    return Status::OK();
}

Status ReadLayout(const json &worldSize, const json &hostWindow, const json &deviceWindow, Layout &layout)
{
    const auto ranks = ReadUnsigned(worldSize, MAX_RANKS);
    const auto host = ReadUnsigned(hostWindow, MAX_WINDOW);
    const auto device = ReadUnsigned(deviceWindow, MAX_WINDOW);
    if (!ranks || !host || !device) {
        return Status::InvalidArgument("Invalid MemoryEngine layout values");
    }
    layout = {static_cast<uint32_t>(*ranks), *host, *device};
    return Status::OK();
}

Status ReadLayout(const Config &config, const char *worldSizeKey, const char *hostWindowKey,
                  const char *deviceWindowKey, Layout &layout)
{
    return ReadLayout(config.get(worldSizeKey, json(0)), config.get(hostWindowKey, json(0)),
                      config.get(deviceWindowKey, json(0)), layout);
}

bool ReadRegion(const json &payload, GvaRegion &region)
{
    const auto address = ReadUnsigned(payload.at("address"), UINT64_MAX);
    const auto size = ReadUnsigned(payload.at("size"), std::numeric_limits<size_t>::max());
    if (!address || !*address || !size || !*size || *size > UINT64_MAX - *address) {
        return false;
    }
    region = {*address, static_cast<size_t>(*size)};
    return true;
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

}
