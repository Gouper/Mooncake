// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "tent/common/utils/ip.h"
#include "memory_engine_common.h"

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t MAX_WINDOW = 1ULL << 50;
constexpr auto MAX_TIMEOUT_MS = static_cast<uint64_t>(std::chrono::duration_cast<Milliseconds>(MAX_TIMEOUT).count());
constexpr std::string_view RPC_SCHEME = "rpc://";
constexpr char ID_KEY[] = "id";
constexpr char WORLD_SIZE_KEY[] = "worldSize";
constexpr char HOST_WINDOW_KEY[] = "hostWindow";
constexpr char DEVICE_WINDOW_KEY[] = "deviceWindow";
constexpr char CALLBACK_KEY[] = "callback";
constexpr char RANK_KEY[] = "rank";
constexpr char LEASE_MS_KEY[] = "leaseMs";
constexpr char HOST_BASE_KEY[] = "hostBase";
constexpr char DEVICE_BASE_KEY[] = "deviceBase";
constexpr char SEGMENT_KEY[] = "segment";
constexpr char IDENTITY_KEY[] = "identity";
constexpr char GENERATION_KEY[] = "generation";
constexpr char REGIONS_KEY[] = "regions";
constexpr char PEERS_KEY[] = "peers";
constexpr char ADDRESS_KEY[] = "address";
constexpr char SIZE_KEY[] = "size";
constexpr char DESCRIPTOR_KEY[] = "descriptor";
constexpr size_t MAX_GVA_DESCRIPTOR_SIZE = 16U << 20;
constexpr char CODE_KEY[] = "code";
constexpr char MESSAGE_KEY[] = "message";

bool ValidRange(uint64_t base, uint64_t window, uint32_t worldSize)
{
    return window ? base && window <= (UINT64_MAX - base) / worldSize : base == 0;
}

Status DecodeReply(const std::string &raw, json &response)
{
    try {
        response = json::parse(raw);
        const auto code = ReadUnsigned(response.at(CODE_KEY), static_cast<uint16_t>(Status::Code::kMaxCode) - 1);
        if (!code) {
            return Status::RpcServiceError("Invalid MemoryEngine RPC status");
        }
        return Status(static_cast<Status::Code>(*code), response.at(MESSAGE_KEY).get<std::string>());
    } catch (const json::exception &) {
        return Status::MalformedJson("Malformed MemoryEngine RPC response");
    }
}

Status DecodeRequestedRank(const json &payload, RegisterRequest &request)
{
    const auto field = payload.find(RANK_KEY);
    if (field == payload.end()) {
        return Status::OK();
    }
    const auto rank = ReadUnsigned(*field, request.layout.worldSize - 1);
    if (!rank) {
        return Status::InvalidArgument("Requested MemoryEngine rank is out of range");
    }
    request.requestedRank = static_cast<uint32_t>(*rank);
    return Status::OK();
}

void EncodeRegion(const GvaRegion &region, json &payload)
{
    payload[ADDRESS_KEY] = region.address;
    payload[SIZE_KEY] = region.size;
    if (!region.descriptor.empty()) {
        payload[DESCRIPTOR_KEY] = region.descriptor;
    }
}

bool ReadRegion(const json &payload, GvaRegion &region)
{
    const auto address = ReadUnsigned(payload.at(ADDRESS_KEY), UINT64_MAX);
    const auto size = ReadUnsigned(payload.at(SIZE_KEY), std::numeric_limits<size_t>::max());
    if (!address || !*address || !size || !*size || *size > UINT64_MAX - *address) {
        return false;
    }
    region = {*address, static_cast<size_t>(*size)};
    return true;
}

bool ReadPublishedRegion(const json &payload, GvaRegion &region)
{
    if (!ReadRegion(payload, region)) [[unlikely]] {
        return false;
    }
    const auto field = payload.find(DESCRIPTOR_KEY);
    const auto *descriptor = field == payload.end() ? nullptr : field->get_ptr<const std::string *>();
    if (!descriptor || descriptor->empty() || descriptor->size() > MAX_GVA_DESCRIPTOR_SIZE) [[unlikely]] {
        return false;
    }
    region.descriptor = *descriptor;
    return true;
}

Status DecodePeerRankAndGeneration(const json &payload, uint32_t &rank, uint64_t &generation)
{
    const auto decodedRank = ReadUnsigned(payload.at(RANK_KEY), MAX_RANKS);
    const auto decodedGeneration = ReadUnsigned(payload.at(GENERATION_KEY), UINT64_MAX);
    if (!decodedRank || !decodedGeneration || !*decodedGeneration) {
        return Status::InvalidArgument("Invalid MemoryEngine peer callback");
    }
    rank = static_cast<uint32_t>(*decodedRank);
    generation = *decodedGeneration;
    return Status::OK();
}

json EncodeSharedPeer(const SharedPeer &peer)
{
    auto regions = json::array();
    regions.get_ref<json::array_t &>().reserve(peer.regions.size());
    for (const auto &region : peer.regions) {
        json entry = json::object();
        EncodeRegion(region, entry);
        regions.push_back(std::move(entry));
    }

    return {{RANK_KEY, peer.rank}, {GENERATION_KEY, peer.generation}, {SEGMENT_KEY, peer.segment},
            {IDENTITY_KEY, peer.identity}, {REGIONS_KEY, std::move(regions)}};
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

Status LoadMemoryEngineConfig(Config &config)
{
    const char *path = std::getenv(config_keys::CONFIG_FILE_ENV);
    std::error_code fileError;
    if (!path || !std::filesystem::is_regular_file(std::filesystem::path(path), fileError)) [[unlikely]] {
        LOG(ERROR) << "Missing or invalid MemoryEngine configuration file, environment variable:"
                   << config_keys::CONFIG_FILE_ENV << ", error:" << fileError.value() << ".";
        return Status::InvalidArgument(std::string("Set ") + config_keys::CONFIG_FILE_ENV +
                                       " to the path of a MemoryEngine JSON file");
    }

    Config fileConfig;
    const auto status = fileConfig.loadFile(std::string(path));
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to load MemoryEngine configuration, environment variable:" << config_keys::CONFIG_FILE_ENV
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }

    const auto options = fileConfig.get(config_keys::CONFIG_SECTION, json());
    if (!options.is_object()) [[unlikely]] {
        LOG(ERROR) << "Missing or invalid MemoryEngine configuration section, environment variable:"
                   << config_keys::CONFIG_FILE_ENV << ", section:" << config_keys::CONFIG_SECTION << ".";
        return Status::InvalidArgument("MemoryEngine configuration requires a top-level memory_engine object");
    }

    config.set(config_keys::CONFIG_SECTION, options);
    return Status::OK();
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

json EncodeRegisterRequest(const RegisterRequest &request)
{
    json payload{{ID_KEY, request.id}, {WORLD_SIZE_KEY, request.layout.worldSize},
                 {HOST_WINDOW_KEY, request.layout.hostWindow}, {DEVICE_WINDOW_KEY, request.layout.deviceWindow},
                 {CALLBACK_KEY, request.callbackAddress}};
    if (request.requestedRank) {
        payload[RANK_KEY] = *request.requestedRank;
    }
    return payload;
}

Status DecodeRegisterRequest(const json &payload, RegisterRequest &request)
{
    try {
        RegisterRequest decoded;
        decoded.id = payload.at(ID_KEY).get<std::string>();
        decoded.callbackAddress = payload.at(CALLBACK_KEY).get<std::string>();
        CHECK_STATUS(ReadLayout(payload.at(WORLD_SIZE_KEY), payload.at(HOST_WINDOW_KEY),
                                payload.at(DEVICE_WINDOW_KEY), decoded.layout));
        CHECK_STATUS(ValidateLayout(decoded.layout));

        Endpoint callback;
        CHECK_STATUS(ParseEndpoint(decoded.callbackAddress, callback));
        if (decoded.id.empty()) {
            return Status::InvalidArgument("Missing MemoryEngine registration ID");
        }
        CHECK_STATUS(DecodeRequestedRank(payload, decoded));

        request = std::move(decoded);
        return Status::OK();
    } catch (const json::exception &) {
        return Status::MalformedJson("Malformed MemoryEngine RPC request");
    }
}

json EncodeRegisterResponse(const RegisterResponse &response, const Status &status)
{
    if (status.ok()) {
        return {{RANK_KEY, response.rank}, {LEASE_MS_KEY, response.leaseMs}};
    }
    // A rejected rank request can still include the lease duration.
    if (response.leaseMs) {
        return {{LEASE_MS_KEY, response.leaseMs}};
    }
    return json::object();
}

Status DecodeRegisterResponse(const json &payload, uint32_t worldSize, RegisterResponse &response)
{
    try {
        const auto rank = ReadUnsigned(payload.at(RANK_KEY), worldSize - 1);
        if (!rank) {
            return Status::RpcServiceError("MemoryEngine server returned a rank outside the layout");
        }
        const auto lease = ReadUnsigned(payload.at(LEASE_MS_KEY), MAX_TIMEOUT_MS);
        if (!lease) {
            return Status::InvalidArgument("MemoryEngine heartbeat interval must be shorter than the server lease");
        }

        response = {static_cast<uint32_t>(*rank), *lease};
        return Status::OK();
    } catch (const json::exception &) {
        return Status::MalformedJson("Invalid MemoryEngine registration reply");
    }
}

json EncodeRegistrationIdRequest(const RegistrationIdRequest &request)
{
    return {{ID_KEY, request.id}};
}

Status DecodeRegistrationIdRequest(const json &payload, RegistrationIdRequest &request)
{
    request.id = payload.at(ID_KEY).get<std::string>();
    return Status::OK();
}

json EncodeConfirmRequest(const ConfirmRequest &request)
{
    return {{ID_KEY, request.id}, {RANK_KEY, request.rank},
            {HOST_BASE_KEY, request.bases.host}, {DEVICE_BASE_KEY, request.bases.device}};
}

Status DecodeConfirmRequest(const json &payload, ConfirmRequest &request)
{
    auto id = payload.at(ID_KEY).get<std::string>();
    const auto host = ReadUnsigned(payload.at(HOST_BASE_KEY), UINT64_MAX);
    const auto device = ReadUnsigned(payload.at(DEVICE_BASE_KEY), UINT64_MAX);
    const auto rank = ReadUnsigned(payload.at(RANK_KEY), MAX_RANKS);
    if (!host || !device || !rank) [[unlikely]] {
        LOG(ERROR) << "Invalid MemoryEngine Confirm request, registration ID:" << id << ", rank:"
                   << (rank ? std::to_string(*rank) : "unknown")
                   << ", valid host base:" << host.has_value() << ", valid device base:" << device.has_value() << ".";
        return Status::InvalidArgument("Invalid MemoryEngine confirmation");
    }
    request = {std::move(id), static_cast<uint32_t>(*rank), {*host, *device}};
    return Status::OK();
}

json EncodeCancelRequest(const CancelRequest &request)
{
    return {{ID_KEY, request.id}, {CALLBACK_KEY, request.callbackAddress}};
}

Status DecodeCancelRequest(const json &payload, CancelRequest &request)
{
    request = {payload.at(ID_KEY).get<std::string>(), payload.at(CALLBACK_KEY).get<std::string>()};
    return Status::OK();
}

json EncodeJoinRequest(const JoinRequest &request)
{
    return {{ID_KEY, request.id}, {SEGMENT_KEY, request.segment}, {IDENTITY_KEY, request.identity}};
}

Status DecodeJoinRequest(const json &payload, JoinRequest &request)
{
    JoinRequest decoded{payload.at(ID_KEY).get<std::string>(), payload.at(SEGMENT_KEY).get<std::string>(),
                        payload.at(IDENTITY_KEY).get<std::string>()};
    if (decoded.segment.empty() || decoded.identity.empty()) {
        return Status::InvalidArgument("Missing MemoryEngine TENT segment or identity");
    }
    request = std::move(decoded);
    return Status::OK();
}

json EncodeRegionRequest(const RegionRequest &request)
{
    json payload{{ID_KEY, request.id}};
    EncodeRegion(request.region, payload);
    return payload;
}

Status DecodeRegionRequest(const json &payload, RegionUpdateAction action, RegionRequest &request)
{
    GvaRegion region;
    const bool publish = action == RegionUpdateAction::Publish;
    const bool valid = publish ? ReadPublishedRegion(payload, region) : ReadRegion(payload, region);
    if (!valid) [[unlikely]] {
        return Status::InvalidArgument("Invalid MemoryEngine region");
    }
    request = {payload.at(ID_KEY).get<std::string>(), std::move(region)};
    return Status::OK();
}

json EncodePeerRequest(const PeerRequest &request)
{
    return {{ID_KEY, request.id}, {RANK_KEY, request.rank}, {GENERATION_KEY, request.generation}};
}

Status DecodePeerRequest(const json &payload, PeerRequest &request)
{
    PeerRequest decoded;
    CHECK_STATUS(DecodePeerRankAndGeneration(payload, decoded.rank, decoded.generation));
    const auto &id = payload.at(ID_KEY);
    if (!id.is_string()) {
        return Status::InvalidArgument("Invalid MemoryEngine peer callback");
    }
    decoded.id = id.get<std::string>();
    request = std::move(decoded);
    return Status::OK();
}

json EncodeSharePeerRequest(const SharePeerRequest &request)
{
    auto payload = EncodeSharedPeer(request.peer);
    payload[ID_KEY] = request.id;
    return payload;
}

Status DecodeSharePeerDetails(const json &payload, SharedPeer &peer)
{
    peer.segment = payload.at(SEGMENT_KEY).get<std::string>();
    peer.identity = payload.at(IDENTITY_KEY).get<std::string>();
    const auto &regions = payload.at(REGIONS_KEY);
    if (peer.segment.empty() || peer.identity.empty() || !regions.is_array()) {
        return Status::InvalidArgument("Invalid MemoryEngine peer description");
    }

    peer.regions.reserve(regions.size());
    for (const auto &entry : regions) {
        GvaRegion region;
        if (!ReadPublishedRegion(entry, region)) [[unlikely]] {
            return Status::InvalidArgument("Invalid MemoryEngine mapped region");
        }
        peer.regions.push_back(std::move(region));
    }
    return Status::OK();
}

json EncodeSharePeersRequest(const SharePeersRequest &request)
{
    auto peers = json::array();
    peers.get_ref<json::array_t &>().reserve(request.peers.size());
    for (const auto &peer : request.peers) {
        peers.push_back(EncodeSharedPeer(peer));
    }
    return {{ID_KEY, request.id}, {PEERS_KEY, std::move(peers)}};
}

Status DecodeSharePeersRequest(const json &payload, uint32_t worldSize, SharePeersRequest &request)
{
    request.id = payload.at(ID_KEY).get<std::string>();
    const auto &peers = payload.at(PEERS_KEY);
    if (!peers.is_array() || peers.empty() || peers.size() >= static_cast<size_t>(worldSize)) {
        return Status::InvalidArgument("Invalid MemoryEngine shared peer list");
    }

    request.peers.reserve(peers.size());
    std::unordered_set<uint32_t> ranks;
    ranks.reserve(peers.size());
    for (const auto &entry : peers) {
        SharedPeer peer;
        CHECK_STATUS(DecodePeerRankAndGeneration(entry, peer.rank, peer.generation));
        if (!ranks.insert(peer.rank).second) {
            return Status::InvalidArgument("Duplicate MemoryEngine shared peer rank");
        }
        CHECK_STATUS(DecodeSharePeerDetails(entry, peer));
        request.peers.push_back(std::move(peer));
    }
    return Status::OK();
}

json EncodePeerRegionRequest(const PeerRequest &peer, const GvaRegion &region)
{
    auto payload = EncodePeerRequest(peer);
    EncodeRegion(region, payload);
    return payload;
}

Status DecodePeerRegion(const json &payload, PeerRegionAction action, GvaRegion &region)
{
    const bool map = action == PeerRegionAction::Map;
    const bool valid = map ? ReadPublishedRegion(payload, region) : ReadRegion(payload, region);
    return valid ? Status::OK() : Status::InvalidArgument("Invalid MemoryEngine peer region");
}

void ClearRegionDescriptor(json &payload)
{
    static_cast<void>(payload.erase(DESCRIPTOR_KEY));
}

std::string EncodeReply(const Status &status, json &response)
{
    response[CODE_KEY] = static_cast<uint16_t>(status.code());
    response[MESSAGE_KEY] = std::string(status.message());
    return response.dump();
}

Status Call(MemoryEngineRpc &rpc, const std::string &endpoint, MemoryRpc op, const json &request, json &response)
{
    std::string raw;
    CHECK_STATUS(rpc.CallOwned(endpoint, static_cast<int>(op), request.dump(), raw));
    return DecodeReply(raw, response);
}

Status Call(MemoryEngineRpc &rpc, const std::string &endpoint, MemoryRpc op, const json &request)
{
    json response;
    return Call(rpc, endpoint, op, request, response);
}

std::future<Status> CallAsync(MemoryEngineRpc &rpc, MemoryRpc op, const PeerCall &peer, Clock::time_point deadline)
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
}
