#include "tent/transport/memfabric/memfabric_transport.h"

#include <acl/acl.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <limits>
#include <map>
#include <optional>
#include <utility>

#include "memfabric_allocator.h"
#include "tent/common/utils/ip.h"

namespace mooncake {
namespace tent {

namespace {

// Must match SMEM Trans's NPU page alignment. All native registration
// boundaries are multiples of this size, including ranges on remote peers.
constexpr uint64_t kRegistrationAlignment = 2ULL * 1024 * 1024;

Status alignedBufferRange(const BufferDesc& desc, uint64_t& begin,
                          uint64_t& end) {
    constexpr auto mask = kRegistrationAlignment - 1;
    constexpr auto max = std::numeric_limits<uint64_t>::max();
    if (desc.addr == 0 || desc.length == 0 || desc.length > max - desc.addr)
        return Status::InvalidArgument(
            "invalid MemFabric buffer range" LOC_MARK);
    end = desc.addr + desc.length;
    if (end > max - mask)
        return Status::InvalidArgument(
            "MemFabric alignment overflows" LOC_MARK);
    begin = desc.addr & ~mask;
    end = (end + mask) & ~mask;
    return Status::OK();
}

std::optional<smem_bm_data_op_type> parseDataOpType(const std::string& str) {
    if (str == "device_sdma") return SMEMB_DATA_OP_SDMA;
    if (str == "device_rdma") return SMEMB_DATA_OP_DEVICE_RDMA;
    if (str == "host_rdma") return SMEMB_DATA_OP_HOST_RDMA;
    if (str == "device_urma") return SMEMB_DATA_OP_DEVICE_URMA;
    if (str == "device_uboe") return SMEMB_DATA_OP_DEVICE_UBOE;
    return std::nullopt;
}

TransferStatusEnum mapBatchStatus(int32_t query_ret, int32_t batch_result) {
    if (query_ret == SM_NEED_RETRY) return TransferStatusEnum::PENDING;
    if (query_ret != SM_OK) return TransferStatusEnum::FAILED;
    switch (static_cast<SMErrorCode>(batch_result)) {
        case SM_OK:
            return TransferStatusEnum::COMPLETED;
        case SM_TIMEOUT:
            return TransferStatusEnum::TIMEOUT;
        case SM_INVALID_PARAM:
            return TransferStatusEnum::INVALID;
        default:
            return TransferStatusEnum::FAILED;
    }
}

}  // namespace

std::atomic<int> MemFabricTransport::g_init_ref_{0};
std::mutex MemFabricTransport::g_init_mutex_;

MemFabricSubBatch::~MemFabricSubBatch() {
    if (pending_batch_count > 0)
        LOG(WARNING) << "~MemFabricSubBatch: " << pending_batch_count
                     << " batch(es) still PENDING out of " << task_list.size()
                     << " tasks";
}

MemFabricTransport::MemFabricTransport() = default;
MemFabricTransport::~MemFabricTransport() { (void)uninstall(); }

bool MemFabricTransport::supportsGlobalAddressing() const {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    return gva_ && gva_->available();
}
bool MemFabricTransport::describeAllocation(void* addr, size_t size,
                                            MemoryOptions& options) const {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    return gva_ && gva_->describe(addr, size, options);
}
bool MemFabricTransport::overlapsAllocation(void* addr, size_t size) const {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    return gva_ && gva_->overlaps(reinterpret_cast<uint64_t>(addr), size);
}
Status MemFabricTransport::allocateLocalMemory(void** addr, size_t size,
                                               MemoryOptions& options) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (!installed_ || stopping_)
        return Status::InternalError("MemFabric unavailable");
    if (!gva_) return Transport::allocateLocalMemory(addr, size, options);
    uint32_t type = 0;
    if (options.location == "cpu" || options.location.rfind("cpu:", 0) == 0)
        type = 2;
    else if (options.location == "npu" || options.location == "*" ||
             options.location == "npu:" + std::to_string(gva_config_.device))
        type = 1;
    else
        return Status::InvalidArgument(
            "GVA location must be cpu or the configured npu device");
    CHECK_STATUS(gva_->allocate(addr, size, type, options.global_address));
    options.type = MEMFABRIC;
    return Status::OK();
}
Status MemFabricTransport::freeLocalMemory(void* addr, size_t size) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (gva_ && gva_->overlaps(reinterpret_cast<uint64_t>(addr), size))
        return gva_->free(addr, size);
    return Transport::freeLocalMemory(addr, size);
}
Status MemFabricTransport::mapRemoteMemory(SegmentID id, void* addr,
                                           size_t size) {
    void* ignored = nullptr;
    return prepareGva(id, addr, size, &ignored, false);
}
Status MemFabricTransport::resolveGlobalAddress(void* gva, size_t size,
                                                void** device_address) {
    if (!device_address)
        return Status::InvalidArgument("Null device address output");
    *device_address = nullptr;
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (!gva_ || stopping_)
        return Status::NotImplemented("MemFabric GVA unavailable");
    return gva_->resolve(reinterpret_cast<uint64_t>(gva), size, device_address);
}
Status MemFabricTransport::mapRemoteMemory(SegmentID id, void* addr,
                                           size_t size, void** device_address) {
    return prepareGva(id, addr, size, device_address, true);
}
Status MemFabricTransport::prepareGva(SegmentID id, void* addr, size_t size,
                                      void** device_address, bool direct) {
    if (!device_address)
        return Status::InvalidArgument("Null device address output");
    *device_address = nullptr;
    std::shared_ptr<MemFabricSharedMemory> context;
    std::shared_ptr<ControlService> metadata;
    {
        std::lock_guard<std::mutex> lock(inflight_mu_);
        if (!gva_ || stopping_)
            return Status::NotImplemented("MemFabric GVA unavailable");
        context = gva_;
        metadata = metadata_;
    }
    SegmentDescRef segment;
    CHECK_STATUS(metadata->segmentManager().getRemoteCached(segment, id));
    auto* buffer = segment->findBuffer(reinterpret_cast<uint64_t>(addr), size);
    if (!buffer || buffer->addr != reinterpret_cast<uint64_t>(addr))
        return Status::InvalidArgument("GVA map requires published pool base");
    auto attribute = buffer->transport_attrs.find(MEMFABRIC);
    if (attribute == buffer->transport_attrs.end() || attribute->second.empty())
        return Status::InvalidArgument("Missing MemFabric GVA descriptor");
    nlohmann::json bundle = nlohmann::json::array();
    for (const auto& candidate : segment->getMemory().buffers) {
        auto attr = candidate.transport_attrs.find(MEMFABRIC);
        if (attr == candidate.transport_attrs.end() || attr->second.empty())
            continue;
        try {
            auto descriptor = nlohmann::json::parse(attr->second);
            if (descriptor.at("address").get<uint64_t>() != candidate.addr ||
                descriptor.at("size").get<uint64_t>() != candidate.length)
                return Status::InvalidArgument(
                    "GVA descriptor disagrees with published buffer");
            bundle.push_back(std::move(descriptor));
        } catch (const nlohmann::json::exception&) {
            return Status::InvalidArgument("Malformed MemFabric GVA metadata");
        }
    }
    {
        std::lock_guard<std::mutex> lock(inflight_mu_);
        gva_remote_[{id, reinterpret_cast<uint64_t>(addr)}] = segment->name;
    }
    if (!direct)
        return context->prepare(segment->name, bundle.dump(),
                                reinterpret_cast<uint64_t>(addr), size);
    CHECK_STATUS(context->map(segment->name, bundle.dump(),
                              reinterpret_cast<uint64_t>(addr), size));
    return context->resolve(reinterpret_cast<uint64_t>(addr), size,
                            device_address);
}
Status MemFabricTransport::unmapRemoteMemory(SegmentID id, void* addr) {
    std::shared_ptr<MemFabricSharedMemory> context;
    std::string peer;
    {
        std::lock_guard<std::mutex> lock(inflight_mu_);
        auto it = gva_remote_.find({id, reinterpret_cast<uint64_t>(addr)});
        if (!gva_ || it == gva_remote_.end())
            return Status::InvalidArgument("Unknown MemFabric GVA mapping");
        context = gva_;
        peer = it->second;
    }
    return context->unmap(peer, reinterpret_cast<uint64_t>(addr));
}

Status MemFabricTransport::loadConfig(const std::shared_ptr<Config>& conf) {
    if (conf) {
        auto limit = conf->get("transports/memfabric/gva/max_pending_copies",
                               int64_t{256});
        if (limit < 1 || limit > 1048576)
            return Status::InvalidArgument("Invalid GVA copy queue capacity");
        max_pending_copies_ = static_cast<size_t>(limit);
    }
    if (conf && conf->get("transports/memfabric/gva/enabled", false)) {
        const std::string prefix = "transports/memfabric/gva/";
        gva_config_.enabled = true;
        gva_config_.protocol = conf->get(prefix + "protocol", "device_sdma");
        gva_config_.trans_url = conf->get(prefix + "trans_url", "");
        gva_config_.group = conf->get(prefix + "group", "");
        const auto ranks = conf->get(prefix + "rank_count", int64_t{0});
        const auto rank = conf->get(prefix + "rank", int64_t{-1});
        const auto device = conf->get(prefix + "device", int64_t{-1});
        const auto entity = conf->get(prefix + "entity", int64_t{64});
        if (ranks < 1 || ranks > UINT32_MAX || rank < 0 || rank >= ranks ||
            device < 0 || device > UINT16_MAX || entity < 64 || entity >= 128)
            return Status::InvalidArgument(
                "Invalid MemFabric GVA configuration");
        gva_config_.rank_count = ranks;
        gva_config_.rank = rank;
        gva_config_.device = device;
        gva_config_.entity = entity;
        gva_config_.hbm_window = conf->get(prefix + "hbm_window", uint64_t{0});
        gva_config_.dram_window =
            conf->get(prefix + "dram_window", uint64_t{0});
        gva_config_.hbm_size = conf->get(prefix + "hbm_size", uint64_t{0});
        gva_config_.dram_size = conf->get(prefix + "dram_size", uint64_t{0});
        gva_config_.explicit_copy = conf->get(prefix + "explicit_copy", false);
    }
    const bool store =
        conf && conf->get("transports/memfabric/store_te_init", false);
    config_.data_op_type_str = memfabric_get_protocol(store);
    if (config_.data_op_type_str.empty()) {
        return Status::InvalidArgument(
            store ? "MF_STORE_PROTOCOL or MF_PROTOCOL must be set" LOC_MARK
                  : "MF_PROTOCOL must be set" LOC_MARK);
    }
    config_.data_op_type = parseDataOpType(config_.data_op_type_str);
    if (!config_.data_op_type.has_value()) {
        LOG(ERROR) << "Invalid data_op_type: " << config_.data_op_type_str;
        return Status::InvalidArgument(
            "Invalid data_op_type: " + config_.data_op_type_str + LOC_MARK);
    }
    LOG(INFO) << "data_op_type=" << config_.data_op_type_str
              << ", store_te_init=" << store;

    transfer_timeout_ms_ = 10000;
    const char* timeout_env = std::getenv("MF_TRANSFER_TIMEOUT");
    if (timeout_env && timeout_env[0] != '\0') {
        try {
            transfer_timeout_ms_ =
                static_cast<int32_t>(std::stoll(timeout_env));
        } catch (const std::exception&) {
            LOG(WARNING) << "MF_TRANSFER_TIMEOUT is not a valid integer: "
                         << timeout_env;
        }
    }
    if (transfer_timeout_ms_ <= 0) transfer_timeout_ms_ = 10000;

    return Status::OK();
}

Status MemFabricTransport::install(std::string& local_segment_name,
                                   std::shared_ptr<ControlService> metadata,
                                   std::shared_ptr<Topology> local_topology,
                                   std::shared_ptr<Config> conf) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (installed_)
        return Status::InvalidArgument("MemFabric already installed" LOC_MARK);
    LOG(INFO) << "install: segment_name=" << local_segment_name;

    local_segment_name_ = local_segment_name;
    metadata_ = metadata;
    CHECK_STATUS(loadConfig(conf));

    if (!metadata_) {
        return Status::InvalidArgument("metadata is null" LOC_MARK);
    }
    const auto local_segment = metadata_->segmentManager().getLocal();
    if (!local_segment) {
        return Status::InvalidArgument("local segment is missing" LOC_MARK);
    }
    const auto [host, rpc_port] =
        parseHostNameWithPort(local_segment->rpc_server_addr, 0);
    if (host.empty() || rpc_port == 0 || host == "0.0.0.0" || host == "::") {
        return Status::InvalidArgument(
            "Invalid local RPC address: " + local_segment->rpc_server_addr +
            LOC_MARK);
    }
    const bool ipv6 = host.find(':') != std::string::npos;
    // Segment names are logical identifiers. Reuse only the RPC host and
    // let MemFabric allocate its own listening port.
    const auto bind_url = buildIpAddrWithPort(host, 0, ipv6);
    const auto nic_url = "tcp://" + buildIpAddrWithPort(host, rpc_port, ipv6);
    const bool device_rdma =
        config_.data_op_type.value() & SMEMB_DATA_OP_DEVICE_RDMA;
    smem_trans_config_t cfg;
    // Reserve room for any assigned port instead of silently truncating an
    // endpoint in the vendor's fixed-size fields.
    if (buildIpAddrWithPort(host, std::numeric_limits<uint16_t>::max(), ipv6)
                .size() >= sizeof(cfg.url) ||
        (device_rdma && nic_url.size() >= sizeof(cfg.nic))) {
        return Status::InvalidArgument(
            "Local RPC address exceeds MemFabric endpoint capacity" LOC_MARK);
    }

    config_.bind_url = bind_url;
    config_.nic_url = device_rdma ? nic_url : "";
    config_.store_url.clear();
    config_.device_id = -1;
    const auto log_level = conf ? conf->get("log_level", "info") : "info";
    config_.log_level = log_level == "error"     ? 3
                        : log_level == "warning" ? 2
                                                 : 1;
    caps = Capabilities{};
    caps.dram_to_dram = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;
    stopping_ = false;
    installed_ = true;
    if (gva_config_.enabled) {
        gva_ = std::make_shared<MemFabricSharedMemory>(
            gva_config_, local_segment_name_, MemFabricGvaApi::load());
    }
    LOG(INFO) << "MemFabricTransport installed; native initialization is "
                 "deferred until the first memory registration";
    return Status::OK();
}

Status MemFabricTransport::bindDevice() const {
    if (config_.device_id < 0 ||
        aclrtSetDevice(config_.device_id) != ACL_ERROR_NONE)
        return Status::InternalError(
            "Unable to bind MemFabric device" LOC_MARK);
    return Status::OK();
}

Status MemFabricTransport::initializeLocked() {
    if (!installed_ || stopping_)
        return Status::InternalError("MemFabric is not available" LOC_MARK);
    if (handle_) return bindDevice();

    // Registration runs on the caller's device-bound thread, unlike the
    // framework's initialization timeout thread. Do not guess a device here.
    int32_t device_id = -1;
    if (aclrtGetDevice(&device_id) != ACL_ERROR_NONE || device_id < 0) {
        LOG(ERROR) << "aclrtGetDevice failed, device may be not set";
        return Status::InternalError(
            "aclrtGetDevice failed, device may be not set" LOC_MARK);
    }
    LOG(INFO) << "aclrtGetDevice: device_id=" << device_id;

    {
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        // Keep the library loaded until this installation owns an init
        // reference.
        if (!MemFabricUnderApi::LoadLibrary()) {
            LOG(ERROR)
                << "MemFabricUnderApi::LoadLibrary failed, libmf_smem.so "
                   "unavailable";
            return Status::DeviceNotFound("libmf_smem.so unavailable" LOC_MARK);
        }
        LOG(INFO) << "LoadLibrary: ok";

        MemFabricUnderApi::SmemSetLogLevel(config_.log_level);

        if (g_init_ref_.load() == 0) {
            auto ret = MemFabricUnderApi::SmemTransInit(device_id, 0);
            LOG(INFO) << "smem_trans_init: ret=" << ret
                      << ", ref=" << g_init_ref_.load();
            if (ret != 0) {
                LOG(ERROR) << "smem_trans_init failed, ret=" << ret;
                MemFabricUnderApi::CleanupLibrary();
                return Status::InternalError("smem_trans_init failed" LOC_MARK);
            }
        }
        g_init_ref_.fetch_add(1);
    }

    smem_trans_config_t cfg;
    if (MemFabricUnderApi::SmemTransConfigInit(&cfg) != 0) {
        LOG(ERROR) << "smem_trans_config_init failed";
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
        }
        return Status::InternalError("smem_trans_config_init failed" LOC_MARK);
    }
    cfg.role = SMEM_TRANS_BOTH;
    cfg.deviceId = device_id;
    cfg.dataOpType = config_.data_op_type.value();

    std::memcpy(cfg.url, config_.bind_url.c_str(), config_.bind_url.size() + 1);
    if (!config_.nic_url.empty()) {
        std::memcpy(cfg.nic, config_.nic_url.c_str(),
                    config_.nic_url.size() + 1);
        LOG(INFO) << "RDMA mode: nic=" << cfg.nic;
    }
    LOG(INFO) << "trans config: url=" << cfg.url << ", nic=" << cfg.nic
              << ", deviceId=" << cfg.deviceId
              << ", dataOpType=" << config_.data_op_type_str;

    handle_ = MemFabricUnderApi::SmemTransCreate(&cfg, 0);
    LOG(INFO) << "smem_trans_create: handle=" << handle_;
    if (!handle_) {
        LOG(ERROR) << "smem_trans_create returned null handle";
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
        }
        return Status::DeviceNotFound("smem_trans_create failed" LOC_MARK);
    }

    uint16_t actual_port = 0;
    if (MemFabricUnderApi::SmemTransGetRpcPort(handle_, &actual_port) == 0 &&
        actual_port != 0) {
        const auto host = parseHostNameWithPort(config_.bind_url, 0).first;
        config_.store_url = buildIpAddrWithPort(
            host, actual_port, host.find(':') != std::string::npos);
        LOG(INFO) << "smem_trans_get_rpc_port: port=" << actual_port
                  << ", store_url=" << config_.store_url;
    } else {
        LOG(ERROR) << "smem_trans_get_rpc_port failed";
        MemFabricUnderApi::SmemTransDestroy(handle_, 0);
        handle_ = nullptr;
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
        }
        return Status::InternalError("smem_trans_get_port failed" LOC_MARK);
    }

    if (auto s = publishLocalDevices(); !s.ok()) {
        LOG(ERROR) << "publishLocalDevices failed";
        MemFabricUnderApi::SmemTransDestroy(handle_, 0);
        handle_ = nullptr;
        std::lock_guard<std::mutex> lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
        }
        return s;
    }

    config_.device_id = device_id;
    LOG(INFO) << "MemFabricTransport initialized: device_id=" << device_id
              << ", url=" << config_.store_url
              << ", dataOpType=" << config_.data_op_type_str;
    return Status::OK();
}

Status MemFabricTransport::publishLocalDevices() {
    LOG(INFO) << "publishLocalDevices: store_url=" << config_.store_url;
    if (!metadata_) {
        LOG(ERROR) << "metadata is null in publishLocalDevices";
        return Status::InvalidArgument("metadata is null" LOC_MARK);
    }
    return metadata_->segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            if (!std::holds_alternative<MemorySegmentDesc>(segment.detail)) {
                segment.detail = MemorySegmentDesc{};
            }
            auto& detail = std::get<MemorySegmentDesc>(segment.detail);
            detail.device_attrs["memfabric_unique_id"] = config_.store_url;
            return Status::OK();
        });
}

Status MemFabricTransport::resolveDestUrl(const Request& request,
                                          std::string& dest_url) const {
    if (!metadata_) {
        LOG(ERROR) << "metadata is null in resolveDestUrl";
        return Status::InternalError("metadata is null" LOC_MARK);
    }
    SegmentDescRef pin;
    return metadata_->segmentManager().withCachedSegment(
        request.target_id, pin, [&](SegmentDesc* desc) -> Status {
            if (!desc) {
                LOG(ERROR) << "Segment not found for target_id="
                           << request.target_id;
                return Status::InvalidArgument("segment not found" LOC_MARK);
            }
            auto& detail = std::get<MemorySegmentDesc>(desc->detail);
            auto it = detail.device_attrs.find("memfabric_unique_id");
            if (it == detail.device_attrs.end() || it->second.empty()) {
                LOG(ERROR) << "memfabric_unique_id missing for target_id="
                           << request.target_id;
                return Status::InvalidArgument(
                    "memfabric_unique_id missing for segment" LOC_MARK);
            }
            dest_url = it->second;
            return Status::OK();
        });
}

Status MemFabricTransport::allocateSubBatch(SubBatchRef& batch,
                                            size_t max_size) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (stopping_)
        return Status::InternalError(
            "MemFabric transport is stopping" LOC_MARK);
    auto mf_batch = std::make_unique<MemFabricSubBatch>();
    mf_batch->task_list.reserve(max_size);
    mf_batch->capacity = max_size;
    batches_.insert(mf_batch.get());
    batch = mf_batch.release();
    return Status::OK();
}

Status MemFabricTransport::freeSubBatch(SubBatchRef& batch) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(transfer_timeout_ms_);
    std::unique_lock<std::mutex> lock(inflight_mu_);
    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch);
    if (!mf_batch) {
        LOG(ERROR) << "Invalid sub-batch type in freeSubBatch";
        return Status::InvalidArgument("invalid sub-batch" LOC_MARK);
    }
    while (mf_batch->pending_batch_count > 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            LOG(WARNING) << "MemFabricTransport freeSubBatch timed out with "
                         << mf_batch->pending_batch_count
                         << " pending native batches";
            return Status::InternalError(
                "MemFabric sub-batch free timed out" LOC_MARK);
        }
        for (size_t i = 0; i < mf_batch->task_list.size(); ++i) {
            if (std::chrono::steady_clock::now() >= deadline) break;
            if (mf_batch->task_list[i].status_word !=
                TransferStatusEnum::PENDING)
                continue;
            TransferStatus status{};
            // Query errors leave the task pending; retry until the deadline.
            (void)getTransferStatusLocked(batch, static_cast<int>(i), status);
        }
        if (mf_batch->pending_batch_count == 0) break;
        inflight_cv_.wait_until(
            lock, std::min(deadline, std::chrono::steady_clock::now() +
                                         std::chrono::milliseconds(1)));
    }
    batches_.erase(mf_batch);
    delete mf_batch;
    batch = nullptr;
    return Status::OK();
}

Status MemFabricTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (stopping_)
        return Status::InternalError(
            "MemFabric transport is stopping" LOC_MARK);
    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch);
    if (!mf_batch) {
        LOG(ERROR) << "Invalid sub-batch type in submitTransferTasks";
        return Status::InvalidArgument("invalid sub-batch" LOC_MARK);
    }

    bool any_gva = false, any_trans = false;
    for (const auto& request : request_list) {
        bool is_gva =
            gva_ && gva_->overlaps(reinterpret_cast<uint64_t>(request.source),
                                   request.length);
        CHECK_STATUS(metadata_->segmentManager().withCachedSegment(
            request.target_id, [&](SegmentDesc* segment) -> Status {
                if (segment->type != SegmentType::Memory) return Status::OK();
                auto* buffer =
                    segment->findBuffer(request.target_offset, request.length);
                if (buffer) {
                    auto attr = buffer->transport_attrs.find(MEMFABRIC);
                    is_gva |= attr != buffer->transport_attrs.end() &&
                              !attr->second.empty();
                }
                return Status::OK();
            }));
        any_gva |= is_gva;
        any_trans |= !is_gva;
    }
    if (any_gva) {
        if (any_trans)
            return Status::InvalidArgument(
                "Submit GVA and ordinary trans copies in separate calls");
        return submitGvaCopies(mf_batch, request_list);
    }
    if (!handle_)
        return Status::InternalError(
            "Register memory before transfer" LOC_MARK);
    CHECK_STATUS(bindDevice());

    uint64_t total_slice_count = 0;
    std::vector<size_t> slice_counts;
    slice_counts.reserve(request_list.size());
    for (const auto& request : request_list) {
        const auto source = reinterpret_cast<uint64_t>(request.source);
        if (gva_ && gva_->overlaps(source, request.length))
            return Status::NotImplemented(
                "Explicit trans copy of GVA pools is not supported");
        CHECK_STATUS(metadata_->segmentManager().withCachedSegment(
            request.target_id, [&](SegmentDesc* segment) -> Status {
                if (segment->type != SegmentType::Memory) return Status::OK();
                for (const auto& buffer : segment->getMemory().buffers) {
                    auto attr = buffer.transport_attrs.find(MEMFABRIC);
                    if (attr != buffer.transport_attrs.end() &&
                        !attr->second.empty() &&
                        request.target_offset >= buffer.addr &&
                        request.target_offset - buffer.addr < buffer.length)
                        return Status::NotImplemented(
                            "Explicit trans copy of remote GVA pools is not "
                            "supported");
                }
                return Status::OK();
            }));
        const auto max = std::numeric_limits<uint64_t>::max();
        if (source == 0 || request.target_offset == 0 || request.length == 0 ||
            request.length > max - source ||
            request.length > max - request.target_offset)
            return Status::InvalidArgument(
                "invalid MemFabric transfer range" LOC_MARK);
        // Count internal boundaries at both endpoints. Equal page offsets
        // share every boundary and must only be counted once.
        const auto local_boundaries =
            (source + request.length - 1) / kRegistrationAlignment -
            source / kRegistrationAlignment;
        const auto remote_boundaries =
            (request.target_offset + request.length - 1) /
                kRegistrationAlignment -
            request.target_offset / kRegistrationAlignment;
        const auto slices =
            1 + local_boundaries +
            (source % kRegistrationAlignment ==
                     request.target_offset % kRegistrationAlignment
                 ? 0
                 : remote_boundaries);
        if (slices > std::numeric_limits<uint32_t>::max() - total_slice_count)
            return Status::InvalidArgument(
                "too many native transfer slices" LOC_MARK);
        total_slice_count += slices;
        slice_counts.push_back(static_cast<size_t>(slices));
    }

    // Resolve every destination before changing the sub-batch so an error
    // leaves existing tasks untouched and dispatches no new work.
    std::vector<std::string> dest_urls(request_list.size());
    for (size_t i = 0; i < request_list.size(); ++i) {
        CHECK_STATUS(resolveDestUrl(request_list[i], dest_urls[i]));
    }

    using GroupKey = std::pair<std::string, bool>;  // {dest_url, is_write}
    std::map<GroupKey, std::vector<size_t>> groups;
    for (size_t i = 0; i < request_list.size(); ++i) {
        bool write = (request_list[i].opcode == Request::WRITE);
        groups[{dest_urls[i], write}].push_back(i);
    }

    const size_t start = mf_batch->task_list.size();
    mf_batch->task_list.resize(start + request_list.size());

    for (auto& [key, indices] : groups) {
        const auto& dest_url = key.first;
        bool is_write = key.second;
        std::vector<void*> src_list;
        std::vector<void*> dest_list;
        std::vector<size_t> size_list;
        size_t group_slice_count = 0;
        for (size_t index : indices) group_slice_count += slice_counts[index];
        src_list.reserve(group_slice_count);
        dest_list.reserve(group_slice_count);
        size_list.reserve(group_slice_count);
        for (size_t index : indices) {
            const auto& req = request_list[index];
            auto source = reinterpret_cast<uint64_t>(req.source);
            auto target = req.target_offset;
            auto remaining = req.length;
            while (remaining != 0) {
                // A logical buffer can span several gap registrations. Split
                // at both endpoints' aligned boundaries so each native copy
                // is wholly inside one local and one remote registration.
                const auto length = std::min<uint64_t>(
                    remaining, std::min(kRegistrationAlignment -
                                            source % kRegistrationAlignment,
                                        kRegistrationAlignment -
                                            target % kRegistrationAlignment));
                src_list.push_back(reinterpret_cast<void*>(source));
                dest_list.push_back(reinterpret_cast<void*>(target));
                size_list.push_back(length);
                source += length;
                target += length;
                remaining -= length;
            }
        }
        const auto batch_size = static_cast<uint32_t>(src_list.size());

        smem_trans_batch_params_t params = {};
        params.destUrl = dest_url.c_str();
        params.srcList = src_list.data();
        params.destList = dest_list.data();
        params.sizeList = size_list.data();
        params.batchSize = batch_size;
        params.opcode = is_write ? SMEM_TRANS_OP_WRITE : SMEM_TRANS_OP_READ;
        params.stream = nullptr;
        params.flags = 0;

        uint64_t batch_id = 0;
        auto ret =
            MemFabricUnderApi::SmemTransBatchCopy(handle_, &params, &batch_id);
        if (ret != 0) {
            LOG(ERROR) << "smem_trans_batch_copy failed, ret=" << ret
                       << ", dest_url=" << dest_url
                       << ", batch_size=" << batch_size;
            for (size_t idx : indices) {
                auto& task = mf_batch->task_list[start + idx];
                task.status_word = TransferStatusEnum::FAILED;
            }
            continue;
        }

        for (size_t idx : indices) {
            auto& task = mf_batch->task_list[start + idx];
            task.request = request_list[idx];
            task.batch_id = batch_id;
            task.length = request_list[idx].length;
            task.status_word = TransferStatusEnum::PENDING;
        }
        inflight_.fetch_add(1, std::memory_order_acq_rel);
        mf_batch->pending_batch_count++;
    }

    return Status::OK();
}

Status MemFabricTransport::submitGvaCopies(
    MemFabricSubBatch* batch, const std::vector<Request>& requests) {
    if (!gva_) return Status::NotImplemented("GVA copy context unavailable");
    if (requests.size() > max_pending_copies_ - std::min(max_pending_copies_,
                                                         gva_pending_->load()))
        return Status::TooManyRequests("GVA copy queue is full");
    struct Copy {
        uint64_t source, destination;
        size_t size;
        uint32_t direction;
    };
    if (requests.size() >
        batch->capacity - std::min(batch->capacity, batch->task_list.size()))
        return Status::TooManyRequests("GVA sub-batch capacity exceeded");
    std::vector<Copy> copies;
    std::vector<std::shared_ptr<MemFabricCopyCompletion>> completions;
    try {
        copies.reserve(requests.size());
        completions.reserve(requests.size());
        for (size_t i = 0; i < requests.size(); ++i)
            completions.push_back(std::make_shared<MemFabricCopyCompletion>());
    } catch (...) {
        return Status::InternalError("Cannot allocate GVA copy tasks");
    }
    auto rollback = [&] {
        for (size_t i = 0; i < copies.size(); ++i) gva_->cancelCopy();
    };
    for (const auto& request : requests) {
        if (request.opcode != Request::READ &&
            request.opcode != Request::WRITE) {
            rollback();
            return Status::InvalidArgument("Invalid GVA copy opcode");
        }
        // Source is the local endpoint for both READ and WRITE. It must be
        // inside an owned GVA pool; beginCopy checks full range/publication.
        auto local = reinterpret_cast<uint64_t>(request.source);
        bool local_owned = false;
        auto status = metadata_->segmentManager().withCachedSegment(
            LOCAL_SEGMENT_ID, [&](SegmentDesc* segment) -> Status {
                auto* buffer = segment->findBuffer(local, request.length);
                if (buffer) {
                    auto attr = buffer->transport_attrs.find(MEMFABRIC);
                    local_owned = attr != buffer->transport_attrs.end() &&
                                  !attr->second.empty();
                }
                return Status::OK();
            });
        if (!status.ok() || !local_owned) {
            rollback();
            return status.ok() ? Status::InvalidArgument(
                                     "Local copy endpoint must be a registered "
                                     "logical GVA pool")
                               : status;
        }
        status = metadata_->segmentManager().withCachedSegment(
            request.target_id, [&](SegmentDesc* segment) -> Status {
                auto* buffer =
                    segment->findBuffer(request.target_offset, request.length);
                if (!buffer)
                    return Status::InvalidArgument(
                        "Remote GVA copy range is not registered");
                auto attr = buffer->transport_attrs.find(MEMFABRIC);
                if (attr == buffer->transport_attrs.end() ||
                    attr->second.empty())
                    return Status::InvalidArgument(
                        "Remote copy endpoint must be a GVA pool");
                auto mapping =
                    gva_remote_.find({request.target_id, buffer->addr});
                if (mapping == gva_remote_.end() ||
                    mapping->second != segment->name)
                    return Status::InvalidArgument(
                        "Map remote GVA pool before copy");
                return gva_->validateCopyTarget(segment->name, attr->second);
            });
        if (!status.ok()) {
            rollback();
            return status;
        }
        Copy copy{local, request.target_offset, request.length, 0};
        if (request.opcode == Request::READ)
            std::swap(copy.source, copy.destination);
        status = gva_->beginCopy(copy.source, copy.destination, copy.size,
                                 copy.direction);
        if (!status.ok()) {
            rollback();
            return status;
        }
        copies.push_back(copy);
    }
    try {
        if (!copy_pool_) copy_pool_ = std::make_unique<ThreadPool>(1);
        batch->task_list.reserve(batch->task_list.size() + requests.size());
    } catch (...) {
        rollback();
        return Status::InternalError("Cannot allocate GVA copy queue");
    }
    for (size_t i = 0; i < copies.size(); ++i) {
        auto completion = completions[i];
        MemFabricTask task;
        task.request = requests[i];
        task.length = requests[i].length;
        task.gva_completion = completion;
        batch->task_list.push_back(task);
        ++batch->pending_batch_count;
        auto context = gva_;
        auto pending = gva_pending_;
        auto copy = copies[i];
        pending->fetch_add(1);
        try {
            copy_pool_->enqueue([context, pending, completion, copy] {
                auto outcome = TransferStatusEnum::FAILED;
                try {
                    auto status =
                        context->runCopy(copy.source, copy.destination,
                                         copy.size, copy.direction);
                    if (status.ok()) outcome = TransferStatusEnum::COMPLETED;
                } catch (...) {
                    context->failCopy();
                }
                completion->status.store(outcome, std::memory_order_release);
                pending->fetch_sub(1, std::memory_order_release);
            });
        } catch (...) {
            context->cancelCopy();
            completion->status.store(TransferStatusEnum::FAILED,
                                     std::memory_order_release);
            pending->fetch_sub(1);
        }
    }
    return Status::OK();
}

Status MemFabricTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                             TransferStatus& status) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    return getTransferStatusLocked(batch, task_id, status);
}

Status MemFabricTransport::getTransferStatusLocked(SubBatchRef batch,
                                                   int task_id,
                                                   TransferStatus& status) {
    auto* mf_batch = dynamic_cast<MemFabricSubBatch*>(batch);
    if (!mf_batch || task_id < 0 ||
        task_id >= static_cast<int>(mf_batch->task_list.size())) {
        LOG(ERROR) << "Invalid task_id=" << task_id << " in getTransferStatus";
        return Status::InvalidArgument("invalid task" LOC_MARK);
    }

    auto& task = mf_batch->task_list[task_id];
    if (task.status_word != TransferStatusEnum::PENDING) {
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    if (task.gva_completion) {
        task.status_word =
            task.gva_completion->status.load(std::memory_order_acquire);
        if (task.status_word != TransferStatusEnum::PENDING) {
            if (mf_batch->pending_batch_count) --mf_batch->pending_batch_count;
            if (task.status_word == TransferStatusEnum::COMPLETED)
                task.transferred_bytes = task.length;
        }
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }
    auto cache_it = mf_batch->batch_status_cache.find(task.batch_id);
    if (cache_it != mf_batch->batch_status_cache.end() &&
        cache_it->second != TransferStatusEnum::PENDING) {
        task.status_word = cache_it->second;
        if (task.status_word == TransferStatusEnum::COMPLETED)
            task.transferred_bytes = task.length;
        status = TransferStatus{task.status_word, task.transferred_bytes};
        return Status::OK();
    }

    CHECK_STATUS(bindDevice());
    int32_t batch_result = 0;
    auto query_ret = MemFabricUnderApi::SmemTransBatchStatusQuery(
        handle_, task.batch_id, &batch_result, 0);
    VLOG(1) << "batch_status_query: batch_id=" << task.batch_id
            << ", query_ret=" << query_ret << ", batch_result=" << batch_result;

    if (query_ret != SM_OK && query_ret != SM_NEED_RETRY) {
        status = TransferStatus{TransferStatusEnum::PENDING, 0};
        return Status::InternalError("MemFabric completion unknown" LOC_MARK);
    }

    TransferStatusEnum batch_status = mapBatchStatus(query_ret, batch_result);
    if (batch_status != TransferStatusEnum::PENDING &&
        batch_status != TransferStatusEnum::COMPLETED) {
        LOG(WARNING) << "smem_trans_batch_status_query: query_ret=" << query_ret
                     << ", batch_result=" << batch_result
                     << ", batch_id=" << task.batch_id
                     << ", mapped=" << static_cast<int>(batch_status);
    }

    if (batch_status != TransferStatusEnum::PENDING) {
        if (mf_batch->pending_batch_count > 0) mf_batch->pending_batch_count--;
        if (inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            inflight_cv_.notify_all();
        }
        mf_batch->batch_status_cache[task.batch_id] = batch_status;
        for (auto& t : mf_batch->task_list) {
            if (!t.gva_completion && t.batch_id == task.batch_id &&
                t.status_word == TransferStatusEnum::PENDING) {
                t.status_word = batch_status;
                if (t.status_word == TransferStatusEnum::COMPLETED)
                    t.transferred_bytes = t.length;
            }
        }
    }

    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status MemFabricTransport::addMemoryBuffer(BufferDesc& desc,
                                           const MemoryOptions& options) {
    std::vector<BufferDesc> buffers{desc};
    CHECK_STATUS(addMemoryBuffer(buffers, options));
    desc = std::move(buffers.front());
    return Status::OK();
}

Status MemFabricTransport::addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                           const MemoryOptions& options) {
    if (desc_list.empty()) return Status::OK();
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (gva_) {
        bool any = false, all = true;
        for (const auto& desc : desc_list) {
            const bool owned = gva_->overlaps(desc.addr, desc.length);
            any |= owned;
            all &= owned;
        }
        if (any) {
            if (!all)
                return Status::InvalidArgument(
                    "Register GVA pools separately from ordinary buffers");
            for (const auto& desc : desc_list) {
                MemoryOptions trusted;
                if (!gva_->describe(reinterpret_cast<void*>(desc.addr),
                                    desc.length, trusted))
                    return Status::InvalidArgument(
                        "Register the complete GVA pool");
            }
            for (auto& desc : desc_list) {
                CHECK_STATUS(gva_->publish(reinterpret_cast<void*>(desc.addr),
                                           desc.length,
                                           desc.transport_attrs[MEMFABRIC]));
                if (std::find(desc.transports.begin(), desc.transports.end(),
                              MEMFABRIC) == desc.transports.end())
                    desc.transports.push_back(MEMFABRIC);
            }
            return Status::OK();
        }
    }
    CHECK_STATUS(initializeLocked());

    const bool local_sdma = config_.data_op_type == SMEMB_DATA_OP_SDMA &&
                            options.perm == kLocalReadWrite;
    if (local_sdma) {
        for (auto& desc : desc_list) {
            if (std::find(desc.transports.begin(), desc.transports.end(),
                          MEMFABRIC) == desc.transports.end())
                desc.transports.push_back(MEMFABRIC);
            desc.transport_attrs[MEMFABRIC] = "";
        }
        return Status::OK();
    }

    std::vector<std::pair<uint64_t, uint64_t>> ranges(desc_list.size());
    for (size_t i = 0; i < desc_list.size(); ++i)
        CHECK_STATUS(alignedBufferRange(desc_list[i], ranges[i].first,
                                        ranges[i].second));

    // Restore native batch coalescing before subtracting existing MRs.
    // Do not merge merely adjacent ranges: they may be separate allocations.
    std::sort(ranges.begin(), ranges.end());
    size_t merged_count = 0;
    for (const auto& range : ranges) {
        if (merged_count > 0 && range.first < ranges[merged_count - 1].second) {
            ranges[merged_count - 1].second =
                std::max(ranges[merged_count - 1].second, range.second);
        } else {
            ranges[merged_count++] = range;
        }
    }
    ranges.resize(merged_count);

    size_t registered_gaps = 0;
    for (size_t range_index = 0; range_index < ranges.size(); ++range_index) {
        const auto [begin, end] = ranges[range_index];
        auto cursor = begin;
        auto it = registered_ranges_.upper_bound(cursor);
        if (it != registered_ranges_.begin()) --it;
        while (cursor < end) {
            while (it != registered_ranges_.end() && it->second <= cursor) ++it;
            if (it != registered_ranges_.end() && it->first <= cursor) {
                cursor = std::min(end, it->second);
                ++it;
                continue;
            }
            const auto gap_end =
                it == registered_ranges_.end() ? end : std::min(end, it->first);
            const auto ret = MemFabricUnderApi::SmemTransRegisterMem(
                handle_, reinterpret_cast<void*>(cursor), gap_end - cursor, 0);
            if (ret != 0) {
                LOG(ERROR) << "SmemTransRegisterMem failed, ret=" << ret
                           << ", protocol=" << config_.data_op_type_str
                           << ", merged_range_index=" << range_index
                           << ", range_begin=" << begin << ", range_end=" << end
                           << ", gap_begin=" << cursor
                           << ", gap_end=" << gap_end
                           << ", registered_gaps=" << registered_gaps
                           << "; successful MRs retained until destruction";
                return Status::InternalError("register_mem failed" LOC_MARK);
            }
            // Commit each successful native range immediately: if a later
            // gap fails, retrying must not register this prefix a second time.
            registered_ranges_.emplace(cursor, gap_end);
            ++registered_gaps;
            cursor = gap_end;
        }
    }
    for (auto& desc : desc_list) {
        if (std::find(desc.transports.begin(), desc.transports.end(),
                      MEMFABRIC) == desc.transports.end())
            desc.transports.push_back(MEMFABRIC);
        desc.transport_attrs[MEMFABRIC] = "";
    }
    return Status::OK();
}

Status MemFabricTransport::removeMemoryBuffer(BufferDesc& desc) {
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (gva_ && gva_->overlaps(desc.addr, desc.length))
        return gva_->unpublish(reinterpret_cast<void*>(desc.addr));
    if (!handle_) return Status::OK();
    if (config_.data_op_type == SMEMB_DATA_OP_SDMA &&
        desc.permission == kLocalReadWrite)
        return Status::OK();
    CHECK_STATUS(bindDevice());

    // SMEM's deregistration currently retains the native MR until destruction;
    // preserve registered_ranges_ so overlapping logical buffers remain valid.
    LOG(INFO) << "removeMemoryBuffer: addr=" << desc.addr;
    auto ret = MemFabricUnderApi::SmemTransDeregisterMem(
        handle_, reinterpret_cast<void*>(desc.addr));
    if (ret != 0) {
        LOG(ERROR) << "smem_trans_deregister_mem failed, ret=" << ret
                   << ", addr=" << desc.addr;
        return Status::InternalError("deregister_mem failed" LOC_MARK);
    }
    return Status::OK();
}

Status MemFabricTransport::quiesce() {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(transfer_timeout_ms_);
    std::unique_lock<std::mutex> lock(inflight_mu_);
    stopping_ = true;
    while (inflight_.load(std::memory_order_acquire) > 0 ||
           gva_pending_->load(std::memory_order_acquire) > 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            LOG(WARNING) << "MemFabricTransport quiesce timed out with "
                         << inflight_.load() << " in-flight batches";
            return Status::InternalError(
                "MemFabric quiesce timed out" LOC_MARK);
        }
        for (auto* batch : batches_) {
            if (std::chrono::steady_clock::now() >= deadline) break;
            if (batch->pending_batch_count == 0) continue;
            for (size_t i = 0; i < batch->task_list.size(); ++i) {
                if (std::chrono::steady_clock::now() >= deadline) break;
                if (batch->task_list[i].status_word !=
                    TransferStatusEnum::PENDING)
                    continue;
                TransferStatus status{};
                // Query errors leave the task pending; retry until the
                // deadline.
                (void)getTransferStatusLocked(batch, static_cast<int>(i),
                                              status);
            }
        }
        if (inflight_.load(std::memory_order_acquire) == 0 &&
            gva_pending_->load(std::memory_order_acquire) == 0)
            break;
        // Release the lock between polls so normal status queries and batch
        // release can proceed. Submission remains blocked by stopping_.
        inflight_cv_.wait_until(
            lock, std::min(deadline, std::chrono::steady_clock::now() +
                                         std::chrono::milliseconds(1)));
    }
    return Status::OK();
}

Status MemFabricTransport::uninstall() {
    auto drain = quiesce();
    if (!drain.ok() && gva_pending_->load()) return drain;
    // Join without inflight_mu_. Jobs own their context and completion state.
    copy_pool_.reset();
    std::lock_guard<std::mutex> lock(inflight_mu_);
    if (!installed_) return Status::OK();
    if (gva_) {
        CHECK_STATUS(gva_->close());
        gva_.reset();
    }
    LOG(INFO) << "uninstall: handle=" << handle_;
    if (handle_) {
        CHECK_STATUS(bindDevice());
        MemFabricUnderApi::SmemTransDestroy(handle_, 0);
        handle_ = nullptr;
        registered_ranges_.clear();
        LOG(INFO) << "smem_trans_destroy: done";
        std::lock_guard<std::mutex> init_lock(g_init_mutex_);
        if (g_init_ref_.fetch_sub(1) == 1) {
            MemFabricUnderApi::SmemTransUninit(0);
            MemFabricUnderApi::CleanupLibrary();
            LOG(INFO) << "smem_trans_uninit + cleanup_library: ref=0";
        }
    }
    installed_ = false;
    config_.device_id = -1;
    metadata_.reset();
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
