// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
// Opt-in real-ACL, public TransferEngine two-process integration smoke.
#include <acl/acl.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include "tent/common/config.h"
#include "tent/transfer_engine.h"

using namespace mooncake::tent;
using Clock = std::chrono::steady_clock;
namespace {
struct Options {
    std::string protocol = "device_sdma", bind_ip = "127.0.0.1";
    int devices[2] = {0, 1};
    size_t pool = 1ULL << 30;
    unsigned timeout = 180;
    size_t allocations = 1;
    size_t allocation_size = 0;
};
void check(const Status& s, const std::string& operation) {
    if (!s.ok()) throw std::runtime_error(operation + ": " + s.ToString());
}
void aclCheck(aclError s, const char* operation) {
    if (s != ACL_ERROR_NONE)
        throw std::runtime_error(std::string(operation) +
                                 ": ACL=" + std::to_string(s));
}
void io(int fd, void* data, size_t size, bool sending) {
    auto* p = static_cast<char*>(data);
    while (size) {
        auto n =
            sending ? send(fd, p, size, MSG_NOSIGNAL) : recv(fd, p, size, 0);
        if (n <= 0) throw std::runtime_error("application handshake failed");
        p += n;
        size -= static_cast<size_t>(n);
    }
}
std::string exchange(int fd, std::string local) {
    uint32_t length = static_cast<uint32_t>(local.size());
    io(fd, &length, sizeof(length), true);
    io(fd, local.data(), local.size(), true);
    io(fd, &length, sizeof(length), false);
    if (length > (1U << 16))
        throw std::runtime_error("invalid handshake length");
    std::string remote(length, '\0');
    io(fd, remote.data(), remote.size(), false);
    return remote;
}
void barrier(int fd, const char* phase) {
    if (exchange(fd, phase) != phase)
        throw std::runtime_error("barrier phase mismatch");
}
uint64_t hash(const std::vector<unsigned char>& data) {
    uint64_t value = 1469598103934665603ULL;
    for (auto c : data) value = (value ^ c) * 1099511628211ULL;
    return value;
}
std::vector<unsigned char> pattern(size_t length, int rank) {
    std::vector<unsigned char> bytes(length);
    for (size_t i = 0; i < length; ++i)
        bytes[i] = static_cast<unsigned char>(
            (i * 17 + i / 251 + rank * 73 + 11) & 255);
    return bytes;
}
void* offset(void* p, size_t n) {
    return reinterpret_cast<void*>(reinterpret_cast<uint64_t>(p) + n);
}
void transfer(TransferEngine& engine, int rank, SegmentID peer,
              Request::OpCode op, const std::vector<void*>& local,
              const std::vector<uint64_t>& remote, size_t local_offset,
              size_t remote_offset, size_t size, unsigned timeout) {
    // Submit all allocations together, with two disjoint pieces per allocation.
    // Request merging can coalesce adjacent pieces; use two allocations to
    // retain multiple native entries in the hardware batch.
    std::vector<Request> requests;
    for (size_t i = 0; i < local.size(); ++i) {
        for (size_t piece = 0; piece < 2; ++piece) {
            Request request{};
            request.opcode = op;
            request.source =
                offset(local[i], local_offset + piece * (size / 2));
            request.target_id = peer;
            request.target_offset =
                remote[i] + remote_offset + piece * (size / 2);
            request.length = size / 2;
            request.transport_hint = MEMFABRIC;
            requests.push_back(request);
        }
    }
    const auto batch = engine.allocateBatch(requests.size());
    check(engine.submitTransfer(batch, requests), "submitTransfer");
    const auto deadline = Clock::now() + std::chrono::seconds(timeout);
    for (size_t i = 0; i < requests.size(); ++i) {
        TransferStatus status{};
        for (;;) {
            check(engine.getTransferStatus(batch, i, status),
                  "getTransferStatus");
            if (status.s == COMPLETED) break;
            if (status.s != INITIAL && status.s != PENDING)
                throw std::runtime_error("transfer terminal status=" +
                                         std::to_string(status.s));
            if (Clock::now() >= deadline)
                throw std::runtime_error("transfer timeout");
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        if (status.transferred_bytes != requests[i].length)
            throw std::runtime_error("incorrect completed bytes");
    }
    std::cerr << "rank=" << rank
              << " op=" << (op == Request::WRITE ? "WRITE" : "READ")
              << " status=COMPLETED batch_requests=" << requests.size()
              << " bytes=" << size * local.size() << std::endl;
    check(engine.freeBatch(batch), "freeBatch");
}
void child(const Options& o, int rank, int fd) {
    try {
        FLAGS_logtostderr = true;
        google::InitGoogleLogging("tent_memfabric_gva_engine_smoke");
        alarm(o.timeout);
        timeval socket_timeout{static_cast<long>(o.timeout), 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &socket_timeout,
                   sizeof(socket_timeout));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &socket_timeout,
                   sizeof(socket_timeout));
        setenv("MF_PROTOCOL", o.protocol.c_str(), 1);
        aclCheck(aclInit(nullptr), "aclInit");
        aclCheck(aclrtSetDevice(o.devices[rank]), "aclrtSetDevice");
        auto config = std::make_shared<Config>();
        config->set("metadata_type", "p2p");
        config->set("metadata_servers", "");
        config->set("rpc_server_hostname", o.bind_ip);
        config->set("rpc_server_port", 0);
        config->set("log_level", "info");
        for (const char* name : {"tcp", "shm", "rdma", "nvlink", "mnnvl", "gds",
                                 "io_uring", "ascend_direct", "hp_tcp", "ub",
                                 "fabric", "mlu", "xpu", "hylink", "mpcomm"})
            config->set(std::string("transports/") + name + "/enable", false);
        config->set("transports/memfabric/enable", true);
        const std::string prefix = "transports/memfabric/gva/";
        config->set(prefix + "enabled", true);
        config->set(prefix + "rank_count", int64_t{2});
        config->set(prefix + "rank", int64_t{rank});
        // hbm_window / dram_window are expressed in GiB, while the pool
        // budget (o.pool) is byte-granular for allocation sizing.
        config->set(prefix + "hbm_window", uint64_t{o.pool >> 30});
        config->set(prefix + "dram_window", uint64_t{0});
        // Deliberately leak on failure: exiting the process is safer than
        // attempting object teardown while failed device work may be in flight.
        auto* engine = new TransferEngine(config);
        if (!engine->available())
            throw std::runtime_error("TransferEngine unavailable");
        barrier(fd, "installed");
        const auto allocation_size =
            o.allocation_size ? o.allocation_size : o.pool / o.allocations;
        constexpr size_t payload_size = 2ULL << 20;
        std::vector<void*> pools(o.allocations, nullptr);
        auto local_addresses = nlohmann::json::array();
        for (size_t index = 0; index < pools.size(); ++index) {
            MemoryOptions options;
            options.type = MEMFABRIC;
            options.location = "npu:" + std::to_string(o.devices[rank]);
            check(engine->allocateLocalMemory(&pools[index], allocation_size,
                                              options),
                  "allocateLocalMemory");
            if (options.global_address !=
                    reinterpret_cast<uint64_t>(pools[index]) ||
                options.type != MEMFABRIC)
                throw std::runtime_error(
                    "allocation options disagree with returned GVA");
            const auto own = pattern(payload_size, rank + 2 * index);
            const std::vector<unsigned char> zeros(payload_size, 0);
            aclCheck(aclrtMemcpy(pools[index], allocation_size, own.data(),
                                 own.size(), ACL_MEMCPY_HOST_TO_DEVICE),
                     "fill source");
            aclCheck(aclrtMemcpy(offset(pools[index], payload_size),
                                 allocation_size - payload_size, zeros.data(),
                                 zeros.size(), ACL_MEMCPY_HOST_TO_DEVICE),
                     "clear destination");
            local_addresses.push_back(reinterpret_cast<uint64_t>(pools[index]));
        }
        check(engine->registerLocalMemory(
                  pools, std::vector<size_t>(pools.size(), allocation_size)),
              "registerLocalMemory batch");
        const auto local = nlohmann::json{
            {"segment", engine->getSegmentName()},
            {"gvas", local_addresses},
            {"allocation_size",
             allocation_size}}.dump();
        const auto remote = nlohmann::json::parse(exchange(fd, local));
        if (remote.at("allocation_size").get<uint64_t>() != allocation_size ||
            remote.at("gvas").size() != pools.size())
            throw std::runtime_error("peer allocation layout mismatch");
        const auto peer_gvas = remote.at("gvas").get<std::vector<uint64_t>>();
        SegmentID peer;
        check(
            engine->openSegment(peer, remote.at("segment").get<std::string>()),
            "openSegment");
        const auto deadline = Clock::now() + std::chrono::seconds(o.timeout);
        for (size_t index = 0; index < pools.size(); ++index) {
            std::cerr << "rank=" << rank << " allocation=" << index
                      << " segment=" << engine->getSegmentName()
                      << " device=" << o.devices[rank]
                      << " GVA=" << pools[index] << " remoteGVA="
                      << reinterpret_cast<void*>(peer_gvas[index])
                      << " allocation_size=" << allocation_size
                      << " total_budget=" << o.pool
                      << " payload=" << payload_size << std::endl;
            unsigned attempts = 0;
            for (;;) {
                const auto status = engine->mapRemoteMemory(
                    peer, reinterpret_cast<void*>(peer_gvas[index]),
                    allocation_size, MEMFABRIC);
                if (status.ok()) break;
                if (attempts++ % 50 == 0)
                    std::cerr << "rank=" << rank << " allocation=" << index
                              << " map retry " << status.ToString()
                              << std::endl;
                if (Clock::now() >= deadline)
                    check(status, "mapRemoteMemory timeout");
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
        barrier(fd, "mapped-and-ready");
        transfer(*engine, rank, peer, Request::WRITE, pools, peer_gvas, 0,
                 payload_size, payload_size, o.timeout);
        barrier(fd, "writes-completed");
        std::vector<unsigned char> actual(payload_size);
        for (size_t index = 0; index < pools.size(); ++index) {
            aclCheck(aclrtMemcpy(actual.data(), actual.size(),
                                 offset(pools[index], payload_size),
                                 payload_size, ACL_MEMCPY_DEVICE_TO_HOST),
                     "read owner destination");
            if (actual != pattern(payload_size, 1 - rank + 2 * index))
                throw std::runtime_error(
                    "WRITE owner bytes differ for allocation " +
                    std::to_string(index));
            std::cerr << "rank=" << rank << " allocation=" << index
                      << " WRITE owner checksum=" << hash(actual) << std::endl;
        }
        barrier(fd, "write-bytes-verified");
        transfer(*engine, rank, peer, Request::READ, pools, peer_gvas,
                 2 * payload_size, payload_size, payload_size, o.timeout);
        for (size_t index = 0; index < pools.size(); ++index) {
            aclCheck(aclrtMemcpy(actual.data(), actual.size(),
                                 offset(pools[index], 2 * payload_size),
                                 payload_size, ACL_MEMCPY_DEVICE_TO_HOST),
                     "read READ result");
            if (actual != pattern(payload_size, rank + 2 * index))
                throw std::runtime_error("READ bytes differ for allocation " +
                                         std::to_string(index));
            std::cerr << "rank=" << rank << " allocation=" << index
                      << " READ checksum=" << hash(actual) << std::endl;
        }
        barrier(fd, "reads-verified-and-stopped");
        for (size_t index = 0; index < pools.size(); ++index) {
            const auto status = engine->unmapRemoteMemory(
                peer, reinterpret_cast<void*>(peer_gvas[index]), MEMFABRIC);
            if (index + 1 == pools.size() || !status.IsTooManyRequests())
                check(status, "unmapRemoteMemory");
        }
        barrier(fd, "both-unmapped");
        check(engine->closeSegment(peer), "closeSegment");
        for (auto pool : pools) {
            check(engine->unregisterLocalMemory(pool, allocation_size),
                  "unregisterLocalMemory");
            check(engine->freeLocalMemory(pool), "freeLocalMemory");
        }
        barrier(fd, "both-freed");
        delete engine;
        aclCheck(aclrtResetDevice(o.devices[rank]), "aclrtResetDevice");
        aclCheck(aclFinalize(), "aclFinalize");
        std::cerr << "rank=" << rank << " PASS public TransferEngine GVA "
                  << o.protocol << std::endl;
        _exit(0);
    } catch (const std::exception& e) {
        std::cerr << "rank=" << rank << " FAIL " << e.what() << std::endl;
        _exit(1);
    }
}
}  // namespace
int main(int argc, char** argv) {
    try {
        Options o;
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--help") {
                std::cout
                    << "--protocol device_sdma|device_rdma|device_urma "
                       "--device0 N --device1 N\n"
                       "--pool-size BYTES (default 1073741824; multiple "
                       "of 1 GiB) --bind-ip IP\n"
                       "--allocations 1..64 (default 1) "
                       "--allocation-size BYTES (default pool/allocations; "
                       "at least 6 MiB, multiple of 2 MiB)\n"
                       "--timeout-sec N (default 180) --hbm-only "
                       "(currently the only mode)\n";
                return 0;
            }
            if (arg == "--hbm-only") continue;
            if (i + 1 >= argc) throw std::runtime_error("missing option value");
            const std::string value = argv[++i];
            if (arg == "--protocol")
                o.protocol = value;
            else if (arg == "--device0")
                o.devices[0] = std::stoi(value);
            else if (arg == "--device1")
                o.devices[1] = std::stoi(value);
            else if (arg == "--pool-size")
                o.pool = std::stoull(value);
            else if (arg == "--bind-ip")
                o.bind_ip = value;
            else if (arg == "--allocations")
                o.allocations = std::stoull(value);
            else if (arg == "--allocation-size")
                o.allocation_size = std::stoull(value);
            else if (arg == "--timeout-sec")
                o.timeout = std::stoul(value);
            else
                throw std::runtime_error("unknown option " + arg);
        }
        if ((o.protocol != "device_sdma" && o.protocol != "device_rdma" &&
             o.protocol != "device_urma") ||
            !o.timeout || o.allocations < 1 || o.allocations > 64 ||
            o.pool < (1ULL << 30) || o.pool % (1ULL << 30) ||
            o.devices[0] < 0 || o.devices[1] < 0 ||
            o.devices[0] == o.devices[1])
            throw std::runtime_error(
                "invalid protocol, timeout, pool (must be 1 GiB aligned) or "
                "devices");
        if (o.allocation_size && (o.allocation_size < (6ULL << 20) ||
                                  o.allocation_size % (2ULL << 20) ||
                                  o.allocation_size > o.pool / o.allocations))
            throw std::runtime_error(
                "allocation size must be at least 6 MiB, 2 MiB aligned, and "
                "fit its share of the pool");
        int sockets[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets))
            throw std::runtime_error("socketpair failed");
        pid_t children[2]{};
        for (int rank = 0; rank < 2; ++rank) {
            children[rank] = fork();
            if (children[rank] < 0) {
                if (children[0] > 0) kill(children[0], SIGKILL);
                throw std::runtime_error("fork failed");
            }
            if (children[rank] == 0) {
                close(sockets[1 - rank]);
                child(o, rank, sockets[rank]);
            }
        }
        close(sockets[0]);
        close(sockets[1]);
        bool failed = false;
        size_t remaining = 2;
        const auto deadline =
            Clock::now() + std::chrono::seconds(o.timeout + 5);
        while (remaining && Clock::now() < deadline && !failed) {
            for (auto& pid : children) {
                if (pid <= 0) continue;
                int status;
                if (waitpid(pid, &status, WNOHANG) == pid) {
                    pid = 0;
                    --remaining;
                    failed |= !WIFEXITED(status) || WEXITSTATUS(status) != 0;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        for (auto pid : children)
            if (pid > 0) {
                kill(pid, SIGKILL);
                waitpid(pid, nullptr, 0);
            }
        return failed || remaining ? 1 : 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL " << e.what() << std::endl;
        return 1;
    }
}
