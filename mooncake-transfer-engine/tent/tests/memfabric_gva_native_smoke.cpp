// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
// Opt-in hardware smoke test. This checks native mappings and optional ACL
// copies. MF_GVA_SMOKE_KERNEL additionally enables actual AI core reads/writes.
#include <acl/acl.h>
#include <arpa/inet.h>
#include <dlfcn.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

#include "tent/transport/memfabric/memfabric_shared_memory.h"
#include "tent/common/concurrent/thread_pool.h"

using namespace mooncake::tent;
namespace {
using Json = nlohmann::json;
constexpr size_t kSize = 2ULL << 20;
// The tested 910B3 runtime requires GiB-sized remote VA reservations in
// prior 56-bit tests. Keep GiB pools while verifying only a 2 MiB payload.
constexpr size_t kWindowSize = 1ULL << 30;
constexpr size_t kDramSize = 1ULL << 30;
constexpr size_t kHbmSize = 1ULL << 30;
constexpr size_t kMaxFrame = 1ULL << 20;

bool bytes(int fd, void* data, size_t size, bool write) {
    auto* cursor = static_cast<char*>(data);
    while (size) {
        const auto result = write ? ::send(fd, cursor, size, MSG_NOSIGNAL)
                                  : ::recv(fd, cursor, size, 0);
        if (result <= 0) return false;
        cursor += result;
        size -= result;
    }
    return true;
}
bool sendFrame(int fd, const std::string& frame) {
    if (frame.size() > kMaxFrame) return false;
    auto size = htonl(static_cast<uint32_t>(frame.size()));
    return bytes(fd, &size, sizeof(size), true) &&
           bytes(fd, const_cast<char*>(frame.data()), frame.size(), true);
}
bool receiveFrame(int fd, std::string& frame) {
    uint32_t size = 0;
    if (!bytes(fd, &size, sizeof(size), false)) return false;
    size = ntohl(size);
    if (size > kMaxFrame) return false;
    frame.resize(size);
    return bytes(fd, frame.data(), size, false);
}
void require(const Status& status, const char* operation) {
    if (!status.ok())
        throw std::runtime_error(std::string(operation) + ": " +
                                 std::string(status.message()));
}
void aclCheck(aclError status, const char* operation) {
    if (status != ACL_SUCCESS)
        throw std::runtime_error(std::string(operation) + ": ACL error " +
                                 std::to_string(status));
}
void barrier(int fd, const char* phase) {
    std::string peer;
    if (!sendFrame(fd, phase) || !receiveFrame(fd, peer) || peer != phase)
        throw std::runtime_error(std::string("Peer barrier failed: ") + phase);
}
int runRank(unsigned rank, unsigned device, int sync, bool copy, bool mismatch,
            bool hbm_only) {
    const char* requested_protocol = std::getenv("MF_GVA_SMOKE_PROTOCOL");
    const std::string protocol =
        requested_protocol ? requested_protocol : "device_sdma";
    const bool network = protocol != "device_sdma";
    try {
        aclCheck(aclInit(nullptr), "aclInit");
        aclCheck(aclrtSetDevice(device), "aclrtSetDevice");
        auto api = MemFabricGvaApi::load();
        if (!api)
            throw std::runtime_error("Compatible HYBM symbols unavailable");
        MemFabricGvaConfig config;
        config.enabled = true;
        config.protocol = protocol;
        if (const char* url = std::getenv("MF_GVA_SMOKE_TRANS_URL"))
            config.trans_url = url;
        if (const char* url = std::getenv(rank ? "MF_GVA_SMOKE_TRANS_URL_1"
                                               : "MF_GVA_SMOKE_TRANS_URL_0"))
            config.trans_url = url;
        config.group = "tent-native-smoke-" + std::to_string(getppid());
        config.rank_count = 2;
        config.rank = rank;
        config.device = device;
        config.entity = 64;
        config.hbm_size = kHbmSize;
        config.dram_size = kDramSize;
        config.hbm_window = config.dram_window = kWindowSize;
        if (hbm_only) config.dram_size = config.dram_window = 0;
        const char* kernel_path =
            network ? nullptr : std::getenv("MF_GVA_SMOKE_KERNEL");
        {
            const auto owner = "rank-" + std::to_string(rank);
            const auto peer = "rank-" + std::to_string(1 - rank);
            MemFabricSharedMemory memory(config, owner, api);
            require(memory.initialize(), "initialize GVA context");
            auto deviceAddress = [&](uint64_t gva, size_t size) {
                void* dva = nullptr;
                require(memory.resolve(gva, size, &dva), "resolve GVA");
                if (!dva || reinterpret_cast<uint64_t>(dva) != gva)
                    throw std::runtime_error(
                        "GVA must equal the directly accessible NPU address");
                if (size > 32) {
                    void* offset = nullptr;
                    require(memory.resolve(gva + 32, size - 32, &offset),
                            "resolve GVA offset");
                    if (reinterpret_cast<uint64_t>(offset) !=
                        reinterpret_cast<uint64_t>(dva) + 32)
                        throw std::runtime_error(
                            "GVA offset translation mismatch");
                }
                return dva;
            };
            void *hbm = nullptr, *dram = nullptr;
            require(memory.allocate(&hbm, kHbmSize, 1, 0), "allocate HBM");
            if (!hbm_only)
                require(memory.allocate(&dram, kDramSize, 2, 0),
                        "allocate DRAM");
            std::cerr << "rank=" << rank << " device=" << device
                      << " HBM=" << hbm << " DRAM=" << dram << std::endl;
            void* hbm_dva =
                deviceAddress(reinterpret_cast<uint64_t>(hbm), kSize);
            void* dram_dva =
                hbm_only ? nullptr
                         : deviceAddress(reinterpret_cast<uint64_t>(dram),
                                         kDramSize);
            std::cerr << "rank=" << rank << " local HBM GVA=" << hbm
                      << " DVA=" << hbm_dva << std::endl;
            std::string hbm_desc, dram_desc;
            require(memory.publish(hbm, kHbmSize, hbm_desc), "publish HBM");
            if (!hbm_only)
                require(memory.publish(dram, kDramSize, dram_desc),
                        "publish DRAM");
            std::vector<unsigned char> host(
                kSize, static_cast<unsigned char>(rank + 1));
            if (copy || kernel_path) {
                aclCheck(aclrtMemcpy(hbm_dva, kSize, host.data(), kSize,
                                     ACL_MEMCPY_HOST_TO_DEVICE),
                         "fill HBM DVA");
                if (!hbm_only) {
                    std::fill(host.begin(), host.end(),
                              static_cast<unsigned char>(rank + 1 + 0x10));
                    aclCheck(aclrtMemcpy(dram_dva, kDramSize, host.data(),
                                         kSize, ACL_MEMCPY_HOST_TO_DEVICE),
                             "fill DRAM DVA");
                }
            }
            const auto publication =
                Json{{"hbm", hbm_desc}, {"dram", dram_desc}}.dump();
            std::string remote_raw;
            if (!sendFrame(sync, publication) ||
                !receiveFrame(sync, remote_raw))
                throw std::runtime_error("Pool exchange failed");
            const auto remote = Json::parse(remote_raw);
            const auto remote_hbm = remote.at("hbm").get<std::string>();
            const auto remote_dram = remote.at("dram").get<std::string>();
            const auto hbm_address =
                Json::parse(remote_hbm).at("address").get<uint64_t>();
            const auto dram_address =
                hbm_only
                    ? uint64_t{0}
                    : Json::parse(remote_dram).at("address").get<uint64_t>();
            Json bundle = Json::array({Json::parse(remote_hbm)});
            if (!hbm_only) bundle.push_back(Json::parse(remote_dram));
            auto descriptor = bundle.dump();
            auto mapPool = [&](uint64_t address, size_t size) {
                Status status = Status::TooManyRequests("peer preparing");
                for (int attempt = 0; attempt < 100; ++attempt) {
                    status =
                        network
                            ? memory.prepare(peer, descriptor, address, size)
                            : memory.map(peer, descriptor, address, size);
                    if (status.ok()) return status;
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
                return status;
            };
            if (!hbm_only)
                require(mapPool(dram_address, kDramSize), "map DRAM first");
            require(mapPool(hbm_address, kHbmSize), "map HBM");
            require(mapPool(hbm_address, kHbmSize), "idempotent map");
            if (!network)
                std::cerr << "rank=" << rank << " remote HBM GVA="
                          << reinterpret_cast<void*>(hbm_address)
                          << " DVA=" << deviceAddress(hbm_address, kSize)
                          << std::endl;
            barrier(sync, "mapped");
            std::vector<uint64_t> addresses{hbm_address};
            if (!hbm_only) addresses.push_back(dram_address);
            if (copy && !network) {
                for (auto address : addresses) {
                    std::fill(host.begin(), host.end(), 0);
                    aclCheck(aclrtMemcpy(host.data(), kSize,
                                         deviceAddress(address, kSize), kSize,
                                         ACL_MEMCPY_DEVICE_TO_HOST),
                             "copy from remote DVA");
                    for (auto value : host)
                        if (value !=
                            2 - rank + (address == dram_address ? 0x10 : 0))
                            throw std::runtime_error("Remote data mismatch");
                }
                std::cerr << "rank=" << rank << " PASS ACL remote HBM"
                          << (hbm_only ? "" : " and DRAM")
                          << " copy verification" << std::endl;
            }
            if (kernel_path) {
                void* library = dlopen(kernel_path, RTLD_NOW | RTLD_LOCAL);
                if (!library)
                    throw std::runtime_error(
                        std::string("Kernel plugin load failed: ") + dlerror());
                using Kernel = int (*)(void*, void*, void*);
                auto kernel =
                    reinterpret_cast<Kernel>(dlsym(library, "gva_kernel_copy"));
                if (!kernel)
                    throw std::runtime_error("Missing gva_kernel_copy symbol");
                void* output = nullptr;
                aclrtStream stream = nullptr;
                aclCheck(aclrtMalloc(&output, 32, ACL_MEM_MALLOC_HUGE_FIRST),
                         "allocate kernel output");
                aclCheck(aclrtCreateStream(&stream), "create kernel stream");
                for (auto address : addresses) {
                    std::array<unsigned char, 32> sentinel{};
                    sentinel.fill(0xA5);
                    aclCheck(
                        aclrtMemcpy(output, sentinel.size(), sentinel.data(),
                                    sentinel.size(), ACL_MEMCPY_HOST_TO_DEVICE),
                        "reset kernel read output");
                    (void)deviceAddress(
                        address, 32);  // Assert P is directly accessible.
                    aclCheck(kernel(reinterpret_cast<void*>(address), output,
                                    stream),
                             "AI core remote GVA read");
                    std::array<unsigned char, 32> actual{};
                    aclCheck(
                        aclrtMemcpy(actual.data(), actual.size(), output,
                                    actual.size(), ACL_MEMCPY_DEVICE_TO_HOST),
                        "read kernel output");
                    for (auto value : actual)
                        if (value !=
                            2 - rank + (address == dram_address ? 0x10 : 0))
                            throw std::runtime_error(
                                "AI core remote GVA data mismatch");
                }
                std::cerr << "rank=" << rank << " PASS AscendC remote HBM"
                          << (hbm_only ? "" : " and DRAM")
                          << " GVA read verification" << std::endl;
                barrier(sync, "kernel-readers-stopped");
                std::array<unsigned char, 32> written{};
                for (auto address : addresses) {
                    written.fill(static_cast<unsigned char>(
                        0x40 + rank + (address == dram_address ? 0x10 : 0)));
                    aclCheck(
                        aclrtMemcpy(output, written.size(), written.data(),
                                    written.size(), ACL_MEMCPY_HOST_TO_DEVICE),
                        "fill kernel write source");
                    (void)deviceAddress(
                        address, 32);  // Assert P is directly accessible.
                    aclCheck(kernel(output, reinterpret_cast<void*>(address),
                                    stream),
                             "AI core remote GVA write");
                }
                barrier(sync, "kernel-writes-done");
                std::vector<void*> local_pools{hbm};
                if (!hbm_only) local_pools.push_back(dram);
                for (auto pool : local_pools) {
                    std::array<unsigned char, 32> actual{};
                    aclCheck(aclrtMemcpy(
                                 actual.data(), actual.size(),
                                 deviceAddress(reinterpret_cast<uint64_t>(pool),
                                               actual.size()),
                                 actual.size(), ACL_MEMCPY_DEVICE_TO_HOST),
                             "read peer kernel write result");
                    for (auto value : actual)
                        if (value !=
                            0x40 + (1 - rank) + (pool == dram ? 0x10 : 0))
                            throw std::runtime_error(
                                "AI core remote GVA write data mismatch");
                }
                std::cerr << "rank=" << rank << " PASS AscendC remote HBM"
                          << (hbm_only ? "" : " and DRAM")
                          << " GVA write verification" << std::endl;
                aclCheck(aclrtDestroyStream(stream), "destroy kernel stream");
                aclCheck(aclrtFree(output), "free kernel output");
                dlclose(library);
            }
            if (copy) {
                // Dedicated single-device worker, as used by the GVA copy path.
                // Keep sources/destinations disjoint while both ranks transfer.
                ThreadPool copy_worker(1);
                std::vector<uint64_t> local_addresses{
                    reinterpret_cast<uint64_t>(hbm)};
                if (!hbm_only)
                    local_addresses.push_back(reinterpret_cast<uint64_t>(dram));
                for (size_t i = 0; i < local_addresses.size(); ++i) {
                    std::fill(
                        host.begin(), host.end(),
                        static_cast<unsigned char>(0x60 + rank + i * 0x10));
                    aclCheck(
                        aclrtMemcpy(deviceAddress(local_addresses[i], kSize),
                                    kSize, host.data(), kSize,
                                    ACL_MEMCPY_HOST_TO_DEVICE),
                        "fill HYBM copy source");
                }
                barrier(sync, "hybm-sources-ready");
                auto nativeCopy = [&](uint64_t source, uint64_t destination) {
                    uint32_t direction = 0;
                    require(
                        memory.beginCopy(source, destination, kSize, direction),
                        "pin HYBM copy");
                    auto completion = copy_worker.enqueue(
                        [&, source, destination, direction] {
                            return memory.runCopy(source, destination, kSize,
                                                  direction);
                        });
                    require(completion.get(), "worker HYBM copy");
                };
                for (size_t i = 0; i < addresses.size(); ++i)
                    nativeCopy(local_addresses[i], addresses[i] + kSize);
                barrier(sync, "hybm-writes-done");
                auto checkDestination = [&](size_t i, size_t offset) {
                    std::fill(host.begin(), host.end(), 0);
                    aclCheck(
                        aclrtMemcpy(
                            host.data(), kSize,
                            deviceAddress(local_addresses[i] + offset, kSize),
                            kSize, ACL_MEMCPY_DEVICE_TO_HOST),
                        "verify HYBM copy destination");
                    for (auto value : host)
                        if (value != 0x60 + (1 - rank) + i * 0x10)
                            throw std::runtime_error("HYBM copy data mismatch");
                };
                for (size_t i = 0; i < addresses.size(); ++i)
                    checkDestination(i, kSize);
                for (size_t i = 0; i < addresses.size(); ++i)
                    nativeCopy(addresses[i], local_addresses[i] + 2 * kSize);
                for (size_t i = 0; i < addresses.size(); ++i)
                    checkDestination(i, 2 * kSize);
                barrier(sync, "hybm-reads-done");
                std::cerr
                    << "rank=" << rank
                    << " PASS worker HYBM logical GVA READ/WRITE verification"
                    << std::endl;
            }
            barrier(sync, "readers-stopped");
            if (!hbm_only &&
                !memory.unmap(peer, dram_address).IsTooManyRequests())
                throw std::runtime_error(
                    "First pool unmap must defer rank cleanup");
            Status cleanup = Status::TooManyRequests("pending");
            for (int attempt = 0; attempt < 100 && cleanup.IsTooManyRequests();
                 ++attempt) {
                cleanup = memory.unmap(peer, hbm_address);
                if (cleanup.IsTooManyRequests())
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            require(cleanup, "rank cleanup");
            if (!hbm_only)
                require(memory.unmap(peer, dram_address), "idempotent cleanup");
            void* stale_dva = nullptr;
            if (memory.resolve(hbm_address, kSize, &stale_dva).ok() ||
                stale_dva)
                throw std::runtime_error("Unmapped GVA still resolves");
            barrier(sync, "released");
            require(memory.unpublish(hbm), "unpublish HBM");
            if (!hbm_only) require(memory.unpublish(dram), "unpublish DRAM");
            require(memory.free(hbm, kHbmSize), "free HBM");
            if (!hbm_only) require(memory.free(dram, kDramSize), "free DRAM");
            require(memory.close(), "close entity");
            barrier(sync, "closed");
        }
        if (mismatch) {
            config.entity = 65;
            MemFabricSharedMemory memory(
                config, "mismatch-" + std::to_string(rank), api);
            require(memory.initialize(), "initialize GVA context");
            void* address = nullptr;
            auto status = memory.allocate(&address, kHbmSize, 1, 1);
            if (!status.IsInvalidArgument() || address)
                throw std::runtime_error(
                    "Strict address mismatch was not rejected");
            require(memory.close(), "close mismatch context");
        }
        aclCheck(aclFinalize(), "aclFinalize");
        std::cerr << "rank=" << rank << " protocol=" << protocol
                  << " PASS native mapping" << (copy ? " and ACL copy" : "")
                  << (kernel_path ? "; AI core remote GVA read/write verified"
                                  : "; NPU kernel access not tested")
                  << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "rank=" << rank << " FAIL " << error.what() << std::endl;
        return 1;
    }
}
}  // namespace
int main(int argc, char** argv) {
    unsigned device0 = 0, device1 = 1;
    bool copy = false, mismatch = false, hbm_only = false;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--copy")
                copy = true;
            else if (arg == "--hbm-only")
                hbm_only = true;
            else if (arg == "--mismatch")
                mismatch = true;
            else if (arg == "--device0" && i + 1 < argc)
                device0 = std::stoul(argv[++i]);
            else if (arg == "--device1" && i + 1 < argc)
                device1 = std::stoul(argv[++i]);
            else
                throw std::runtime_error(
                    "Usage: smoke [--device0 0] [--device1 1] [--hbm-only] "
                    "[--copy] [--mismatch]");
        }
        if (device0 == device1 || device0 > UINT16_MAX || device1 > UINT16_MAX)
            throw std::runtime_error("Use two distinct valid NPU device IDs");
        signal(SIGPIPE, SIG_IGN);
        int channels[1][2];
        for (auto& channel : channels) {
            if (socketpair(AF_UNIX, SOCK_STREAM, 0, channel))
                throw std::runtime_error("socketpair failed");
            timeval timeout{30, 0};
            for (int fd : channel) {
                setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                           sizeof(timeout));
                setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                           sizeof(timeout));
            }
        }
        std::array<pid_t, 2> children;
        for (unsigned rank = 0; rank < 2; ++rank) {
            const auto pid = fork();
            if (pid < 0) throw std::runtime_error("fork failed");
            if (pid == 0) {
                const int sync = channels[0][rank];
                close(channels[0][1 - rank]);
                _exit(runRank(rank, rank ? device1 : device0, sync, copy,
                              mismatch, hbm_only));
            }
            children[rank] = pid;
        }
        for (auto& channel : channels)
            for (int fd : channel) close(fd);
        bool passed = true;
        for (auto pid : children) {
            int status = 0;
            if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status) ||
                WEXITSTATUS(status))
                passed = false;
        }
        return passed ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 2;
    }
}
