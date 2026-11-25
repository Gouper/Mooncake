// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "memory_engine_rpc.h"

#include <glog/logging.h>
#include <async_simple/executors/SimpleExecutor.h>

#include <stdexcept>
#include <vector>

#include "transfer_engine_rpc_client_io_context.h"
#include "tent/common/utils/ip.h"
#include "tent/common/utils/random.h"

namespace mooncake {
namespace tent {

using namespace coro_rpc;
using namespace async_simple::coro;

namespace {
thread_local bool tl_inside_rpc_handler = false;

struct RpcHandlerScope {
    explicit RpcHandlerScope(bool& flag) : flag_(flag), prev_(flag) {
        flag_ = true;
    }
    ~RpcHandlerScope() { flag_ = prev_; }

   private:
    bool& flag_;
    bool prev_;
};
}

class MemoryEngineRpcClientPool {
   public:
    struct Lease {
        std::unique_ptr<coro_rpc_client> client;
        // Which generation the client was drawn from. Flushing is keyed on
        // this: see clearIfCurrent().
        uint64_t generation = 0;
    };

    Lease acquire() {
        std::lock_guard<std::mutex> guard(lock_);
        if (!idle_clients_.empty()) {
            auto client = std::move(idle_clients_.back());
            idle_clients_.pop_back();
            return Lease{std::move(client), generation_};
        }
        return Lease{nullptr, generation_};
    }

    // A client is only released after a call succeeded on it, which proves it
    // is alive, so it goes back regardless of which generation it came from.
    void release(std::unique_ptr<coro_rpc_client> client) {
        if (!client) return;
        std::lock_guard<std::mutex> guard(lock_);
        idle_clients_.push_back(std::move(client));
    }

    // Drop every idle client, but only on behalf of a caller that is still
    // looking at the generation it drew from. Several callers notice a peer
    // restart at once; without this, the second one to fail would throw away
    // the connections the first one has already re-established, and the pool
    // would churn for as long as stale clients keep surfacing.
    void clearIfCurrent(uint64_t generation) {
        std::lock_guard<std::mutex> guard(lock_);
        if (generation != generation_) return;
        ++generation_;
        idle_clients_.clear();
    }

    void clear() {
        std::lock_guard<std::mutex> guard(lock_);
        ++generation_;
        idle_clients_.clear();
    }

   private:
    std::mutex lock_;
    uint64_t generation_ = 0;
    std::vector<std::unique_ptr<coro_rpc_client>> idle_clients_;
};

struct ClientLease {
    std::unique_ptr<coro_rpc_client> client;
    std::shared_ptr<MemoryEngineRpcClientPool> pool;
    bool broken = false;

    ~ClientLease() {
        if (client && !broken) {
            pool->release(std::move(client));
        }
    }

    coro_rpc_client* operator->() const { return client.get(); }
};

MemoryEngineRpc::MemoryEngineRpc() = default;

MemoryEngineRpc::~MemoryEngineRpc() { static_cast<void>(Stop()); }

Status MemoryEngineRpc::RegisterFunction(int funcId, const Function& func,
                                      bool offload) {
    std::lock_guard<std::mutex> guard(mHandlerMutex);
    mHandlers[funcId] = Handler{func, offload};
    return Status::OK();
}

Status MemoryEngineRpc::Start(uint16_t& port, bool ipv6, size_t threads) {
    const static uint16_t kStartPort = 15000;
    const static uint16_t kPortRange = 2000;
    const static int kMaxRetry = 10;
    if (mRunning) {
        return Status::InvalidArgument("RPC server already started" LOC_MARK);
    }
    easylog::set_min_severity(easylog::Severity::FATAL);
    if (threads > 1) {
        LOG(INFO) << "MemoryEngineRpc: RPC server threads set to " << threads;
    }
    for (int retry = 0; retry < kMaxRetry; ++retry) {
        if (port == 0) {
            port = kStartPort + SimpleRandom::Get().next(kPortRange);
        }
        if (StartListener(ipv6 ? "::" : "0.0.0.0", port, threads).ok()) {
            return Status::OK();
        }
        port = 0;
    }
    return Status::RpcServiceError("Failed to bind any RPC port" LOC_MARK);
}

Status MemoryEngineRpc::StartAt(const std::string &host, uint16_t port, size_t threads)
{
    if (mRunning) {
        return Status::InvalidArgument("RPC server already started" LOC_MARK);
    }
    easylog::set_min_severity(easylog::Severity::FATAL);
    return StartListener(host, port, threads);
}

Status MemoryEngineRpc::StartListener(const std::string &host, uint16_t port, size_t threads)
{
    try {
        auto server = std::make_unique<coro_rpc::coro_rpc_server>(threads, port, host);
        server->register_handler<&MemoryEngineRpc::Process>(this);
        server->async_start();
        const auto error = server->get_errc();
        if (error) {
            LOG(WARNING) << "Failed to start RPC server on port " << port << ", error:" << error.message() << ".";
            return Status::RpcServiceError("Failed to bind RPC listener");
        }
        mServer = server.release();
        mRunning = true;
        return Status::OK();
    } catch (const std::exception &) {
        LOG(WARNING) << "Failed to start RPC server on port " << port << ".";
        return Status::RpcServiceError("Failed to start RPC listener");
    }
}

Status MemoryEngineRpc::Stop() {
    if (mServer) {
        mServer->stop();
        delete mServer;
        mServer = nullptr;
    }

    std::lock_guard<std::mutex> lock(mPoolsMutex);
    for (auto& entry : mPools) {
        entry.second->clear();
    }
    mPools.clear();
    return Status::OK();
}

Lazy<void> MemoryEngineRpc::Process(int funcId) {
    auto* ctx = co_await coro_rpc::get_context_in_coro();
    // RegisterFunction() may run while the server is serving, and a rehash
    // under a bare find() is a data race. The handler is copied out and the
    // lock released before the coroutine can suspend below.
    Handler handler;
    {
        std::lock_guard<std::mutex> guard(mHandlerMutex);
        auto it = mHandlers.find(funcId);
        if (it == mHandlers.end()) {
            throw std::runtime_error("Unknown MemoryEngine RPC function: " +
                                     std::to_string(funcId));
        }
        handler = it->second;
    }

    auto request = ctx->get_request_attachment();
    std::string response;
    if (handler.offload) {
        // Suspends this coroutine, freeing the io_context thread. The request
        // buffer belongs to the context_info this coroutine keeps alive, so
        // the view survives the hop. tl_inside_rpc_handler is deliberately
        // not set: it guards against deadlocking the RPC thread, which an
        // offloaded handler no longer occupies.
        //
        // post() reports a throwing handler through the Try instead of
        // unwinding, so rethrow here: the router turns that into the same
        // rpc_throw_exception an inline handler would produce. Dropping it
        // would answer a malformed request with an empty success.
        auto result =
            co_await coro_io::post([&] { handler.func(request, response); });
        result.value();
    } else {
        RpcHandlerScope handler_scope(tl_inside_rpc_handler);
        handler.func(request, response);
    }
    ctx->set_response_attachment(std::move(response));
}

std::shared_ptr<MemoryEngineRpcClientPool> MemoryEngineRpc::GetOrCreatePool(
    const std::string& serverAddr) {
    std::lock_guard<std::mutex> lock(mPoolsMutex);
    auto it = mPools.find(serverAddr);
    if (it == mPools.end()) {
        it = mPools.emplace(serverAddr, std::make_shared<MemoryEngineRpcClientPool>()).first;
    }
    return it->second;
}

namespace {

// True when the failure came back from the peer as a reply, which means the
// connection carried a full round trip and is still usable. Every other
// failure leaves the connection in an unknown state.
bool peerAnswered(coro_rpc::errc ec) {
    switch (ec) {
        case coro_rpc::errc::rpc_throw_exception:
        case coro_rpc::errc::function_not_registered:
        case coro_rpc::errc::invalid_rpc_arguments:
        case coro_rpc::errc::invalid_rpc_result:
        case coro_rpc::errc::message_too_large:
            return true;
        default:
            return false;
    }
}

bool rpcFunctionWasNotFound(coro_rpc::errc ec, std::string_view message,
                            int funcId) {
    if (ec == coro_rpc::errc::function_not_registered) return true;
    if (ec != coro_rpc::errc::rpc_throw_exception) return false;
    const std::string expected =
        "Unknown MemoryEngine RPC function: " + std::to_string(funcId);
    return message.find(expected) != std::string_view::npos;
}

}

Lazy<std::pair<Status, std::string>> MemoryEngineRpc::CallBefore(
    std::string serverAddr, int funcId, std::string request,
    std::chrono::steady_clock::time_point deadline)
{
    if (tl_inside_rpc_handler) {
        co_return std::make_pair(
            Status::InvalidArgument(
                "RPC call from RPC handler is forbidden" LOC_MARK),
            "");
    }

    const bool bounded = deadline != std::chrono::steady_clock::time_point::max();
    auto pool = GetOrCreatePool(serverAddr);
    auto acquired = pool->acquire();
    const uint64_t generation = acquired.generation;
    ClientLease lease{std::move(acquired.client), pool, false};
    const bool fromPool = lease.client != nullptr;

    if (!lease.client) {
        lease.client = std::make_unique<coro_rpc_client>(
            GetTransferEngineRpcClientIoContextPool().get_executor());
        const auto budget = deadline - std::chrono::steady_clock::now();
        if (bounded && budget <= std::chrono::steady_clock::duration::zero()) {
            lease.broken = true;
            co_return std::make_pair(Status::TimedOut("RPC connection deadline expired"), "");
        }
        auto connect = bounded ? lease.client->connect(serverAddr, budget) : lease.client->connect(serverAddr);
        auto connectResult = co_await std::move(connect);
        if (connectResult.val() != 0) {
            lease.broken = true;
            auto msg =
                "Failed to connect RPC server. server: " + serverAddr +
                ", funcId: " + std::to_string(funcId) +
                ", message: " + std::string{connectResult.message()};
            co_return std::make_pair(
                Status::RpcConnectionError(msg + LOC_MARK), "");
        }
    }

    lease->set_req_attachment(request);

    const auto budget = deadline - std::chrono::steady_clock::now();
    if (bounded && budget <= std::chrono::steady_clock::duration::zero()) {
        co_return std::make_pair(Status::TimedOut("RPC request deadline expired"), "");
    }
    auto requestCall = bounded
                            ? lease->call_for<&MemoryEngineRpc::Process>(
                                  std::chrono::ceil<std::chrono::milliseconds>(budget), funcId)
                            : lease->call<&MemoryEngineRpc::Process>(funcId);
    auto callResult = co_await std::move(requestCall);

    if (!callResult.has_value()) {
        lease.broken = true;
        // An idle pooled connection only learns that its peer restarted
        // when it is next used. Drop the whole pool so a retry starts
        // from a fresh connection.
        if (fromPool && !peerAnswered(callResult.error().code)) {
            pool->clearIfCurrent(generation);
        }
        const auto& error = callResult.error();
        auto msg = "Failed to call RPC function. server: " + serverAddr +
                   ", funcId: " + std::to_string(funcId) +
                   ", message: " + std::string{error.msg};
        if (rpcFunctionWasNotFound(error.code, error.msg, funcId)) {
            co_return std::make_pair(
                Status::RpcFunctionNotFound(msg + LOC_MARK), "");
        }
        co_return std::make_pair(Status::RpcServiceError(msg + LOC_MARK),
                                 "");
    }

    // The internal buffer is padded for small attachments; trim the moved
    // string to the real attachment length from the view.
    const size_t len = lease->get_resp_attachment().size();
    std::string response = lease->release_resp_attachment();
    response.resize(len);

    co_return std::make_pair(Status::OK(), std::move(response));
}

Status MemoryEngineRpc::CallOwned(const std::string& serverAddr, int funcId,
                               std::string request, std::string& response) {
    auto [status, resp] = async_simple::coro::syncAwait(
        CallBefore(serverAddr, funcId, std::move(request), std::chrono::steady_clock::time_point::max()));

    if (status.ok()) {
        response = std::move(resp);
    }
    return status;
}

}
}
