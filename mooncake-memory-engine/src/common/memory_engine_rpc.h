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

#ifndef MOONCAKE_MEMORY_ENGINE_RPC_H
#define MOONCAKE_MEMORY_ENGINE_RPC_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <unordered_map>
#include <mutex>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <async_simple/coro/Lazy.h>

#include "tent/common/status.h"
#include "ylt/coro_io/coro_io.hpp"

namespace mooncake {
namespace tent {

class MemoryEngineRpcClientPool;

class MemoryEngineRpc {
   public:
    MemoryEngineRpc();

    ~MemoryEngineRpc();

    MemoryEngineRpc(const MemoryEngineRpc &) = delete;
    MemoryEngineRpc &operator=(const MemoryEngineRpc &) = delete;

   public:
    using Function = std::function<void(const std::string_view & /* request */,
                                        std::string & /* response */)>;

    // Handlers run on the server io_context thread pool (the threads
    // argument of start(), 1 by default), so a blocking one stalls every
    // connection sharing its thread, not just its own.
    //
    // offload=false (default): inline, no thread hop. Right for anything that
    // returns promptly.
    // offload=true: runs on ylt's blocking executor while the connection
    // coroutine suspends, keeping the io_context free. Callers see the same
    // request/response, but ordering within a connection is no longer
    // guaranteed.
    Status RegisterFunction(int funcId, const Function &func,
                            bool offload = false);

    Status Start(uint16_t &port, bool ipv6 = false, size_t threads = 1);

    Status StartAt(const std::string &host, uint16_t port, size_t threads);

    Status Stop();

    Status CallOwned(const std::string &serverAddr, int funcId, std::string request, std::string &response);

    async_simple::coro::Lazy<std::pair<Status, std::string>> CallBefore(
        std::string serverAddr, int funcId, std::string request,
        std::chrono::steady_clock::time_point deadline);

   private:
    Status StartListener(const std::string &host, uint16_t port, size_t threads);

    async_simple::coro::Lazy<void> Process(int funcId);

    std::shared_ptr<MemoryEngineRpcClientPool> GetOrCreatePool(const std::string &serverAddr);

   private:
    struct Handler {
        Function func;
        bool offload = false;
    };

    coro_rpc::coro_rpc_server *mServer = nullptr;

    std::mutex mPoolsMutex;
    std::unordered_map<std::string, std::shared_ptr<MemoryEngineRpcClientPool>> mPools;

    std::mutex mHandlerMutex;
    std::unordered_map<int, Handler> mHandlers;

    std::atomic<bool> mRunning{false};
};

}
}
#endif  // MOONCAKE_MEMORY_ENGINE_RPC_H
