// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_MEMORY_ENGINE_SERVER_H
#define TENT_MEMORY_ENGINE_SERVER_H

#include <memory>
#include <mutex>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

class Config;

class MemoryEngineServer {
   public:
    static MemoryEngineServer &Instance();

    // Starting again on the same endpoint keeps the current configuration.
    Status Start(const Config &config);
    Status Start();
    // Stop listening and discard the in-memory cluster state.
    void Stop();

   private:
    struct Impl;
    MemoryEngineServer();
    ~MemoryEngineServer();

    std::mutex mLifecycleMutex;
    std::unique_ptr<Impl> mImpl;
};

}
}

#endif
