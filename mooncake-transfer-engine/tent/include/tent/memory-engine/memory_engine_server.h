// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_MEMORY_ENGINE_SERVER_H
#define TENT_MEMORY_ENGINE_SERVER_H

#include <memory>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

class Config;

class MemoryEngineServer {
   public:
    // Keep the returned object alive for as long as the service should listen.
    static Status Start(const Config &config, std::unique_ptr<MemoryEngineServer> *server);
    static Status Start(std::unique_ptr<MemoryEngineServer> *server);
    ~MemoryEngineServer();

   private:
    struct Impl;
    explicit MemoryEngineServer(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> mImpl;
};

}
}

#endif
