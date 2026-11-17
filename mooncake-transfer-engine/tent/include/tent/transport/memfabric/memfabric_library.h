// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#ifndef TENT_MEMFABRIC_LIBRARY_H
#define TENT_MEMFABRIC_LIBRARY_H

#include <dlfcn.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>

namespace mooncake {
namespace tent {

// All MemFabric function tables share this loader. Tables still choose their
// own required symbols: old trans libraries need not expose the HYBM GVA API.
class MemFabricLibrary {
   public:
    static std::shared_ptr<MemFabricLibrary> load(
        const std::string& name, int flags = RTLD_NOW | RTLD_LOCAL,
        std::string* error = nullptr) {
        auto& cache = libraries();
        std::lock_guard<std::mutex> lock(cache.mutex);
        // Visibility is part of the key so a local load cannot accidentally
        // replace the global-symbol contract of the existing trans loader.
        const auto key = std::make_pair(name, flags);
        const auto found = cache.entries.find(key);
        if (found != cache.entries.end()) {
            if (auto library = found->second.lock()) return library;
        }
        auto library = std::shared_ptr<MemFabricLibrary>(new MemFabricLibrary);
        library->handle_ = dlopen(name.c_str(), flags);
        if (!library->handle_) {
            if (error) {
                const char* detail = dlerror();
                *error = detail ? detail : "dlopen failed";
            }
            return nullptr;
        }
        cache.entries[key] = library;
        return library;
    }

    ~MemFabricLibrary() {
        if (handle_) (void)dlclose(handle_);
    }
    MemFabricLibrary(const MemFabricLibrary&) = delete;
    MemFabricLibrary& operator=(const MemFabricLibrary&) = delete;

    // An aliasing shared_ptr can expose this native handle while keeping its
    // loader alive, as required by the existing trans MemoryApi and its mocks.
    void* handle() const { return handle_; }

    template <typename Function>
    Function symbol(const char* name) const {
        return resolve<Function>(handle_, name);
    }

    template <typename Function>
    static Function resolve(void* handle, const char* name) {
        static_assert(
            std::is_pointer<Function>::value &&
                std::is_function<
                    typename std::remove_pointer<Function>::type>::value,
            "MemFabric symbols must use function pointer types");
        return handle ? reinterpret_cast<Function>(dlsym(handle, name))
                      : nullptr;
    }

   private:
    MemFabricLibrary() = default;
    struct Cache {
        std::mutex mutex;
        std::map<std::pair<std::string, int>, std::weak_ptr<MemFabricLibrary>>
            entries;
    };
    static Cache& libraries() {
        static Cache cache;
        return cache;
    }
    void* handle_ = nullptr;
};

}  // namespace tent
}  // namespace mooncake
#endif
