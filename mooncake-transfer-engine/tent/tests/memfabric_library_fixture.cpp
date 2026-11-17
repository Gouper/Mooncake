// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
namespace {
void (*on_close)(void*) = nullptr;
void* close_context = nullptr;
}  // namespace
extern "C" int memfabric_fixture_value() { return 42; }
extern "C" void memfabric_fixture_on_close(void (*callback)(void*),
                                           void* context) {
    on_close = callback;
    close_context = context;
}
__attribute__((destructor)) static void closeFixture() {
    if (on_close) on_close(close_context);
}
