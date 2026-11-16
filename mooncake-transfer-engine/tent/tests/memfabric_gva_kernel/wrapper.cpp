// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "acl/acl.h"
extern "C" void gva_kernel_launch(void *, void *, void *);
extern "C" int gva_kernel_copy(void *source, void *destination,
                               void *acl_stream) {
    if (source == nullptr || destination == nullptr || acl_stream == nullptr) {
        return ACL_ERROR_INVALID_PARAM;
    }
    gva_kernel_launch(source, destination, acl_stream);
    return aclrtSynchronizeStream(acl_stream);
}
