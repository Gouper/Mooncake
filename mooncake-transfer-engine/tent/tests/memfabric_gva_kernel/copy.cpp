// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "kernel_operator.h"
extern "C" __global__ __aicore__ void gva_copy_32(GM_ADDR source,
                                                  GM_ADDR destination) {
    AscendC::TPipe pipe;
    AscendC::TBuf<AscendC::TPosition::VECCALC> buffer;
    pipe.InitBuffer(buffer, 32);
    AscendC::LocalTensor<uint32_t> local = buffer.Get<uint32_t>();
    AscendC::GlobalTensor<uint32_t> source_tensor;
    AscendC::GlobalTensor<uint32_t> destination_tensor;
    source_tensor.SetGlobalBuffer(reinterpret_cast<__gm__ uint32_t *>(source),
                                  8);
    destination_tensor.SetGlobalBuffer(
        reinterpret_cast<__gm__ uint32_t *>(destination), 8);
    AscendC::DataCopy(local, source_tensor, 8);
    AscendC::PipeBarrier<PIPE_ALL>();
    AscendC::DataCopy(destination_tensor, local, 8);
    AscendC::PipeBarrier<PIPE_ALL>();
}
extern "C" void gva_kernel_launch(void *source, void *destination,
                                  void *stream) {
    gva_copy_32<<<1, nullptr, stream>>>(static_cast<uint8_t *>(source),
                                        static_cast<uint8_t *>(destination));
}
