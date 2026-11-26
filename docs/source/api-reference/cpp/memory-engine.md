# MemoryEngine C++ API Reference

MemoryEngine manages ranks and global virtual addresses (GVAs) across processes.
Clients use TENT and MemFabric to access memory shared by other ranks.

## Build and setup

Build with `-DUSE_TENT=ON -DUSE_MEMFABRIC=ON` and the MemFabric HYBM headers.
Link `mooncake_memory_engine` and make `libmf_hybm_core.so` available to the
client at runtime. Include `memory_engine_client.h` for `MemoryEngine` and
`memory_engine_server.h` for `MemoryEngineServer`.

Copy `mooncake-memory-engine/config/memory-engine.json` and edit its top-level
`memory_engine` object. The example uses one rank on the local host with a
1 GiB device GVA window. To use `Start()` and `Create(&engine)`, point
`MC_MEMORY_ENGINE_CONF` to this JSON file in both the server and client
environments. These overloads read the top-level `memory_engine` object.

```bash
export MC_MEMORY_ENGINE_CONF=/path/to/memory-engine.json
```

Set `MF_PROTOCOL` on each client. To supply a custom TENT configuration on
a client, set `MC_TENT_CONF` to a separate TENT JSON file and set
`transports.memfabric.enable` to `true` in that file.

```bash
export MF_PROTOCOL=device_sdma
```

For multiple hosts, choose an `MF_PROTOCOL` supported by the deployment and
use mutually reachable addresses for `server_url` and `client_host` in the
MemoryEngine file and `rpc_server_hostname` in the TENT file. Use the same
`world_size`, `host_mem_max`, and `device_mem_max` on every client.

For Ascend A2 with `MF_PROTOCOL=device_rdma` and `MF_HYBM_RDMA_USE_HCOMM=1`,
install the HYBM AICPU kernel in each client's CANN environment with
`mfcli kernel install`.

## Server and client lifecycle

1. Start the server with `MemoryEngineServer::Instance().Start()`. `Start()`
   loads the file specified by `MC_MEMORY_ENGINE_CONF`. `Start(config)` accepts
   a `Config` directly. Starting again on the same endpoint returns `OK` and keeps
   the current configuration. Starting on a different endpoint while the server
   is running returns `InvalidEntry`.
2. Bind the calling thread to its Ascend device, then call `MemoryEngine::Create`
   followed by `Join()`. Use `Create(config, &engine)` to pass the configuration
   directly.
3. Call `Allocate()` to get a GVA, and pass that address to `Free()` to release
   it. Stop direct access to the region on all participating ranks before
   calling `Free()`.
4. For a planned shutdown, stop direct GVA access and let clients `Leave()`
   before calling `MemoryEngineServer::Instance().Stop()`. `Stop()` closes the
   listener and clears the in-memory cluster state. A subsequent `Start()`
   begins with a fresh state.

## Configuration

| Key under `memory_engine` | Meaning |
| --- | --- |
| `enabled` | Allows clients to call `MemoryEngine::Create`. Defaults to `true`. |
| `server_url` | Server listen and client connect endpoint, in `host:port` form. |
| `world_size` | Number of ranks. Required on clients. |
| `host_mem_max`, `device_mem_max` | Per-rank GVA window sizes in bytes. Use whole GiB and set an unused window to zero. Enable at least one window. |
| `auto_ranking` | Lets the server assign each rank. Defaults to `true`. |
| `local_rank` | Required when `auto_ranking` is `false`. Use a unique rank in `[0, world_size)`. |
| `client_host` | Client callback address. An empty value enables automatic selection. |
| `heartbeat_interval_ms` | Client heartbeat interval in milliseconds. Defaults to `3000`. Must be shorter than `server.rank_lease_ms`. |
| `join_timeout_ms` | Registration and join timeout in milliseconds. Defaults to `120000`. |
| `batch_copy_timeout_ms` | Batch copy deadline in milliseconds. Defaults to `30000`. Set to `0` for no deadline. |
| `server.world_size`, `server.host_mem_max`, `server.device_mem_max` | Expected layout. Each nonzero value must match the client value. |
| `server.gva_base_host`, `server.gva_base_device` | Expected GVA base addresses. For each zero value the server accepts the first confirmed base. |
| `server.rank_lease_ms` | Rank lease duration in milliseconds. Defaults to `20000`. The server starts the lease on confirmation and renews it on each accepted heartbeat. |
| `server.rpc_server_threads` | Server RPC worker count. Defaults to `4`. |

## Batch copy

After `Join()`, call `BatchCopy(entries, &results)` with a source address,
destination address, and byte count for each entry. The client accepts copies
between local user buffers, between a local user buffer and a GVA in either
direction, and between two GVAs owned by the calling rank. Keep both buffers
valid for the call and place dependent copies in separate batches. Source and
destination ranges must be disjoint or identical.

When the call returns `OK`, `results[i]` gives the status of entry `i` as a
`MemoryEngineCopyResult`. A copy between GVAs on different ranks returns
`Unsupported` (`-6`) for that entry. An already submitted synchronous HYBM
transfer can finish after the deadline, and its destination may be updated
even when the result is `TimedOut`.
