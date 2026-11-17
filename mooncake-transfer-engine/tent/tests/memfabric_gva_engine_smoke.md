# Public TransferEngine MemFabric GVA hardware smoke

This opt-in Linux executable forks two independent processes, selects one NPU
per child, and uses real TransferEngine APIs and real ACL. It registers only
MemFabric. No mocked transport, private backend resolve, or direct HYBM calls
are used. A socketpair carries application handshake/barriers only.

Configure the normal production build with `USE_TENT=ON`, `USE_MEMFABRIC=ON`,
`BUILD_UNIT_TESTS=OFF`, and `BUILD_MEMFABRIC_GVA_ENGINE_SMOKE=ON`, pointing
`MEMFABRIC_HYBM_INCLUDE_DIR` to the compatible HYBM headers and setting
`ASCEND_HOME_PATH` for ACL discovery. Preserve the other options used by your
working production build. Build with:

```sh
cmake --build build --target tent_memfabric_gva_engine_smoke -j4
```

The executable is under `build/mooncake-transfer-engine/tent/` for a top-level
Mooncake build. It is never added to CTest. With an existing shared TENT build,
you can compile manually (substitute actual paths):

```sh
c++ -std=c++17 -pthread tests/memfabric_gva_engine_smoke.cpp \
  -Iinclude -I"$ASCEND_HOME_PATH/include" \
  -L/path/to/build/mooncake-transfer-engine/tent/src -ltent_shared \
  -L"$ASCEND_HOME_PATH/lib64" -lascendcl -lglog -o /tmp/mf-engine-smoke
```

Run from the `mooncake-transfer-engine/tent` directory for that manual command.
When only static archives are available, use the CMake target: `tent_link_group`
provides the archive rescan group and all transport/runtime dependencies.
Set `LD_LIBRARY_PATH` to the production TENT, compatible HYBM, CANN and driver
library directories before running.

```sh
./tent_memfabric_gva_engine_smoke --protocol device_sdma \
  --device0 0 --device1 1 --pool-size 1073741824 --hbm-only

./tent_memfabric_gva_engine_smoke --protocol device_rdma \
  --device0 0 --device1 1 --pool-size 1073741824 --bind-ip 192.0.2.10 \
  --url0 tcp://192.0.2.10:19001 --url1 tcp://192.0.2.10:19002
```

Use real reachable local IPs, unused transport ports and idle devices. This is a
single-host/two-process test (socketpair), even when it selects RDMA. It does not
claim multi-host coverage. URMA is selectable with `--protocol device_urma` when
the deployment supports it. The HYBM VMM window requires 1 GiB alignment. This harness sets the window
equal to the pool size, so `--pool-size` must be a positive multiple of 1 GiB.
The default is 1 GiB per NPU (2 GiB total), with fixed 2 MiB payloads at
nonzero offsets. Smaller pools are rejected before starting child processes.

Each rank writes a distinct pattern into its local source, publishes its pool,
opens the remote segment, and concurrently calls public map until ready. It then
submits WRITE to the peer's nonzero destination offset and polls completion;
the peer reads its owner allocation with ACL and checks every byte. A READ back
into a second local offset is also polled and checked. Both directions run in
both processes. Logs include device, segment, GVA, task status, bytes and hashes.

Cleanup waits for both processes to finish all access, unmaps both peers, waits
again, unregisters/frees local pools and destroys engines. All blocking activity
has a process-wide deadline (`--timeout-sec`, default 180). Failures terminate
the children rather than trying to free memory whose device accesses may still
be in flight. A successful network map means copy-ready, not kernel dereference
of remote P. Kernel direct-access verification remains a separate SDMA test.
