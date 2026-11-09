# FlowLens 第十二阶段：内存结构优化与原题兼容修复报告

## 0. 结论摘要

- `mvn clean test`：129 tests，0 failures，0 errors，0 skipped，BUILD SUCCESS。
- 本阶段未修改算法精度参数（SpaceSaving C=256、HLL p=13、Histogram 505、Bloom FP=1e-6、Shard=4、RingBuffer=128、队列容量 2048/Shard）。
- 原题 API 路径已增加兼容别名，原 `/api/v1/*` 路径继续保留。
- G1 + `-Xmx160m` 在 256 MiB cgroup 下完成 5k/10k/20k events/sec 各 30s，无 cgroup OOM、无 Java Heap OOM。
- 生产 Dockerfile 与 run.ps1 本轮未改动，仍为 `-Xmx200m`；实验通过容器 `--entrypoint` 覆盖 JVM 参数完成。

## 1. RingBuffer 最坏内存核算（源码证据）

`RingBuffer.BUCKETS = 128`，每个 Shard 独立持有 128 个 Slot，稳态 128 秒后每个 Shard 存活 128 个 Bucket，4 Shard 共 512 个 Bucket。已封闭且仍在 128 秒保留窗口内的桶继续保留；只有超过 128 秒的最旧桶才会被复用覆盖。

每个 Bucket 含：
- 2 个 `SpaceSaving`（URL 256 字节键、USER 64 字节键，容量均为 256）。
- 1 个 `HyperLogLog`（p=13，8192 字节寄存器）。
- 1 个 `LogHistogram`（505 个 `long`）。

按实际数组计算（不含对象头，数组 payload）：
- 单个 URL SpaceSaving：table int[512] 2048 + hi/lo/count/error long[256] 4×2048=8192 + nameLen byte[256] 256 + arena byte[65536] 65536 + heap int[256] 1024 + heapPos int[256] 1024 + scratch 16 ≈ 78,096 字节。
- 单个 USER SpaceSaving：同上，arena 为 16384 ≈ 28,944 字节。
- HLL：8192 字节；Histogram：4040 字节。
- 单个 Bucket payload ≈ 78,096 + 28,944 + 8192 + 4040 = 119,272 字节 ≈ 116.5 KiB。

512 个 Bucket：119,272 × 512 = 61,067,264 字节 ≈ 58.2 MiB（再计入数组对象头与 Bucket/SpaceSaving 等对象头，按 ~61 MiB 估算）。

结论：必须按 128 桶/Shard 核算，之前按 60 桶的预算低估了 Bucket 内存。这是与 QueryWorkspace 并列的重要常驻内存来源。

## 2. QueryWorkspace 优化前后

修改文件：`src/main/java/io/flowlens/sketch/TopKMerger.java`。

变更：URL/user arena 从构造时一次性分配 `61440 × maxKeyBytes`，改为按需增长，初始 `min(61440, 256) × maxKeyBytes`，每次按 `max(needed, old×2)` 增长，上限仍为 `61440 × maxKeyBytes`。`reset()` 只清表与计数，不回收 arena，避免反复扩容时新旧数组同时驻留。

未改变：候选上限 61440、合并表 131072、C=256、字节碰撞防护、完整候选合并语义。

估算（数组 payload，不含对象头）：
- 旧 QueryWorkspace 两个 merger 的 arena 固定为 15,728,640 + 3,932,160 = 19,660,800 字节，加上 hi/lo/count/error/nameLen/table/快照/HLL/Histogram 约 5.6 MiB，总计约 24–25 MiB。
- 新 QueryWorkspace 两个 merger 的 arena 初始为 65,536 + 16,384 = 81,920 字节，其余固定数组不变，总计约 5.6 MiB，节省约 18.7–19.6 MiB。

新增 `TopKMergerTest` 7 个测试，覆盖空 workspace 小 arena、少量候选小 arena、arena 有界增长、候选上限、哈希碰撞区分、相同 key 合并、reset 保留 arena。

## 3. JVM 参数实验（真实执行）

生产镜像 `flowlens:0.2.0-phase12`（ENTRYPOINT 仍为 `-Xms64m -Xmx200m`）。实验容器均以 `--memory=256m --memory-swap=256m` 运行，用 `--entrypoint java` 覆盖堆参数。

| 配置 | 空载 memory.current | 5k/s×30s 后 | 10k/s×30s 后 | 20k/s×30s 后 | OOM |
|---|---|---|---|---|---|
| G1 + Xmx200m | 236,052,480 B ≈ 225.1 MiB | OOMKilled=true，峰值 268,554,240 B ≈ 256.1 MiB | - | - | cgroup OOM |
| G1 + Xmx168m | 201,179,136 B ≈ 191.9 MiB | 268,304,384 B ≈ 255.9 MiB，memory.events max=216 | 未执行 | 未执行 | 无 oom_kill，但无余量 |
| G1 + Xmx160m | 151,433,216 B ≈ 144.4 MiB | 212,271,104 B ≈ 202.4 MiB | 227,586,048 B ≈ 217.0 MiB | 220,807,168 B ≈ 210.6 MiB，峰值 228,204,544 B ≈ 217.6 MiB | 无 Java Heap OOM、无 cgroup OOM |

SerialGC 本轮未测试，原因：Xmx160 已满足阶段内 20k/s 目标，且阶段允许“必要时才测试”。

推荐：下一阶段把生产 ENTRYPOINT/run.ps1 调整为 `-Xmx160m`（或经更多测试确认的具体值），本轮未改生产 Dockerfile。

## 4. API 兼容测试结果

修改文件：`src/main/java/io/flowlens/http/FlowLensHttpServer.java`、`HealthHandler.java`。

新增路径，原路径保留：

- `POST /v1/ingest`（原 `/api/v1/ingest`）
- `GET /v1/topk`（原 `/api/v1/topk`）
- `GET /v1/stats`（原 `/api/v1/stats`）
- `GET /v1/topk/stream`（原 `/api/v1/stream`）
- `GET /healthz` 增加 `memory_bytes`（Linux 读取 `/proc/self/status` 的 `VmRSS`，非 Linux 返回 -1，不把 JVM heap 冒充 RSS）

新增 `FlowLensHttpServerTest` 4 个测试：originalIngestPathAccepted、originalTopKAndStatsPathsWork、originalStreamPathAcceptsSseConnection、healthzIncludesMemoryBytes。全部通过，并已在真实容器中验证 `/v1/*` 路径与 healthz 字段。

## 5. 新增测试数量与通过情况

- 本阶段新增 11 个测试：`TopKMergerTest` 7 个 + `FlowLensHttpServerTest` 4 个。
- 总测试数：118 → 129。
- `mvn clean test`：Tests run: 129, Failures: 0, Errors: 0, Skipped: 0，BUILD SUCCESS。

## 6. Docker memory.current / memory.peak

以下为 G1 + Xmx160m、`--memory=256m --memory-swap=256m` 容器真实数据：

- 空载 current：151,433,216 B ≈ 144.4 MiB；peak 151,695,360 B ≈ 144.7 MiB。
- 5k/s 后 current：212,271,104 B ≈ 202.4 MiB；peak 212,271,104 B。
- 10k/s 后 current：227,586,048 B ≈ 217.0 MiB；peak 227,586,048 B。
- 20k/s 后 current：220,807,168 B ≈ 210.6 MiB；peak 228,204,544 B ≈ 217.6 MiB。
- 稳态 128 桶 + 全部数据后 current：228,282,368 B ≈ 217.7 MiB；peak 229,826,560 B ≈ 219.2 MiB。
- memory.max：268,435,456 B = 256 MiB；memory.events：oom=0，oom_kill=0。

## 7. 查询 p99

在 G1 + Xmx160m 稳态 128 桶数据上测量：

- Top-K：p50 44 ms，p95 48 ms，p99 51 ms。
- Stats：p50 44 ms，p95 48 ms，p99 49 ms。
- 持续 ingest（5k/s）期间 Top-K：p50 44 ms，p95 50 ms，p99 52 ms。
- 测量期间出现 1 个 429，由 SSE SnapshotPublisher 与 HTTP 查询共享 querySemaphore 导致，符合预期。

原题要求 p99 < 50ms，本轮 Xmx160m 下 Top-K 边缘超过约 1–2ms。需在后续阶段结合生产 Xmx 与 GC 策略继续压测/调优；本轮不通过降低 Xmx 来伪造达标。

## 8. 是否发生 OOM

- G1 + Xmx200m：5k/s 测试中发生 cgroup OOM（容器 `flowlens-phase12-idle` OOMKilled=true，ExitCode 137，现场保留）。
- G1 + Xmx168m：5k/s 后 current 达到 255.9 MiB，memory.events max=216，但未 oom_kill；未继续加压。
- G1 + Xmx160m：5k/10k/20k/s 各 30s 无 Java Heap OOM、无 cgroup OOM、oom=0、oom_kill=0。

## 9. 是否达到 20k/s

是。G1 + Xmx160m 容器 20,000 events/sec × 30s：offered=600000，accepted=600000，accepted_rate=19995.0/s，0 errors，0 overloaded，0 unprocessed。

## 10. 失败现场保护

已保留：`flowlens:0.1.0`、`flowlens:0.1.0-diag`、`flowlens-load`（Exited 137）、`flowlens-phase12-idle`（Exited 137）、`flowlens-phase12-xmx168`、`flowlens-phase12-xmx160`，以及历史结果文件。未删除旧镜像、失败容器或历史压测文件。

## 11. 修改文件清单

- `src/main/java/io/flowlens/sketch/TopKMerger.java`（arena 延迟分配）
- `src/main/java/io/flowlens/http/FlowLensHttpServer.java`（原题路径别名）
- `src/main/java/io/flowlens/http/HealthHandler.java`（memory_bytes）
- `src/test/java/io/flowlens/TopKMergerTest.java`（新增）
- `src/test/java/io/flowlens/http/FlowLensHttpServerTest.java`（新增 4 个兼容测试）
- `tools/loadtest/LoadTest.java`（新增 `--mode topk|stats|sse` 查询压测）
- `tools/loadtest/README.md`（查询模式说明及 `javac -d .` 修正）
- 本报告 `STAGE12_MEMORY_REPORT.md`

未修改：生产 Dockerfile、run.ps1、算法参数、既有测试断言。

## 12. 尚存风险

- 生产 ENTRYPOINT/run.ps1 仍是 `-Xmx200m`，在该配置下 5k/s 已 OOM，必须在下阶段切换到经实验验证的 `-Xmx160m` 或更优配置。
- `-Xmx160m` 的 cgroup current 已到 217–219 MiB，200k/s 或 15 分钟稳态仍需重新验证，Bucket/Bloom 常驻内存没有进一步压缩空间。
- Top-K p99 边缘接近 50ms，需确认生产配置和 GC 选择。
- cgroup `file` 页缓存在不同运行下波动，报告只记录当前实测值，不视为固定预算。`r`n- Docker 停止容器时观察到 exit code 143/137 且未见 shutdown 日志，优雅关闭链路需要在下一阶段单独复查；本阶段未将其纳入内存验收结论。