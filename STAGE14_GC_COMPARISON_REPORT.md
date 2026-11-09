# FlowLens 第十四阶段：GC 对照实验与内存预算复核（第一轮只读分析）

本报告为第十四阶段第一轮只读分析结果。未修改生产代码、Dockerfile、run.ps1，未删除任何历史报告、镜像或失败容器，也未执行 GC 对照压测实验。

## 1. 只读审查结论

### 1.1 历史报告的 GC 标签需要修正

- `STAGE12_MEMORY_REPORT.md` 把三组实验都标为“G1 + Xmx…”。这不可信，需要修正口径。
- 证据链：第十二阶段实验通过 `--entrypoint java` 只覆盖了 `-Xms/-Xmx/…`，没有显式 `-XX:+UseG1GC`。
- 第十三阶段对同样缺少显式 GC 参数的进程执行 `jcmd 1 VM.flags`，结果包含 `-XX:+UseSerialGC`。因此第十二阶段的三个“G1”实验实际应为“SerialGC（自动选择）”，属于**推断**，不是 GC 日志直接证明；历史容器日志未保留 GC 启动行，无法直接回看。

### 1.2 当前生产文件

- `Dockerfile`：`-Xmx160m` + `-XX:+UseG1GC`（已显式）。
- `run.ps1`：`-Xmx160m` + `-XX:+UseG1GC`（已显式）。
- 两者其余参数一致。

### 1.3 当前镜像与 GC 对照

| 镜像 | ENTRYPOINT 中的 GC 参数 | 实际 GC |
|---|---|---|
| `flowlens:0.2.0-phase12` | 无显式 GC | SerialGC（推断，自动选择） |
| `flowlens:0.3.0-phase13` | 无显式 GC | SerialGC（推断，自动选择） |
| `flowlens:0.3.0-phase13-g1` | `-XX:+UseG1GC` | G1GC（显式，已由 VM.flags 验证） |
| `flowlens:0.3.0-phase13-diag` | 无显式 GC | SerialGC（推断，自动选择） |

因此第十三阶段“G1 实验数据”确实来自 `flowlens:0.3.0-phase13-g1`；第十二阶段的数据不应与 G1 数据混合比较。

### 1.4 已有实测 vs 推断 vs 估算

- 实测：第十三阶段 `flowlens:0.3.0-phase13-g1` 的 cgroup 内存、查询 p99、优雅关闭日志；第十三阶段 diag 的 `VM.flags`（SerialGC）。
- 推断：第十二阶段三组实验为 SerialGC 自动选择；第十二阶段空载/各档 memory 数据作为 SerialGC 参考值保留。
- 估算：STAGE12 的 RingBuffer/QueryWorkspace 内存核算为源码估算，不是 RSS/NMT 实测。

## 2. 复核后的历史数据归属

### 2.1 SerialGC（自动选择，推断）参考数据

来自 STAGE12 报告，但应改标为 SerialGC：

| 配置 | 空载 current | 5k 后 | 10k 后 | 20k 后 | 结果 |
|---|---|---|---|---|---|
| SerialGC + Xmx200m | 225.1 MiB | OOM（OOMKilled） | - | - | cgroup OOM |
| SerialGC + Xmx168m | 191.9 MiB | 255.9 MiB | 未执行 | 未执行 | 无余量 |
| SerialGC + Xmx160m | 144.4 MiB | 202.4 MiB | 217.0 MiB | 210.6 MiB / peak 217.6 MiB | 20k 通过 |

注意：这些数值的 GC 属性是推断，必须在下轮对照实验中重新实测 SerialGC。

### 2.2 显式 G1 实测数据

来自 STAGE13，标签可信：

| 档位 | current 后 | peak 后 |
|---|---|---|
| 空载 | 146.1 MiB | 146.2 MiB |
| 5k/s | 232.1 MiB | 232.9 MiB |
| 10k/s | 237.6 MiB | 238.4 MiB |
| 20k/s | 244.1 MiB | 245.2–245.5 MiB |

结论：显式 G1 的 cgroup 占用明显高于推断的 SerialGC 参考值，20k 时越过 240 MiB 安全阈值，继续加压不安全。

## 3. 第十四阶段 GC 对照实验方案（待批准，尚未执行）

### 3.1 固定条件

- 相同 Xmx：`-Xmx160m`。
- 相同 Xms：`-Xms64m`。
- 相同容器内存：`--memory=256m --memory-swap=256m`。
- 相同镜像基础代码：使用同一份源码构建的 jar（Serial 与 G1 仅 JVM 参数不同）。
- 相同负载工具与事件字段：`tools/loadtest/LoadTest.java`。
- 相同 batch=500、concurrency=8、request-timeout=10s。
- 相同档位与时长：空载 60s；5k、10k、20k 各 30s；只有安全余量足够才继续 30k+。
- 每档使用唯一 `--id-prefix`，保证 event_id 全局唯一。
- 预热：每档开始前先确认 healthz/readyz 正常，并运行一个短 ingest 预热批次。

### 3.2 两组参数

A. 显式 SerialGC：

`java -Xms64m -Xmx160m -XX:+UseSerialGC -XX:MaxMetaspaceSize=64m -XX:ReservedCodeCacheSize=64m -XX:+UseCodeCacheFlushing -XX:MaxDirectMemorySize=16m -XX:NativeMemoryTracking=summary -Xss256k -XX:ActiveProcessorCount=4 -jar /app/flowlens.jar`

B. 显式 G1GC：

`java -Xms64m -Xmx160m -XX:+UseG1GC -XX:MaxMetaspaceSize=64m -XX:ReservedCodeCacheSize=64m -XX:+UseCodeCacheFlushing -XX:MaxDirectMemorySize=16m -XX:NativeMemoryTracking=summary -Xss256k -XX:ActiveProcessorCount=4 -jar /app/flowlens.jar`

### 3.3 必须采集指标

每档记录：

- `memory.current`、`memory.peak`、`memory.max`。
- `memory.stat` 的 `anon`、`file`、`inactive_file`、`active_file`、`slab`。
- `memory.events` 的 `oom`、`oom_kill`。
- `OOMKilled` 与容器退出码。
- `docker stats --no-stream` 的 CPU 与 MemUsage。
- accepted、deduped、rejected、overloaded、concurrency503、errors、unprocessed。
- offered_rate、accepted_rate、p50/p95/p99。
- GC 次数与停顿：运行容器加 `-Xlog:gc*:file=/app/gc.log::filesize=10m` 或使用 JDK 诊断镜像跑 `jcmd GC.class_histogram`/`GC.heap_info`；生产 JRE 镜像无 jcmd，需用 diag 镜像。
- 健康状态：healthz、readyz、degraded。

### 3.4 停止条件

任一条件触发立即停止：

- `memory.current` ≥ 240 MiB。
- `memory.events.oom > 0` 或 `OOMKilled=true`。
- healthz/readyz 出现 degraded 或非 ready。
- 持续大量 503/overloaded。
- 容器退出。

## 4. 查询 p99 分析（只读，基于源码）

### 4.1 已核实调用链

Top-K：

`QueryService.queryTopK` 加全局 `ReentrantLock` → 对 4 个 Shard × 60 桶循环 → `RingBuffer.bucketFor` → `BucketSnapshot.read(SpaceSaving, StampedLock)` → 拷贝最多 256 个候选到快照数组 → 逐候选 `TopKMerger.merge`（最多 61,440 次）→ `TopKMerger.top` 最小堆取 Top-K。

Stats：

`QueryService.queryStats` 同样加全局锁 → 对 240 桶循环 → `QueryWorkspace.readStats(Bucket)` 在 `StampedLock` 下拷贝 HLL 8192 字节 + Histogram 505 个 long → `distinctUsers/p95Bytes` 计算。

### 4.2 p99 > 50ms 的候选原因（按可能性排序，非 profiler 结论）

1. **240 桶 × 每桶快照/合并是固定大循环**。满数据时无法跳桶，Top-K 最多 61,440 次 merge，Stats 约 240×(8192+505) 次数组读写。这是两个查询都接近 50ms 的共同主因。
2. **StampedLock 乐观读 + 失败重试**。写入线程持续写入时，`BucketSnapshot.read` 或 `readStats` 可能多次重试，放大尾延迟。
3. **QueryService 单锁串行化 + SSE 共享 `querySemaphore`**。HTTP 查询与 SnapshotPublisher 会互相排队/429，增加 p99。
4. **满堆 GC 压力**。G1 在 20k 后堆接近上限，查询产生的 `PriorityQueue`、`int[]`、`ArrayList<TopKEntry>`、`String` 等短生命周期对象会触发 Young/Mixed GC，放大 p99。
5. `TopKMerger.merge` 的开放寻址与字节比较虽单次便宜，但 61,440 次累加后不可忽略。

### 4.3 不降低算法精度的优化方案

- 方案 1（Top-K 去中间拷贝）：在桶读锁内直接把 SpaceSaving 候选合并进 `TopKMerger`，去掉 `BucketSnapshot` 的 256 元素拷贝；保留字节比较与碰撞防护。
- 方案 2（Stats 预聚合）：每个 Shard 维护滑动 60 秒 HLL/Histogram 增量聚合，查询只合并 4 个 Shard 而非 240 桶；不改变 HLL p 和 Histogram bins。
- 方案 3（降低乐观读失败）：对查询路径使用短临界区的 `StampedLock.readLock`，或版本号 + 内存屏障，减少重试；不改变数据一致性语义。
- 方案 4（复用查询对象）：复用 Top-K 输出数组/PriorityQueue，避免每次查询分配；不降低精度。
- 方案 5（GC 对照）：确认 SerialGC 与 G1 对查询 p99 的差异，选择更适合 256 MiB 约束的 GC。

以上均为实现优化，不动算法精度参数。

## 5. 后续分级压测计划（待批准）

在完成 SerialGC vs G1 对照并选定内存余量足够的配置后，才按顺序执行：

30k → 50k → 100k → 150k → 200k，每档 30s；200k 通过后再做 15 分钟稳态。

每档确认：

- 无 OOM、OOMKilled=false。
- memory.peak < 240 MiB（保留安全余量）。
- accepted_rate 接近 offered_rate，overloaded/errors 不持续增长。
- query p99 稳定并尽量 < 50ms。

## 6. 风险与下一步

- 现有证据不足以把第十二阶段数据当作 G1；必须重跑对照实验。
- 显式 G1 + Xmx160m 的 20k 内存余量不足，不能继续 200k 压测。
- 查询 p99 尚未达标，需要 profiler 实测确认 4.2 的候选原因后实施优化。
- 在获得批准前，不修改 Dockerfile/run.ps1、不构建新镜像、不运行对照实验。

下一步建议：批准后按第 3 节方法先跑 SerialGC vs G1 对照，采集 GC 日志和查询 profiler 数据；根据结果决定 GC 选择和查询优化实施顺序。

## 7. 已批准后的对照实验结果（真实执行）

实验时间：2026-11-09。容器内存限制均为 `--memory=256m --memory-swap=256m`，Xmx 均为 160m，batch=500，concurrency=8，每档 30 秒，event_id 使用唯一前缀。

### 7.1 显式 SerialGC 容器 `flowlens-phase14-serial2`

镜像 `flowlens:0.3.0-phase13`，入口覆盖 `-XX:+UseSerialGC`，GC 日志确认 `Using Serial`。

| 档位 | offered | accepted | errors | p50 | p99 | memory.current 后 | memory.peak 后 |
|---|---|---|---|---|---|---|---|
| 空载 | - | - | - | - | - | 171,749,376 B ≈ 163.8 MiB | 171,896,832 B ≈ 163.9 MiB |
| 5k/s | 150000 | 150000 | 0 | 10 ms | 57 ms | 219,783,168 B ≈ 209.6 MiB | 226,713,600 B ≈ 216.2 MiB |
| 10k/s | 300000 | 300000 | 0 | 9 ms | 53 ms | 215,212,032 B ≈ 205.2 MiB | 226,713,600 B ≈ 216.2 MiB |
| 20k/s | 600000 | 599000 | 0 | 8 ms | 39 ms | 215,781,376 B ≈ 205.8 MiB | 226,713,600 B ≈ 216.2 MiB |

GC 日志：本组共记录约 53 次可解析 pause，合计约 964 ms，最大单次 Full GC 43.0 ms。

### 7.2 显式 G1GC 容器 `flowlens-phase14-g1`

镜像 `flowlens:0.3.0-phase13`，入口覆盖 `-XX:+UseG1GC`，GC 日志确认 `Using G1`。

| 档位 | offered | accepted | errors | p50 | p99 | memory.current 后 | memory.peak 后 |
|---|---|---|---|---|---|---|---|
| 空载 | - | - | - | - | - | 155,521,024 B ≈ 148.3 MiB | 155,525,120 B ≈ 148.3 MiB |
| 5k/s | 150000 | 150000 | 0 | 11 ms | 100 ms | 221,708,288 B ≈ 211.4 MiB | 242,397,184 B ≈ 231.2 MiB |
| 10k/s | 300000 | 300000 | 0 | 9 ms | 55 ms | 228,073,472 B ≈ 217.5 MiB | 242,397,184 B ≈ 231.2 MiB |
| 20k/s | 600000 | 598500 | 0 | 8 ms | 29 ms | 237,019,136 B ≈ 226.0 MiB | 242,397,184 B ≈ 231.2 MiB |

GC 日志：本组共记录约 79 次可解析 pause，合计约 216 ms；主要 Young 停顿约 1–2 ms，但堆接近满时也出现 Full G1 Compaction Pause，最大 41.6 ms。

### 7.3 查询 p99（20k 数据后）

- SerialGC：Top-K p50 48 ms / p99 55 ms；Stats p50 47 ms / p99 59 ms。
- G1GC：Top-K p50 47 ms / p99 53 ms；Stats p50 47 ms / p99 52 ms。

两种 GC 的查询 p99 都未达到 `<50ms`，且相差不大；因此 p99 超标主因更可能是查询路径本身的 240 桶快照/合并成本，而不是 GC 选择。

### 7.4 对照结论

- 在本次受控实验中，SerialGC 的 memory.peak 明显低于 G1（216.2 MiB vs 231.2 MiB），余量更大。
- G1 的总 GC 停顿小于 Serial（约 216 ms vs 964 ms），但两类都在堆接近满时出现 Full GC。
- 查询 p99 两种 GC 均超过 50ms，GC 不是唯一瓶颈。
- 没有 OOM：两者 `oom=0`、`oom_kill=0`、`OOMKilled=false`，20k 后均健康。
- 注意：cgroup `file` 页缓存在不同启动间波动较大，G1 的 231.2 MiB peak 是在本次较低文件缓存下的值；第十三阶段 G1 曾因较高 file 缓存达到 245.5 MiB。因此仍不能认为 G1 有足够余量跑到 200k/s。

## 8. 查询 p99 超标原因（结合本实验）

GC 对照说明：查询 p99 对 GC 选择不敏感，主要成本在查询算法路径。

应优先实施的优化：

1. Top-K 去掉 `BucketSnapshot` 中间拷贝，在桶读锁内直接合并。
2. Stats 使用 Shard 级滑动 HLL/Histogram 预聚合，查询只合并 4 Shard。
3. 查询输出对象复用，减少 `PriorityQueue`、`ArrayList<TopKEntry>`、`String` 的每次分配。
4. 保持 C=256、HLL p=13、Histogram 505、Bloom FP=1e-6 等算法参数不变。

## 9. 后续压测计划（待查询优化与 GC 决策后执行）

若选择 SerialGC：20k peak 216.2 MiB，余量约 39.8 MiB，可谨慎进入 30k → 50k 分级；若选择 G1GC：20k peak 231.2 MiB，余量仅约 24.8 MiB，需先降低 file 页缓存波动或压低 Xmx 再继续。

每档仍必须检查 OOM、memory.peak、accepted_rate、overloaded、query p99；达到 240 MiB 安全阈值立即停止。