# FlowLens 第十三阶段：生产 JVM 参数切换与受控性能验收报告

## 1. 结论摘要

- 生产参数已从 `-Xmx200m` 切换为 `-Xmx160m`，并显式加入 `-XX:+UseG1GC`。
- 重要发现：切换前实际 JVM 被自动选择为 `-XX:+UseSerialGC`，并非 G1。本次已修正为显式 G1。
- `mvn clean test`：129 tests，0 failures，0 errors，0 skipped，BUILD SUCCESS。
- G1 + Xmx160m 通过 5k/10k/20k events/sec 各 30 秒，但 20k 后 `memory.current` 达到 244.1 MiB、`memory.peak` 245.2–245.5 MiB，超过本阶段“240 MiB 停止加压”的安全阈值，因此未继续 30k+。
- 查询 p99：Top-K 52 ms，Stats 52 ms，未达到 `<50ms`。
- 优雅关闭：使用 `docker stop --timeout 30` 得到 exit 143 + 完整 shutdown 日志，`drained=true`、`runtimeDrained=true`、`unprocessed=0`。
- 未执行：200k/s 与 15 分钟持续负载测试；未宣称整体达标。

## 2. 生产参数切换

修改文件：

- `Dockerfile`：`-Xmx200m` → `-Xmx160m`，并在 ENTRYPOINT 增加 `-XX:+UseG1GC`。
- `run.ps1`：`-Xmx200m` → `-Xmx160m`，并增加 `-XX:+UseG1GC`。

两者参数保持一致；其余参数不变：

`-Xms64m -Xmx160m -XX:+UseG1GC -XX:MaxMetaspaceSize=64m -XX:ReservedCodeCacheSize=64m -XX:+UseCodeCacheFlushing -XX:MaxDirectMemorySize=16m -XX:NativeMemoryTracking=summary -Xss256k -XX:ActiveProcessorCount=4`

镜像标签：

- 保留旧标签：`flowlens:0.1.0`、`flowlens:0.1.0-diag`、`flowlens:0.2.0-phase12`、`flowlens:0.3.0-phase13`。
- 新构建：`flowlens:0.3.0-phase13-g1`（生产 Dockerfile 当前对应版本）。
- 诊断镜像：`flowlens:0.3.0-phase13-diag`。

## 3. 回归测试（真实执行）

`mvn clean test` 输出摘要：

`Tests run: 129, Failures: 0, Errors: 0, Skipped: 0`，`BUILD SUCCESS`。

未跳过、未删除测试；新增/已有测试全部执行。

## 4. Docker 内存验证（G1 + Xmx160m，`--memory=256m --memory-swap=256m`）

容器：`flowlens-phase13-g1`，镜像 `flowlens:0.3.0-phase13-g1`，端口映射 8086。

| 档位 | offered | accepted | deduped | rejected | overloaded | errors | memory.current 后 | memory.peak 后 |
|---|---|---|---|---|---|---|---|---|
| 空载 | - | - | - | - | - | - | 153,157,632 B ≈ 146.1 MiB | 153,333,760 B ≈ 146.2 MiB |
| 5k/s ×30s | 150000 | 150000 | 0 | 0 | 0 | 0 | 243,372,032 B ≈ 232.1 MiB | 244,244,480 B ≈ 232.9 MiB |
| 10k/s ×30s | 300000 | 300000 | 0 | 0 | 0 | 0 | 249,167,872 B ≈ 237.6 MiB | 249,913,344 B ≈ 238.4 MiB |
| 20k/s ×30s | 600000 | 599500 | 0 | 0 | 0 | 0 | 255,991,808 B ≈ 244.1 MiB | 257,081,344 B ≈ 245.2 MiB |

监控记录最终 `memory.peak = 257,449,984 B ≈ 245.5 MiB`，`memory.max = 268,435,456 B = 256 MiB`。

`memory.events`：`oom=0`，`oom_kill=0`，`oom_group_kill=0`。容器未 OOM，`OOMKilled=false`。

结论：20k/s 功能通过，但 G1 内存占用已接近 256 MiB，且越过 240 MiB 安全阈值；按本阶段要求停止后续加压。

## 5. 更高吞吐量测试

未执行 30k/50k/100k/150k/200k 和 15 分钟持续负载。

停止原因：20k/s 后 G1 `memory.current=255,991,808 B ≈ 244.1 MiB`、`memory.peak≈245.2–245.5 MiB`，超过 240 MiB 阈值，继续加压存在 OOM 风险。

## 6. 查询 p99 专项（G1 + Xmx160m）

在 5k/10k/20k 数据（约 1.05M 事件）下实测：

- Top-K：p50 47 ms，p95 50 ms，p99 52 ms。
- Stats：p50 44 ms，p95 48 ms，p99 52 ms。
- 持续 ingest 期间未单独重测，因为 20k 后已停止加压；空闲/满数据下 p99 已超过 50ms。

耗时来源定位（基于源码，非 profiler 实测）：Top-K 查询需遍历 4 Shard × 60 桶，每桶经 `BucketSnapshot.read` 拷贝最多 256 个候选，再逐候选 `TopKMerger.merge`（最多 61,440 次）；Stats 查询需遍历 240 个桶复制 HLL 8192 字节寄存器与 505 个直方图 bin。两者共同成本是 240 桶的 `StampedLock` 快照与数组复制/合并，且 QueryService 用单锁串行化查询，SSE 发布器还会竞争同一 `querySemaphore`。

最小化优化建议（未实施，待批准）：

- Top-K：在桶读锁内直接从 SpaceSaving 合并到 TopKMerger，去掉 `BucketSnapshot` 的中间拷贝。
- Stats：为每个 Shard 维护 60 秒窗口的增量 HLL/Histogram 预聚合，查询时合并 4 个 Shard 而非 240 个桶。
- 不改 SpaceSaving C、HLL p、Histogram bins、Bloom FP 等算法精度参数。

## 7. 优雅关闭专项

- 使用 `docker stop --timeout 30 flowlens-phase13-g1`。
- 容器退出码 143（SIGTERM），`OOMKilled=false`。
- 日志完整打印：

`FlowLens shutdown: drained=true httpIncomplete=false handlersNotExited=false runtimeDrained=true unprocessed=0`

结论：在 30 秒 grace 窗口内，HTTP 停止、SSE 关闭、队列 drain、Shard 写线程退出均正常完成；143 表示 SIGTERM 触发的受控退出，不等同于 137 SIGKILL。之前默认 stop 超时下观察到的 137 是 stop grace 不足，需在部署说明中建议使用 `--timeout 30`。

## 8. 失败现场与历史记录保护

保留：`flowlens-load`（Exited 137 OOM）、`flowlens-phase12-idle`、`flowlens-phase12-xmx168`、`flowlens-phase12-xmx160`、`flowlens-phase13-load`（SerialGC 自动选择时的验证容器）、`flowlens-phase13-g1`。

保留旧镜像：`flowlens:0.1.0`、`flowlens:0.1.0-diag`、`flowlens:0.2.0-phase12`、`flowlens:0.3.0-phase13`。

## 9. 真实执行结果 vs 设计估算 vs 未执行

- 真实执行：Xmx160 切换、G1 显式启用、129 测试、5k/10k/20k、查询 p99、优雅关闭、内存采样。
- 设计估算：未新增；内存数据均为 cgroup 实测。
- 未执行：30k–200k 分级吞吐、15 分钟持续负载、查询优化实施。
- 未达到：p99 < 50ms；200,000 events/sec；256 MiB 下稳态高吞吐。

## 10. 下一阶段建议

1. 先决策 G1 下内存预算：`Xmx160m` 在 20k 已到 244–245 MiB，需评估 `Xmx144m` 或 `Xmx128m`，或改为显式 SerialGC（此前 SerialGC 20k peak 约 233 MiB，余量更大），但必须与“保持 G1 GC”要求协调。
2. 实施查询路径的最小化优化，先把 p99 压到 <50ms。
3. 通过后才重测 30k → 200k，并以 15 分钟稳态作为最终吞吐验收。
4. 部署脚本中明确建议 `docker stop --timeout 30`，并保留 shutdown 日志校验。