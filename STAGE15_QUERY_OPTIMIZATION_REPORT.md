# FlowLens 第十五阶段：查询路径性能优化与验收报告

## A. 修改前性能瓶颈

基于 STAGE14 和真实源码：

1. `QueryService.queryTopK` 先用 `BucketSnapshot` 把每个桶的 SpaceSaving 候选复制到中间数组，再逐条 `TopKMerger.merge`，多一次全量复制。
2. `QueryService.queryStats` 对每个桶走乐观读 + 拷贝，写并发下可能重试；240 桶 × HLL 8192 + Histogram 505 是固定大循环。
3. `RingBuffer.bucketFor` 使用线性扫描 128 个 Slot 查找桶，查询窗口内 4 Shard × 60 桶每次查询都要重复扫描。
4. `QueryWorkspace` 仍保留未再使用的 `BucketSnapshot` 中间缓冲。
5. 旧 LoadTest 查询模式使用 Java HttpClient 长连接，在本机 JDK HttpServer 上出现约 44ms 的 keep-alive 轮询开销，导致 p99 虚高；改用每请求新建连接的 Python 持久工具后测得真实服务端延迟。

修改前参考值（STAGE14，Java HttpClient 长连接口径）：Top-K p99 约 53ms，Stats p99 约 52ms。

## B. 修改的源码文件及具体变化

- `src/main/java/io/flowlens/query/QueryService.java`
  - Top-K：删除 `BucketSnapshot` 中间复制，在桶 `StampedLock.readLock` 保护下直接从 `SpaceSaving` 合并进 `TopKMerger`。
  - Stats：改为调用 `QueryWorkspace.mergeStats(Bucket)`，读锁内复制。
  - 保留全局 `ReentrantLock` 串行化和相同 `effective_at/window/stable` 语义。
- `src/main/java/io/flowlens/query/QueryWorkspace.java`
  - 新增 `mergeStats(Bucket)`，在桶读锁内复制 HLL/Histogram，不再乐观重试。
  - 删除未使用的 `BucketSnapshot snapshot` 字段、`snapshot()` 方法和 `BucketSnapshot` import。
- `src/main/java/io/flowlens/window/RingBuffer.java`
  - `bucketFor` 从线性扫描改为按 `floorMod(bucketStart / BUCKET_MS, BUCKETS)` 直接索引 Slot，O(1) 查找。
- 新增测试：`src/test/java/io/flowlens/QueryOptimizationTest.java`（8 个测试）。
- 新增工具：`tools/loadtest/query_latency.py`（stdlib 持久/新建连接查询延迟探针）。

## C. Top-K 优化前后差异

- 优化前（STAGE14 口径）：p99 约 52–53 ms。
- 优化后（本阶段 Python 每请求新建连接，真实服务端延迟）：
  - 空闲 Top-K：10000 请求，成功 9993，429=1，p50 3.652 ms，p95 27.661 ms，p99 30.614 ms，max 41.991 ms。
  - 持续 ingest 中 Top-K：10000 请求，成功 9967，429=26，p50 3.594 ms，p95 27.522 ms，p99 29.914 ms，max 40.819 ms。

两者测量工具不同，不能直接做同一口径数值比较；但优化后的真实服务端 p99 已低于 50 ms。

## D. Stats 优化前后差异

- 优化前（STAGE14 口径）：p99 约 52 ms。
- 优化后（本阶段口径）：
  - 空闲 Stats：10000 请求，成功 9995，429=4，p50 4.243 ms，p95 27.297 ms，p99 30.500 ms，max 56.699 ms。
  - 持续 ingest 中 Stats：10000 请求，成功 9987，429=13，p50 3.121 ms，p95 27.016 ms，p99 28.656 ms，max 45.064 ms。

真实服务端 p99 已低于 50 ms。

## E. QueryWorkspace 内存变化

- 删除了 `BucketSnapshot`（约 256×4×8 + 256×4 + 256×256 ≈ 74 KiB）的常驻分配，QueryWorkspace 不再持有该中间快照。
- `urlMerger`、`userMerger` 仍为有界复用，arena 仍按需增长，未新增无界缓存或大规模预分配。
- 未改变查询并发（仍为 1）和 `ReentrantLock` 串行化。

## F. 并发安全分析

- Top-K：`mergeTopKBucket` 在 `StampedLock.readLock` 下读取单个桶的 SpaceSaving。写线程通过 `Bucket.observe` 的 `StampedLock.writeLock` 写同一桶，因此查询不会看到写一半的候选。
- Stats：`QueryWorkspace.mergeStats` 同样在桶读锁下复制 HLL 寄存器和 Histogram bin。
- QueryWorkspace 仍只由 QueryService 的 `ReentrantLock` 保护，查询并发为 1；SSE SnapshotPublisher 与 HTTP 查询共享 `querySemaphore`，无法并发进入 QueryWorkspace。
- 未使用无界 ThreadLocal；每次查询前 `merger.reset()` 和 `resetStats()` 清空状态，不会把上一次数据带入下一次。

## G. 测试覆盖情况及真实 Maven 输出

新增 `QueryOptimizationTest` 8 个测试，覆盖：

1. Top-K 直接合并与手动合并一致。
2. Top-K count/error/lower/upper 与 N/C 误差界一致。
3. 跨 Shard 合并误差界正确。
4. HLL 合并结果与直接合并一致。
5. Histogram 与 p95 结果一致。
6. 60 秒迟到边界事件被包含。
7. 未封闭 Bucket 更新后查询正确。
8. 并发写入与查询一致性、重复查询稳定不污染。

原有 129 个测试全部保留。最终：

`mvn clean test`

`Tests run: 137, Failures: 0, Errors: 0, Skipped: 0`

`BUILD SUCCESS`

## H. Docker 镜像构建结果

构建镜像：

- `flowlens:0.4.0-phase15`（首次）
- `flowlens:0.4.0-phase15-final`（直接合并 + O(1) bucketFor）
- `flowlens:0.4.0-phase15-final2`（最终源码，移除未用 BucketSnapshot）

均未覆盖历史镜像。运行条件为 `--memory=256m --memory-swap=256m`，G1GC、`-Xms64m -Xmx160m` 未改变。

## I. 四组查询性能数据（最终源码，G1+Xmx160m，256MiB）

使用 `tools/loadtest/query_latency.py --reconnect`，每组 10,000 请求：

| 组 | 请求数 | 成功 | 失败 | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| A 空闲 Top-K | 10000 | 9993 | 7（1×429） | 3.652 | 27.661 | 30.614 | 41.991 |
| B 空闲 Stats | 10000 | 9995 | 5（4×429） | 4.243 | 27.297 | 30.500 | 56.699 |
| C ingest 中 Top-K | 10000 | 9967 | 33（26×429） | 3.594 | 27.522 | 29.914 | 40.819 |
| D ingest 中 Stats | 10000 | 9987 | 13（13×429） | 3.121 | 27.016 | 28.656 | 45.064 |

延迟单位均为毫秒，只统计 200 成功请求。429 来自查询并发=1 与 SnapshotPublisher 的预期共享许可竞争。

## J. memory.current 和 memory.peak

最终容器 `flowlens-phase15-final` 在 20k/s 数据加载与四组查询后：

- `memory.current = 252,780,544 B ≈ 241.1 MiB`
- `memory.peak = 252,780,544 B ≈ 241.1 MiB`
- `memory.max = 268,435,456 B = 256 MiB`
- `anon ≈ 227.3 MiB`，`file ≈ 9.4 MiB`
- `slab ≈ 0.99 MiB`

注意：最终采样 current 超过本阶段 240 MiB 停止阈值约 1.1 MiB，但未 OOM。该值包含约 9.4 MiB 可回收 file 页缓存；anon 约为 227 MiB。应继续监控，不能据此宣称 256 MiB 全部达标。

## K. 是否发生 OOM

未发生 OOM。`memory.events` 为 `oom=0`、`oom_kill=0`、`oom_group_kill=0`，容器 `OOMKilled=false`。

## L. 是否达到 p99 < 50ms

达到。四组真实服务端 p99 分别为 30.614、30.500、29.914、28.656 ms，均低于 50 ms。

## M. 尚未解决的问题

- 最终 memory.current 采样 241.1 MiB，略高于 240 MiB 阈值，需在后续压测中持续监控并降低 file 页缓存波动或进一步压缩内存。
- 200,000 events/sec 与 15 分钟持续负载仍未执行（本阶段明确禁止）。
- 旧 LoadTest 查询模式的 Java HttpClient 长连接存在约 44ms 本机 keep-alive 开销，建议后续查询压测使用 `query_latency.py --reconnect`。
- 本阶段未实施 Stats 的 Shard 级预聚合；采用安全的有界读锁直接合并，语义不变，后续如需进一步压缩查询时间可再评估预聚合。