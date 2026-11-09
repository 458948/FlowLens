# FlowLens 第五阶段 C：最终专项修复与 Docker 前验收报告

> 标签：**理论计算**、**设计估算**、**本地实测**。Docker 实测尚未执行。

## A. 修改文件清单

- `src/test/java/io/flowlens/SlidingBloomFilterTest.java`：4 个测试改用注入固定时钟。
- `src/test/java/io/flowlens/EventParserTest.java`：新增 4 个流式/并发测试。
- `src/test/java/io/flowlens/http/FlowLensHttpServerTest.java`：新增 1 个压缩体超限测试。
- `run.ps1`（新增）：统一启动脚本，内置 JVM 内存参数与 NMT。
- `README.md`：补充 run.ps1 与 JVM 参数说明。

未修改算法参数、HTTP API、关闭生命周期。

## B. SlidingBloomFilter 时钟修复

`SlidingBloomFilter` 生产默认仍使用 `monotonicEpochMs()`（nanoTime 锚点的单调时钟），满足此前“单调时间源”要求；可注入 `LongSupplier` 的构造器已存在。

测试修复：`saturatesAndDegrades`、`acceptedEventsAreAlwaysMarkedEvenAtCapacity`、`nearCapacityNeverAcceptsWithoutMarking`、`concurrentCapacityOverAcceptIsMeasuredAndMarked` 四个测试改用 `() -> 0L` 固定时钟，消除真实时钟跨 30 秒时间片边界的偶发失败。未放宽断言、未删减测试。

生产轮换/容量逻辑复查：`add()` 总是先写位再递增并判饱和；CAS 位更新原子；片回收按槽位索引替换并重置 `saturated`。未发现生产缺陷，问题确属测试时钟依赖。

## C. 新增测试清单（5 个）

- `EventParserTest.boundedCompressedExactlyAtLimitIsAllowed`
- `EventParserTest.boundedCompressedOverLimitThrowsNotEof`
- `EventParserTest.concurrentStreamingJsonParseDoesNotCrossContaminate`
- `EventParserTest.concurrentStreamingNdjsonParseDoesNotCrossContaminate`
- `FlowLensHttpServerTest.compressedBodyOverLimitReturns413`

## D. 18 项测试覆盖矩阵（更新后）

| # | 要求 | 覆盖 | 对应测试 |
|---|---|---|---|
| 1 | JSON InputStream 解析 | 是 | `jsonArrayInputStreamParse` |
| 2 | gzip InputStream 解析 | 是 | `gzipIngestAccepted` |
| 3 | 压缩体积超限 | 是 | `boundedCompressedOverLimitThrowsNotEof`、`compressedBodyOverLimitReturns413` |
| 4 | 解压体积超限 | 是 | `oversizedDecompressedBodyReturns413` |
| 5 | gzip CRC 错误 | 是 | `gzipCrcErrorIsRejected` |
| 6 | gzip trailer 不完整 | 是 | `truncatedGzipTrailerIsRejected` |
| 7 | JSON 中途语法错误整批失败 | 是 | `syntaxErrorRejectsWholeBatch` |
| 8 | JSON 数组后非法尾随 | 是 | `trailingContentAfterJsonArrayIsRejected` |
| 9 | 事件数超限 | 是 | `tooManyEventsReturns413` |
| 10 | NDJSON 流式解析 | 是 | `ndjsonInputStreamParse` |
| 11 | 并发流式解析 | 是 | `concurrentStreamingJsonParse...`、`concurrentStreamingNdjsonParse...` |
| 12 | 解析失败无部分提交 | 是 | `parseFailureDoesNotEnqueue` |
| 13 | 4 并发 ingest | 是 | `ingestConcurrencyIsFour` |
| 14 | 超出 ingest 并发 | 是 | `ingestConcurrencyIsFour` |
| 15 | 查询并发=1 | 是 | `queryConcurrencyLimitReturns429` |
| 16 | SSE 上限 16 | 是 | `sseConnectionLimitIsEnforced` |
| 17 | 慢客户端缓冲上限 | 是 | `slowClientQueueIsBounded` |
| 18 | 关闭并发测试 | 是 | 关闭相关多个测试 |

18 项全部覆盖并有真实断言。

## E. GZIP 完整性验证

- CRC 错误 → 400：`gzipCrcErrorIsRejected`（`GZIPInputStream` 在 EOF 校验）。
- trailer 不完整 → 400：`truncatedGzipTrailerIsRejected`。
- 压缩体超限 → 413：`compressedBodyOverLimitReturns413`。
- 解压体超限 → 413：`oversizedDecompressedBodyReturns413`。
- JSON 语法错误整批 400：`syntaxErrorRejectsWholeBatch`；解析成功前不调用 ingest（`parseFailureDoesNotEnqueue` 验证 healthz `events_ingested` 保持 0）。
- gzip 读取至 EOF 才完成 CRC/trailer 校验；JSON 数组解析的尾随检查会读到 EOF，不提前停止。

## F. SSE 缓冲真实内存边界（设计估算）

`SseClient` 使用容量 1 的 `ArrayBlockingQueue<Snapshot>`。快照为共享的不可变 `Snapshot` 对象，广播给所有客户端，客户端只持有同一对象的引用。

单快照最坏内容：URL Top-10 + User Top-10，共 20 个 `TopKEntry`；每个 `name` 最长 256 字符（UTF-16 最坏约 512 字节 + String 对象头约 24 字节），加 3 个 long 与 record 开销，单条约 600–900 字节；20 条约 12–18 KiB。16 个客户端各只存一个引用（8 字节）。

结论：SSE 缓冲上限约为“1 个共享快照（≤20 KiB）+ 16 个引用”，远小于 64KB/客户端。实现记录为“容量 1 个快照”，而非 64KB 字节缓冲。

## G. JVM 启动参数及依据（设计估算）

`run.ps1`：

```text
-Xms64m
-Xmx200m
-XX:MaxMetaspaceSize=64m
-XX:ReservedCodeCacheSize=64m
-XX:+UseCodeCacheFlushing
-XX:MaxDirectMemorySize=16m
-XX:NativeMemoryTracking=summary
-Xss256k
-XX:ActiveProcessorCount=4
```

依据（设计估算）：

- 常驻 Heap：固定数组 156.3 + 队列/在途 4.4 + ingest 暂存 2.2 + 对象 0.5 ≈ 163.4 MiB。
- 峰值瞬时 + GC 余量 ≈ 20 MiB。
- `-Xmx200m` 给常驻 + 瞬时留出余量，不盲目 184m。
- Native：Metaspace cap 64（resident 估约 30）、CodeCache reserved 64 + flushing（resident 估约 20）、Direct cap 16（resident 估约 1）、平台线程栈 8×256KB≈2、JVM native ≈25 → Native ≈78 MiB。

RSS 设计估算 ≈ Heap committed(~180) + Native(~78) ≈ 258 MiB，略超 256，属于待实测区间（**设计估算，非实测**）。

## H. Maven clean test 实际结果

```text
Tests run: 117, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## I. Maven clean package 实际结果

```text
BUILD SUCCESS
（生成 target/flowlens-0.1.0-SNAPSHOT.jar）
```

## J. 六接口启动验证（run.ps1）

`run.ps1` 启动成功，实测：

- `/healthz` → `{"ok":true,"state":"RUNNING","degraded":false,"events_ingested":0}`
- `/readyz` → `{"ready":true,"state":"RUNNING","degraded":false}`
- `POST /api/v1/ingest` → `{"accepted":1,"deduped":0,"rejected":0,"overloaded":false,"unprocessed":0}`
- `/api/v1/topk` → 返回 `/a/b` 及误差区间
- `/api/v1/stats` → 返回近似 UV/p95 与误差说明
- `/api/v1/stream` → 返回 `retry/id/event/data` SSE 帧

关闭后无遗留 SSE 客户端。

## K. 当前 RSS 设计估算

Heap 设计约 183.4 MiB（committed 估算约 180 MiB），Native 设计约 78 MiB，RSS 设计估算约 258 MiB，略高于 256 MiB。此为设计估算，需 Docker 实测确认。

## L. 仍然存在的风险

- RSS 设计估算略超 256 MiB，Docker 实测可能超过；若超过，后续需评估 Bloom FP 放宽或降低 GC/暂存余量（本轮未动）。
- 200,000 events/sec 未验证；本地单线程突发 accepted/processed 约 44.9 万/s（含 overloaded），不代表稳态达标。
- 生产默认时钟保持单调 nanoTime（未改为 `System::currentTimeMillis`，以免破坏单调性要求）。

## M. 是否批准进入 Docker 256MiB 实测

批准进入 Docker 实测。流式解析、降并发、测试时钟缺陷均已修复，117 测试全绿；当前唯一悬而未决的是 RSS 实测是否 ≤256 MiB。应在 Docker `--memory=256m --memory-swap=256m` 下用 `memory.current/peak` 与 NMT 实测，再决定是否需进一步权衡。
## N. 后续更新（文档一致性修复时追加）

第五阶段 C 报告原文记录 117 个测试；该记录为当时历史验收结果，予以保留。

之后补充了 1 个针对性测试：

- `SlidingBloomFilterTest.duplicateInRetentionStillDedupedAcrossDegradationAndRecovery`

最新完整回归（`mvn clean test`，真实执行）：

```text
Tests run: 118, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```
