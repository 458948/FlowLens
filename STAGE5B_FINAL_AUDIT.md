# FlowLens 第五阶段 B 最终代码验收

> 本轮只核查，未修改代码。发现的问题在 G 节列明，等待审核。

## A. 并发参数核查

| 参数 | 目标 | 实际 | 文件 / 位置 |
|---|---:|---|---|
| Ingest 最大并发 | 4 | 4 | `FlowLensHttpServer.java`：`INGEST_CONCURRENCY = 4` |
| Query 最大并发 | 1 | 1 | 同文件：`QUERY_CONCURRENCY = 1` |
| SSE 最大连接 | 16 | 16 | 同文件：`SSE_MAX_CONNECTIONS = 16` |
| SSE 单客户端缓冲 | 64KB | 容量 1 的快照队列 | `SseClient.java`：`new ArrayBlockingQueue<>(1)` |

说明：SSE 未使用 64KB 字节缓冲，而是每个客户端一个容量为 1 的 `Snapshot` 队列，内存远小于 64KB，因此满足“≤64KB”的意图，不是缺陷。

## B. 18 项测试覆盖矩阵

| # | 要求 | 覆盖 | 测试类 | 方法 | 真实断言 |
|---|---|---|---|---|---|
| 1 | JSON InputStream 解析 | 是 | EventParserTest | `jsonArrayInputStreamParse` | 是 |
| 2 | gzip InputStream 解析 | 是 | FlowLensHttpServerTest | `gzipIngestAccepted` | 是 |
| 3 | 压缩体积超限 | 部分 | EventParserTest | `compressedBodyLimitIsEnforced`（`readCompressedLimited`） | 是 |
| 4 | 解压体积超限 | 是 | FlowLensHttpServerTest | `oversizedDecompressedBodyReturns413` | 是 |
| 5 | gzip CRC 错误 | 是 | EventParserTest | `gzipCrcErrorIsRejected` | 是 |
| 6 | gzip trailer 不完整 | 是 | EventParserTest | `truncatedGzipTrailerIsRejected` | 是 |
| 7 | JSON 中途语法错误整批失败 | 是 | FlowLensHttpServerTest | `syntaxErrorRejectsWholeBatch` | 是 |
| 8 | JSON 数组后非法尾随内容 | 是 | EventParserTest | `trailingContentAfterJsonArrayIsRejected` | 是 |
| 9 | 事件数超限 | 是 | FlowLensHttpServerTest | `tooManyEventsReturns413` | 是 |
| 10 | NDJSON 流式解析 | 是 | EventParserTest | `ndjsonInputStreamParse` | 是 |
| 11 | 并发流式解析 | 否 | — | — | 无 |
| 12 | 解析失败无部分提交 | 是 | FlowLensHttpServerTest | `parseFailureDoesNotEnqueue` | 是 |
| 13 | 4 并发 ingest 许可 | 是 | FlowLensHttpServerTest | `ingestConcurrencyIsFour` | 是 |
| 14 | 超出 ingest 并发 | 是 | FlowLensHttpServerTest | `ingestConcurrencyIsFour` | 是 |
| 15 | 查询并发=1 | 是 | FlowLensHttpServerTest | `queryConcurrencyLimitReturns429` | 是 |
| 16 | SSE 上限 16 | 是 | FlowLensHttpServerTest | `sseConnectionLimitIsEnforced` | 是 |
| 17 | 慢客户端缓冲上限 | 是 | FlowLensHttpServerTest | `slowClientQueueIsBounded` | 是 |
| 18 | 关闭并发测试继续通过 | 是 | FlowLensHttpServerTest | 关闭相关多个测试 | 是 |

未覆盖项：#3 压缩体积超限仅覆盖了旧 `readCompressedLimited`，未覆盖新的 `boundedCompressed` 流式路径；#11 无并发流式解析测试。

## C. 流式解析正确性

调用链已按设计实现：HTTP InputStream → `boundedCompressed` → `GZIPInputStream` → `limitedGzip` → Jackson → 有界 `List<RawEvent>` → 解析完成后统一提交。

- gzip CRC/trailer：由 `GZIPInputStream` 在读到 EOF 时校验；JSON 数组解析的尾随检查会读至 EOF。
- 超限：`CompressedLimitExceededException` / `DecompressedLimitExceededException` 在流上抛，不伪装 EOF。
- 原子性：解析全部完成后才调用 `runtime.ingest`，语法错误整批 400。
- 资源：`JsonParser` try-with-resources、HTTP 流在 try 内构造、`HttpJson.send` 关闭 exchange。
- 字段校验：仍走 `EventParser.parseEvent` 与 `UrlNormalizer`，未绕过。

## D. GZIP 完整性验证

由 `GZIPInputStream` 原生校验 CRC32 与 ISIZE/trailer；`limitedGzip` 负责解压上限；`boundedCompressed` 负责压缩上限。测试覆盖 CRC 错误与 trailer 截断。

## E. JVM 内存参数

- `FlowLensApplication`、`pom.xml` 均未设置 `-Xmx`、Metaspace/CodeCache/Direct cap、NMT。
- `DESIGN.md` 仅给出建议参数，未在启动入口落实。
- 当前 jar 以 JVM 默认堆运行，未强制 256 MiB 约束；`reserved`/`committed`/`RSS` 未实测，不能混为一谈。

## F. Maven 实际测试结果（本轮真实执行）

```text
Tests run: 112, Failures: 1, Errors: 0, Skipped: 0
BUILD FAILURE
```

失败项：

```text
SlidingBloomFilterTest.concurrentCapacityOverAcceptIsMeasuredAndMarked
expected: <1600> but was: <1583>
```

（同一文件的其他 SlidingBloomFilter 测试在上一轮亦出现 `nearCapacityNeverAcceptsWithoutMarking` 偶发失败。）

## G. 尚未解决的问题 / 发现

1. **SlidingBloomFilter 测试偶发失败**：这些测试用 `new SlidingBloomFilter(1000)` 的真实单调时钟，并断言 `insertionsInCurrentSlice` 精确等于 1600、或对全部 id 断言 `mightContain` 为 true。当测试恰好跨 30 秒时间片边界时，部分插入落到旧片，导致计数/标记断言失败。这是测试对真实时钟的依赖缺陷，不是生产逻辑错误。
   - 文件/方法：`SlidingBloomFilterTest.concurrentCapacityOverAcceptIsMeasuredAndMarked`、`nearCapacityNeverAcceptsWithoutMarking`。
   - 修复建议：给这两个测试注入可控 `LongSupplier` 时钟，固定在同一时间片内完成，消除跨边界竞态。
2. 压缩体积超限的 `boundedCompressed` 流式路径缺少直接 HTTP 测试。
3. 缺少并发流式解析测试。
4. 启动 jar 未内置 JVM 内存参数与 NMT。

## H. 是否批准进入 Docker 256MiB 实测

暂缓批准。流式解析与降并发已完成，RSS 设计估算约 238 MiB；但在进入 Docker 实测前，应先修复 G.1 的测试时钟缺陷（避免 CI 偶发红），补 G.2/G.3 测试，并在启动方式中显式落实 JVM 参数与 `-XX:NativeMemoryTracking=summary`，以便对照 reserved/committed/RSS。