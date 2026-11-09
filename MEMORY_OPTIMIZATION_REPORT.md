# FlowLens 第五阶段 B：内存优化实施与验证报告

> 标签：**理论计算**、**设计估算**、**本地实测**。Docker 实测未完成（守护进程未运行）。

## A. 修改文件清单

- `src/main/java/io/flowlens/ingest/GzipLimiter.java`：新增 `boundedCompressed` 与专用超限异常，区分压缩/解压超限。
- `src/main/java/io/flowlens/ingest/EventParser.java`：JSON 数组尾随内容校验、流式 `parseNdjson(InputStream)`、透传被 Jackson 包裹的底层 IOException。
- `src/main/java/io/flowlens/http/IngestHandler.java`：改为流式 ingest，不再物化压缩/解压 `byte[]`。
- `src/main/java/io/flowlens/http/FlowLensHttpServer.java`：`INGEST_CONCURRENCY 8→4`、`SSE_MAX_CONNECTIONS 32→16`。
- 测试：`EventParserTest`（+5）、`FlowLensHttpServerTest`（+2）。

未修改 SpaceSaving C=256、HLL p=13、Histogram 505、Bloom FP=1e-6、Shard/Ring/Watermark 语义，未创建 Dockerfile。

## B. 流式解析调用链

```text
HTTP request InputStream
  -> GzipLimiter.boundedCompressed(...)     # 压缩体 ≤1 MiB
  -> GZIPInputStream                        # gzip 解压，校验 CRC/trailer
  -> GzipLimiter.limitedGzip(...)           # 解压体 ≤1 MiB
  -> Jackson JsonParser                     # 流式解析
```

压缩/解压体积在流上按字节计数，超限抛专用异常（不再伪装 EOF）；gzip CRC/尾部错误由 `GZIPInputStream` 抛 `ZipException`/`EOFException`，HTTP 映射为 400；超限映射 413。

## C. GZIP 内存变化

旧实现：压缩 `byte[]`（≤1 MiB）+ 解压 `byte[]`（≤1 MiB）+ 解析暂存 0.54 MiB，单请求约 2.55 MiB；8 并发约 20.4 MiB。

新实现：流式读取，不保留压缩/解压 `byte[]`；仅 Jackson 缓冲（约 4 KiB）+ `List<RawEvent>`（≤0.54 MiB），单请求约 0.55 MiB；4 并发约 2.2 MiB（**设计估算**）。节省约 18 MiB。

## D. Ingest 并发变化

`INGEST_CONCURRENCY` 8→4。超出返回 503，不静默丢事件；accepted 后仍必写 Bloom；队满重试幂等语义不变。

## E. 查询和 SSE 资源变化

- 查询并发：已为 1（`QUERY_CONCURRENCY=1`），本轮未变；HTTP Top-K/Stats 与 SnapshotPublisher 共享同一 `querySemaphore`。
- SSE 连接：32→16。
- SSE 单客户端缓冲：实现为容量 1 的快照队列（远小于 64 KiB），无 128 KiB 缓冲可降，保持不变。

## F. 新增测试（7 个）

EventParserTest：`jsonArrayInputStreamParse`、`ndjsonInputStreamParse`、`trailingContentAfterJsonArrayIsRejected`、`gzipCrcErrorIsRejected`、`truncatedGzipTrailerIsRejected`。

FlowLensHttpServerTest：`ingestConcurrencyIsFour`、`parseFailureDoesNotEnqueue`。

旧 105 个测试全部保留。

## G. 新旧内存预算对比（设计估算）

| 项 | 旧 | 新 |
|---|---:|---:|
| 固定数组（Bloom+TopK+HLL+Hist+Workspace） | 156.32 | 156.32 |
| 队列 + 在途 RawEvent | 4.4 | 4.4 |
| Shard/Runtime 对象 | 0.3 | 0.3 |
| HTTP ingest 暂存 | 20.4 | 2.2 |
| SSE/发布器 | 0.2 | 0.2 |
| GC 余量 | 20 | 20 |
| Heap 设计 | 201.6 | 183.4 |
| Native（估算） | 55 | 55 |
| RSS 估算 | 256.6 | 238.4 |

新配置 RSS 估算约 238 MiB，落在 240 MiB 目标内（**设计估算，非实测**）。

## H. 实际 Maven 输出

```text
Tests run: 112, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## I. 尚未解决的问题

- Docker 256 MiB 未实测（Docker Desktop 守护进程未运行）。
- 200,000 events/sec 未验证；本地单线程突发 accepted/processed 约 44.9 万/s（含 overloaded），不代表稳态达标。
- 可执行 JAR 未内置 `-Xmx`，需显式传入建议参数。
- Bloom FP 保持 1e-6（本轮未放宽，按任务要求）。

## J. 是否建议进入 Docker 实测

建议进入。流式解析 + 降并发后，RSS 设计估算约 238 MiB（低于 240 目标），应在 Docker `--memory=256m --memory-swap=256m` 下用 `memory.current/peak` 实测确认，再决定是否继续调整。