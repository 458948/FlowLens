# FlowLens HTTP API

所有时间参数均使用 **epoch 毫秒**（`at`），`window` 使用 **秒**。

并发限制：ingest 最多 4；Top-K/Stats 查询共享 1 个许可；SSE 最多 16 个连接。

## 1. POST /api/v1/ingest

接收批量事件。支持 `application/json`（JSON 数组）与 `application/x-ndjson`（每行一条事件）。支持 `Content-Encoding: gzip`。

请求体上限：压缩后 ≤ 1 MiB，解压后 ≤ 1 MiB，单批事件数 ≤ 1000。

事件字段：

- `event_id`：全局唯一，重复表示重试（≤128 UTF-8 字节）。
- `ts`：epoch 毫秒，允许乱序，迟到不超过一个窗口（60s）。
- `user_id`：≤64 UTF-8 字节。
- `url`：原始 ≤1024 UTF-8 字节，归一化后 ≤256 UTF-8 字节。
- `bytes`：非负 long。

处理顺序：限流 → Content-Type/Encoding 校验 → 流式读取 → 大小校验 → 整批解析 → 逐条准入。JSON 语法错误整批返回 400，不会提前入队。

状态码：

- 202：批次处理完成。
- 400：JSON 语法或请求参数错误。
- 413：请求体或事件数超限。
- 415：不支持的 Content-Type / Content-Encoding。
- 503：过载、队列满或 Runtime 已关闭。

响应：

```json
{"accepted":100,"deduped":5,"rejected":2,"overloaded":false,"unprocessed":0}
```

## 2. GET /api/v1/topk

参数：`metric=url|user`（默认 url）、`k=1..100`（默认 10）、`window=1..60` 秒（默认 60）、`at` 可选 epoch 毫秒（默认 now）。

响应：

```json
{"effective_at":1794000000000,"stable":false,"window":60,"metric":"url",
 "items":[{"name":"/product/1","estimated_count":120,"lower_bound":100,"upper_bound":140}]}
```

Top-K 使用 SpaceSaving，`estimated_count` 是估计值，误差区间为 `[lower_bound, upper_bound]`。

## 3. GET /api/v1/stats

参数：`window=1..60` 秒（默认 60）、`at` 可选 epoch 毫秒。

响应：

```json
{"effective_at":1794000000000,"stable":false,"window":60,
 "approx_distinct_users":12345,"approx_p95_bytes":1024,
 "p95_error_note":"log-histogram estimate; bin relative width ~9.05%"}
```

`approx_distinct_users` 使用 HyperLogLog（标准误约 1.15%），`approx_p95_bytes` 使用 LogHistogram，二者均为估计值。

## 4. GET /api/v1/stream

SSE 实时推送。全局 SnapshotPublisher 每秒计算一次快照并广播给所有客户端，客户端不单独执行查询。

响应头：`Content-Type: text/event-stream`、`Cache-Control: no-cache`、`Connection: keep-alive`、`X-Accel-Buffering: no`。

消息格式：

```text
retry: 3000

id: 1
event: snapshot
data: {"effective_at":...,"window":60000,"sequence":1,"approx_distinct_users":...,"approx_p95_bytes":...,"url_top":[...],"user_top":[...]}

: keep-alive

```

## 5. GET /healthz

存活检查，返回 200 与 `state`、`degraded`、`events_ingested`、`memory_bytes`。`memory_bytes` 在 Linux 下读取 `/proc/self/status` 的 `VmRSS`，表示进程 RSS；非 Linux 返回 -1。

## 6. GET /readyz

就绪检查。Runtime RUNNING 且无 Shard/Bloom degraded 时返回 200；否则 503。

## 估算误差

- SpaceSaving：合并误差上界 E=N/C，返回 `[est-E, est+E]`。
- HyperLogLog：标准误约 1.15%。
- LogHistogram p95：bin 内插值为估计，bin 相对宽度约 9.05%。
- Sliding Bloom Filter 去重为概率去重，不是精确去重；保留 60–90 秒，进程重启后丢失。

## 路径别名

原题接口路径均已支持，且原 /api/v1/* 路径继续保留：/v1/ingest、/v1/topk、/v1/stats、/v1/topk/stream、/healthz。/api/v1/stream 仍可用于 SSE。
