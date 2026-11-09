# FlowLens 最终验收报告

## 1. 环境和镜像版本

- Windows 11，Java 21 Temurin，Maven 3.8.8。
- Docker Desktop Linux Engine。
- 生产镜像：`flowlens:0.5.0-phase16`。
- JVM：G1GC，`-Xms64m -Xmx160m`。
- 容器限制：`--memory=256m --memory-swap=256m`。

## 2. Maven 测试结果

真实执行 `mvn test`：

`Tests run: 138, Failures: 0, Errors: 0, Skipped: 0`

`BUILD SUCCESS`

## 3. 200k/s 实测结果

使用 `tools/loadtest/LoadTest.java`，`batch=1000`，`concurrency=4`，全新 event_id 前缀 `p17-200k-`，目标 200,000 events/sec × 30s。

- offered = 6,000,000
- accepted = 6,000,000
- deduped = 0
- rejected = 0
- overloaded = 0
- unprocessed = 0
- errors = 0
- concurrency503 = 0
- 实际 offered_rate = 73,493.3 events/sec
- 实际 accepted_rate = 73,493.3 events/sec
- 请求延迟：p50 52 ms，p95 59 ms，p99 60 ms
- 客户端 CPU ≈ 19.8%

结论：未达到 200,000 events/sec。稳定吞吐约为 73,500 events/sec。所有发出的事件都被接受，没有丢事件和 503，瓶颈是请求处理延迟约 52ms，而非客户端饱和。

## 4. accepted 与 processed 区别

- `accepted` 表示事件已通过去重准入并成功入队，最终 accepted = 6,000,000。
- `processed` 表示 Shard 写线程已完成聚合，但当前 HTTP/healthz 接口未暴露 processed 计数。
- 本测试中 accepted=offered，无 overloaded/unprocessed 报错；`processed` 未直接观测，需在后续阶段补充接口或诊断手段才能严格断言写线程最终追平。

## 5. 内存峰值及 OOM 状态

200k/s 尝试结束时：

- memory.current = 257,523,712 B ≈ 245.6 MiB
- memory.peak = 268,435,456 B = 256 MiB（达到硬上限）
- memory.max = 256 MiB
- anon ≈ 228,274,176 B
- file ≈ 23,339,008 B
- memory.events：oom=0，oom_kill=0，oom_group_kill=0，max=16
- OOMKilled=false

结论：未发生 OOM，但 200k/s 尝试期间 memory.peak 已触及 256 MiB 硬上限，内存余量不足，不能视为安全通过。

## 6. Top-K 和 Stats p99

在持续 ingest 状态下，使用 `query_latency.py --reconnect`，各 1,000 次请求：

- Top-K：成功 997，p99 = 27.716 ms
- Stats：成功 998，p99 = 27.332 ms

两项均低于 50 ms，达标。

## 7. 原题接口验收结果

- `POST /v1/ingest`：正常。同一批次中 accepted=1、deduped=1、rejected=1，非法 bytes 被拒绝。
- `GET /v1/topk`：200，返回 effective_at/window/metric/items，字段正确。
- `GET /v1/stats`：200，返回 approx_distinct_users/approx_p95_bytes，字段正确。
- `GET /v1/topk/stream`：SSE 可连接并推送 `retry/id/event/data`。
- `GET /healthz`：200，包含 state、degraded、events_ingested、memory_bytes。

全部接口通过。

## 8. 未达到的指标及原因

- 200,000 events/sec 未达到：实际稳定吞吐约 73,500 events/sec。
- 直接瓶颈：服务端单请求处理 p50 约 52 ms；4 并发下约 77 请求/秒 × 1000 事件/请求 ≈ 77k events/sec，与实测 73.5k/s 相符。
- 内存：未 OOM，但 200k/s 尝试期间 peak 触及 256 MiB 上限，不满足安全余量。

## 9. 最终是否满足原题要求

部分满足：

- 已满足：4 Shard、SpaceSaving C=256、HLL p=13、Histogram 505、Bloom 3×30s、RingBuffer 128、六个 HTTP/SSE 接口、重复去重、非法拒绝、查询 p99 < 50ms、无 OOM。
- 未满足：200,000 events/sec 稳态吞吐；200k/s 时内存 peak 触及 256 MiB 硬上限。

因此不能宣告整体通过原题。需要在后续阶段针对 ingest 单请求延迟和 200k/s 内存余量做进一步工作。