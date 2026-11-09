# FlowLens load test tools

## LoadTest.java

Standalone JDK-only ingest load generator. Build:

```bash
cd tools/loadtest
javac -d . LoadTest.java
```

Run:

```bash
java -cp tools/loadtest loadtest.LoadTest --url http://127.0.0.1:8080/api/v1/ingest --rate 200000 --duration 60 --batch 500 --concurrency 8
```

Options:

- `--url`：ingest 地址（默认 http://127.0.0.1:8080/api/v1/ingest）
- `--rate`：目标 offered events/sec（默认 100000）
- `--duration`：测试秒数（默认 30）
- `--batch`：每请求事件数（默认 500，建议 ≤1000）
- `--concurrency`：客户端并发线程数（默认 4）

输出：offered/accepted/deduped/rejected/overloaded/unprocessed/errors、offered/accepted 速率、p50/p95/p99 请求延迟、HTTP 状态码分布。

事件字段与 EventParser 一致：event_id、ts、user_id、url、bytes；event_id 唯一，url 限定在 1000 个不同值，内存有界。

## monitor-memory.ps1

持续采集容器 cgroup 内存数据到 CSV：

```powershell
cd tools/loadtest
./monitor-memory.ps1 -Container flowlens-load -IntervalSec 1 -OutFile memory.csv
```

输出列：ts、memory_current、memory_peak、memory_max、oom、oom_kill、mem_usage。


## Query mode

`--mode` 可选 `ingest`（默认）、`topk`、`stats`、`sse`。非 ingest 模式通过 GET 测量服务端查询延迟，把 429 单独统计：

```bash
java -cp tools/loadtest loadtest.LoadTest --mode topk --url "http://127.0.0.1:8080/v1/topk?metric=url&k=10&window=60" --duration 10 --concurrency 1
java -cp tools/loadtest loadtest.LoadTest --mode stats --url "http://127.0.0.1:8080/v1/stats?window=60" --duration 10 --concurrency 1
java -cp tools/loadtest loadtest.LoadTest --mode sse --url "http://127.0.0.1:8080/v1/topk/stream" --duration 10 --concurrency 1
```

查询模式输出 `success`、`failures`、`rate_limited_429`、SSE 事件数以及成功/失败请求的 p50/p95/p99。