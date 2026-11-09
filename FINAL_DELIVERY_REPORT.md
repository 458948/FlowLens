# FlowLens 最终交付报告

## 1. 项目简介与技术架构

FlowLens 是有界内存的实时流量分析服务，统计最近 60 秒窗口内热点 URL / user_id Top-K、近似去重用户数和响应字节 p95。

技术栈：Java 21 Temurin、Maven、JDK HttpServer、Java 21 虚拟线程、Jackson Core（流式 JSON）、JUnit 5。

运行时架构：4 个 Shard，每个 Shard 一个容量 2048 的有界队列 + 单写线程 + 128 秒级桶 RingBuffer；全局唯一 QueryService / QueryWorkspace / DedupAdmission / SnapshotPublisher；HTTP ingest 并发 4，查询并发 1，SSE 最大 16 连接。

## 2. 核心算法及其实现

- SpaceSaving：C=256，固定容量原生数组、开放寻址、最小堆、128 位指纹 + 原始字节碰撞校验；合并误差 E=N/C。
- HyperLogLog：p=13，8192 寄存器，标准误约 1.15%。
- LogHistogram：505 bins，整数位运算定位，p95 桶内 log-linear 插值。
- Sliding Bloom Filter：3×30 秒片，每片 1,800,000 容量，20 次探测，目标 FP=1e-6，SipHash128 指纹，单调时间源与饱和恢复。
- RingBuffer + Watermark：128 秒级桶，60 秒迟到窗口，W=now-60000ms。
- QueryWorkspace：TopKMerger arena 延迟分配；Top-K 在桶读锁内直接合并；Stats 读锁内合并 HLL/Histogram。

## 3. HTTP/SSE 接口清单

- `POST /v1/ingest` 与 `/api/v1/ingest`
- `GET /v1/topk` 与 `/api/v1/topk`
- `GET /v1/stats` 与 `/api/v1/stats`
- `GET /v1/topk/stream` 与 `/api/v1/stream`
- `GET /healthz`
- `GET /readyz`

## 4. Docker 构建和运行方法

构建：

```bash
docker build -t flowlens:0.5.0-phase16 .
```

运行：

```bash
docker run -d --name flowlens -p 8080:8080 --memory=256m --memory-swap=256m flowlens:0.5.0-phase16
```

健康检查：

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

停止：

```bash
docker stop --timeout 30 flowlens
```

## 5. 测试结果

本轮真实执行 `mvn clean test`：

`Tests run: 138, Failures: 0, Errors: 0, Skipped: 0`

`BUILD SUCCESS`

## 6. 实际吞吐与查询延迟

- 实际稳定 ingest 吞吐约 71,852 events/sec（30 秒墙钟窗口，batch=1000，concurrency=4）。
- 持续 ingest 下查询 p99：Top-K 28.137 ms，Stats 28.231 ms。

## 7. 256 MiB 内存测试结果

200k/s 尝试时：

- memory.peak = 268,435,456 B = 256 MiB（触及 cgroup 硬上限）。
- OOMKilled=false，oom=0，oom_kill=0。

未发生 OOM，但峰值触及上限，不能宣称拥有充足安全余量。

## 8. 已通过的原题要求

- 4 Shard 单写线程 + 有界队列 + RingBuffer。
- SpaceSaving C=256、HLL p=13、Histogram 505、Bloom 3×30s、FP=1e-6。
- HTTP 六接口与 SSE 推送。
- 重复 event_id 去重；非法事件拒绝。
- Top-K p99 < 50ms。
- Stats p99 < 50ms。
- Docker 256 MiB 内未发生 OOM。

## 9. 尚未通过的原题要求

- 200,000 events/sec 未达到，实际稳定吞吐约 71,852 events/sec。
- 200k/s 下内存峰值触及 256 MiB 上限，不具备安全余量。

## 10. 当前已知限制

- ingest 单请求延迟 p50 约 55ms，4 并发下约 72 请求/秒 × 1000 事件 ≈ 72k events/sec，是当前吞吐瓶颈。
- 未提供 processed 计数的 HTTP 接口。
- 进程重启后 Bloom 去重状态丢失（概率去重语义）。
- file 页缓存随运行环境波动，cgroup memory.current/peak 不能等价于 JVM RSS。

## 11. 项目目录结构

- `src/main/java/io/flowlens`：bench、dedup、hash、http、ingest、query、runtime、sketch、window。
- `src/test/java/io/flowlens`：单元/集成测试。
- `tools/loadtest`：LoadTest.java、query_latency.py、monitor-memory.ps1、README 与 results。
- 根目录：Dockerfile、.dockerignore、run.ps1、pom.xml、README.md、API.md、DESIGN.md、AI_LOG.md 及各阶段报告。

## 12. 最终运行命令

```powershell
mvn clean test
mvn clean package
./run.ps1
```

或：

```bash
docker run -d --name flowlens -p 8080:8080 --memory=256m --memory-swap=256m flowlens:0.5.0-phase16
```