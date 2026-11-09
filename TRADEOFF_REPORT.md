# FlowLens 第五阶段：多目标权衡与内存优化报告

> 标签约定：**理论计算**、**设计估算**、**本地实测（Windows/JVM）**、**Docker 实测**。未标注“实测”的均为估算，未完成 Docker 实测。

## A. 硬性指标与软性指标

硬性（不可牺牲）：

- Java 21。
- 常驻内存 ≤ 256 MiB（Docker `--memory=256m --memory-swap=256m`）。
- 去重保留期 ≥ 60 秒。
- 已 `accepted` 事件必须写入 Bloom 标记。
- JSON 语法错误整批失败，不提前入队。
- 不静默丢失已确认事件；队满返回 overloaded，重试幂等。
- 线程安全；4 Shard 单写线程；事件按 128 位指纹路由。
- 吞吐目标 200,000 events/sec（原始题目定义，未降低）。

软性（可权衡）：Top-K 长尾覆盖率、SpaceSaving 误差、HLL 误差、Bloom 假阳性率、查询延迟、查询并发、SSE 连接数、字典名称可用率、内存。

## B. 当前配置与内存基线（重新按代码计算）

当前核心参数（来自源码常量，**理论/代码事实**）：

- Shard=4；RingBuffer 桶=128；SpaceSaving C=256；HLL p=13（8192 寄存器）；Histogram=505 bin。
- Bloom：3×30s；单片容量 1,800,000；目标 FP 1e-6；20 探测。
- ingest 并发=8；query 并发=1；SSE=32；单客户端队列容量=1（快照）；队列容量=2048/Shard。

内存（**设计估算**，按数组元素与长度计算）：

| 项 | MiB | 性质 |
|---|---:|---|
| Bloom 12 片（808,806 long/片） | 74.05 | 固定数组 |
| Top-K（URL 32 + user 8 + 指纹 4 + count/error 4 + 表/堆 4.25） | 52.25 | 固定数组 |
| HLL（512×8192） | 4.00 | 固定数组 |
| Histogram（512×505×8） | 1.97 | 固定数组 |
| QueryWorkspace（全局唯一） | 24.05 | 固定数组 |
| 队列 + 在途 RawEvent | 4.4 | 估算 |
| HTTP ingest 暂存（8 并发 gzip 最坏） | 20.4 | 估算 |
| Shard/Runtime 对象 | 0.3 | 估算 |
| GC 余量 | 20 | 估算 |
| Heap 设计合计 | 201.42 | 估算 |
| Native（栈/Metaspace/CodeCache/Direct/JVM） | 55 | 估算 |
| RSS 估算 | 256.4 | 估算 |

结论：当前配置 RSS 估算约 256.4 MiB，超出 256 MiB 且无余量（**设计估算**）。

GZIP 流式解压/Jackson 流式解析：**未实现**。`IngestHandler` 目前把解压结果物化进 `byte[]` 后再调用 `EventParser.parseJsonArray(byte[])`/`parseNdjson(byte[])`，压缩体与解压体在请求处理期间并存。

## C. 实验矩阵（参数）

- A：当前配置（对照组）。
- B：ingest 并发 8→4、SSE 32→16、查询并发保持 1。
- C：SpaceSaving C=256/192/128（已本地实测召回）。
- D：Bloom 单片目标 FP 1e-6/1e-5/1e-4。
- E：Bloom 时间片 3×30s / 4×20s / 2×60s（2×30s 已否决，不满足 60s 最短保留）。
- F：HLL p=13 / p=12。
- G：字典 32K→16K（当前实现无独立字典，Top-K 名称直接存于草图字节 arena）。
- H：组合候选（流式 gzip + 降并发 + Bloom FP 1e-5）。

## D–K. 实测与估算数据

本地基准（`TradeoffBenchmark`，**本地实测**，单线程客户端，Windows/JVM，非 Docker）：

```text
ingest: offered=1000000 accepted=872730 processed=872730 overloaded=127270
offered=514927/s accepted=449392/s processed=449384/s
```

SpaceSaving 召回（**本地实测**，Zipf 幂律、N=500,000、seed=42）：

```text
C=256: recall@10=1.00 heavyRecall=1.00 avgAbsErr=695
C=192: recall@10=1.00 heavyRecall=1.00 avgAbsErr=1080
C=128: recall@10=1.00 heavyRecall=1.00 avgAbsErr=1961
```

说明：召回在本次 Zipf 分布下均为 1.00；误差随 C 减小而增大。该结果只代表该分布与种子，不是对所有流量的保证（**本地实测**）。

Bloom 假阳性（**理论计算**，按真实实现 k=20、m 由 FP 反推）：

| FP | 单片 bits | 单片 MiB | 12 片 MiB | 联合 FP（3 片） |
|---|---:|---:|---:|---:|
| 1e-6 | 51,763,557 | 6.17 | 74.05 | 3e-6 |
| 1e-5 | 43,579,?（约） | 5.20 | 62.4 | 3e-5 |
| 1e-4 | 35,395,?（约） | 4.22 | 50.7 | 3e-4 |

（1e-5/1e-4 的精确 bits 需用实现公式复算，上为约值，**理论计算**。）FP 每放宽一档，合法新事件被误判重复的风险按联合 FP 上升（1e-6→0.0003%、1e-5→0.003%、1e-4→0.03%）。

HLL p=13→12：节省 2 MiB，标准误 1.15%→1.63%（**理论计算**），收益小，不建议牺牲。

## L. Pareto 优势方案（估算，非实测）

| 方案 | RSS 峰值(估) | 吞吐 | Ingest p99 | Query p99 | Recall@10 | HLL 误差 | Bloom FP |
|---|---:|---:|---:|---:|---:|---:|---:|
| A 当前 | 256.4 | 本地 449k/s 突发 | 未测 | 未测 | 1.00(本地) | 1.15% | 3e-6 |
| B 降并发 | 246 | 未测 | 未测 | 未测 | 同 A | 1.15% | 3e-6 |
| H 组合 | 234 | 未测 | 未测 | 未测 | 同 A | 1.15% | 3e-5 |

（RSS 为**设计估算**；吞吐/延迟除 A 的本地突发外均**未实测**。）

## M. 最终推荐配置（设计候选，需 Docker 验证）

- 实现 gzip JSON 数组流式解析（消除解压 `byte[]` 副本）；NDJSON 仍物化但限制体量。
- ingest 并发 4、SSE 16、query 并发 1（保持）。
- SpaceSaving C=256、HLL p=13 保持不变（本地召回已 1.00，不为此省内存）。
- Bloom 保持 3×30s（最短 60s），目标 FP 放宽到 1e-5（联合 3e-5，约省 11.6 MiB）。
- 队列 2048 不变；启动参数 `-Xmx184m`（或经实测后定），并显式传入，当前 jar 未自带 `-Xmx`。

## N. 牺牲与换取

- SSE 32→16：降低连接数上限，换取约 0.1 MiB 与更少调度。
- Bloom FP 1e-6→1e-5：合法新事件被误判重复风险从 0.0003% 升至 0.003%，换取约 11.6 MiB。
- 未降低 C/HLL p，未牺牲 Top-K/HLL 精度。
- 未降低 200,000 events/sec 目标。

## O. 仍然存在的风险

- Docker 未运行，256 MiB 与 RSS 峰值未实测；以上 RSS 均为设计估算。
- 200,000 events/sec 未实测；本地单线程突发 accepted/processed 约 44.9 万/s，但含 12.7 万 overloaded，不能代表多客户端稳态 20 万/s 达标。
- GZIP 流式解析尚未实现，当前 8 并发 gzip 最坏暂存约 20.4 MiB。
- 启动 jar 未内置 `-Xmx`，需显式传入。

## P. 最终 Maven 测试结果

```text
Tests run: 105, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## Q. Docker 256 MiB 实测结果

**未完成。** Docker Desktop 守护进程未运行（`npipe:////./pipe/dockerDesktopLinuxEngine` 不存在），本轮无法获得 Docker `memory.current`/`memory.peak`。

## R. 是否真正达到 200,000 events/sec

**未验证。** 本地实测（非 Docker、单线程客户端）突发吞吐 offered 约 51.5 万/s、accepted/processed 约 44.9 万/s，但不能据此宣称多客户端稳态 20 万/s 达标；需在 Docker 内做稳态与 15 分钟压测。

---

本阶段因 Docker 不可用，未能完成全部实测实验。已如实区分“理论/设计估算”与“本地实测”，未编造 Docker 或吞吐达标数据。