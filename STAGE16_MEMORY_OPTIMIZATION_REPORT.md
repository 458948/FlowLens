# FlowLens 第十六阶段：内存余量优化与稳定性验收报告

## A. 第十五阶段内存问题复核

第十五阶段查询 p99 已达标，但最终 `memory.current/peak` 为 252,780,544 B ≈ 241.1 MiB，超过 240 MiB 阈值；anon 约 227 MiB，file 约 9.4 MiB。未发生 OOM，但余量不足。

根因不是单一对象，而是：

1. G1 在 Xmx160 下 Heap committed 达到 160 MiB；20k 写入后 Heap used 约 148.7 MiB，几乎打满。
2. Bloom 位数组是最大固定 Heap 对象：3 片 × 4 Shard 的 `AtomicLongArray` 底层 `long[]` 合计约 74 MiB（class histogram 中 `[J` 约 77.8 MiB）。
3. Bucket 中两个 SpaceSaving 的固定步长 arena 在 128 桶稳态下是第二大头：URL `256×256` + USER `256×64`，512 桶合计约 40 MiB，且多数名称远短于 maxKeyBytes，存在大量浪费。
4. G1 GC 本身 NMT committed 约 44 MiB；NMT 总 committed 约 235 MiB，加上 cgroup file 页缓存，逼近 256 MiB。

## B. 内存组成实测（诊断镜像，G1，旧代码 20k 后）

使用 `flowlens:0.4.0-phase16-diag` 采集 `jcmd`：

- `GC.heap_info`：G1 heap total 160 MiB，used 148.7 MiB（128 桶满载后）。
- `VM.native_memory summary`：总 committed 235 MB。
  - Java Heap committed 160 MB。
  - GC committed 44 MB。
  - Shared class space committed 13 MB。
  - Code committed 7 MB。
  - Class/Metaspace committed 5 MB。
  - Thread committed 3 MB。
  - Internal/Symbol 约 2 MB。
- cgroup：anon 约 227.7 MiB，file 约 35.9 MiB，slab 约 1.4 MiB。

这些是诊断镜像 JDK 口径，生产 JRE 的 file 页缓存较小（约 9.4 MiB），但 heap/native 组成一致。

## C. 主要内存消耗结构排名

1. Bloom 位数组：约 74 MiB（禁止修改参数，未动）。
2. Bucket SpaceSaving arena（固定步长）：约 40 MiB（本次优化目标）。
3. G1 GC 结构：约 44 MiB。
4. QueryWorkspace 固定数组：约 5.6 MiB（第十五阶段已优化）。
5. HLL 寄存器：4.2 MiB；Histogram：2.1 MiB。
6. 队列、RawEvent 在途、HTTP 暂存：动态、有界。

## D. 具体优化措施

修改 `SpaceSaving.java`，把名称存储从固定步长 `capacity × maxKeyBytes` 的 arena 改为紧凑、可增长的字节 arena + 每候选 offset 数组：

- 新增 `int[] nameOffset`，新增 `nameArenaLength()`。
- 新增候选时按实际 `len` 写入 arena，不再占用完整 maxKeyBytes。
- 被淘汰候选若新名称长度 ≤ 旧名称长度，原位覆盖；否则追加并在必要时压缩回收死字节。
- `ensureArenaCapacity` 在写满前压缩活候选；压缩后 arena 长度仅略大于活名称总字节数，上限仍不超过 `capacity × maxKeyBytes`。
- `bytesEqual` 和 `nameOffset` 改为使用 offset 表，完整 key 校验不变。
- count/error/lower/upper、C=256、跨 Shard 合并误差界均未改变。

未修改 Bloom 参数、HLL p、Histogram bins、Shard/Ring/Watermark 语义。

## E. 修改文件清单

- `src/main/java/io/flowlens/sketch/SpaceSaving.java`
- `src/test/java/io/flowlens/SpaceSavingTest.java`（新增 1 个紧凑存储与淘汰测试）
- 本报告 `STAGE16_MEMORY_OPTIMIZATION_REPORT.md`
- `AI_LOG.md` 待同步

新增诊断/生产镜像：

- `flowlens:0.4.0-phase16-diag`（诊断镜像，JDK，独立标签）
- `flowlens:0.5.0-phase16`（生产镜像，JRE，G1+Xmx160，独立标签）

## F. 优化前后内存对比

优化前（第十五阶段，G1+Xmx160，20k 后 + 查询）：

- memory.current/peak 约 241.1 MiB。

优化后（本阶段，G1+Xmx160，20k/s × 60s + 2.4M events + 四组查询）：

- 最终 current 234,844,160 B ≈ 224.0 MiB。
- 峰值 240,283,648 B ≈ 229.2 MiB。

降低约 12 MiB 峰值，达到 ≤ 235 MiB 目标。

## G. GC 对照实验

在第十五阶段代码上做 SerialGC 与 G1GC 对照（Xmx160，256 MiB，20k 后）：

| 指标 | G1GC | SerialGC |
|---|---|---|
| NMT total committed | 235 MB | 191 MB |
| Java Heap committed | 160 MB | 160 MB |
| GC committed | 44 MB | 1 MB |
| Heap used（20k 后） | 148.7 MiB | 约 145.8 MiB |
| 查询 Top-K p99 | 约 29–30 ms | 28.0 ms |
| 查询 Stats p99 | 约 28–30 ms | 27.9 ms |

结论：SerialGC 的 NMT GC 结构更小，但 cgroup anon 在两种 GC 下都约 227 MiB，差异不大；G1 查询 p99 已达标且当前生产参数为 G1，因此本阶段仍保留 G1，未切换 GC。

## H. Maven 测试真实结果

`mvn clean test`

`Tests run: 138, Failures: 0, Errors: 0, Skipped: 0`

`BUILD SUCCESS`

## I. Docker 镜像构建结果

构建成功：

- `flowlens:0.4.0-phase16-diag`
- `flowlens:0.5.0-phase16`

历史镜像与失败容器全部保留。

## J. 20k/s 内存实测（最终优化代码，G1+Xmx160）

容器 `flowlens-phase16-g1final`，`--memory=256m --memory-swap=256m`：

- 20k events/sec × 60s：offered=1,200,000，accepted=1,199,500，无 OOM。
- 后续 20k/s × 120s：offered=2,400,000，accepted=2,400,000，无 OOM。
- 最终：
  - memory.current = 234,844,160 B ≈ 224.0 MiB
  - memory.peak = 240,283,648 B ≈ 229.2 MiB
  - memory.max = 256 MiB
  - anon = 228,679,680 B ≈ 218.1 MiB
  - file = 40,960 B ≈ 0.04 MiB
  - slab ≈ 0.98 MiB

## K. 四组查询 p99（最终优化代码）

每组 10,000 请求：

| 场景 | 成功 | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 空闲 Top-K | 9995 | 3.446 | 26.843 | 28.572 | 32.150 |
| 空闲 Stats | 9987 | 2.529 | 26.210 | 28.012 | 34.318 |
| Ingest 中 Top-K | 9960 | 3.659 | 27.226 | 29.919 | 45.274 |
| Ingest 中 Stats | 9979 | 3.132 | 26.991 | 29.188 | 42.035 |

全部 p99 < 50 ms。

## L. OOM 和 cgroup 事件

- `oom=0`，`oom_kill=0`，`oom_group_kill=0`。
- `OOMKilled=false`。
- 容器退出码 143（SIGTERM 受控退出）。

## M. 未解决风险

- file 页缓存随运行和镜像波动；本次生产 JRE 下极低（0.04 MiB），但历史运行曾出现 9–41 MiB，后续仍要持续监控。
- 未执行 30k/50k/100k/150k/200k 和 15 分钟稳态测试。
- Bloom 74 MiB 是最大固定项，受约束不能缩减。
- G1 在 heap 接近 Xmx 时仍会出现 Full/Mixed GC，查询 p99 虽达标但需在更高吞吐下复测。

## N. 下一阶段建议

- 用 `flowlens:0.5.0-phase16` 进入 30k → 200k 分级吞吐测试。
- 每秒监控 memory.current/peak/stat/events，240 MiB 立即停止。
- 200k 通过后执行 15 分钟持续负载。
- 保留 G1GC，除非更高档位出现内存紧张且 SerialGC 复测仍满足查询 p99 才考虑切换。