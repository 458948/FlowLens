# AI_LOG

记录关键 AI 交互、采纳或拒绝的理由。

## 2026-11-05

- 第一、二、三轮架构设计：完成需求分析、算法设计、内存预算与并发模型，经过三轮人工评审修订。
- 第三轮修订确定：去重采用 3×30 秒 Sliding Bloom Filter；Top-K 采用固定容量原生数组 + 开放寻址 + 最小堆；Shard 数 4；Watermark 封闭与查询稳定性边界明确。
- 最终开发方案确定后，进入项目实施。
## 2026-11-05 项目实施（核心算法 + 测试）

- 完成 Maven 项目初始化：pom.xml、.gitignore、README.md、DESIGN.md、AI_LOG.md。
- 实现核心算法：SipHash128、SpaceSaving、HyperLogLog、LogHistogram、SlidingBloomFilter、RingBuffer、Watermark、TopKMerger。
- 实现输入与准入辅助：RawEvent、UrlNormalizer、GzipLimiter、EventParser、DedupAdmission。
- mvn clean test 全绿：Tests run: 29, Failures: 0, Errors: 0, Skipped: 0。
- 修复问题：HyperLogLog 索引与余数使用同一段哈希位导致偏差（改为低位索引 + 高位余数）；TopKMerger 比较器方向；Bloom 轮换改为按槽位索引回收以保证 60–90s 保留；RingBuffer 查询对空桶按空处理、越界才报错；JSON 语法错误包装为 ParseException。
- 尚未完成：HTTP 服务层、SSE、4 Shard 单写线程与有界队列的完整编排、Main 入口、Dockerfile/run.sh、200k events/s 与 Docker 256m 实测。

## 2026-11-06 核心算法审查问题复核

- 实际读取 DedupAdmission、SpaceSaving、SlidingBloomFilter，确认三个审查问题在当前代码中均已修复：局部指纹数组、有界探测与墓碑复用、按片饱和的动态降级与恢复。
- 未对生产代码做盲目重改；补充 1 个针对性测试：`SlidingBloomFilterTest.duplicateInRetentionStillDedupedAcrossDegradationAndRecovery`。
- `mvn clean test`：118 个测试全部通过。

## 2026-11-06 Docker 容器化部署

- 创建 Dockerfile（多阶段：maven:3.8.8-eclipse-temurin-21 构建 + eclipse-temurin:21-jre-jammy 运行，非 root 用户 flowlens，SIGTERM 优雅关闭）与 .dockerignore。
- `mvn clean package`：118 测试通过，产出 shaded fat jar。
- `docker build -t flowlens:0.1.0 .` 成功，镜像大小 407MB。
- `docker run -d --name flowlens-smoke -p 8080:8080 flowlens:0.1.0` 启动成功，healthz/readyz/ingest 均正常。
- `docker stop flowlens-smoke` 优雅关闭：`drained=true unprocessed=0`，随后删除 smoke 容器。


## 2026-11-06 第十二阶段：内存结构优化与原题兼容修复

- 新增 TopKMerger URL/user arena 延迟分配，QueryWorkspace 峰值约减少 18.7–19.6 MiB；新增 TopKMergerTest 7 项。
- 增加原题 API 别名：`/v1/ingest`、`/v1/topk`、`/v1/stats`、`/v1/topk/stream`，healthz 增加 `memory_bytes`（VmRSS）；新增 FlowLensHttpServerTest 4 项。
- 构建镜像 `flowlens:0.2.0-phase12`，保留旧镜像与失败现场。
- JVM 实验：Xmx200m 在 5k/s OOM；Xmx168m 在 5k/s 到 255.9MiB；Xmx160m 通过 5k/10k/20k/s 各 30s，peak 229.8MiB，无 OOM。
- `mvn clean test`：129 个测试全部通过，BUILD SUCCESS。
- 修复压测工具包路径与查询模式，新增 `--mode topk|stats|sse`。
- 详细数据见 `STAGE12_MEMORY_REPORT.md`。


## 2026-11-06 第十三阶段：生产 JVM 参数切换与受控性能验收

- 将生产 Dockerfile/run.ps1 从 `-Xmx200m` 切换为 `-Xmx160m`，并显式加入 `-XX:+UseG1GC`；发现切换前实际为自动选择 SerialGC。
- `mvn clean test`：129 个测试全部通过。
- 构建镜像 `flowlens:0.3.0-phase13`（SerialGC 自动）与 `flowlens:0.3.0-phase13-g1`（显式 G1），保留旧标签。
- G1 + Xmx160m：5k/10k/20k events/sec 各 30s 通过；20k 后 memory.current 244.1 MiB、peak 245.2–245.5 MiB，超过 240 MiB 安全阈值，停止更高档位。
- 查询 p99：Top-K 52 ms、Stats 52 ms，未达到 <50ms；定位为 240 桶快照/合并成本，提出未实施优化方案。
- 优雅关闭：`docker stop --timeout 30` 得到 143 + 完整 shutdown 日志，drained/runtimeDrained 均 true。
- 详细数据见 `STAGE13_FINAL_REPORT.md`。


## 2026-11-09 第十四阶段：GC 对照实验与内存预算复核

- 只读复核确认第十二阶段“G1”数据实际为自动 SerialGC，生成 STAGE14_GC_COMPARISON_REPORT.md。
- 批准后执行显式 SerialGC vs G1GC 对照（Xmx160m、256MiB、batch500/concurrency8、每档30s）。
- SerialGC：20k peak 216.2 MiB；GC 总 pause 约 964 ms，Full GC 最大 43 ms。
- G1GC：20k peak 231.2 MiB；GC 总 pause 约 216 ms，Full Compaction 最大 41.6 ms。
- 查询 p99：Serial Top-K 55ms/Stats 59ms；G1 Top-K 53ms/Stats 52ms，均未 <50ms。
- 结论：内存余量 Serial 更大；查询 p99 主因是 240 桶快照/合并，不是 GC 选择。
- 未修改生产代码、Dockerfile、run.ps1；保留全部历史容器与镜像。


## 2026-11-09 第十五阶段：查询路径性能优化与验收

- 优化 Top-K：删除 BucketSnapshot 中间复制，在桶 StampedLock 读锁内直接合并 SpaceSaving。
- 优化 Stats：新增 QueryWorkspace.mergeStats 读锁内复制，移除未用 BucketSnapshot。
- 优化 RingBuffer.bucketFor：线性扫描 128 Slot 改为 O(1) 索引。
- 新增 QueryOptimizationTest 8 项；`mvn clean test`：137 tests，0 failures/errors/skipped，BUILD SUCCESS。
- 新增 `tools/loadtest/query_latency.py`（持久连接/每请求新建连接探针）。
- Docker 构建：`flowlens:0.4.0-phase15`、`flowlens:0.4.0-phase15-final`、`flowlens:0.4.0-phase15-final2`。
- G1+Xmx160m 下四组查询 p99：空闲 Top-K 30.6ms、空闲 Stats 30.5ms、ingest 中 Top-K 29.9ms、ingest 中 Stats 28.7ms，均 <50ms。
- 最终 memory.current 241.1 MiB（略超 240 MiB 阈值，未 OOM），见 STAGE15_QUERY_OPTIMIZATION_REPORT.md。


## 2026-11-09 第十六阶段：内存余量优化与稳定性验收

- 根因：20k 后 G1 Heap used 148.7MiB；最大对象为 Bloom 74MiB，其次 Bucket SpaceSaving 固定步长 arena 约40MiB。
- 优化：SpaceSaving 名称存储改为紧凑可变长 arena + offset，淘汰时可原位复用/压缩回收，C=256 与误差语义不变。
- `mvn clean test`：138 tests，0 failures/errors/skipped，BUILD SUCCESS。
- 构建镜像：`flowlens:0.4.0-phase16-diag`、`flowlens:0.5.0-phase16`。
- G1+Xmx160 下 20k/s 写入与四组查询后：memory.peak 229.2MiB，memory.current 224.0MiB，oom=0/oom_kill=0。
- 四组查询 p99：空闲 Top-K 28.6ms、空闲 Stats 28.0ms、ingest 中 Top-K 29.9ms、ingest 中 Stats 29.2ms。
- 详细数据见 `STAGE16_MEMORY_OPTIMIZATION_REPORT.md`。


## 2026-11-09 第十七阶段：最低要求快速验收与最终交付

- `mvn test`：138/138 通过。
- 复用镜像 `flowlens:0.5.0-phase16`，200k/s 尝试实际 offered/accepted 均 73,493 events/sec，未达标。
- 未 OOM，但 memory.peak 触及 256MiB 上限；接口与查询 p99 均达标。
- 生成 `FINAL_ACCEPTANCE_REPORT.md`，明确部分达标、200k/s 未达标。


## 2026-11-09 第十八阶段：最后一轮最小性能优化

- 核查发现 LoadTest ingest 模式按事件总数而非墙钟时间结束，导致 30 秒测试实际运行约 81.6 秒。
- 修复 LoadTest 为严格 wall-clock duration；未修改业务代码。
- 30 秒墙钟 200k/s 尝试：offered/accepted 2,158,000，实际吞吐 71,852.5 events/sec，未达标。
- memory.peak 触及 256MiB，OOMKilled=false；查询 p99 约 28ms 达标；接口验收通过。
- 生成 `FINAL_ACCEPTANCE_REPORT_V2.md`，明确 200k/s 未达标。


## 2026-11-09 第十九阶段：最终项目整理与交付

- 修正 README 当前 JVM/镜像运行参数，保留历史报告。
- `mvn clean test`：138 tests，0 failures/errors/skipped，BUILD SUCCESS。
- 轻量 Docker 启动/healthz/readyz 检查通过。
- 生成 `FINAL_DELIVERY_REPORT.md`，明确 200k/s 未达标（实际约 71,852 events/sec）且 256MiB 峰值触及上限。