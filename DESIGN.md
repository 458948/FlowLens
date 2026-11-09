# FlowLens 设计文档

当前阶段：核心算法、运行时编排、HTTP/SSE、流式解析与内存优化均已完成；Docker 256MiB 实测与 200,000 events/sec 验收尚未完成。

## 架构

- 单 JVM 进程，4 个 Shard，每个 Shard 一个单写线程 + 有界事件队列（容量 2048）。
- 事件按 event_id 的 128 位指纹路由，同一 event_id 进入同一 Shard。
- 每个 Shard 维护 128 个秒级桶（Ring Buffer）。
- HTTP：JDK HttpServer + Java 21 虚拟线程；ingest 并发 4、query 并发 1、SSE 最大连接 16。
- 全局唯一 QueryService / QueryWorkspace；全局唯一 SnapshotPublisher。

## 核心算法

- SpaceSaving：每 Shard 每桶 URL / user_id 两个草图，C=256，固定容量原生数组、开放寻址、最小堆。候选项保存 128 位指纹、原始 UTF-8 字节、长度、count、error。合并误差 E = N/C，返回 lower/upper 区间。
- HyperLogLog：自行实现，p=13，8192 寄存器，查询时逐位取最大值。
- LogHistogram：505 桶，整数位运算定位，桶内插值为估计值。
- Sliding Bloom Filter：3×30 秒时间片，每片 1,800,000 event_id，目标假阳性 1e-6，20 个哈希探测，128 位 SipHash 指纹。
- Ring Buffer + Watermark：128 个秒级桶，W = now - 60000ms。

## 内存预算（当前，设计估算）

- 固定数组：Bloom 74.05 + Top-K 52.25 + HLL 4.00 + Histogram 1.97 + QueryWorkspace 24.05 = 156.32 MiB。
- 队列/在途 RawEvent 约 4.4 MiB、ingest 流式暂存约 2.2 MiB、Shard/Runtime 对象约 0.5 MiB、GC 余量约 20 MiB。
- Heap 设计合计约 183.4 MiB。
- Native（Metaspace/CodeCache/Direct/线程栈/JVM）设计约 78 MiB。
- RSS 设计估算约 258 MiB，略高于 256 MiB，属于待 Docker `--memory=256m --memory-swap=256m` 实测确认的区间。

当前启动参数见 `run.ps1`：

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

以上均为设计估算，不是实测 RSS。

## 历史内存预算（早期设计值，非当前配置）

以下为早期阶段的历史估算，保留备查，不代表当前配置：

- Heap：Bloom 61.7、Top-K 52.5、HLL 4、Histogram 2、队列 3.5、Ingest 暂存 4、查询工作区 20、HTTP/SSE 2、控制结构 4、GC 余量 12，约 165.7 MiB。
- 历史 JVM 建议：`-Xms64m -Xmx184m -XX:MaxMetaspaceSize=32m -XX:ReservedCodeCacheSize=16m -XX:MaxDirectMemorySize=8m -Xss256k -XX:ActiveProcessorCount=4`。

当前以 `run.ps1` 的 `-Xmx200m` 等参数为准。

## 已修复问题与并发保证

### DedupAdmission 指纹竞争

`admit` 每次调用使用局部 `long[] fp`，不共享可变输出数组；同一组不可变 `hi/lo` 用于 Shard 路由、条带锁选择、Bloom 查询与标记。条带锁保证相同 event_id 并发只成功入队一次。顺序固定为：查重 → 入队 → 成功后写标记；入队失败不写标记，重试幂等。

### SpaceSaving 墓碑探测死循环

开放寻址表的 `observe/estimate/insertTable/removeTable` 探测循环均以 `table.length` 为上限；插入优先复用墓碑，空槽耗尽时抛异常或安全失败，不会无限循环。count/error 与 N/C 误差保证不变。

### SlidingBloomFilter 降级恢复

每个时间片有独立的 `saturated` 标志；`add` 先写位图再判饱和，保证已 accepted 事件必写入标记。`isDegraded` 返回“是否存在活动窗口内的饱和片”；片被轮换回收时重置 `saturated`，仅当所有饱和片都退出活动窗口后才恢复，不会在有效保留期内随意清空旧片、不引入假阴性。