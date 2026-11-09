# FlowLens 最终验收报告 V2

## 1. 修改了哪些文件

仅修改压测工具，未修改业务代码：

- `tools/loadtest/LoadTest.java`：将 ingest 压测从“按事件总数结束”改为“按 wall-clock duration 结束”。此前 `--duration 30` 实际会发送 `rate*duration` 个事件，若服务端跟不上，测试会超过 30 秒；本次修正为严格 30 秒墙钟窗口，并在窗口内测量 offered/accepted_rate。

未修改任何算法、JVM、Docker、HTTP/SSE 生产代码。

## 2. 修改前后吞吐

- 修改前（口径错误）：`--rate 200000 --duration 30` 实际运行约 81.64 秒，offered/accepted=6,000,000，报告 offered_rate 73,493.3/s。
- 修改后（30 秒墙钟）：`--rate 200000 --duration 30`，offered=2,158,000，accepted=2,158,000，offered_rate=71,852.5/s，accepted_rate=71,852.5/s。

结论：真实稳定吞吐约 71.9k events/sec，未达到 200k/s。

## 3. 修改前后内存峰值

- 修改前 200k 尝试：memory.peak 触及 256 MiB。
- 修改后 30 秒 200k 尝试：memory.peak=268,435,456 B = 256 MiB，memory.current 结束采样约 248,987,648 B ≈ 237.5 MiB。

仍会触及硬上限，但未发生 OOM。

## 4. 查询 p99

持续 ingest 下，各 1000 次请求：

- Top-K p99 = 28.137 ms
- Stats p99 = 28.231 ms

两项均 < 50ms，达标。

## 5. Maven 测试结果

`mvn test`：138 tests，0 failures，0 errors，0 skipped，BUILD SUCCESS。

## 6. 原题各项要求是否通过

- 六个 HTTP/SSE 接口：通过。
- 重复 event_id 去重：通过。
- 非法事件拒绝：通过。
- Top-K p99 < 50ms：通过。
- Stats p99 < 50ms：通过。
- Docker 256 MiB 内无 OOM：通过（OOMKilled=false，oom=0）。
- 200,000 events/sec：未通过，实际约 71.9k events/sec。
- 200k/s 下内存安全余量：未通过，memory.peak 触及 256 MiB。

## 7. 未通过的项目及客观原因

- 200k/s 未达标：30 秒墙钟内 offered 2,158,000 个事件，约 71.9k/s。服务端单请求延迟 p50 约 55ms；4 并发下约 72 请求/秒 × 1000 事件/请求 ≈ 72k events/sec，与实测一致。瓶颈是 ingest 热路径的每请求延迟，不是客户端饱和（客户端 CPU 约 30%）。
- 内存峰值触及 256 MiB：未 OOM，但没有留下安全余量；200k/s 所需吞吐和 256MiB 约束在当前实现下无法同时满足。

## 8. 最终结论

当前交付版本是功能完整、查询 p99 达标、接口正确、256 MiB 下无 OOM 的版本，但 200,000 events/sec 未达标，实际稳定吞吐约 71.9k events/sec。不能宣告原题全部通过。