# FlowLens

FlowLens 是有界内存的实时流量分析服务，在最近 60 秒滑动窗口内统计热点 URL / user_id Top-K、近似去重用户数与响应字节 p95。

技术栈：Java 21、JDK HttpServer、Jackson Core、Maven、JUnit 5、Java 21 虚拟线程。

## 构建与测试

```bash
mvn clean test
mvn package
```

## 启动

推荐使用启动脚本（内置 JVM 内存参数与 NMT）：

```powershell
./run.ps1
```

或直接运行：

```bash
java -Xms64m -Xmx160m -XX:+UseG1GC -XX:MaxMetaspaceSize=64m -XX:ReservedCodeCacheSize=64m -XX:+UseCodeCacheFlushing -XX:MaxDirectMemorySize=16m -XX:NativeMemoryTracking=summary -Xss256k -XX:ActiveProcessorCount=4 -jar target/flowlens-0.1.0-SNAPSHOT.jar
```

环境变量：`FLOWLENS_HOST`（默认 127.0.0.1）、`FLOWLENS_PORT`（默认 8080）。

## Docker 运行

```bash
# 构建镜像
docker build -t flowlens:0.5.0-phase16 .

# 启动容器（容器内监听 0.0.0.0:8080）
docker run -d --name flowlens-smoke -p 8080:8080 --memory=256m --memory-swap=256m flowlens:0.5.0-phase16

# 健康检查
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz

# 停止并删除容器
docker stop flowlens-smoke
docker rm flowlens-smoke
```

## 示例

```bash
# 健康检查
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz

# 写入事件
curl -X POST http://127.0.0.1:8080/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '[{"event_id":"e1","ts":'$(date +%s%3N)',"user_id":"u1","url":"/a?x=1","bytes":100}]'

# gzip 写入
curl -X POST http://127.0.0.1:8080/api/v1/ingest \
  -H 'Content-Type: application/json' -H 'Content-Encoding: gzip' \
  --data-binary @batch.json.gz

# Top-K
curl 'http://127.0.0.1:8080/api/v1/topk?metric=url&k=10&window=60'

# Stats
curl 'http://127.0.0.1:8080/api/v1/stats?window=60'

# SSE
curl -N http://127.0.0.1:8080/api/v1/stream
```

原题接口路径 `/v1/ingest`、`/v1/topk`、`/v1/stats`、`/v1/topk/stream`、`/healthz` 均已支持，原 `/api/v1/*` 路径继续保留；healthz 增加 `memory_bytes`（Linux 下为进程 VmRSS）。完整 API 见 `API.md`，设计见 `DESIGN.md`，AI 交互见 `AI_LOG.md`。