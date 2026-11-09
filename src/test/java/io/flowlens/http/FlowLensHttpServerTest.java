package io.flowlens.http;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.flowlens.dedup.SlidingBloomFilter;
import io.flowlens.ingest.RawEvent;
import io.flowlens.runtime.FlowLensRuntime;
import io.flowlens.runtime.IngestOutcome;
import io.flowlens.runtime.Shard;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowLensHttpServerTest {

    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    private static FlowLensHttpServer start(FlowLensRuntime runtime) throws IOException {
        return start(runtime, 120L);
    }

    private static FlowLensHttpServer start(FlowLensRuntime runtime, long intervalMs) throws IOException {
        FlowLensHttpServer server = new FlowLensHttpServer(runtime, "127.0.0.1", 0, intervalMs);
        server.start();
        return server;
    }

    private static String base(FlowLensHttpServer server) {
        return "http://127.0.0.1:" + server.port();
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(data);
        }
        return bos.toByteArray();
    }

    private static String eventJson(String id, long ts) {
        return "{\"event_id\":\"" + id + "\",\"ts\":" + ts + ",\"user_id\":\"u\",\"url\":\"/u\",\"bytes\":1000}";
    }

    private static RawEvent rawEvent(String id, long ts) {
        return new RawEvent(id.getBytes(StandardCharsets.UTF_8), "u".getBytes(StandardCharsets.UTF_8),
                "/u".getBytes(StandardCharsets.UTF_8), ts, 1000L);
    }

    private static HttpRequest post(String url, byte[] body, String contentType, String encoding) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (encoding != null) {
            b.header("Content-Encoding", encoding);
        }
        return b.build();
    }

    private static Map<String, Object> parse(byte[] body) throws IOException {
        try (JsonParser p = new JsonFactory().createParser(body)) {
            p.nextToken();
            return (Map<String, Object>) parseValue(p);
        }
    }

    private static Object parseValue(JsonParser p) throws IOException {
        JsonToken t = p.currentToken();
        switch (t) {
            case START_OBJECT -> {
                Map<String, Object> m = new LinkedHashMap<>();
                while (p.nextToken() != JsonToken.END_OBJECT) {
                    String key = p.currentName();
                    p.nextToken();
                    m.put(key, parseValue(p));
                }
                return m;
            }
            case START_ARRAY -> {
                List<Object> list = new ArrayList<>();
                while (p.nextToken() != JsonToken.END_ARRAY) {
                    list.add(parseValue(p));
                }
                return list;
            }
            case VALUE_STRING -> {
                return p.getText();
            }
            case VALUE_NUMBER_INT -> {
                return p.getLongValue();
            }
            case VALUE_NUMBER_FLOAT -> {
                return p.getDoubleValue();
            }
            case VALUE_TRUE -> {
                return Boolean.TRUE;
            }
            case VALUE_FALSE -> {
                return Boolean.FALSE;
            }
            case VALUE_NULL -> {
                return null;
            }
            default -> throw new IOException("unexpected token " + t);
        }
    }

    private static long asLong(Map<String, Object> m, String key) {
        return ((Number) m.get(key)).longValue();
    }

    // --- Ingest ---

    @Test
    void jsonArrayIngestAccepted() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            String body = "[" + eventJson("e1", ts) + "," + eventJson("e2", ts) + "]";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, resp.statusCode());
            Map<String, Object> json = parse(resp.body());
            assertEquals(2L, asLong(json, "accepted"));
            assertEquals(0L, asLong(json, "deduped"));
        } finally {
            server.close();
        }
    }

    @Test
    void ndjsonIngestAccepted() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            String body = eventJson("n1", ts) + "\n" + eventJson("n2", ts) + "\n";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/x-ndjson", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, resp.statusCode());
            assertEquals(2L, asLong(parse(resp.body()), "accepted"));
        } finally {
            server.close();
        }
    }

    @Test
    void gzipIngestAccepted() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            String body = "[" + eventJson("g1", ts) + "]";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    gzip(body.getBytes(StandardCharsets.UTF_8)), "application/json", "gzip"), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, resp.statusCode());
            assertEquals(1L, asLong(parse(resp.body()), "accepted"));
        } finally {
            server.close();
        }
    }

    @Test
    void invalidGzipReturns400() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    "not-gzip".getBytes(StandardCharsets.UTF_8), "application/json", "gzip"), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(400, resp.statusCode());
        } finally {
            server.close();
        }
    }

    @Test
    void syntaxErrorRejectsWholeBatch() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    "[{\"event_id\":\"e1\",".getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(400, resp.statusCode());
        } finally {
            server.close();
        }
    }

    @Test
    void oversizedDecompressedBodyReturns413() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            StringBuilder big = new StringBuilder("[");
            long ts = System.currentTimeMillis() - 1_000;
            for (int i = 0; i < 1000; i++) {
                if (i > 0) {
                    big.append(',');
                }
                big.append("{\"event_id\":\"m").append(i)
                   .append("\",\"ts\":").append(ts)
                   .append(",\"user_id\":\"").append("u".repeat(64))
                   .append("\",\"url\":\"").append("x".repeat(1024))
                   .append("\",\"bytes\":1000}");
            }
            big.append(']');
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    gzip(big.toString().getBytes(StandardCharsets.UTF_8)), "application/json", "gzip"), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(413, resp.statusCode());
        } finally {
            server.close();
        }
    }

    @Test
    void tooManyEventsReturns413() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < 1001; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(eventJson("m" + i, ts));
            }
            sb.append(']');
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    sb.toString().getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(413, resp.statusCode());
        } finally {
            server.close();
        }
    }

    @Test
    void duplicateEventIdIsDeduped() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            String body = "[" + eventJson("dup", ts) + "," + eventJson("dup", ts) + "]";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, resp.statusCode());
            Map<String, Object> json = parse(resp.body());
            assertEquals(1L, asLong(json, "accepted"));
            assertEquals(1L, asLong(json, "deduped"));
        } finally {
            server.close();
        }
    }

    @Test
    void partialSuccessCountsAreCorrect() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            String valid = eventJson("p1", ts);
            String dup = eventJson("p1", ts);
            String bad = "{\"event_id\":\"p2\",\"ts\":" + ts + ",\"user_id\":\"u\",\"url\":\"/u\",\"bytes\":-1}";
            String body = "[" + valid + "," + dup + "," + bad + "]";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, resp.statusCode());
            Map<String, Object> json = parse(resp.body());
            assertEquals(1L, asLong(json, "accepted"));
            assertEquals(1L, asLong(json, "deduped"));
            assertEquals(1L, asLong(json, "rejected"));
        } finally {
            server.close();
        }
    }

    @Test
    void unsupportedContentTypeReturns415() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    "{}".getBytes(StandardCharsets.UTF_8), "text/plain", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(415, resp.statusCode());
        } finally {
            server.close();
        }
    }

    @Test
    void realQueueOverloadReturns503AndRetrySucceeds() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        FlowLensRuntime runtime = new FlowLensRuntime(
                System::currentTimeMillis,
                i -> new SlidingBloomFilter(),
                (i, ring) -> new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
                    try {
                        block.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
                }));
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            for (int i = 0; i < 9000; i++) {
                runtime.ingest(rawEvent("fill-" + i, ts));
            }
            String body = "[" + eventJson("overload-probe", ts) + "]";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(503, resp.statusCode());
            assertTrue((Boolean) parse(resp.body()).get("overloaded"));

            block.countDown();
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                long proc = 0;
                long acc = 0;
                for (Shard s : runtime.shards()) {
                    proc += s.processed();
                    acc += s.accepted();
                }
                if (proc >= acc && acc > 0) {
                    break;
                }
                Thread.sleep(10);
            }

            HttpResponse<byte[]> retry = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, retry.statusCode());
            assertEquals(1L, asLong(parse(retry.body()), "accepted"));
        } finally {
            block.countDown();
            server.close();
        }
    }

    // --- Query ---

    @Test
    void urlAndUserTopKAndStats() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < 20; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append("{\"event_id\":\"q" + i + "\",\"ts\":" + ts + ",\"user_id\":\"user-hot\",\"url\":\"url-hot\",\"bytes\":1000}");
            }
            sb.append(']');
            HttpResponse<byte[]> ingest = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    sb.toString().getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, ingest.statusCode());

            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                long proc = 0;
                for (Shard s : runtime.shards()) {
                    proc += s.processed();
                }
                if (proc >= 20) {
                    break;
                }
                Thread.sleep(10);
            }

            HttpResponse<byte[]> topk = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/topk?metric=url&k=5&window=60")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, topk.statusCode());
            Map<String, Object> top = parse(topk.body());
            assertFalse(((List<?>) top.get("items")).isEmpty());
            Map<String, Object> first = (Map<String, Object>) ((List<?>) top.get("items")).get(0);
            assertEquals("url-hot", first.get("name"));
            assertNotNull(first.get("estimated_count"));

            HttpResponse<byte[]> stats = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stats?window=60")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, stats.statusCode());
            Map<String, Object> stat = parse(stats.body());
            assertTrue(((Number) stat.get("approx_distinct_users")).doubleValue() >= 1);
            assertTrue(((Number) stat.get("approx_p95_bytes")).doubleValue() >= 0);
        } finally {
            server.close();
        }
    }

    @Test
    void invalidTopKParamsRejected() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            assertEquals(400, CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/topk?k=0")).GET().build(), HttpResponse.BodyHandlers.ofByteArray()).statusCode());
            assertEquals(400, CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/topk?k=101")).GET().build(), HttpResponse.BodyHandlers.ofByteArray()).statusCode());
            assertEquals(400, CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/topk?window=0")).GET().build(), HttpResponse.BodyHandlers.ofByteArray()).statusCode());
            assertEquals(400, CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stats?window=61")).GET().build(), HttpResponse.BodyHandlers.ofByteArray()).statusCode());
        } finally {
            server.close();
        }
    }

    @Test
    void queryConcurrencyLimitReturns429() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            server.querySemaphore.acquire();
            HttpResponse<byte[]> resp = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/topk")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(429, resp.statusCode());
            assertNotNull(resp.headers().firstValue("Retry-After").orElse(null));
        } finally {
            server.querySemaphore.release();
            server.close();
        }
    }

    // --- SSE ---

    @Test
    void sseHeadersAndSnapshot() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime, 120L);
        try {
            HttpResponse<InputStream> resp = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            assertEquals(200, resp.statusCode());
            assertEquals("text/event-stream; charset=utf-8", resp.headers().firstValue("Content-Type").orElse(""));
            assertEquals("no-cache", resp.headers().firstValue("Cache-Control").orElse(""));

            BufferedReader reader = new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8));
            assertEquals("retry: 3000", readLine(reader, 5_000).trim());

            String data = readUntil(reader, line -> line.startsWith("data:"), 10_000);
            assertNotNull(data);
            String json = data.substring("data:".length()).trim();
            Map<String, Object> snapshot = parse(json.getBytes(StandardCharsets.UTF_8));
            assertNotNull(snapshot.get("effective_at"));
            assertNotNull(snapshot.get("sequence"));
            assertNotNull(snapshot.get("url_top"));
            assertNotNull(snapshot.get("user_top"));
        } finally {
            server.close();
        }
    }

    @Test
    void sseHeartbeatIsSent() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime, 2000L);
        try {
            HttpResponse<InputStream> resp = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            BufferedReader reader = new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8));
            String heartbeat = readUntil(reader, line -> line.trim().equals(": keep-alive"), 10_000);
            assertNotNull(heartbeat);
        } finally {
            server.close();
        }
    }

    @Test
    void sseConnectionLimitIsEnforced() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            for (int i = 0; i < FlowLensHttpServer.SSE_MAX_CONNECTIONS; i++) {
                server.sseSemaphore.acquire();
            }
            HttpResponse<byte[]> resp = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stream")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(503, resp.statusCode());
        } finally {
            server.sseSemaphore.release(FlowLensHttpServer.SSE_MAX_CONNECTIONS);
            server.close();
        }
    }

    @Test
    void sseDisconnectReleasesPermit() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime, 120L);
        try {
            HttpResponse<InputStream> resp = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            resp.body().close();
            long deadline = System.currentTimeMillis() + 5_000;
            while (server.sseSemaphore.availablePermits() < FlowLensHttpServer.SSE_MAX_CONNECTIONS && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(FlowLensHttpServer.SSE_MAX_CONNECTIONS, server.sseSemaphore.availablePermits());
        } finally {
            server.close();
        }
    }

    @Test
    void serverShutdownClosesSseConnections() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime, 120L);
        HttpResponse<InputStream> resp = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
        server.close();

        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> future = reader.submit(() -> {
                InputStream in = resp.body();
                try {
                    while (in.read() != -1) {
                        // drain buffered SSE bytes until EOF
                    }
                } catch (IOException e) {
                    // Server closed the connection; treat as stream ended.
                }
                return true;
            });
            assertTrue(future.get(5, TimeUnit.SECONDS));
        } finally {
            reader.shutdownNow();
        }
    }

    @Test
    void twoSseClientsShareOneSnapshotComputation() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime, 150L);
        try {
            HttpResponse<InputStream> a = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            HttpResponse<InputStream> b = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            BufferedReader ra = new BufferedReader(new InputStreamReader(a.body(), StandardCharsets.UTF_8));
            BufferedReader rb = new BufferedReader(new InputStreamReader(b.body(), StandardCharsets.UTF_8));
            readUntil(ra, line -> line.startsWith("retry:"), 5_000);
            readUntil(rb, line -> line.startsWith("retry:"), 5_000);

            java.util.Set<Long> seqA = new java.util.HashSet<>();
            java.util.Set<Long> seqB = new java.util.HashSet<>();
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline && (seqA.size() < 2 || seqB.size() < 2)) {
                String da = readUntil(ra, line -> line.startsWith("data:"), 2_000);
                String db = readUntil(rb, line -> line.startsWith("data:"), 2_000);
                if (da != null) {
                    seqA.add(asLong(parse(da.substring(5).trim().getBytes(StandardCharsets.UTF_8)), "sequence"));
                }
                if (db != null) {
                    seqB.add(asLong(parse(db.substring(5).trim().getBytes(StandardCharsets.UTF_8)), "sequence"));
                }
            }
            assertFalse(seqA.isEmpty());
            assertFalse(seqB.isEmpty());
            seqA.retainAll(seqB);
            assertFalse(seqA.isEmpty(), "clients must share at least one snapshot sequence");
        } finally {
            server.close();
        }
    }

    @Test
    void slowClientQueueIsBounded() throws InterruptedException {
        SseClient client = new SseClient(1);
        Snapshot s = new Snapshot(0L, 60_000L, 1L, List.of(), List.of(), 0.0, 0.0);
        client.enqueue(s);
        client.enqueue(s);
        assertEquals(1, client.consecutiveDrops());
        assertNotNull(client.poll(0));
        assertNull(client.poll(0));
    }

    // --- Health ---

    @Test
    void healthAndReadyReturn200() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            HttpResponse<byte[]> health = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/healthz")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, health.statusCode());
            assertTrue((Boolean) parse(health.body()).get("ok"));

            HttpResponse<byte[]> ready = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/readyz")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, ready.statusCode());
            assertTrue((Boolean) parse(ready.body()).get("ready"));
        } finally {
            server.close();
        }
    }

    @Test
    void readyzReturns503WhenShardDegraded() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime(
                System::currentTimeMillis,
                i -> new SlidingBloomFilter(),
                (i, ring) -> new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
                    if ("bad".equals(new String(e.eventId, StandardCharsets.UTF_8))) {
                        throw new RuntimeException("boom");
                    }
                    r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
                }));
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            String body = "[" + eventJson("bad", ts) + "]";
            CLIENT.send(post(base(server) + "/api/v1/ingest", body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());

            long deadline = System.currentTimeMillis() + 5_000;
            while (!runtime.isDegraded() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertTrue(runtime.isDegraded());

            HttpResponse<byte[]> ready = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/readyz")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(503, ready.statusCode());
        } finally {
            server.close();
        }
    }

    @Test
    void readyzReturns503WhenRuntimeClosed() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            runtime.close();
            HttpResponse<byte[]> ready = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/readyz")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(503, ready.statusCode());
        } finally {
            server.close();
        }
    }

    private static void awaitTrue(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean());
    }

    private static String readLine(BufferedReader reader, long timeoutMs) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> future = executor.submit(reader::readLine);
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private static String readUntil(BufferedReader reader, java.util.function.Predicate<String> predicate, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            String line = readLine(reader, Math.max(1, deadline - System.currentTimeMillis()));
            if (line != null && predicate.test(line)) {
                return line;
            }
            if (line == null) {
                break;
            }
        }
        return null;
    }

    @Test
    void closeRejectsNewRequestsAndDrainsInFlight() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        long ts = System.currentTimeMillis() - 1_000;
        CountDownLatch start = new CountDownLatch(1);
        int threads = 4;
        Thread[] producers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            producers[t] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < 100; i++) {
                    try {
                        String body = "[" + eventJson("close-" + tid + "-" + i, ts) + "]";
                        HttpResponse<byte[]> r = CLIENT.send(post(base(server) + "/api/v1/ingest",
                                body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
                        int status = r.statusCode();
                        assertTrue(status == 202 || status == 503);
                    } catch (Exception ignored) {
                        // Connection rejected after close is expected.
                    }
                }
            });
            producers[t].start();
        }
        start.countDown();
        Thread.sleep(100);

        FlowLensHttpServer.CloseResult result = server.close();
        for (Thread p : producers) {
            p.join(5_000);
        }

        assertTrue(result.drained());
        assertEquals(0, server.inFlightCount());
        assertEquals(0, server.publisherClientCount());
        long accepted = 0;
        long processed = 0;
        for (Shard s : runtime.shards()) {
            accepted += s.accepted();
            processed += s.processed();
        }
        assertEquals(accepted, processed);
    }

    @Test
    void sseRegisterCloseRaceReleasesClients() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime, 120L);
        HttpResponse<InputStream> resp = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
        resp.body().close();
        server.close();
        assertEquals(0, server.publisherClientCount());
        assertEquals(FlowLensHttpServer.SSE_MAX_CONNECTIONS, server.sseSemaphore.availablePermits());
    }

    @Test
    void publisherSkipsTickWhenQueryPermitHeld() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime, 120L);
        server.querySemaphore.acquire();
        try {
            Thread.sleep(500);
            assertTrue(server.publisherSkippedTicks() > 0);
        } finally {
            server.querySemaphore.release();
            server.close();
        }
    }

    @Test
    void closeAndQueryRace() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        CountDownLatch start = new CountDownLatch(1);
        Thread query = new Thread(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                return;
            }
            for (int i = 0; i < 200; i++) {
                try {
                    CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/api/v1/topk")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
                } catch (Exception ignored) {
                    // Rejected after close is expected.
                }
            }
        });
        query.start();
        start.countDown();
        Thread.sleep(50);
        server.close();
        query.join(5_000);
        assertEquals(0, server.inFlightCount());
    }

    @Test
    void closeTimeoutReportsUnprocessed() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        FlowLensRuntime runtime = new FlowLensRuntime(
                System::currentTimeMillis,
                i -> new SlidingBloomFilter(),
                (i, ring) -> new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
                    try {
                        block.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("interrupted");
                    }
                    r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
                }));
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            for (int i = 0; i < 10; i++) {
                runtime.ingest(rawEvent("timeout-" + i, ts));
            }
            Thread.sleep(50);
            FlowLensHttpServer.CloseResult result = server.close();
            assertFalse(result.drained());
            assertFalse(result.runtimeDrained());
            assertTrue(result.unprocessedEvents() >= 1);
        } finally {
            block.countDown();
            server.close();
        }
    }

    @Test
    void stalledRequestBodyMarksHttpIncomplete() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = new FlowLensHttpServer(runtime, "127.0.0.1", 0, 120L, 200L);
        server.start();
        try (Socket socket = new Socket("127.0.0.1", server.port())) {
            OutputStream out = socket.getOutputStream();
            String headers = "POST /api/v1/ingest HTTP/1.1\r\n"
                    + "Host: 127.0.0.1\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Content-Length: 100\r\n\r\n";
            out.write(headers.getBytes(StandardCharsets.UTF_8));
            out.write("[{\"event_id\":\"x\"".getBytes(StandardCharsets.UTF_8));
            out.flush();
            Thread.sleep(200);

            FlowLensHttpServer.CloseResult result = server.close();
            assertTrue(result.httpIncomplete());
            assertTrue(result.runtimeDrained());
            assertFalse(result.drained());
        }
    }

    @Test
    void closeWaitsForAdmittedHandlerBeforeClosingRuntime() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try (Socket socket = new Socket("127.0.0.1", server.port())) {
            OutputStream out = socket.getOutputStream();
            String headers = "POST /api/v1/ingest HTTP/1.1\r\n"
                    + "Host: 127.0.0.1\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Content-Length: 100\r\n\r\n";
            byte[] partial = "[{\"event_id\":\"x\"".getBytes(StandardCharsets.UTF_8);
            out.write(headers.getBytes(StandardCharsets.UTF_8));
            out.write(partial);
            out.flush();

            awaitTrue(() -> server.inFlightCount() == 1, 5_000);

            ExecutorService ex = Executors.newSingleThreadExecutor();
            Future<FlowLensHttpServer.CloseResult> closeFuture = ex.submit(server::close);

            long deadline = System.currentTimeMillis() + 2_000;
            while (!closeFuture.isDone() && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertFalse(closeFuture.isDone(), "close must wait for the admitted handler");
            assertTrue(runtime.state() == FlowLensRuntime.State.RUNNING);

            out.write(new byte[100 - partial.length]);
            out.flush();
            socket.shutdownOutput();

            FlowLensHttpServer.CloseResult result = closeFuture.get(10, TimeUnit.SECONDS);
            assertTrue(result.drained());
            assertFalse(result.httpIncomplete());
            ex.shutdownNow();
        } finally {
            server.close();
        }
    }

    @Test
    void ingestConcurrencyIsFour() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            for (int i = 0; i < FlowLensHttpServer.INGEST_CONCURRENCY; i++) {
                server.ingestSemaphore.acquire();
            }
            long ts = System.currentTimeMillis() - 1_000;
            String body = "[" + eventJson("c-1", ts) + "]";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(503, resp.statusCode());
        } finally {
            server.ingestSemaphore.release(FlowLensHttpServer.INGEST_CONCURRENCY);
            server.close();
        }
    }

    @Test
    void parseFailureDoesNotEnqueue() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            String malformed = "[{\"event_id\":\"e1\",";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    malformed.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(400, resp.statusCode());

            HttpResponse<byte[]> health = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/healthz")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            Map<String, Object> json = parse(health.body());
            assertEquals(0L, asLong(json, "events_ingested"));
        } finally {
            server.close();
        }
    }

    @Test
    void compressedBodyOverLimitReturns413() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            StringBuilder big = new StringBuilder("[");
            long ts = System.currentTimeMillis() - 1_000;
            for (int i = 0; i < 1000; i++) {
                if (i > 0) {
                    big.append(',');
                }
                big.append("{\"event_id\":\"m").append(i)
                   .append("\",\"ts\":").append(ts)
                   .append(",\"user_id\":\"").append("u".repeat(64))
                   .append("\",\"url\":\"").append("x".repeat(1024))
                   .append("\",\"bytes\":1000}");
            }
            big.append(']');
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/api/v1/ingest",
                    big.toString().getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(413, resp.statusCode());
        } finally {
            server.close();
        }
    }

    @Test
    void originalIngestPathAccepted() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            String body = "[" + eventJson("orig-ingest", ts) + "]";
            HttpResponse<byte[]> resp = CLIENT.send(post(base(server) + "/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, resp.statusCode());
            Map<String, Object> json = parse(resp.body());
            assertEquals(1L, asLong(json, "accepted"));
        } finally {
            server.close();
        }
    }

    @Test
    void originalTopKAndStatsPathsWork() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            long ts = System.currentTimeMillis() - 1_000;
            String body = "[" + eventJson("orig-query", ts) + "]";
            HttpResponse<byte[]> ingest = CLIENT.send(post(base(server) + "/v1/ingest",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", null), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(202, ingest.statusCode());

            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                long proc = 0;
                for (Shard s : runtime.shards()) {
                    proc += s.processed();
                }
                if (proc >= 1) {
                    break;
                }
                Thread.sleep(10);
            }

            HttpResponse<byte[]> topk = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/v1/topk?metric=url&k=5&window=60")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, topk.statusCode());
            assertFalse(((List<?>) parse(topk.body()).get("items")).isEmpty());

            HttpResponse<byte[]> stats = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/v1/stats?window=60")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, stats.statusCode());
            assertTrue(((Number) parse(stats.body()).get("approx_distinct_users")).doubleValue() >= 1);
        } finally {
            server.close();
        }
    }

    @Test
    void originalStreamPathAcceptsSseConnection() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime, 120L);
        try {
            HttpResponse<InputStream> resp = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/v1/topk/stream")).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            assertEquals(200, resp.statusCode());
            assertTrue(resp.headers().firstValue("Content-Type").orElse("").startsWith("text/event-stream"));
            resp.body().close();
        } finally {
            server.close();
        }
    }

    @Test
    void healthzIncludesMemoryBytes() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = start(runtime);
        try {
            HttpResponse<byte[]> health = CLIENT.send(HttpRequest.newBuilder(URI.create(base(server) + "/healthz")).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, health.statusCode());
            Map<String, Object> json = parse(health.body());
            assertNotNull(json.get("memory_bytes"));
            Number memory = (Number) json.get("memory_bytes");
            assertTrue(memory.longValue() == -1L || memory.longValue() > 0L);
        } finally {
            server.close();
        }
    }
}
