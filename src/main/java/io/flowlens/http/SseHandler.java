package io.flowlens.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/** GET /api/v1/stream. One shared snapshot stream with hard connection limits. */
final class SseHandler implements HttpHandler {

    private final SnapshotPublisher publisher;
    private final Semaphore sseSemaphore;
    private final AtomicInteger idSequence = new AtomicInteger();

    SseHandler(SnapshotPublisher publisher, Semaphore sseSemaphore) {
        this.publisher = publisher;
        this.sseSemaphore = sseSemaphore;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!sseSemaphore.tryAcquire()) {
            HttpJson.error(exchange, 503, "too many SSE connections");
            return;
        }

        SseClient client = new SseClient(exchange, idSequence.incrementAndGet());
        if (!publisher.register(client)) {
            sseSemaphore.release();
            exchange.close();
            return;
        }
        try {
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.getResponseHeaders().set("Connection", "keep-alive");
            exchange.getResponseHeaders().set("X-Accel-Buffering", "no");
            exchange.sendResponseHeaders(200, 0);

            OutputStream out = exchange.getResponseBody();
            client.attach(Thread.currentThread());
            write(out, "retry: 3000\n\n");

            while (!client.isClosed()) {
                Snapshot snapshot;
                try {
                    snapshot = client.poll(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                if (snapshot != null) {
                    write(out, "id: " + snapshot.sequence() + "\n");
                    write(out, "event: snapshot\n");
                    write(out, "data: " + snapshotJson(snapshot) + "\n\n");
                    out.flush();
                    client.resetDrops();
                } else {
                    write(out, ": keep-alive\n\n");
                    out.flush();
                }
                if (client.consecutiveDrops() > SseClient.SLOW_DROP_LIMIT) {
                    client.close();
                }
            }
        } catch (IOException e) {
            // Client disconnected; fall through to cleanup.
        } finally {
            publisher.unregister(client);
            client.close();
            sseSemaphore.release();
            exchange.close();
        }
    }

    private static String snapshotJson(Snapshot snapshot) throws IOException {
        byte[] bytes = HttpJson.build(g -> {
            g.writeStartObject();
            g.writeNumberField("effective_at", snapshot.effectiveAt());
            g.writeNumberField("window", snapshot.windowMs());
            g.writeNumberField("sequence", snapshot.sequence());
            g.writeNumberField("approx_distinct_users", snapshot.distinctUsers());
            g.writeNumberField("approx_p95_bytes", snapshot.p95Bytes());
            g.writeArrayFieldStart("url_top");
            for (io.flowlens.sketch.TopKEntry entry : snapshot.urlTop()) {
                g.writeStartObject();
                g.writeStringField("name", entry.name());
                g.writeNumberField("estimated_count", entry.estimatedCount());
                g.writeEndObject();
            }
            g.writeEndArray();
            g.writeArrayFieldStart("user_top");
            for (io.flowlens.sketch.TopKEntry entry : snapshot.userTop()) {
                g.writeStartObject();
                g.writeStringField("name", entry.name());
                g.writeNumberField("estimated_count", entry.estimatedCount());
                g.writeEndObject();
            }
            g.writeEndArray();
            g.writeEndObject();
        });
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void write(OutputStream out, String text) throws IOException {
        out.write(text.getBytes(StandardCharsets.UTF_8));
    }
}