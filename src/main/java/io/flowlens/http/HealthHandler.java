package io.flowlens.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.flowlens.runtime.FlowLensRuntime;
import io.flowlens.runtime.Shard;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** GET /healthz. Liveness only; degradation does not mean the JVM is dead. */
final class HealthHandler implements HttpHandler {

    private final FlowLensRuntime runtime;

    HealthHandler(FlowLensRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        long total = 0;
        for (Shard shard : runtime.shards()) {
            total += shard.accepted();
        }
        final long eventsIngested = total;

        byte[] body = HttpJson.build(g -> {
            g.writeStartObject();
            g.writeBooleanField("ok", true);
            g.writeStringField("state", runtime.state().name());
            g.writeBooleanField("degraded", runtime.isDegraded());
            g.writeNumberField("events_ingested", eventsIngested);
            g.writeNumberField("memory_bytes", vmRss());
            g.writeEndObject();
        });
        HttpJson.send(exchange, 200, body);
    }

    private static long vmRss() {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.trim().split("\\s+")[1]) * 1024L;
                }
            }
        } catch (Exception ignored) {
            // non-Linux or unavailable
        }
        return -1L;
    }
}