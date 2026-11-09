package io.flowlens.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.flowlens.runtime.FlowLensRuntime;
import io.flowlens.window.StatsResult;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Semaphore;

/** GET /api/v1/stats. Estimates are never wrapped as exact values. */
final class StatsHandler implements HttpHandler {

    private final FlowLensRuntime runtime;
    private final Semaphore querySemaphore;

    StatsHandler(FlowLensRuntime runtime, Semaphore querySemaphore) {
        this.runtime = runtime;
        this.querySemaphore = querySemaphore;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!querySemaphore.tryAcquire()) {
            exchange.getResponseHeaders().set("Retry-After", "1");
            HttpJson.error(exchange, 429, "query concurrency limit reached");
            return;
        }
        try {
            handleInternal(exchange);
        } finally {
            querySemaphore.release();
        }
    }

    private void handleInternal(HttpExchange exchange) throws IOException {
        Map<String, String> params = HttpJson.params(exchange.getRequestURI());
        int window = intParam(params, "window", 60, 1, 60);
        if (window < 0) {
            HttpJson.error(exchange, 400, "window must be an integer in 1..60");
            return;
        }
        long at = System.currentTimeMillis();
        if (params.containsKey("at")) {
            try {
                at = Long.parseLong(params.get("at"));
            } catch (NumberFormatException e) {
                HttpJson.error(exchange, 400, "at must be epoch milliseconds");
                return;
            }
        }

        StatsResult result;
        try {
            result = runtime.queryStats(at, window * 1000L);
        } catch (IllegalArgumentException e) {
            HttpJson.error(exchange, 400, e.getMessage() == null ? "invalid query window" : e.getMessage());
            return;
        }

        byte[] body = HttpJson.build(g -> {
            g.writeStartObject();
            g.writeNumberField("effective_at", result.effectiveAt());
            g.writeBooleanField("stable", result.stable());
            g.writeNumberField("window", window);
            g.writeNumberField("approx_distinct_users", result.distinctUsers());
            g.writeNumberField("approx_p95_bytes", result.p95Bytes());
            g.writeStringField("p95_error_note", "log-histogram estimate; bin relative width ~9.05%");
            g.writeEndObject();
        });
        HttpJson.send(exchange, 200, body);
    }

    private static int intParam(Map<String, String> params, String key, int def, int min, int max) {
        String raw = params.get(key);
        if (raw == null) {
            return def;
        }
        int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            return -1;
        }
        return (value >= min && value <= max) ? value : -1;
    }
}