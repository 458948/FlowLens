package io.flowlens.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.flowlens.runtime.FlowLensRuntime;
import io.flowlens.sketch.TopKEntry;
import io.flowlens.window.QueryResult;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Semaphore;

/** GET /api/v1/topk. Cross-shard aggregation via the global QueryService. */
final class TopKHandler implements HttpHandler {

    private final FlowLensRuntime runtime;
    private final Semaphore querySemaphore;

    TopKHandler(FlowLensRuntime runtime, Semaphore querySemaphore) {
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
        String metric = params.getOrDefault("metric", "url");
        boolean byUrl;
        if ("url".equals(metric)) {
            byUrl = true;
        } else if ("user".equals(metric)) {
            byUrl = false;
        } else {
            HttpJson.error(exchange, 400, "metric must be url or user");
            return;
        }

        int k = intParam(params, "k", 10, 1, 100, "k must be 1..100");
        if (k < 0) {
            HttpJson.error(exchange, 400, "k must be an integer in 1..100");
            return;
        }
        int window = intParam(params, "window", 60, 1, 60, "window must be 1..60 seconds");
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

        QueryResult result;
        try {
            result = runtime.queryTopK(at, window * 1000L, byUrl, k);
        } catch (IllegalArgumentException e) {
            HttpJson.error(exchange, 400, e.getMessage() == null ? "invalid query window" : e.getMessage());
            return;
        }

        byte[] body = HttpJson.build(g -> {
            g.writeStartObject();
            g.writeNumberField("effective_at", result.effectiveAt());
            g.writeBooleanField("stable", result.stable());
            g.writeNumberField("window", window);
            g.writeStringField("metric", metric);
            g.writeArrayFieldStart("items");
            for (TopKEntry entry : result.items()) {
                g.writeStartObject();
                g.writeStringField("name", entry.name());
                g.writeNumberField("estimated_count", entry.estimatedCount());
                g.writeNumberField("lower_bound", entry.lowerBound());
                g.writeNumberField("upper_bound", entry.upperBound());
                g.writeEndObject();
            }
            g.writeEndArray();
            g.writeEndObject();
        });
        HttpJson.send(exchange, 200, body);
    }

    private static int intParam(Map<String, String> params, String key, int def, int min, int max, String err) {
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