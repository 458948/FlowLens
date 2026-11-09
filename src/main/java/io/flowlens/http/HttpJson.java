package io.flowlens.http;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Jackson-core based JSON writing helpers. No Jackson Databind is used. */
final class HttpJson {

    private static final JsonFactory FACTORY = new JsonFactory();

    @FunctionalInterface
    interface JsonWriter {
        void write(JsonGenerator generator) throws IOException;
    }

    private HttpJson() {
    }

    static byte[] build(JsonWriter writer) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (JsonGenerator g = FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
            writer.write(g);
        }
        return out.toByteArray();
    }

    static void send(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    static void error(HttpExchange exchange, int status, String message) throws IOException {
        byte[] body = build(g -> {
            g.writeStartObject();
            g.writeStringField("error", message);
            g.writeEndObject();
        });
        send(exchange, status, body);
    }

    static Map<String, String> params(URI uri) {
        Map<String, String> map = new HashMap<>();
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return map;
        }
        for (String pair : query.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String key = eq >= 0 ? pair.substring(0, eq) : pair;
            String value = eq >= 0 ? pair.substring(eq + 1) : "";
            map.put(URLDecoder.decode(key, StandardCharsets.UTF_8),
                    URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
        return map;
    }
}