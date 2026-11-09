package io.flowlens.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.flowlens.ingest.EventParser;
import io.flowlens.ingest.GzipLimiter;
import io.flowlens.ingest.RawEvent;
import io.flowlens.runtime.FlowLensRuntime;
import io.flowlens.runtime.IngestOutcome;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Semaphore;
import java.util.zip.ZipException;

/** POST /api/v1/ingest. Streams the body; never bypasses DedupAdmission or queues. */
final class IngestHandler implements HttpHandler {

    static final int MAX_COMPRESSED_BYTES = 1 << 20;
    static final int MAX_DECOMPRESSED_BYTES = 1 << 20;

    private final FlowLensRuntime runtime;
    private final Semaphore ingestSemaphore;

    IngestHandler(FlowLensRuntime runtime, Semaphore ingestSemaphore) {
        this.runtime = runtime;
        this.ingestSemaphore = ingestSemaphore;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!ingestSemaphore.tryAcquire()) {
            HttpJson.error(exchange, 503, "ingest concurrency limit reached");
            return;
        }
        try {
            handleInternal(exchange);
        } finally {
            ingestSemaphore.release();
        }
    }

    private void handleInternal(HttpExchange exchange) throws IOException {
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        String encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");

        boolean ndjson = contentType != null && contentType.toLowerCase(Locale.ROOT).contains("x-ndjson");
        boolean json = contentType != null && contentType.toLowerCase(Locale.ROOT).contains("json") && !ndjson;
        if (!json && !ndjson) {
            HttpJson.error(exchange, 415, "unsupported Content-Type");
            return;
        }
        boolean gzip = "gzip".equalsIgnoreCase(encoding);
        if (encoding != null && !encoding.isBlank()
                && !"identity".equalsIgnoreCase(encoding) && !gzip) {
            HttpJson.error(exchange, 415, "unsupported Content-Encoding");
            return;
        }

        EventParser.ParseResult parsed;
        try {
            InputStream in = GzipLimiter.boundedCompressed(exchange.getRequestBody(), MAX_COMPRESSED_BYTES);
            if (gzip) {
                in = GzipLimiter.limitedGzip(in, MAX_DECOMPRESSED_BYTES);
            }
            parsed = ndjson
                    ? EventParser.parseNdjson(in, EventParser.MAX_EVENTS_PER_BATCH)
                    : EventParser.parseJsonArray(in, EventParser.MAX_EVENTS_PER_BATCH);
        } catch (GzipLimiter.CompressedLimitExceededException e) {
            HttpJson.error(exchange, 413, e.getMessage());
            return;
        } catch (GzipLimiter.DecompressedLimitExceededException e) {
            HttpJson.error(exchange, 413, e.getMessage());
            return;
        } catch (ZipException e) {
            HttpJson.error(exchange, 400, "invalid gzip body");
            return;
        } catch (EOFException e) {
            HttpJson.error(exchange, 400, "truncated gzip body");
            return;
        } catch (EventParser.ParseException e) {
            String message = e.getMessage() == null ? "invalid request body" : e.getMessage();
            int status = (message.contains("too many events") || message.contains("body exceeds")) ? 413 : 400;
            HttpJson.error(exchange, status, message);
            return;
        } catch (IOException e) {
            String message = e.getMessage() == null ? "invalid request body" : e.getMessage();
            if (message.contains("exceeds")) {
                HttpJson.error(exchange, 413, message);
            } else {
                HttpJson.error(exchange, 400, "invalid request body");
            }
            return;
        }

        List<RawEvent> events = parsed.events;
        int accepted = 0;
        int deduped = 0;
        int rejected = parsed.rejected;
        boolean overloaded = false;
        int i = 0;
        for (; i < events.size(); i++) {
            IngestOutcome outcome = runtime.ingest(events.get(i));
            switch (outcome) {
                case ACCEPTED -> accepted++;
                case DEDUPED -> deduped++;
                case REJECTED -> rejected++;
                case OVERLOADED, CLOSED -> {
                    overloaded = true;
                }
            }
            if (outcome == IngestOutcome.OVERLOADED || outcome == IngestOutcome.CLOSED) {
                break;
            }
        }
        int unprocessed = events.size() - i;

        final int acc = accepted;
        final int ded = deduped;
        final int rej = rejected;
        final boolean ov = overloaded;
        final int unp = unprocessed;

        byte[] response = HttpJson.build(g -> {
            g.writeStartObject();
            g.writeNumberField("accepted", acc);
            g.writeNumberField("deduped", ded);
            g.writeNumberField("rejected", rej);
            g.writeBooleanField("overloaded", ov);
            g.writeNumberField("unprocessed", unp);
            g.writeEndObject();
        });
        HttpJson.send(exchange, overloaded ? 503 : 202, response);
    }
}