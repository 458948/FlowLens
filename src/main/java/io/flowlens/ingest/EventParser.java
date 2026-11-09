package io.flowlens.ingest;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming Jackson-core parser. The whole batch is syntactically validated
 * before any event is returned, so a JSON syntax error rejects the entire
 * batch. Field-level violations are counted as rejected.
 *
 * Body-size enforcement: byte[] overloads and InputStream overloads both wrap
 * the input with the same cap; the HTTP layer additionally bounds the
 * compressed and decompressed stream sizes via GzipLimiter.
 */
public final class EventParser {

    public static final int MAX_EVENTS_PER_BATCH = 1000;
    public static final int MAX_BODY_BYTES = 1 << 20;

    private static final JsonFactory FACTORY = new JsonFactory();

    private EventParser() {
    }

    public static ParseResult parseJsonArray(byte[] data, int maxEvents) throws IOException {
        if (data.length > MAX_BODY_BYTES) {
            throw new ParseException("body exceeds " + MAX_BODY_BYTES + " bytes");
        }
        return parseJsonArray(new ByteArrayInputStream(data), maxEvents);
    }

    public static ParseResult parseJsonArray(InputStream in, int maxEvents) throws IOException {
        List<RawEvent> events = new ArrayList<>();
        int rejected = 0;
        try {
            try (JsonParser p = FACTORY.createParser(new BodyLimitedInputStream(in, MAX_BODY_BYTES))) {
                if (p.nextToken() != JsonToken.START_ARRAY) {
                    throw new ParseException("expected a JSON array");
                }
                JsonToken t;
                while ((t = p.nextToken()) != JsonToken.END_ARRAY) {
                    if (t != JsonToken.START_OBJECT) {
                        throw new ParseException("expected an event object");
                    }
                    RawEvent event = parseEvent(p);
                    if (event == null) {
                        rejected++;
                    } else {
                        events.add(event);
                        if (events.size() > maxEvents) {
                            throw new ParseException("too many events in batch");
                        }
                    }
                }
                if (p.nextToken() != null) {
                    throw new ParseException("unexpected trailing content after JSON array");
                }
            }
        } catch (com.fasterxml.jackson.core.JsonParseException e) {
            if (e.getCause() instanceof IOException cause) {
                throw cause;
            }
            throw new ParseException(e.getMessage());
        }
        return new ParseResult(events, rejected);
    }

    public static ParseResult parseNdjson(byte[] data, int maxEvents) throws IOException {
        if (data.length > MAX_BODY_BYTES) {
            throw new ParseException("body exceeds " + MAX_BODY_BYTES + " bytes");
        }
        return parseNdjson(new ByteArrayInputStream(data), maxEvents);
    }

    public static ParseResult parseNdjson(InputStream in, int maxEvents) throws IOException {
        List<RawEvent> events = new ArrayList<>();
        int rejected = 0;
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new BodyLimitedInputStream(in, MAX_BODY_BYTES), StandardCharsets.UTF_8), 8192);
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                try (JsonParser p = FACTORY.createParser(trimmed)) {
                    if (p.nextToken() != JsonToken.START_OBJECT) {
                        throw new ParseException("expected a JSON object in NDJSON line");
                    }
                    RawEvent event = parseEvent(p);
                    if (event == null) {
                        rejected++;
                    } else {
                        events.add(event);
                        if (events.size() > maxEvents) {
                            throw new ParseException("too many events in batch");
                        }
                    }
                }
            }
        } catch (com.fasterxml.jackson.core.JsonParseException e) {
            if (e.getCause() instanceof IOException cause) {
                throw cause;
            }
            throw new ParseException(e.getMessage());
        }
        return new ParseResult(events, rejected);
    }

    private static RawEvent parseEvent(JsonParser p) throws IOException {
        byte[] eventId = null;
        byte[] userId = null;
        byte[] url = null;
        long ts = Long.MIN_VALUE;
        long bytes = Long.MIN_VALUE;
        boolean bad = false;

        JsonToken t;
        while ((t = p.nextToken()) != JsonToken.END_OBJECT) {
            String field = p.currentName();
            p.nextToken();
            switch (field) {
                case "event_id" -> {
                    eventId = p.getValueAsString().getBytes(StandardCharsets.UTF_8);
                    if (eventId.length == 0 || eventId.length > UrlNormalizer.MAX_EVENT_ID_BYTES) {
                        bad = true;
                    }
                }
                case "user_id" -> {
                    userId = p.getValueAsString().getBytes(StandardCharsets.UTF_8);
                    if (userId.length == 0 || userId.length > UrlNormalizer.MAX_USER_ID_BYTES) {
                        bad = true;
                    }
                }
                case "url" -> {
                    String raw = p.getValueAsString();
                    byte[] rawBytes = raw.getBytes(StandardCharsets.UTF_8);
                    try {
                        url = UrlNormalizer.normalize(rawBytes, 0, rawBytes.length);
                    } catch (UrlNormalizer.FieldTooLongException ex) {
                        bad = true;
                    }
                }
                case "ts" -> {
                    if (p.currentToken() == JsonToken.VALUE_NUMBER_INT) {
                        ts = p.getLongValue();
                    } else {
                        bad = true;
                    }
                }
                case "bytes" -> {
                    if (p.currentToken() == JsonToken.VALUE_NUMBER_INT) {
                        bytes = p.getLongValue();
                    } else {
                        bad = true;
                    }
                }
                default -> {
                    // ignore unknown fields
                }
            }
        }

        if (bad || eventId == null || userId == null || url == null
                || ts == Long.MIN_VALUE || ts < 0 || bytes == Long.MIN_VALUE || bytes < 0) {
            return null;
        }
        return new RawEvent(eventId, userId, url, ts, bytes);
    }

    public static final class ParseResult {
        public final List<RawEvent> events;
        public final int rejected;

        public ParseResult(List<RawEvent> events, int rejected) {
            this.events = events;
            this.rejected = rejected;
        }
    }

    public static final class ParseException extends IOException {
        public ParseException(String message) {
            super(message);
        }
    }

    private static final class BodyLimitedInputStream extends InputStream {
        private final InputStream in;
        private final int max;
        private int count;

        BodyLimitedInputStream(InputStream in, int max) {
            this.in = in;
            this.max = max;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b != -1 && ++count > max) {
                throw new ParseException("body exceeds " + max + " bytes");
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n > 0) {
                count += n;
                if (count > max) {
                    throw new ParseException("body exceeds " + max + " bytes");
                }
            }
            return n;
        }
    }
}