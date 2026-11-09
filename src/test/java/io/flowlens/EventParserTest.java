package io.flowlens;

import io.flowlens.ingest.EventParser;
import io.flowlens.ingest.EventParser.ParseResult;
import io.flowlens.ingest.GzipLimiter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventParserTest {

    @Test
    void validArrayIsParsedAndUrlNormalized() throws Exception {
        byte[] body = ("[{\"event_id\":\"e1\",\"ts\":1700000000000,"
                + "\"user_id\":\"u1\",\"url\":\"/a/b?x=1#frag\",\"bytes\":100}]").getBytes(StandardCharsets.UTF_8);
        ParseResult r = EventParser.parseJsonArray(body, 1000);
        assertEquals(1, r.events.size());
        assertEquals(0, r.rejected);
        assertEquals("/a/b", new String(r.events.get(0).url, StandardCharsets.UTF_8));
    }

    @Test
    void syntaxErrorRejectsWholeBatch() {
        byte[] body = "[{\"event_id\":\"e1\",".getBytes(StandardCharsets.UTF_8);
        assertThrows(EventParser.ParseException.class, () -> EventParser.parseJsonArray(body, 1000));
    }

    @Test
    void invalidFieldsAreRejectedNotAccepted() throws Exception {
        String badEventId = "{\"event_id\":\"" + "a".repeat(200) + "\",\"ts\":1,\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":1}";
        String badUserId = "{\"event_id\":\"e\",\"ts\":1,\"user_id\":\"" + "u".repeat(100) + "\",\"url\":\"/x\",\"bytes\":1}";
        String badUrl = "{\"event_id\":\"e\",\"ts\":1,\"user_id\":\"u\",\"url\":\"" + "a".repeat(2000) + "\",\"bytes\":1}";
        String longNormalized = "{\"event_id\":\"e\",\"ts\":1,\"user_id\":\"u\",\"url\":\"http://example.com/" + "a".repeat(300) + "\",\"bytes\":1}";
        String negativeBytes = "{\"event_id\":\"e\",\"ts\":1,\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":-1}";

        byte[] body = ("[" + badEventId + "," + badUserId + "," + badUrl + "," + longNormalized + "," + negativeBytes + "]")
                .getBytes(StandardCharsets.UTF_8);
        ParseResult r = EventParser.parseJsonArray(body, 1000);
        assertEquals(0, r.events.size());
        assertEquals(5, r.rejected);
    }

    @Test
    void fieldTypeAndRangeErrorsAreRejectedPerEvent() throws Exception {
        String stringTs = "{\"event_id\":\"e1\",\"ts\":\"1700000000000\",\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":1}";
        String stringBytes = "{\"event_id\":\"e2\",\"ts\":1,\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":\"100\"}";
        String negativeTs = "{\"event_id\":\"e3\",\"ts\":-1,\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":1}";
        String negativeBytes = "{\"event_id\":\"e4\",\"ts\":1,\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":-1}";

        byte[] body = ("[" + stringTs + "," + stringBytes + "," + negativeTs + "," + negativeBytes + "]")
                .getBytes(StandardCharsets.UTF_8);
        ParseResult r = EventParser.parseJsonArray(body, 1000);
        assertEquals(0, r.events.size());
        assertEquals(4, r.rejected);
    }

    @Test
    void gzipDecompressionLimitIsEnforced() throws Exception {
        byte[] gz = gzip("hello ".repeat(2000).getBytes(StandardCharsets.UTF_8));
        InputStream limited = GzipLimiter.limitedGzip(new ByteArrayInputStream(gz), 100);
        assertThrows(IOException.class, () -> readAll(limited));
    }

    @Test
    void compressedBodyLimitIsEnforced() throws Exception {
        byte[] gz = gzip("hello ".repeat(2000).getBytes(StandardCharsets.UTF_8));
        InputStream in = new ByteArrayInputStream(gz);
        assertThrows(IOException.class, () -> GzipLimiter.readCompressedLimited(in, 10));
    }

    @Test
    void ndjsonSizeLimitIsEnforced() {
        byte[] oversized = new byte[EventParser.MAX_BODY_BYTES + 1];
        java.util.Arrays.fill(oversized, (byte) 'x');
        assertThrows(EventParser.ParseException.class, () -> EventParser.parseNdjson(oversized, 1000));
    }

    private static byte[] gzip(byte[] data) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(data);
        }
        return bos.toByteArray();
    }

    private static void readAll(InputStream in) throws IOException {
        byte[] buf = new byte[1024];
        while (in.read(buf) != -1) {
            // drain
        }
    }

    @Test
    void jsonArrayInputStreamParse() throws Exception {
        byte[] body = ("[{\"event_id\":\"e1\",\"ts\":1700000000000,"
                + "\"user_id\":\"u1\",\"url\":\"/a/b?x=1\",\"bytes\":100}]").getBytes(StandardCharsets.UTF_8);
        EventParser.ParseResult r = EventParser.parseJsonArray(new ByteArrayInputStream(body), 1000);
        assertEquals(1, r.events.size());
        assertEquals(0, r.rejected);
    }

    @Test
    void ndjsonInputStreamParse() throws Exception {
        String body = "{\"event_id\":\"e1\",\"ts\":1,\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":1}\n"
                + "{\"event_id\":\"e2\",\"ts\":1,\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":1}\n";
        EventParser.ParseResult r = EventParser.parseNdjson(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)), 1000);
        assertEquals(2, r.events.size());
    }

    @Test
    void trailingContentAfterJsonArrayIsRejected() {
        byte[] body = ("[{\"event_id\":\"e1\",\"ts\":1,\"user_id\":\"u\",\"url\":\"/x\",\"bytes\":1}] {}")
                .getBytes(StandardCharsets.UTF_8);
        assertThrows(EventParser.ParseException.class,
                () -> EventParser.parseJsonArray(new ByteArrayInputStream(body), 1000));
    }

    @Test
    void gzipCrcErrorIsRejected() throws Exception {
        byte[] gz = gzip("hello ".repeat(200).getBytes(StandardCharsets.UTF_8));
        gz[gz.length / 2] ^= 0x01;
        InputStream in = GzipLimiter.limitedGzip(new ByteArrayInputStream(gz), 1 << 20);
        assertThrows(IOException.class, () -> readAll(in));
    }

    @Test
    void truncatedGzipTrailerIsRejected() throws Exception {
        byte[] gz = gzip("hello ".repeat(200).getBytes(StandardCharsets.UTF_8));
        byte[] truncated = Arrays.copyOf(gz, gz.length - 4);
        InputStream in = GzipLimiter.limitedGzip(new ByteArrayInputStream(truncated), 1 << 20);
        assertThrows(IOException.class, () -> readAll(in));
    }

    @Test
    void boundedCompressedExactlyAtLimitIsAllowed() throws Exception {
        byte[] data = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        InputStream in = GzipLimiter.boundedCompressed(new ByteArrayInputStream(data), 16);
        byte[] out = new byte[32];
        int total = 0;
        int n;
        while ((n = in.read(out)) != -1) {
            total += n;
        }
        assertEquals(16, total);
    }

    @Test
    void boundedCompressedOverLimitThrowsNotEof() {
        byte[] data = "0123456789abcdef0".getBytes(StandardCharsets.UTF_8);
        InputStream in = GzipLimiter.boundedCompressed(new ByteArrayInputStream(data), 16);
        assertThrows(GzipLimiter.CompressedLimitExceededException.class, () -> readAll(in));
    }

    @Test
    void concurrentStreamingJsonParseDoesNotCrossContaminate() throws Exception {
        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < 50; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append("{\"event_id\":\"e").append(tid).append('-').append(i)
                      .append("\",\"ts\":1,\"user_id\":\"u").append(tid)
                      .append("\",\"url\":\"/x\",\"bytes\":1}");
                }
                sb.append(']');
                try {
                    EventParser.ParseResult r = EventParser.parseJsonArray(
                            new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)), 1000);
                    assertEquals(50, r.events.size());
                    assertEquals("e" + tid + "-0", new String(r.events.get(0).eventId, StandardCharsets.UTF_8));
                    assertEquals("e" + tid + "-49", new String(r.events.get(49).eventId, StandardCharsets.UTF_8));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        ready.await();
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();
    }

    @Test
    void concurrentStreamingNdjsonParseDoesNotCrossContaminate() throws Exception {
        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 50; i++) {
                    sb.append("{\"event_id\":\"n").append(tid).append('-').append(i)
                      .append("\",\"ts\":1,\"user_id\":\"u").append(tid)
                      .append("\",\"url\":\"/x\",\"bytes\":1}\n");
                }
                try {
                    EventParser.ParseResult r = EventParser.parseNdjson(
                            new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)), 1000);
                    assertEquals(50, r.events.size());
                    assertEquals("n" + tid + "-0", new String(r.events.get(0).eventId, StandardCharsets.UTF_8));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        ready.await();
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();
    }
}
