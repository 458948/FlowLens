package io.flowlens;

import io.flowlens.query.QueryService;
import io.flowlens.sketch.HyperLogLog;
import io.flowlens.sketch.LogHistogram;
import io.flowlens.sketch.TopKEntry;
import io.flowlens.window.Bucket;
import io.flowlens.window.QueryResult;
import io.flowlens.window.RingBuffer;
import io.flowlens.window.StatsResult;
import io.flowlens.window.Watermark;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryOptimizationTest {

    private static final long NOW = 1_000_000_000_000L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RingBuffer[] fourShards() {
        RingBuffer[] shards = new RingBuffer[4];
        for (int i = 0; i < 4; i++) {
            shards[i] = new RingBuffer();
        }
        return shards;
    }

    @Test
    void topKDirectMergeMatchesManualMerge() {
        RingBuffer[] shards = fourShards();
        for (int s = 0; s < 4; s++) {
            Bucket bucket = shards[s].acquire(NOW - 1_000, NOW);
            for (int i = 0; i < 100; i++) {
                bucket.observe(NOW - 1_000, b("user-" + i), b("url-hot"), 1000);
            }
            for (int i = 0; i < 50; i++) {
                bucket.observe(NOW - 1_000, b("user-x"), b("shard-" + s), 1000);
            }
        }

        QueryService service = new QueryService(shards);
        QueryResult result = service.queryTopK(NOW, 60_000, true, 10, NOW);
        assertFalse(result.items().isEmpty());
        TopKEntry first = result.items().get(0);
        assertEquals("url-hot", first.name());
        assertEquals(400, first.estimatedCount());
        assertTrue(first.lowerBound() <= 400 && 400 <= first.upperBound());
    }

    @Test
    void topKErrorBoundsArePreserved() {
        RingBuffer[] shards = fourShards();
        int total = 0;
        for (int s = 0; s < 4; s++) {
            Bucket bucket = shards[s].acquire(NOW - 1_000, NOW);
            for (int i = 0; i < 80; i++) {
                bucket.observe(NOW - 1_000, b("user-" + i), b("hot"), 1000);
                total++;
            }
        }
        QueryService service = new QueryService(shards);
        QueryResult result = service.queryTopK(NOW, 60_000, true, 5, NOW);
        TopKEntry hot = result.items().stream().filter(e -> e.name().equals("hot")).findFirst().orElseThrow();
        long e = total / RingBuffer.CAPACITY;
        assertEquals(320, hot.estimatedCount());
        assertEquals(Math.max(0L, 320 - e), hot.lowerBound());
        assertEquals(320 + e, hot.upperBound());
    }

    @Test
    void statsHllMatchesDirectMerge() {
        RingBuffer[] shards = fourShards();
        HyperLogLog expected = new HyperLogLog();
        for (int i = 0; i < 1000; i++) {
            Bucket bucket = shards[i % 4].acquire(NOW - 1_000, NOW);
            bucket.observe(NOW - 1_000, b("user-" + i), b("url"), 1000);
            bucket.hll().mergeInto(expected);
        }
        QueryService service = new QueryService(shards);
        StatsResult stats = service.queryStats(NOW, 60_000, NOW);
        assertEquals(expected.estimate(), stats.distinctUsers(), 1e-6);
    }

    @Test
    void statsHistogramAndP95MatchDirectMerge() {
        RingBuffer[] shards = fourShards();
        LogHistogram expected = new LogHistogram();
        for (int s = 0; s < 4; s++) {
            Bucket bucket = shards[s].acquire(NOW - 1_000, NOW);
            for (int i = 0; i < 500; i++) {
                long bytes = 100 + (i % 900);
                bucket.observe(NOW - 1_000, b("user-" + i), b("url"), bytes);
            }
            bucket.histogram().mergeInto(expected);
        }
        QueryService service = new QueryService(shards);
        StatsResult stats = service.queryStats(NOW, 60_000, NOW);
        assertEquals(expected.estimateQuantile(0.95), stats.p95Bytes(), 0.001);
    }

    @Test
    void exactlyLateEventIsIncluded() {
        RingBuffer[] shards = fourShards();
        long ts = NOW - Watermark.MAX_LATENESS_MS;
        shards[0].acquire(ts, NOW).observe(ts, b("late-user"), b("late-url"), 1);
        QueryService service = new QueryService(shards);
        QueryResult top = service.queryTopK(NOW, 60_000, true, 10, NOW);
        assertFalse(top.items().isEmpty());
        assertTrue(top.items().stream().anyMatch(e -> e.name().equals("late-url")));
    }

    @Test
    void unclosedBucketUpdateIsReflected() {
        RingBuffer[] shards = fourShards();
        Bucket bucket = shards[0].acquire(NOW - 1_000, NOW);
        bucket.observe(NOW - 1_000, b("u"), b("hot"), 1000);
        QueryService service = new QueryService(shards);
        assertEquals(1, service.queryTopK(NOW, 60_000, true, 10, NOW).items().get(0).estimatedCount());
        for (int i = 0; i < 9; i++) {
            bucket.observe(NOW - 1_000, b("u" + i), b("hot"), 1000);
        }
        assertEquals(10, service.queryTopK(NOW, 60_000, true, 10, NOW).items().get(0).estimatedCount());
    }

    @Test
    void concurrentWriteAndQueryRemainConsistent() throws Exception {
        RingBuffer[] shards = fourShards();
        QueryService service = new QueryService(shards);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Thread writer = new Thread(() -> {
            for (int i = 0; i < 5_000; i++) {
                Bucket bucket = shards[0].acquire(NOW - 1_000, NOW);
                bucket.observe(NOW - 1_000, b("user-" + (i % 500)), b("hot"), 1000);
            }
        });
        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < 300; i++) {
                    QueryResult top = service.queryTopK(NOW, 60_000, true, 10, NOW);
                    StatsResult stats = service.queryStats(NOW, 60_000, NOW);
                    if (top.items() == null || stats.distinctUsers() < 0) {
                        throw new IllegalStateException("bad query");
                    }
                }
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            }
        });

        writer.start();
        reader.start();
        writer.join();
        reader.join();
        assertNull(error.get(), () -> String.valueOf(error.get()));
        QueryResult finalTop = service.queryTopK(NOW, 60_000, true, 10, NOW);
        assertFalse(finalTop.items().isEmpty());
        assertEquals("hot", finalTop.items().get(0).name());
    }

    @Test
    void repeatedQueriesAreStableAndDoNotLeak() {
        RingBuffer[] shards = fourShards();
        Bucket bucket = shards[0].acquire(NOW - 1_000, NOW);
        for (int i = 0; i < 100; i++) {
            bucket.observe(NOW - 1_000, b("user-" + i), b("hot"), 1000);
        }
        QueryService service = new QueryService(shards);
        QueryResult first = service.queryTopK(NOW, 60_000, true, 10, NOW);
        for (int i = 0; i < 200; i++) {
            QueryResult again = service.queryTopK(NOW, 60_000, true, 10, NOW);
            assertEquals(first.items().size(), again.items().size());
            assertEquals(first.items().get(0).name(), again.items().get(0).name());
            assertEquals(first.items().get(0).estimatedCount(), again.items().get(0).estimatedCount());
        }
    }
}