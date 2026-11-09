package io.flowlens;

import io.flowlens.query.QueryService;
import io.flowlens.sketch.TopKEntry;
import io.flowlens.window.Bucket;
import io.flowlens.window.QueryResult;
import io.flowlens.window.RingBuffer;
import io.flowlens.window.StatsResult;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryServiceTest {

    private static final long NOW = 1_000_000_000_000L;
    private static final long TS = NOW - 1_000;

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
    void fourShardUrlTopKMerge() {
        RingBuffer[] shards = fourShards();
        for (int s = 0; s < 4; s++) {
            Bucket bucket = shards[s].acquire(TS, NOW);
            for (int i = 0; i < 100; i++) {
                bucket.observe(TS, b("user-" + i), b("url-hot"), 1000);
            }
            for (int i = 0; i < 50; i++) {
                bucket.observe(TS, b("user-x"), b("shard-" + s), 1000);
            }
        }

        QueryService service = new QueryService(shards);
        QueryResult result = service.queryTopK(NOW, 60_000, true, 10, NOW);
        assertFalse(result.items().isEmpty());
        TopKEntry first = result.items().get(0);
        assertEquals("url-hot", first.name());
        assertTrue(first.estimatedCount() >= 400, "merged count: " + first.estimatedCount());
    }

    @Test
    void fourShardUserTopKMerge() {
        RingBuffer[] shards = fourShards();
        for (int s = 0; s < 4; s++) {
            Bucket bucket = shards[s].acquire(TS, NOW);
            for (int i = 0; i < 100; i++) {
                bucket.observe(TS, b("user-hot"), b("url-" + i), 1000);
            }
        }

        QueryService service = new QueryService(shards);
        QueryResult result = service.queryTopK(NOW, 60_000, false, 10, NOW);
        assertFalse(result.items().isEmpty());
        assertEquals("user-hot", result.items().get(0).name());
    }

    @Test
    void fourShardHllMerge() {
        RingBuffer[] shards = fourShards();
        int distinct = 1000;
        for (int i = 0; i < distinct; i++) {
            Bucket bucket = shards[i % 4].acquire(TS, NOW);
            bucket.observe(TS, b("user-" + i), b("url"), 1000);
        }

        QueryService service = new QueryService(shards);
        StatsResult stats = service.queryStats(NOW, 60_000, NOW);
        assertTrue(stats.distinctUsers() >= 800 && stats.distinctUsers() <= 1200,
                "distinct users: " + stats.distinctUsers());
    }

    @Test
    void fourShardP95Merge() {
        RingBuffer[] shards = fourShards();
        for (int s = 0; s < 4; s++) {
            Bucket bucket = shards[s].acquire(TS, NOW);
            for (int i = 0; i < 500; i++) {
                bucket.observe(TS, b("user-" + i), b("url"), 1000);
            }
        }

        QueryService service = new QueryService(shards);
        StatsResult stats = service.queryStats(NOW, 60_000, NOW);
        assertTrue(stats.p95Bytes() >= 939.0 && stats.p95Bytes() <= 1024.0,
                "p95: " + stats.p95Bytes());
    }

    @Test
    void repeatedQueriesDoNotGrowWorkspace() {
        RingBuffer[] shards = fourShards();
        for (int s = 0; s < 4; s++) {
            Bucket bucket = shards[s].acquire(TS, NOW);
            for (int i = 0; i < 200; i++) {
                bucket.observe(TS, b("user-" + i), b("url-hot"), 1000);
            }
        }

        QueryService service = new QueryService(shards);
        for (int i = 0; i < 2_000; i++) {
            QueryResult top = service.queryTopK(NOW, 60_000, true, 10, NOW);
            StatsResult stats = service.queryStats(NOW, 60_000, NOW);
            assertFalse(top.items().isEmpty());
            assertEquals("url-hot", top.items().get(0).name());
            assertTrue(stats.distinctUsers() >= 100 && stats.distinctUsers() <= 300,
                    "distinct users: " + stats.distinctUsers());
        }
    }

    @Test
    void concurrentQueriesAreSerialized() throws Exception {
        RingBuffer[] shards = fourShards();
        for (int s = 0; s < 4; s++) {
            Bucket bucket = shards[s].acquire(TS, NOW);
            for (int i = 0; i < 1_000; i++) {
                bucket.observe(TS, b("user-" + i), b("url-hot"), 1000);
            }
        }

        QueryService service = new QueryService(shards);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Thread topThread = new Thread(() -> {
            try {
                for (int i = 0; i < 300; i++) {
                    QueryResult top = service.queryTopK(NOW, 60_000, true, 10, NOW);
                    assertFalse(top.items().isEmpty());
                    assertEquals("url-hot", top.items().get(0).name());
                }
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            }
        });
        Thread statsThread = new Thread(() -> {
            try {
                for (int i = 0; i < 300; i++) {
                    StatsResult stats = service.queryStats(NOW, 60_000, NOW);
                    assertTrue(stats.distinctUsers() >= 500 && stats.distinctUsers() <= 1500,
                            "distinct users: " + stats.distinctUsers());
                }
            } catch (Throwable t) {
                error.compareAndSet(null, t);
            }
        });

        topThread.start();
        statsThread.start();
        topThread.join();
        statsThread.join();

        assertNull(error.get(), () -> String.valueOf(error.get()));
    }

    @Test
    void windowResetDoesNotLeakPreviousData() {
        RingBuffer[] shards = fourShards();
        Bucket bucket = shards[0].acquire(TS, NOW);
        for (int i = 0; i < 500; i++) {
            bucket.observe(TS, b("user-" + i), b("url-hot"), 1000);
        }

        QueryService service = new QueryService(shards);

        QueryResult withData = service.queryTopK(NOW, 60_000, true, 10, NOW);
        assertFalse(withData.items().isEmpty());

        QueryResult emptyWindow = service.queryTopK(NOW - 30_000, 30_000, true, 10, NOW);
        assertTrue(emptyWindow.items().isEmpty());

        StatsResult emptyStats = service.queryStats(NOW - 30_000, 30_000, NOW);
        assertEquals(0.0, emptyStats.distinctUsers(), 0.001);
    }

    @Test
    void snapshotReadDuringWritesIsConsistent() throws Exception {
        RingBuffer[] shards = fourShards();
        AtomicReference<Throwable> error = new AtomicReference<>();

        Thread writer = new Thread(() -> {
            for (int i = 0; i < 10_000; i++) {
                Bucket bucket = shards[0].acquire(TS, NOW);
                bucket.observe(TS, b("user-" + (i % 200)), b("url-hot"), 1000);
            }
        });
        QueryService service = new QueryService(shards);
        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < 300; i++) {
                    QueryResult top = service.queryTopK(NOW, 60_000, true, 10, NOW);
                    StatsResult stats = service.queryStats(NOW, 60_000, NOW);
                    if (top.items() == null || stats.distinctUsers() < 0) {
                        throw new IllegalStateException("invalid query result");
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
        assertEquals("url-hot", finalTop.items().get(0).name());
    }
}