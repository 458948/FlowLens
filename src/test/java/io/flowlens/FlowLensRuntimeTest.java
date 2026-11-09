package io.flowlens;

import io.flowlens.dedup.SlidingBloomFilter;
import io.flowlens.ingest.DedupAdmission;
import io.flowlens.ingest.RawEvent;
import io.flowlens.runtime.FlowLensRuntime;
import io.flowlens.runtime.IngestOutcome;
import io.flowlens.runtime.Shard;
import io.flowlens.sketch.TopKEntry;
import io.flowlens.window.QueryResult;
import io.flowlens.window.RingBuffer;
import io.flowlens.window.StatsResult;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowLensRuntimeTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RawEvent event(String id, long ts) {
        return event(id, ts, "/url-hot", "user-1");
    }

    private static RawEvent event(String id, long ts, String url, String userId) {
        return new RawEvent(b(id), b(userId), b(url), ts, 1000L);
    }

    private static long total(Shard[] shards, java.util.function.ToLongFunction<Shard> fn) {
        long sum = 0;
        for (Shard s : shards) {
            sum += fn.applyAsLong(s);
        }
        return sum;
    }

    private static void awaitTotalProcessed(Shard[] shards, long expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (total(shards, Shard::processed) >= expected) {
                return;
            }
            Thread.sleep(5);
        }
        assertEquals(expected, total(shards, Shard::processed));
    }

    @Test
    void runtimeStartsFourShards() {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            Shard[] shards = runtime.shards();
            assertEquals(4, shards.length);
            for (Shard shard : shards) {
                assertEquals(Shard.QUEUE_CAPACITY, shard.queueCapacity());
                assertTrue(shard.isWriterAlive());
            }
        } finally {
            runtime.close();
        }
    }

    @Test
    void sameEventIdAlwaysRoutesToSameShard() {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            byte[] id = b("route-me");
            int first = runtime.shardFor(id);
            for (int i = 0; i < 1000; i++) {
                assertEquals(first, runtime.shardFor(id));
            }
            assertEquals(first, DedupAdmission.shard(id, 0, id.length, 4));
        } finally {
            runtime.close();
        }
    }

    @Test
    void differentEventIdsSpreadAcrossShards() {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            Set<Integer> shards = new HashSet<>();
            for (int i = 0; i < 200; i++) {
                shards.add(runtime.shardFor(b("spread-" + i)));
            }
            assertTrue(shards.size() >= 2, "expected spread across shards but got " + shards.size());
        } finally {
            runtime.close();
        }
    }

    @Test
    void fourShardsAllProcessEvents() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            long now = System.currentTimeMillis();
            long ts = now - 1_000;
            for (int i = 0; i < 200; i++) {
                runtime.ingest(event("all-" + i, ts));
            }
            awaitTotalProcessed(runtime.shards(), 200, 5_000);
            for (Shard shard : runtime.shards()) {
                assertTrue(shard.processed() > 0);
            }
        } finally {
            runtime.close();
        }
    }

    @Test
    void sameEventIdConcurrentSubmitAcceptedOnce() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            long ts = System.currentTimeMillis() - 1_000;
            RawEvent event = event("shared-id", ts);
            int threads = 8;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<IngestOutcome>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return runtime.ingest(event);
                }));
            }
            ready.await();
            start.countDown();
            int accepted = 0;
            int deduped = 0;
            for (Future<IngestOutcome> f : futures) {
                IngestOutcome o = f.get();
                if (o == IngestOutcome.ACCEPTED) {
                    accepted++;
                } else if (o == IngestOutcome.DEDUPED) {
                    deduped++;
                }
            }
            executor.shutdown();

            awaitTotalProcessed(runtime.shards(), 1, 5_000);
            assertEquals(1, accepted);
            assertEquals(threads - 1, deduped);
            assertEquals(1, total(runtime.shards(), Shard::accepted));
            assertEquals(threads - 1, total(runtime.shards(), Shard::deduped));
        } finally {
            runtime.close();
        }
    }

    @Test
    void queueFullReturnsOverloadSignal() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        RingBuffer ring = new RingBuffer();
        Shard shard = new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
            try {
                block.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
        });
        shard.start();
        long ts = System.currentTimeMillis() - 1_000;
        try {
            int offered = 0;
            while (shard.tryEnqueue(event("fill-" + offered, ts))) {
                offered++;
            }
            assertTrue(offered >= Shard.QUEUE_CAPACITY);
            assertFalse(shard.tryEnqueue(event("overflow", ts)));
            block.countDown();
            shard.close(5_000);
            assertEquals(offered, shard.processed());
        } finally {
            block.countDown();
            shard.close(5_000);
        }
    }

    @Test
    void retryAfterDrainSucceeds() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        RingBuffer ring = new RingBuffer();
        Shard shard = new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
            try {
                block.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
        });
        shard.start();
        long ts = System.currentTimeMillis() - 1_000;
        try {
            int offered = 0;
            while (shard.tryEnqueue(event("fill-" + offered, ts))) {
                offered++;
            }
            RawEvent retry = event("retry-event", ts);
            assertFalse(shard.tryEnqueue(retry));
            block.countDown();
            long deadline = System.currentTimeMillis() + 5_000;
            while (shard.queueSize() > 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertTrue(shard.tryEnqueue(retry));
            shard.close(5_000);
        } finally {
            block.countDown();
            shard.close(5_000);
        }
    }

    @Test
    void retryDoesNotDoubleCount() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            long ts = System.currentTimeMillis() - 1_000;
            RawEvent event = event("once-only", ts);
            assertEquals(IngestOutcome.ACCEPTED, runtime.ingest(event));
            assertEquals(IngestOutcome.DEDUPED, runtime.ingest(event));
            awaitTotalProcessed(runtime.shards(), 1, 5_000);
            assertEquals(1, total(runtime.shards(), Shard::accepted));
            assertEquals(1, total(runtime.shards(), Shard::deduped));
            assertEquals(1, total(runtime.shards(), Shard::processed));
        } finally {
            runtime.close();
        }
    }

    @Test
    void allAcceptedEventsAreProcessed() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            long ts = System.currentTimeMillis() - 1_000;
            int n = 100;
            for (int i = 0; i < n; i++) {
                assertEquals(IngestOutcome.ACCEPTED, runtime.ingest(event("proc-" + i, ts)));
            }
            awaitTotalProcessed(runtime.shards(), n, 5_000);
            assertEquals(n, total(runtime.shards(), Shard::accepted));
            assertEquals(n, total(runtime.shards(), Shard::processed));
        } finally {
            runtime.close();
        }
    }

    @Test
    void writerExceptionDegradesShard() throws Exception {
        RingBuffer ring = new RingBuffer();
        Shard shard = new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
            if ("bad".equals(new String(e.eventId, StandardCharsets.UTF_8))) {
                throw new RuntimeException("boom");
            }
            r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
        });
        shard.start();
        long ts = System.currentTimeMillis() - 1_000;
        try {
            assertTrue(shard.tryEnqueue(event("bad", ts)));
            long deadline = System.currentTimeMillis() + 5_000;
            while (shard.processingErrors() < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(1, shard.processingErrors());
            assertTrue(shard.isDegraded());
        } finally {
            shard.close(5_000);
        }
    }

    @Test
    void gracefulCloseProcessesQueuedEvents() throws Exception {
        RingBuffer ring = new RingBuffer();
        Shard shard = new Shard(ring);
        shard.start();
        long ts = System.currentTimeMillis() - 1_000;
        int n = 100;
        for (int i = 0; i < n; i++) {
            assertTrue(shard.tryEnqueue(event("close-" + i, ts)));
        }
        shard.close(5_000);
        assertEquals(n, shard.processed());
        assertFalse(shard.isWriterAlive());
    }

    @Test
    void closedRuntimeRejectsNewEvents() {
        FlowLensRuntime runtime = new FlowLensRuntime();
        runtime.close();
        long ts = System.currentTimeMillis() - 1_000;
        assertEquals(IngestOutcome.CLOSED, runtime.ingest(event("after-close", ts)));
    }

    @Test
    void queryServiceCrossShardCorrect() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            long now = System.currentTimeMillis();
            long ts = now - 1_000;
            int n = 200;
            for (int i = 0; i < n; i++) {
                runtime.ingest(event("q-" + i, ts, "/url-hot", "user-" + (i % 50)));
            }
            awaitTotalProcessed(runtime.shards(), n, 5_000);

            QueryResult top = runtime.queryTopK(now, 60_000, true, 10);
            assertFalse(top.items().isEmpty());
            TopKEntry first = top.items().get(0);
            assertEquals("/url-hot", first.name());
            assertTrue(first.estimatedCount() >= n);

            StatsResult stats = runtime.queryStats(now, 60_000);
            assertTrue(stats.distinctUsers() >= 20 && stats.distinctUsers() <= 80,
                    "distinct users: " + stats.distinctUsers());
        } finally {
            runtime.close();
        }
    }

    @Test
    void concurrentIngestHasNoDataRace() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        try {
            long ts = System.currentTimeMillis() - 1_000;
            int threads = 8;
            int perThread = 25;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < perThread; i++) {
                        runtime.ingest(event("c-" + tid + "-" + i, ts));
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
            executor.shutdown();

            awaitTotalProcessed(runtime.shards(), threads * perThread, 5_000);
            assertEquals(threads * perThread, total(runtime.shards(), Shard::accepted));
            assertEquals(0, total(runtime.shards(), Shard::deduped));
        } finally {
            runtime.close();
        }
    }

    @Test
    void bloomDegradedRejectsNewIngest() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime(System::currentTimeMillis, i -> new SlidingBloomFilter(10));
        try {
            long ts = System.currentTimeMillis() - 1_000;
            for (int i = 0; i < 400; i++) {
                runtime.ingest(event("deg-" + i, ts));
            }
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                if (total(runtime.shards(), Shard::processed) >= total(runtime.shards(), Shard::accepted)) {
                    break;
                }
                Thread.sleep(5);
            }
            assertEquals(total(runtime.shards(), Shard::accepted), total(runtime.shards(), Shard::processed));
            assertTrue(runtime.isDegraded());

            IngestOutcome outcome = runtime.ingest(event("after-degraded", ts));
            assertEquals(IngestOutcome.OVERLOADED, outcome);
            assertTrue(total(runtime.shards(), Shard::overloaded) >= 1);
        } finally {
            runtime.close();
        }
    }

    private static void awaitTrue(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean());
    }

    @Test
    void realQueueFullThenRetrySucceeds() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        FlowLensRuntime runtime = new FlowLensRuntime(
                System::currentTimeMillis,
                i -> new SlidingBloomFilter(),
                (i, ring) -> new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
                    try {
                        block.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
                }));
        try {
            long ts = System.currentTimeMillis() - 1_000;
            for (int i = 0; i < 9000; i++) {
                runtime.ingest(event("fill-" + i, ts));
            }
            RawEvent failed = event("failed-id", ts);
            assertEquals(IngestOutcome.OVERLOADED, runtime.ingest(failed));

            block.countDown();
            awaitTrue(() -> total(runtime.shards(), Shard::processed) >= total(runtime.shards(), Shard::accepted), 10_000);
            assertEquals(total(runtime.shards(), Shard::accepted), total(runtime.shards(), Shard::processed));

            long before = total(runtime.shards(), Shard::accepted);
            assertEquals(IngestOutcome.ACCEPTED, runtime.ingest(failed));
            awaitTrue(() -> total(runtime.shards(), Shard::processed) >= before + 1, 5_000);
            assertEquals(before + 1, total(runtime.shards(), Shard::accepted));
            assertEquals(0, total(runtime.shards(), Shard::deduped));
        } finally {
            block.countDown();
            runtime.close();
        }
    }

    @Test
    void closeWhileConcurrentSubmitLosesNothing() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime();
        long ts = System.currentTimeMillis() - 1_000;
        AtomicInteger idx = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean(false);

        Thread producer = new Thread(() -> {
            while (!stop.get()) {
                runtime.ingest(event("prod-" + idx.getAndIncrement(), ts));
            }
        });
        producer.start();
        Thread.sleep(50);

        boolean drained = runtime.close();
        stop.set(true);
        producer.join(5_000);

        assertTrue(drained);
        assertEquals(FlowLensRuntime.State.CLOSED, runtime.state());
        assertEquals(0, runtime.unprocessedEvents());
        assertEquals(total(runtime.shards(), Shard::accepted), total(runtime.shards(), Shard::processed));
    }

    @Test
    void closeTimeoutReportsUnprocessed() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        RingBuffer ring = new RingBuffer();
        Shard shard = new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
            try {
                block.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted");
            }
            r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
        });
        shard.start();
        long ts = System.currentTimeMillis() - 1_000;
        try {
            assertTrue(shard.tryEnqueue(event("timeout-1", ts)));
            assertTrue(shard.tryEnqueue(event("timeout-2", ts)));
            Thread.sleep(50);

            boolean drained = shard.close(200);
            assertFalse(drained);
            assertTrue(shard.unprocessed() >= 1);

            long deadline = System.currentTimeMillis() + 2_000;
            while (shard.isWriterAlive() && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertFalse(shard.isWriterAlive());
        } finally {
            block.countDown();
            shard.close(5_000);
        }
    }

    @Test
    void degradedShardRejectsNewEvents() throws Exception {
        FlowLensRuntime runtime = new FlowLensRuntime(
                System::currentTimeMillis,
                i -> new SlidingBloomFilter(),
                (i, ring) -> new Shard(ring, System::currentTimeMillis, (r, now, e) -> {
                    if ("bad".equals(new String(e.eventId, StandardCharsets.UTF_8))) {
                        throw new RuntimeException("boom");
                    }
                    r.acquire(e.ts, now).observe(e.ts, e.userId, e.url, e.bytes);
                }));
        try {
            long ts = System.currentTimeMillis() - 1_000;
            assertEquals(IngestOutcome.ACCEPTED, runtime.ingest(event("bad", ts)));

            int badShard = runtime.shardFor(b("bad"));
            Shard target = runtime.shards()[badShard];
            awaitTrue(() -> target.processingErrors() >= 1, 5_000);
            assertTrue(target.isDegraded());
            assertTrue(runtime.isDegraded());

            String after = null;
            for (int i = 0; i < 10_000; i++) {
                String candidate = "after-" + i;
                if (runtime.shardFor(b(candidate)) == badShard) {
                    after = candidate;
                    break;
                }
            }
            assertNotNull(after);

            long processedBefore = total(runtime.shards(), Shard::processed);
            assertEquals(IngestOutcome.OVERLOADED, runtime.ingest(event(after, ts)));
            assertEquals(processedBefore, total(runtime.shards(), Shard::processed));
            assertTrue(target.overloaded() >= 1);
        } finally {
            runtime.close();
        }
    }
}
