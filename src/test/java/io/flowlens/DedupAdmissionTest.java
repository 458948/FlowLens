package io.flowlens;

import io.flowlens.dedup.SlidingBloomFilter;
import io.flowlens.ingest.DedupAdmission;
import io.flowlens.ingest.DedupAdmission.Outcome;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DedupAdmissionTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void concurrentSameIdIsAcceptedExactlyOnce() throws Exception {
        DedupAdmission admission = new DedupAdmission(new SlidingBloomFilter());
        byte[] id = b("shared-event-id");
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger deduped = new AtomicInteger();

        runConcurrently(8, () -> {
            Outcome o = admission.admit(id, 0, id.length, 0, shard -> true);
            if (o == Outcome.ACCEPTED) {
                accepted.incrementAndGet();
            } else if (o == Outcome.DEDUPED) {
                deduped.incrementAndGet();
            }
        });

        assertEquals(1, accepted.get());
        assertEquals(7, deduped.get());
    }

    @Test
    void concurrentDifferentIdsAreEachAcceptedOnce() throws Exception {
        DedupAdmission admission = new DedupAdmission(new SlidingBloomFilter());
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger deduped = new AtomicInteger();

        int n = 64;
        AtomicInteger idx = new AtomicInteger();
        runConcurrently(n, () -> {
            byte[] id = b("distinct-" + idx.getAndIncrement());
            Outcome o = admission.admit(id, 0, id.length, 0, shard -> true);
            if (o == Outcome.ACCEPTED) {
                accepted.incrementAndGet();
            } else if (o == Outcome.DEDUPED) {
                deduped.incrementAndGet();
            }
        });

        assertEquals(n, accepted.get());
        assertEquals(0, deduped.get());
    }

    @Test
    void mixedConcurrentSameAndDifferentIds() throws Exception {
        DedupAdmission admission = new DedupAdmission(new SlidingBloomFilter());
        byte[] shared = b("shared-event-id");
        AtomicInteger sharedAccepted = new AtomicInteger();
        AtomicInteger sharedDeduped = new AtomicInteger();
        AtomicInteger distinctAccepted = new AtomicInteger();

        int distinct = 32;
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            tasks.add(() -> {
                Outcome o = admission.admit(shared, 0, shared.length, 0, shard -> true);
                if (o == Outcome.ACCEPTED) {
                    sharedAccepted.incrementAndGet();
                } else if (o == Outcome.DEDUPED) {
                    sharedDeduped.incrementAndGet();
                }
            });
        }
        for (int i = 0; i < distinct; i++) {
            final int idx = i;
            tasks.add(() -> {
                byte[] id = b("distinct-" + idx);
                Outcome o = admission.admit(id, 0, id.length, 0, shard -> true);
                if (o == Outcome.ACCEPTED) {
                    distinctAccepted.incrementAndGet();
                }
            });
        }

        runTasks(tasks);

        assertEquals(1, sharedAccepted.get());
        assertEquals(7, sharedDeduped.get());
        assertEquals(distinct, distinctAccepted.get());
    }

    @Test
    void sameEventIdAlwaysRoutesToSameShard() {
        byte[] id = b("event-id-for-routing");
        int first = DedupAdmission.shard(id, 0, id.length, 4);
        for (int i = 0; i < 1000; i++) {
            assertEquals(first, DedupAdmission.shard(id, 0, id.length, 4));
        }
        assertTrue(first >= 0 && first < 4);
    }

    @Test
    void failedEnqueueDoesNotMarkBloomAndRetrySucceeds() {
        DedupAdmission admission = new DedupAdmission(new SlidingBloomFilter());
        byte[] a = b("event-a");
        byte[] b = b("event-b");

        AtomicInteger queue = new AtomicInteger(0);
        Outcome oa = admission.admit(a, 0, a.length, 0, shard -> queue.compareAndSet(0, 1));
        assertEquals(Outcome.ACCEPTED, oa);
        assertEquals(1, queue.get());

        Outcome ob = admission.admit(b, 0, b.length, 0, shard -> queue.compareAndSet(0, 1));
        assertEquals(Outcome.QUEUE_FULL, ob);
        assertEquals(1, queue.get());

        queue.set(0);
        Outcome obRetry = admission.admit(b, 0, b.length, 0, shard -> queue.compareAndSet(0, 1));
        assertEquals(Outcome.ACCEPTED, obRetry);
        assertEquals(1, queue.get());

        Outcome oaRetry = admission.admit(a, 0, a.length, 0, shard -> true);
        assertEquals(Outcome.DEDUPED, oaRetry);
    }

    private static void runConcurrently(int threads, Runnable task) throws Exception {
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(task);
        }
        runTasks(tasks);
    }

    private static void runTasks(List<Runnable> tasks) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
        CountDownLatch ready = new CountDownLatch(tasks.size());
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (Runnable task : tasks) {
            futures.add(executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                task.run();
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