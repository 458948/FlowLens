package io.flowlens;

import io.flowlens.dedup.SlidingBloomFilter;
import io.flowlens.hash.SipHash128;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlidingBloomFilterTest {

    private static void fp(String s, long[] out) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        SipHash128.INSTANCE.hash(b, 0, b.length, out);
    }

    private static SlidingBloomFilter withClock(int capacity, AtomicLong clock) {
        return new SlidingBloomFilter(capacity, clock::get);
    }

    @Test
    void detectsDuplicatesWithinRetention() {
        SlidingBloomFilter bloom = new SlidingBloomFilter();
        long[] a = new long[2];
        long[] other = new long[2];
        fp("event-a", a);
        fp("event-other", other);

        assertTrue(bloom.add(a[0], a[1], 0));
        assertTrue(bloom.mightContain(a[0], a[1], 0));
        assertFalse(bloom.mightContain(other[0], other[1], 0));
    }

    @Test
    void falsePositiveRateIsBounded() {
        SlidingBloomFilter bloom = new SlidingBloomFilter();
        int insert = 100_000;
        long[] out = new long[2];
        for (int i = 0; i < insert; i++) {
            fp("seen-" + i, out);
            bloom.add(out[0], out[1], 0);
        }
        int checks = 200_000;
        int falsePositives = 0;
        for (int i = 0; i < checks; i++) {
            fp("unseen-" + i, out);
            if (bloom.mightContain(out[0], out[1], 0)) {
                falsePositives++;
            }
        }
        assertTrue(falsePositives <= 20, "too many false positives: " + falsePositives);
    }

    @Test
    void rotationExpiresOldSlices() {
        AtomicLong clock = new AtomicLong(0);
        SlidingBloomFilter bloom = withClock(SlidingBloomFilter.DEFAULT_CAPACITY_PER_SLICE, clock);
        long[] a = new long[2];
        long[] b = new long[2];
        fp("old-event", a);
        fp("recent-event", b);

        clock.set(0);
        bloom.add(a[0], a[1], 0);
        clock.set(60_000);
        bloom.add(b[0], b[1], 0);
        clock.set(90_000);
        bloom.ensureRotation(0);

        assertFalse(bloom.mightContain(a[0], a[1], 0), "old slice should be expired");
        assertTrue(bloom.mightContain(b[0], b[1], 0), "recent slice should be retained");
    }

    @Test
    void saturatesAndDegrades() {
        SlidingBloomFilter bloom = new SlidingBloomFilter(10, () -> 0L);
        long[] out = new long[2];
        for (int i = 0; i < 10; i++) {
            fp("sat-" + i, out);
            bloom.add(out[0], out[1], 0);
        }
        assertTrue(bloom.isDegraded());
    }

    @Test
    void staysDegradedWhileSaturatedSliceActive() {
        AtomicLong clock = new AtomicLong(0);
        SlidingBloomFilter bloom = withClock(10, clock);
        long[] out = new long[2];
        for (int i = 0; i < 10; i++) {
            fp("sat-" + i, out);
            bloom.add(out[0], out[1], 0);
        }
        clock.set(30_000);
        bloom.ensureRotation(0);
        assertTrue(bloom.isDegraded());
    }

    @Test
    void recoversAfterSaturatedSliceExpires() {
        AtomicLong clock = new AtomicLong(0);
        SlidingBloomFilter bloom = withClock(10, clock);
        long[] out = new long[2];
        for (int i = 0; i < 10; i++) {
            fp("sat-" + i, out);
            bloom.add(out[0], out[1], 0);
        }
        clock.set(30_000);
        bloom.ensureRotation(0);
        clock.set(60_000);
        bloom.ensureRotation(0);
        clock.set(90_000);
        bloom.ensureRotation(0);

        assertFalse(bloom.isDegraded());
        fp("fresh", out);
        assertTrue(bloom.add(out[0], out[1], 0));
        assertFalse(bloom.isDegraded());
    }

    @Test
    void acceptedEventsAreAlwaysMarkedEvenAtCapacity() {
        SlidingBloomFilter bloom = new SlidingBloomFilter(10, () -> 0L);
        long[] out = new long[2];
        for (int i = 0; i < 10; i++) {
            fp("sat-" + i, out);
            bloom.add(out[0], out[1], 0);
        }
        fp("extra", out);
        assertTrue(bloom.add(out[0], out[1], 0));
        assertTrue(bloom.mightContain(out[0], out[1], 0));
        assertTrue(bloom.isDegraded());
    }

    @Test
    void timeRewindDoesNotClearNewerSlice() {
        AtomicLong clock = new AtomicLong(0);
        SlidingBloomFilter bloom = withClock(1000, clock);
        long[] a = new long[2];
        fp("recent", a);
        clock.set(90_000);
        bloom.add(a[0], a[1], 0);
        clock.set(0);
        bloom.ensureRotation(0);
        assertTrue(bloom.mightContain(a[0], a[1], 0));
    }

    @Test
    void concurrentAddAndRotationDoesNotCorrupt() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        SlidingBloomFilter bloom = withClock(SlidingBloomFilter.DEFAULT_CAPACITY_PER_SLICE, clock);
        int threads = 4;
        int perThread = 10_000;

        Thread rotator = new Thread(() -> {
            for (int w = 30_000; w <= 120_000; w += 30_000) {
                clock.set(w);
                bloom.ensureRotation(0);
                Thread.yield();
            }
        });

        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
                long[] out = new long[2];
                for (int i = 0; i < perThread; i++) {
                    fp("t" + tid + "-" + i, out);
                    bloom.add(out[0], out[1], 0);
                }
            });
        }

        rotator.start();
        for (Thread w : workers) {
            w.start();
        }
        rotator.join();
        for (Thread w : workers) {
            w.join();
        }
        assertFalse(bloom.isDegraded());
    }

    @Test
    void concurrentWritesDifferentIdsHaveNoFalseNegative() throws Exception {
        SlidingBloomFilter bloom = new SlidingBloomFilter();
        int threads = 8;
        int perThread = 2_000;

        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
                long[] out = new long[2];
                for (int i = 0; i < perThread; i++) {
                    fp("w" + tid + "-" + i, out);
                    bloom.add(out[0], out[1], 0);
                }
            });
        }
        for (Thread w : workers) {
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }

        long[] check = new long[2];
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                fp("w" + t + "-" + i, check);
                assertTrue(bloom.mightContain(check[0], check[1], 0),
                        "confirmed event must not be a false negative: w" + t + "-" + i);
            }
        }
    }

    @Test
    void concurrentQueriesDifferentIds() throws Exception {
        SlidingBloomFilter bloom = new SlidingBloomFilter();
        long[] out = new long[2];
        for (int i = 0; i < 100; i++) {
            fp("q" + i, out);
            bloom.add(out[0], out[1], 0);
        }

        int threads = 8;
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                long[] q = new long[2];
                for (int i = 0; i < 100; i++) {
                    fp("q" + i, q);
                    assertTrue(bloom.mightContain(q[0], q[1], 0));
                }
            });
        }
        for (Thread w : workers) {
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }
    }

    @Test
    void nearCapacityNeverAcceptsWithoutMarking() throws Exception {
        SlidingBloomFilter bloom = new SlidingBloomFilter(1000, () -> 0L);
        int threads = 8;
        int perThread = 200;

        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
                long[] out = new long[2];
                for (int i = 0; i < perThread; i++) {
                    fp("near-" + tid + "-" + i, out);
                    assertTrue(bloom.add(out[0], out[1], 0));
                }
            });
        }
        for (Thread w : workers) {
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }

        long[] check = new long[2];
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                fp("near-" + t + "-" + i, check);
                assertTrue(bloom.mightContain(check[0], check[1], 0),
                        "accepted event must be marked: near-" + t + "-" + i);
            }
        }
        assertTrue(bloom.isDegraded());
    }

    @Test
    void monotonicForwardAndOldTimeInput() {
        AtomicLong clock = new AtomicLong(0);
        SlidingBloomFilter bloom = withClock(SlidingBloomFilter.DEFAULT_CAPACITY_PER_SLICE, clock);
        long[] old = new long[2];
        long[] mid = new long[2];
        long[] recent = new long[2];
        fp("old", old);
        fp("mid", mid);
        fp("recent", recent);

        clock.set(0);
        bloom.add(old[0], old[1], 0);
        clock.set(30_000);
        bloom.add(mid[0], mid[1], 0);
        clock.set(60_000);
        bloom.add(recent[0], recent[1], 0);

        clock.set(90_000);
        bloom.ensureRotation(0);
        assertFalse(bloom.mightContain(old[0], old[1], 0));
        assertTrue(bloom.mightContain(mid[0], mid[1], 0));
        assertTrue(bloom.mightContain(recent[0], recent[1], 0));

        clock.set(30_000);
        bloom.ensureRotation(0);
        assertTrue(bloom.mightContain(recent[0], recent[1], 0));
        assertFalse(bloom.mightContain(old[0], old[1], 0));

        clock.set(120_000);
        bloom.ensureRotation(0);
        assertFalse(bloom.mightContain(mid[0], mid[1], 0));
        assertTrue(bloom.mightContain(recent[0], recent[1], 0));
    }

    @Test
    void concurrentCapacityOverAcceptIsMeasuredAndMarked() throws Exception {
        SlidingBloomFilter bloom = new SlidingBloomFilter(1000, () -> 0L);
        int threads = 8;
        int perThread = 200;

        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
                long[] out = new long[2];
                for (int i = 0; i < perThread; i++) {
                    fp("cap-" + tid + "-" + i, out);
                    bloom.add(out[0], out[1], 0);
                }
            });
        }
        for (Thread w : workers) {
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }

        assertTrue(bloom.isDegraded());
        assertEquals(threads * perThread, bloom.insertionsInCurrentSlice(0));

        long[] check = new long[2];
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                fp("cap-" + t + "-" + i, check);
                assertTrue(bloom.mightContain(check[0], check[1], 0),
                        "accepted event must be marked: cap-" + t + "-" + i);
            }
        }
    }

    @Test
    void duplicateInRetentionStillDedupedAcrossDegradationAndRecovery() {
        AtomicLong clock = new AtomicLong(0);
        SlidingBloomFilter bloom = withClock(10, clock);
        long[] out = new long[2];
        long[] dup = new long[2];
        fp("dup", dup);

        for (int i = 0; i < 8; i++) {
            fp("r" + i, out);
            bloom.add(out[0], out[1], 0);
        }
        bloom.add(dup[0], dup[1], 0);
        assertTrue(bloom.mightContain(dup[0], dup[1], 0));

        fp("s", out);
        bloom.add(out[0], out[1], 0);
        assertTrue(bloom.isDegraded());
        assertTrue(bloom.mightContain(dup[0], dup[1], 0), "duplicate in active retention must still be recognized while degraded");

        clock.set(90_000);
        bloom.ensureRotation(0);
        assertFalse(bloom.isDegraded());

        long[] fresh = new long[2];
        fp("fresh-dup", fresh);
        bloom.add(fresh[0], fresh[1], 0);
        assertTrue(bloom.mightContain(fresh[0], fresh[1], 0));
    }
}
