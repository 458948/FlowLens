package io.flowlens.dedup;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/**
 * Sliding Bloom Filter for event_id deduplication.
 *
 * Three 30-second slices form a 60-90 second retention window. Each slice has a
 * fixed bit array and a per-slice capacity (default 1,800,000 event ids) at a
 * target per-slice false positive rate of 1e-6 with 20 hash probes.
 *
 * This is probabilistic deduplication, not exact deduplication, and all state
 * is lost on process restart.
 *
 * Saturation semantics: an accepted event is always written to the current
 * slice, even once the slice has reached its design capacity. When that
 * happens the slice is marked saturated and the filter is degraded. Degraded
 * status clears only after every saturated slice has been recycled out of the
 * active window.
 *
 * Slice rotation is driven by a monotonic time source (default: an anchor on
 * System.nanoTime). The business event timestamp is not used for rotation, and
 * the nowMs parameters on the public methods are retained only for API
 * compatibility. A stale time value can never rewind a newer slice.
 */
public final class SlidingBloomFilter {

    public static final int SLICES = 3;
    public static final long SLICE_DURATION_MS = 30_000L;
    public static final int DEFAULT_CAPACITY_PER_SLICE = 1_800_000;
    public static final int PROBES = 20;
    public static final double TARGET_FP = 1e-6;

    private static final long ANCHOR_NANO = System.nanoTime();
    private static final long ANCHOR_EPOCH_MS = System.currentTimeMillis();

    private final int capacityPerSlice;
    private final int bitsPerSlice;
    private final int longsPerSlice;
    private final LongSupplier timeSource;

    private final Slice[] slices = new Slice[SLICES];
    private final AtomicLong monotonicNowMs = new AtomicLong(Long.MIN_VALUE);

    public SlidingBloomFilter() {
        this(DEFAULT_CAPACITY_PER_SLICE);
    }

    public SlidingBloomFilter(int capacityPerSlice) {
        this(capacityPerSlice, SlidingBloomFilter::monotonicEpochMs);
    }

    public SlidingBloomFilter(int capacityPerSlice, LongSupplier timeSource) {
        if (capacityPerSlice <= 0) {
            throw new IllegalArgumentException("capacityPerSlice must be positive");
        }
        if (timeSource == null) {
            throw new IllegalArgumentException("timeSource must not be null");
        }
        this.capacityPerSlice = capacityPerSlice;
        this.bitsPerSlice = computeBits(capacityPerSlice);
        this.longsPerSlice = (bitsPerSlice + 63) / 64;
        this.timeSource = timeSource;
        for (int i = 0; i < SLICES; i++) {
            slices[i] = new Slice(0L, longsPerSlice);
        }
    }

    public boolean mightContain(long hi, long lo, long nowMs) {
        currentSlice();
        for (Slice s : slices) {
            ReentrantReadWriteLock.ReadLock r = s.lock.readLock();
            r.lock();
            try {
                if (contains(s, hi, lo)) {
                    return true;
                }
            } finally {
                r.unlock();
            }
        }
        return false;
    }

    /**
     * Marks a fingerprint in the current slice. An accepted event is always
     * written, even when the slice is at capacity; saturation is recorded
     * without rejecting the already-accepted event.
     */
    public boolean add(long hi, long lo, long nowMs) {
        while (true) {
            long needed = Math.floorDiv(currentMonotonicMs(), SLICE_DURATION_MS) * SLICE_DURATION_MS;
            Slice s = currentSlice();
            ReentrantReadWriteLock.ReadLock r = s.lock.readLock();
            r.lock();
            try {
                if (s.startMs != needed) {
                    continue;
                }
                setBits(s, hi, lo);
                long n = s.insertions.incrementAndGet();
                if (n >= capacityPerSlice) {
                    s.saturated = true;
                }
                return true;
            } finally {
                r.unlock();
            }
        }
    }

    public boolean isDegraded() {
        for (Slice s : slices) {
            if (s.saturated) {
                return true;
            }
        }
        return false;
    }

    public long insertionsInCurrentSlice(long nowMs) {
        return currentSlice().insertions.get();
    }

    public void ensureRotation(long nowMs) {
        currentSlice();
    }

    public int capacityPerSlice() {
        return capacityPerSlice;
    }

    public int bitsPerSlice() {
        return bitsPerSlice;
    }

    public int longsPerSlice() {
        return longsPerSlice;
    }

    private long currentMonotonicMs() {
        long now = timeSource.getAsLong();
        long cur;
        do {
            cur = monotonicNowMs.get();
            if (now <= cur) {
                return cur;
            }
        } while (!monotonicNowMs.compareAndSet(cur, now));
        return now;
    }

    private Slice currentSlice() {
        long t = currentMonotonicMs();
        long needed = Math.floorDiv(t, SLICE_DURATION_MS) * SLICE_DURATION_MS;
        long bucket = Math.floorDiv(t, SLICE_DURATION_MS);
        int idx = (int) Math.floorMod(bucket, SLICES);
        Slice s = slices[idx];
        if (s.startMs != needed) {
            s.recycle(needed);
        }
        return s;
    }

    private boolean contains(Slice s, long hi, long lo) {
        long h2 = lo | 1L;
        for (int i = 0; i < PROBES; i++) {
            long g = hi + (long) i * h2;
            int bit = (int) Long.remainderUnsigned(g, bitsPerSlice);
            int word = bit >>> 6;
            long mask = 1L << (bit & 63);
            if ((s.bits.get(word) & mask) == 0) {
                return false;
            }
        }
        return true;
    }

    private void setBits(Slice s, long hi, long lo) {
        long h2 = lo | 1L;
        for (int i = 0; i < PROBES; i++) {
            long g = hi + (long) i * h2;
            int bit = (int) Long.remainderUnsigned(g, bitsPerSlice);
            int word = bit >>> 6;
            long mask = 1L << (bit & 63);
            while (true) {
                long cur = s.bits.get(word);
                if ((cur & mask) != 0) {
                    break;
                }
                if (s.bits.compareAndSet(word, cur, cur | mask)) {
                    break;
                }
            }
        }
    }

    private static long monotonicEpochMs() {
        return ANCHOR_EPOCH_MS + (System.nanoTime() - ANCHOR_NANO) / 1_000_000L;
    }

    private static int computeBits(int capacity) {
        double pPow = Math.exp(Math.log(TARGET_FP) / PROBES);
        double denom = -Math.log(1.0 - pPow);
        long m = (long) Math.ceil(PROBES * (double) capacity / denom);
        if ((m & 1L) == 0) {
            m++;
        }
        return (int) m;
    }

    private static final class Slice {
        private final int longs;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private volatile long startMs;
        private volatile AtomicLongArray bits;
        private final AtomicLong insertions = new AtomicLong();
        private volatile boolean saturated;

        Slice(long startMs, int longs) {
            this.longs = longs;
            this.startMs = startMs;
            this.bits = new AtomicLongArray(longs);
        }

        void recycle(long newStartMs) {
            ReentrantReadWriteLock.WriteLock w = lock.writeLock();
            w.lock();
            try {
                bits = new AtomicLongArray(longs);
                insertions.set(0);
                saturated = false;
                startMs = newStartMs;
            } finally {
                w.unlock();
            }
        }
    }
}