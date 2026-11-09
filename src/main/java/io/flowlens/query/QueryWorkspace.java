package io.flowlens.query;

import io.flowlens.sketch.HyperLogLog;
import io.flowlens.sketch.LogHistogram;
import io.flowlens.sketch.TopKMerger;
import io.flowlens.window.Bucket;
import java.util.Arrays;
import java.util.concurrent.locks.StampedLock;

/**
 * Global, fixed-capacity query workspace. Exactly one instance exists per
 * process, owned by QueryService. It does not grow with the number of queries
 * and must be reset before each use.
 */
public final class QueryWorkspace {

    public static final int MAX_CANDIDATES = 61_440;
    public static final int MERGE_TABLE = 131_072;

    private final TopKMerger urlMerger = new TopKMerger(MAX_CANDIDATES, MERGE_TABLE, 256);
    private final TopKMerger userMerger = new TopKMerger(MAX_CANDIDATES, MERGE_TABLE, 64);
    private final byte[] hllRegisters = new byte[HyperLogLog.M];
    private final long[] histogramBins = new long[LogHistogram.BINS];

    public TopKMerger urlMerger() {
        return urlMerger;
    }

    public TopKMerger userMerger() {
        return userMerger;
    }

    public void resetStats() {
        Arrays.fill(hllRegisters, (byte) 0);
        Arrays.fill(histogramBins, 0L);
    }

    /** Merges a bucket under its read lock without an optimistic-copy retry. */
    public void mergeStats(Bucket b) {
        StampedLock lock = b.lock();
        long stamp = lock.readLock();
        try {
            copyStats(b);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /** Safely merges a bucket's HLL and histogram into the scratch state. */
    public void readStats(Bucket b) {
        StampedLock lock = b.lock();
        while (true) {
            long stamp = lock.tryOptimisticRead();
            copyStats(b);
            if (lock.validate(stamp)) {
                return;
            }
            stamp = lock.readLock();
            try {
                copyStats(b);
                return;
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }

    private void copyStats(Bucket b) {
        byte[] reg = b.hll().registers();
        long[] bins = b.histogram().bins();
        for (int i = 0; i < reg.length; i++) {
            int r = reg[i] & 0xff;
            if (r > (hllRegisters[i] & 0xff)) {
                hllRegisters[i] = reg[i];
            }
        }
        for (int i = 0; i < bins.length; i++) {
            histogramBins[i] += bins[i];
        }
    }

    public double distinctUsers() {
        int m = HyperLogLog.M;
        double alpha = 0.7213 / (1.0 + 1.079 / m);
        double sum = 0.0;
        int zeros = 0;
        for (int i = 0; i < m; i++) {
            int r = hllRegisters[i] & 0xff;
            if (r == 0) {
                zeros++;
            }
            sum += 1.0 / (1L << r);
        }
        double e = alpha * m * m / sum;
        if (e <= 2.5 * m && zeros > 0) {
            e = m * Math.log((double) m / zeros);
        }
        return e;
    }

    public double p95Bytes() {
        long total = 0;
        for (long b : histogramBins) {
            total += b;
        }
        if (total == 0) {
            return 0.0;
        }
        long target = (long) Math.ceil(0.95 * total);
        long cumulative = 0;
        for (int i = 0; i < histogramBins.length; i++) {
            long before = cumulative;
            cumulative += histogramBins[i];
            if (cumulative >= target) {
                long cnt = histogramBins[i];
                if (i == 0 || cnt == 0) {
                    return LogHistogram.lowerBound(i);
                }
                double frac = (double) (target - before) / cnt;
                return LogHistogram.lowerBound(i) * Math.pow(2.0, frac / 8.0);
            }
        }
        return LogHistogram.upperBound(LogHistogram.BINS - 1);
    }
}