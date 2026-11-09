package io.flowlens.window;

import io.flowlens.sketch.HyperLogLog;
import io.flowlens.sketch.LogHistogram;
import io.flowlens.sketch.SpaceSaving;
import java.util.concurrent.locks.StampedLock;

/** One second-long bucket. Mutated by a single writer thread. */
public final class Bucket {

    private final long bucketStart;
    private final SpaceSaving urlSketch;
    private final SpaceSaving userSketch;
    private final HyperLogLog hll;
    private final LogHistogram histogram;
    private final StampedLock lock = new StampedLock();

    public Bucket(long bucketStart, int capacity, int urlMaxBytes, int userMaxBytes) {
        this.bucketStart = bucketStart;
        this.urlSketch = new SpaceSaving(capacity, urlMaxBytes);
        this.userSketch = new SpaceSaving(capacity, userMaxBytes);
        this.hll = new HyperLogLog();
        this.histogram = new LogHistogram();
    }

    public void observe(long ts, byte[] userId, byte[] url, long bytes) {
        long stamp = lock.writeLock();
        try {
            urlSketch.observe(url, 0, url.length);
            userSketch.observe(userId, 0, userId.length);
            hll.add(userId, 0, userId.length);
            histogram.add(bytes);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public long bucketStart() {
        return bucketStart;
    }

    public SpaceSaving urlSketch() {
        return urlSketch;
    }

    public SpaceSaving userSketch() {
        return userSketch;
    }

    public HyperLogLog hll() {
        return hll;
    }

    public LogHistogram histogram() {
        return histogram;
    }

    public StampedLock lock() {
        return lock;
    }
}