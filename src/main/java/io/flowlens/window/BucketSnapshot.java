package io.flowlens.window;

import io.flowlens.sketch.SpaceSaving;
import java.util.concurrent.locks.StampedLock;

/**
 * Fixed-capacity per-bucket snapshot buffer.
 *
 * read() copies a consistent view of a SpaceSaving sketch under the bucket's
 * StampedLock using optimistic reads with validation, falling back to a read
 * lock. Once read() returns, the copied bytes are stable for immediate merge;
 * the caller must merge them before reusing the buffer for another bucket.
 */
public final class BucketSnapshot {

    public static final int CAPACITY = 256;
    public static final int MAX_KEY = 256;

    public final long[] hi = new long[CAPACITY];
    public final long[] lo = new long[CAPACITY];
    public final long[] count = new long[CAPACITY];
    public final long[] error = new long[CAPACITY];
    public final int[] nameLen = new int[CAPACITY];
    public final byte[] arena = new byte[CAPACITY * MAX_KEY];

    public int size;
    public long observations;

    public void read(SpaceSaving ss, StampedLock lock) {
        while (true) {
            long stamp = lock.tryOptimisticRead();
            readInto(ss);
            if (lock.validate(stamp)) {
                return;
            }
            stamp = lock.readLock();
            try {
                readInto(ss);
                return;
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }

    private void readInto(SpaceSaving ss) {
        size = ss.size();
        observations = ss.observations();
        byte[] src = ss.nameBytes();
        for (int i = 0; i < size; i++) {
            hi[i] = ss.fpHi(i);
            lo[i] = ss.fpLo(i);
            count[i] = ss.count(i);
            error[i] = ss.error(i);
            int len = ss.nameLength(i);
            nameLen[i] = len;
            System.arraycopy(src, ss.nameOffset(i), arena, i * MAX_KEY, len);
        }
    }
}