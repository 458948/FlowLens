package io.flowlens.ingest;

import io.flowlens.dedup.SlidingBloomFilter;
import io.flowlens.hash.SipHash128;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Stripe-locked dedup admission across one or more shards.
 *
 * The fingerprint is computed once per call into a local array; the same
 * immutable hi/lo values select the shard, the stripe lock, the bloom lookup
 * and the bloom mark. Enqueue happens before marking, and a failed enqueue
 * never writes the bloom filter, so retrying a batch is idempotent.
 */
public final class DedupAdmission {

    private static final int STRIPES = 256;

    private final SlidingBloomFilter[] blooms;
    private final ReentrantLock[] locks = new ReentrantLock[STRIPES];

    public DedupAdmission(SlidingBloomFilter... blooms) {
        if (blooms == null || blooms.length == 0) {
            throw new IllegalArgumentException("at least one bloom filter is required");
        }
        this.blooms = blooms.clone();
        for (int i = 0; i < STRIPES; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    public Outcome admit(byte[] eventId, int off, int len, long nowMs, ShardEnqueue tryEnqueue) {
        long[] fp = new long[2];
        SipHash128.INSTANCE.hash(eventId, off, len, fp);
        long hi = fp[0];
        long lo = fp[1];
        int shard = Math.floorMod(hi, blooms.length);

        ReentrantLock lock = locks[(int) ((hi ^ (hi >>> 33)) & (STRIPES - 1))];
        lock.lock();
        try {
            SlidingBloomFilter bloom = blooms[shard];
            if (bloom.isDegraded()) {
                return Outcome.QUEUE_FULL;
            }
            if (bloom.mightContain(hi, lo, nowMs)) {
                return Outcome.DEDUPED;
            }
            if (tryEnqueue.tryEnqueue(shard)) {
                bloom.add(hi, lo, nowMs);
                return Outcome.ACCEPTED;
            }
            return Outcome.QUEUE_FULL;
        } finally {
            lock.unlock();
        }
    }

    /** Deterministic shard routing for an event id, based on its 128-bit fingerprint. */
    public int shard(byte[] eventId, int off, int len) {
        long[] fp = new long[2];
        SipHash128.INSTANCE.hash(eventId, off, len, fp);
        return Math.floorMod(fp[0], blooms.length);
    }

    public static int shard(byte[] eventId, int off, int len, int shards) {
        long[] fp = new long[2];
        SipHash128.INSTANCE.hash(eventId, off, len, fp);
        return Math.floorMod(fp[0], shards);
    }

    public int shardCount() {
        return blooms.length;
    }

    public boolean anyBloomDegraded() {
        for (SlidingBloomFilter bloom : blooms) {
            if (bloom.isDegraded()) {
                return true;
            }
        }
        return false;
    }

    public enum Outcome {
        ACCEPTED,
        DEDUPED,
        QUEUE_FULL
    }
}