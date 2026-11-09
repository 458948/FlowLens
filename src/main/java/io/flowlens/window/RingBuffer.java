package io.flowlens.window;

/**
 * 128-slot ring buffer of one-second buckets.
 *
 * Each slot records its real bucketStart; validity is never inferred from the
 * array index alone. Before an old bucket is overwritten it must be finally
 * closed by the watermark.
 *
 * This class only owns time-bucket management, event writes, watermark checks,
 * bucket closing and safe bucket lookup. All query merging is performed by the
 * global QueryService/QueryWorkspace.
 */
public final class RingBuffer {

    public static final int BUCKETS = 128;
    public static final int CAPACITY = 256;
    public static final int URL_MAX_BYTES = 256;
    public static final int USER_MAX_BYTES = 64;

    private final Slot[] slots = new Slot[BUCKETS];

    /** Returns the bucket for an event timestamp, creating or recycling a slot as needed. */
    public Bucket acquire(long ts, long nowMs) {
        if (!Watermark.isLegal(ts, nowMs)) {
            throw new IllegalArgumentException("timestamp out of legal range: ts=" + ts + " now=" + nowMs);
        }
        long start = Watermark.bucketStart(ts);
        int idx = (int) Math.floorMod(start / Watermark.BUCKET_MS, BUCKETS);
        Slot slot = slots[idx];
        if (slot == null) {
            slot = new Slot();
            slots[idx] = slot;
        }
        if (slot.valid && slot.bucketStart == start) {
            return slot.bucket;
        }
        if (slot.valid && !Watermark.isClosed(slot.bucketStart, nowMs)) {
            throw new IllegalStateException("attempt to overwrite unclosed bucket start=" + slot.bucketStart);
        }
        slot.bucket = new Bucket(start, CAPACITY, URL_MAX_BYTES, USER_MAX_BYTES);
        slot.bucketStart = start;
        slot.valid = true;
        return slot.bucket;
    }

    /** Returns the bucket for a bucketStart, or null if that second is not retained/filled. */
    public Bucket bucketFor(long bucketStart) {
        int idx = (int) Math.floorMod(bucketStart / Watermark.BUCKET_MS, BUCKETS);
        Slot slot = slots[idx];
        return (slot != null && slot.valid && slot.bucketStart == bucketStart) ? slot.bucket : null;
    }

    /** Throws if a query window start is older than the ring retention. */
    public void requireRetained(long start, long nowMs) {
        long oldest = nowMs - (long) BUCKETS * Watermark.BUCKET_MS;
        if (start < oldest) {
            throw new IllegalArgumentException("query window out of retention: start=" + start);
        }
    }

    private Slot find(long bucketStart) {
        for (Slot slot : slots) {
            if (slot != null && slot.valid && slot.bucketStart == bucketStart) {
                return slot;
            }
        }
        return null;
    }

    private static final class Slot {
        long bucketStart;
        boolean valid;
        Bucket bucket;
    }
}