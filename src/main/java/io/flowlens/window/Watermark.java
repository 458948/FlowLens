package io.flowlens.window;

/** Time-window helpers shared by the ring buffer and query path. */
public final class Watermark {

    public static final long MAX_LATENESS_MS = 60_000L;
    public static final long BUCKET_MS = 1_000L;
    public static final long MAX_WINDOW_MS = 60_000L;

    private Watermark() {
    }

    public static long watermark(long nowMs) {
        return nowMs - MAX_LATENESS_MS;
    }

    public static boolean isLegal(long ts, long nowMs) {
        return ts >= watermark(nowMs) && ts <= nowMs;
    }

    public static long bucketStart(long ts) {
        return Math.floorDiv(ts, BUCKET_MS) * BUCKET_MS;
    }

    public static long bucketEnd(long bucketStart) {
        return bucketStart + BUCKET_MS;
    }

    public static boolean isClosed(long bucketStart, long nowMs) {
        return bucketEnd(bucketStart) <= watermark(nowMs);
    }

    public static long effectiveAt(long atMs) {
        return Math.floorDiv(atMs, BUCKET_MS) * BUCKET_MS;
    }
}