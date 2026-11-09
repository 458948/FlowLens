package io.flowlens.query;

import io.flowlens.sketch.SpaceSaving;
import io.flowlens.sketch.TopKEntry;
import io.flowlens.sketch.TopKMerger;
import io.flowlens.window.Bucket;
import io.flowlens.window.QueryResult;
import io.flowlens.window.RingBuffer;
import io.flowlens.window.StatsResult;
import io.flowlens.window.Watermark;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

/**
 * Global query service that aggregates the four shard ring buffers.
 *
 * All shards are read with the same effective_at and window. A single
 * ReentrantLock serializes access to the single shared QueryWorkspace, which is
 * required because the query workspace contains reusable fixed arrays.
 *
 * Query reads take each bucket's StampedLock read lock while copying or merging
 * a single bucket's sketch. This avoids an extra full-snapshot buffer for
 * Top-K and avoids optimistic-read retries while still never observing a
 * half-written bucket.
 */
public final class QueryService {

    private final RingBuffer[] shards;
    private final QueryWorkspace workspace = new QueryWorkspace();
    private final ReentrantLock lock = new ReentrantLock();

    public QueryService(RingBuffer... shards) {
        if (shards == null || shards.length == 0) {
            throw new IllegalArgumentException("at least one shard is required");
        }
        this.shards = shards.clone();
    }

    public QueryResult queryTopK(long atMs, long windowMs, boolean byUrl, int k, long nowMs) {
        lock.lock();
        try {
            validateWindow(windowMs);
            long effectiveAt = Watermark.effectiveAt(atMs);
            long start = effectiveAt - windowMs;
            long end = effectiveAt;
            boolean stable = effectiveAt <= Watermark.watermark(nowMs);

            TopKMerger merger = byUrl ? workspace.urlMerger() : workspace.userMerger();
            merger.reset();
            long totalObservations = 0;

            for (RingBuffer shard : shards) {
                shard.requireRetained(start, nowMs);
                for (long bs = start; bs < end; bs += Watermark.BUCKET_MS) {
                    Bucket bucket = shard.bucketFor(bs);
                    if (bucket == null) {
                        continue;
                    }
                    totalObservations += mergeTopKBucket(bucket, byUrl, merger);
                }
            }

            List<TopKEntry> items = merger.top(k, totalObservations, RingBuffer.CAPACITY);
            return new QueryResult(effectiveAt, stable, items);
        } finally {
            lock.unlock();
        }
    }

    public StatsResult queryStats(long atMs, long windowMs, long nowMs) {
        lock.lock();
        try {
            validateWindow(windowMs);
            long effectiveAt = Watermark.effectiveAt(atMs);
            long start = effectiveAt - windowMs;
            long end = effectiveAt;
            boolean stable = effectiveAt <= Watermark.watermark(nowMs);

            workspace.resetStats();
            for (RingBuffer shard : shards) {
                shard.requireRetained(start, nowMs);
                for (long bs = start; bs < end; bs += Watermark.BUCKET_MS) {
                    Bucket bucket = shard.bucketFor(bs);
                    if (bucket == null) {
                        continue;
                    }
                    workspace.mergeStats(bucket);
                }
            }

            return new StatsResult(effectiveAt, stable, workspace.distinctUsers(), workspace.p95Bytes());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Merges one bucket's URL or user sketch directly into the reusable merger
     * while holding the bucket read lock. Returns the bucket's observation count.
     */
    private static long mergeTopKBucket(Bucket bucket, boolean byUrl, TopKMerger merger) {
        SpaceSaving ss = byUrl ? bucket.urlSketch() : bucket.userSketch();
        StampedLock bucketLock = bucket.lock();
        long stamp = bucketLock.readLock();
        try {
            int n = ss.size();
            byte[] arena = ss.nameBytes();
            for (int i = 0; i < n; i++) {
                merger.merge(ss.fpHi(i), ss.fpLo(i), ss.count(i), ss.error(i),
                        arena, ss.nameOffset(i), ss.nameLength(i));
            }
            return ss.observations();
        } finally {
            bucketLock.unlockRead(stamp);
        }
    }

    private static void validateWindow(long windowMs) {
        if (windowMs < 1 || windowMs > Watermark.MAX_WINDOW_MS) {
            throw new IllegalArgumentException("window must be 1..60000 ms");
        }
    }
}