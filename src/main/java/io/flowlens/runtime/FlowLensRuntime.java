package io.flowlens.runtime;

import io.flowlens.dedup.SlidingBloomFilter;
import io.flowlens.ingest.DedupAdmission;
import io.flowlens.ingest.RawEvent;
import io.flowlens.query.QueryService;
import io.flowlens.window.QueryResult;
import io.flowlens.window.RingBuffer;
import io.flowlens.window.StatsResult;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

/**
 * Unified runtime: four shards, four bloom filters, one global DedupAdmission
 * and one global QueryService.
 *
 * Lifecycle states: RUNNING -> CLOSING -> CLOSED. A read/write lock serializes
 * ingest (readers) against close (writer), so no event can be enqueued after
 * close begins and every enqueued event is drained before close reports.
 */
public final class FlowLensRuntime {

    public enum State {
        RUNNING,
        CLOSING,
        CLOSED
    }

    public static final int SHARD_COUNT = 4;
    private static final long CLOSE_TIMEOUT_MS = 5_000L;

    private final Shard[] shards = new Shard[SHARD_COUNT];
    private final SlidingBloomFilter[] blooms = new SlidingBloomFilter[SHARD_COUNT];
    private final DedupAdmission admission;
    private final QueryService queryService;
    private final LongSupplier nowMs;

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    public FlowLensRuntime() {
        this(System::currentTimeMillis, i -> new SlidingBloomFilter());
    }

    public FlowLensRuntime(LongSupplier nowMs, IntFunction<SlidingBloomFilter> bloomFactory) {
        this(nowMs, bloomFactory, (i, ring) -> new Shard(ring, nowMs, Shard::defaultProcess));
    }

    public FlowLensRuntime(LongSupplier nowMs, IntFunction<SlidingBloomFilter> bloomFactory,
                           BiFunction<Integer, RingBuffer, Shard> shardFactory) {
        this.nowMs = nowMs;
        RingBuffer[] rings = new RingBuffer[SHARD_COUNT];
        for (int i = 0; i < SHARD_COUNT; i++) {
            rings[i] = new RingBuffer();
            blooms[i] = bloomFactory.apply(i);
            shards[i] = shardFactory.apply(i, rings[i]);
        }
        this.admission = new DedupAdmission(blooms);
        this.queryService = new QueryService(rings);
        for (Shard shard : shards) {
            shard.start();
        }
    }

    public IngestOutcome ingest(RawEvent event) {
        lifecycleLock.readLock().lock();
        try {
            if (state.get() != State.RUNNING) {
                return IngestOutcome.CLOSED;
            }
            long now = nowMs.getAsLong();
            int shardIdx = admission.shard(event.eventId, 0, event.eventId.length);
            Shard shard = shards[shardIdx];

            if (shard.isDegraded()) {
                shard.recordOverloaded();
                return IngestOutcome.OVERLOADED;
            }
            if (event.ts < now - 60_000 || event.ts > now || event.bytes < 0) {
                shard.recordRejected();
                return IngestOutcome.REJECTED;
            }

            DedupAdmission.Outcome outcome = admission.admit(
                    event.eventId, 0, event.eventId.length, now,
                    idx -> shards[idx].tryEnqueue(event));

            switch (outcome) {
                case ACCEPTED -> shard.recordAccepted();
                case DEDUPED -> shard.recordDeduped();
                case QUEUE_FULL -> shard.recordOverloaded();
            }
            return switch (outcome) {
                case ACCEPTED -> IngestOutcome.ACCEPTED;
                case DEDUPED -> IngestOutcome.DEDUPED;
                case QUEUE_FULL -> IngestOutcome.OVERLOADED;
            };
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    public int shardFor(byte[] eventId) {
        return admission.shard(eventId, 0, eventId.length);
    }

    public QueryResult queryTopK(long atMs, long windowMs, boolean byUrl, int k) {
        return queryService.queryTopK(atMs, windowMs, byUrl, k, nowMs.getAsLong());
    }

    public StatsResult queryStats(long atMs, long windowMs) {
        return queryService.queryStats(atMs, windowMs, nowMs.getAsLong());
    }

    public boolean isDegraded() {
        for (Shard shard : shards) {
            if (shard.isDegraded()) {
                return true;
            }
        }
        return admission.anyBloomDegraded();
    }

    public State state() {
        return state.get();
    }

    /** Closes all shards. Returns true when every enqueued event was drained. */
    public boolean close() {
        lifecycleLock.writeLock().lock();
        try {
            if (state.get() != State.RUNNING) {
                return state.get() == State.CLOSED && unprocessedEvents() == 0;
            }
            state.set(State.CLOSING);
            boolean allDrained = true;
            for (Shard shard : shards) {
                allDrained &= shard.close(CLOSE_TIMEOUT_MS);
            }
            state.set(State.CLOSED);
            return allDrained;
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    public long unprocessedEvents() {
        long total = 0;
        for (Shard shard : shards) {
            total += shard.unprocessed();
        }
        return total;
    }

    public Shard[] shards() {
        return shards.clone();
    }
}