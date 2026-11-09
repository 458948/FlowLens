package io.flowlens;

import io.flowlens.window.Bucket;
import io.flowlens.window.RingBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingBufferTest {

    private static final long NOW = 1_000_000_000_000L;

    @Test
    void acceptsExactlySixtySecondsLateEvent() {
        RingBuffer ring = new RingBuffer();
        Bucket bucket = ring.acquire(NOW - 60_000, NOW);
        assertEquals(NOW - 60_000, bucket.bucketStart());
    }

    @Test
    void rejectsMoreThanSixtySecondsLateEvent() {
        RingBuffer ring = new RingBuffer();
        assertThrows(IllegalArgumentException.class, () -> ring.acquire(NOW - 60_001, NOW));
    }

    @Test
    void recyclesOnlyClosedBucket() {
        RingBuffer ring = new RingBuffer();
        long ts0 = NOW - 60_000;
        Bucket b0 = ring.acquire(ts0, NOW);
        assertEquals(ts0, b0.bucketStart());

        long now1 = NOW + 68_000;
        long ts1 = now1;
        Bucket b1 = ring.acquire(ts1, now1);
        assertEquals(ts1, b1.bucketStart());

        assertNull(ring.bucketFor(ts0));
        assertThrows(IllegalArgumentException.class, () -> ring.requireRetained(ts0 - 1_000, now1));
    }
}