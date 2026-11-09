package io.flowlens;

import io.flowlens.sketch.LogHistogram;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogHistogramTest {

    @Test
    void binBoundaries() {
        LogHistogram h = new LogHistogram();
        assertEquals(0, h.indexFor(0));
        assertEquals(1, h.indexFor(1));
        assertEquals(9, h.indexFor(2));
        assertEquals(64, h.indexFor(255));
        assertEquals(65, h.indexFor(256));
        assertEquals(504, h.indexFor(Long.MAX_VALUE));
    }

    @Test
    void negativeBytesAreRejected() {
        LogHistogram h = new LogHistogram();
        assertThrows(IllegalArgumentException.class, () -> h.add(-1));
    }

    @Test
    void quantileFallsWithinExactBucket() {
        LogHistogram h = new LogHistogram();
        long value = 1000;
        for (int i = 0; i < 100; i++) {
            h.add(value);
        }
        int bin = h.indexFor(value);
        double q = h.estimateQuantile(0.95);
        assertTrue(q >= LogHistogram.lowerBound(bin), "quantile below bucket lower bound");
        assertTrue(q <= LogHistogram.upperBound(bin), "quantile above bucket upper bound");
    }
}