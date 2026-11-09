package io.flowlens;

import io.flowlens.sketch.HyperLogLog;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HyperLogLogTest {

    private static void add(HyperLogLog hll, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        hll.add(b, 0, b.length);
    }

    @Test
    void estimatesCardinalityWithinTolerance() {
        HyperLogLog hll = new HyperLogLog();
        int n = 100_000;
        for (int i = 0; i < n; i++) {
            add(hll, "u" + i);
        }
        assertEquals(n, hll.estimate(), n * 0.05);
    }

    @Test
    void mergePreservesCardinality() {
        HyperLogLog a = new HyperLogLog();
        HyperLogLog b = new HyperLogLog();
        int n = 50_000;
        for (int i = 0; i < n; i++) {
            add(a, "a" + i);
            add(b, "b" + i);
        }
        HyperLogLog merged = HyperLogLog.merge(a, b);
        assertEquals(100_000, merged.estimate(), 100_000 * 0.05);
    }

    @Test
    void emptySetEstimatesZero() {
        HyperLogLog hll = new HyperLogLog();
        assertEquals(0.0, hll.estimate(), 0.01);
    }

    @Test
    void smallCardinalities() {
        assertEquals(1.0, singleCardinality(1), 0.5);
        assertEquals(2.0, singleCardinality(2), 0.5);
        assertEquals(3.0, singleCardinality(3), 0.5);
        assertEquals(5.0, singleCardinality(5), 0.5);
    }

    @Test
    void duplicatesDoNotIncreaseCardinality() {
        HyperLogLog hll = new HyperLogLog();
        for (int i = 0; i < 100; i++) {
            add(hll, "dup");
        }
        assertEquals(1.0, hll.estimate(), 0.5);
    }

    @Test
    void mergeEmptyAndMultipleSmallHlls() {
        HyperLogLog empty = new HyperLogLog();
        HyperLogLog a = new HyperLogLog();
        add(a, "x");
        assertEquals(1.0, HyperLogLog.merge(empty, a).estimate(), 0.5);

        HyperLogLog b = new HyperLogLog();
        add(b, "y");
        HyperLogLog c = new HyperLogLog();
        add(c, "z");
        HyperLogLog merged = HyperLogLog.merge(HyperLogLog.merge(a, b), c);
        assertEquals(3.0, merged.estimate(), 0.5);
    }

    private static double singleCardinality(int n) {
        HyperLogLog hll = new HyperLogLog();
        for (int i = 0; i < n; i++) {
            add(hll, "e" + i);
        }
        return hll.estimate();
    }
}