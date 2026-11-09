package io.flowlens;

import io.flowlens.hash.Fingerprint128;
import io.flowlens.sketch.SpaceSaving;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpaceSavingTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void estimateIsUpperBoundAndErrorBounded() {
        int c = 16;
        SpaceSaving ss = new SpaceSaving(c, 64);
        Map<String, Integer> truth = new HashMap<>();

        for (int i = 0; i < 5; i++) {
            String key = "hot" + i;
            int f = 150;
            truth.put(key, f);
            for (int j = 0; j < f; j++) {
                ss.observe(b(key), 0, key.length());
            }
        }
        for (int i = 0; i < 20; i++) {
            String key = "cold" + i;
            int f = 20;
            truth.put(key, f);
            for (int j = 0; j < f; j++) {
                ss.observe(b(key), 0, key.length());
            }
        }

        long n = ss.observations();
        long e = n / c;
        for (Map.Entry<String, Integer> entry : truth.entrySet()) {
            String key = entry.getKey();
            long trueCount = entry.getValue();
            long est = ss.estimate(b(key), 0, key.length());
            if (trueCount > e) {
                assertTrue(est != -1, "heavy key must be present: " + key);
                assertTrue(est >= trueCount, "estimate must be an upper bound");
                assertTrue(est - trueCount <= e, "estimate error must be <= N/C");
            } else if (est != -1) {
                assertTrue(est >= trueCount, "estimate must be an upper bound");
                assertTrue(est - trueCount <= e, "estimate error must be <= N/C");
            }
        }
    }

    @Test
    void hashCollisionsAreResolvedByByteComparison() {
        Fingerprint128 constant = (data, off, len, out) -> {
            out[0] = 123456789L;
            out[1] = 987654321L;
        };
        SpaceSaving ss = new SpaceSaving(256, 64, constant);
        for (int i = 0; i < 100; i++) {
            ss.observe(b("a"), 0, 1);
        }
        for (int i = 0; i < 200; i++) {
            ss.observe(b("b"), 0, 1);
        }
        assertEquals(100, ss.estimate(b("a"), 0, 1));
        assertEquals(200, ss.estimate(b("b"), 0, 1));
        assertTrue(ss.size() >= 2);
    }

    @Test
    void compactNameStoragePreservesNamesAcrossEvictions() {
        Fingerprint128 constant = (data, off, len, out) -> {
            out[0] = 42L;
            out[1] = 42L;
        };
        SpaceSaving ss = new SpaceSaving(4, 64, constant);
        for (int i = 0; i < 20; i++) {
            ss.observe(b("a"), 0, 1);
        }
        for (int i = 0; i < 15; i++) {
            ss.observe(b("bb"), 0, 2);
        }
        for (int i = 0; i < 10; i++) {
            ss.observe(b("ccc"), 0, 3);
        }
        for (int i = 0; i < 5; i++) {
            ss.observe(b("dddd"), 0, 4);
        }
        // Trigger eviction with a longer name.
        ss.observe(b("eeeeeee"), 0, 7);

        assertEquals(15, ss.estimate(b("bb"), 0, 2));
        assertEquals(6, ss.estimate(b("eeeeeee"), 0, 7));
        assertTrue(ss.nameArenaLength() <= 4 * 64);
    }}