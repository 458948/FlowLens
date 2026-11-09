package io.flowlens;

import io.flowlens.sketch.SpaceSaving;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpaceSavingStressTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void sustainedDistinctKeysKeepBoundedSpaceAndInvariants() {
        int c = 256;
        SpaceSaving ss = new SpaceSaving(c, 64);
        int n = 100_000;

        for (int i = 0; i < n; i++) {
            byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            ss.observe(key, 0, key.length);
            if (i % 1000 == 0) {
                byte[] probe = ("key-" + (i / 2)).getBytes(StandardCharsets.UTF_8);
                long est = ss.estimate(probe, 0, probe.length);
                assertTrue(est >= -1);
            }
        }

        assertEquals(n, ss.observations());
        assertTrue(ss.size() <= c, "sketch size must stay bounded");
        assertEquals(c, ss.size());

        long sum = 0;
        for (int i = 0; i < ss.size(); i++) {
            assertTrue(ss.count(i) > 0, "every candidate must have positive count");
            assertTrue(ss.error(i) >= 0, "error must be non-negative");
            sum += ss.count(i);
        }
        assertEquals(n, sum, "sum of counts must equal total observations");
    }
}