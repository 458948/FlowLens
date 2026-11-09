package io.flowlens;

import io.flowlens.sketch.SpaceSaving;
import io.flowlens.sketch.TopKEntry;
import io.flowlens.sketch.TopKMerger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SpaceSavingMergeTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void crossShardMergeRespectsErrorBound() {
        int c = 16;
        int shards = 4;
        SpaceSaving[] sketches = new SpaceSaving[shards];
        for (int i = 0; i < shards; i++) {
            sketches[i] = new SpaceSaving(c, 64);
        }

        Map<String, Integer> truth = new HashMap<>();
        int idx = 0;
        for (int i = 0; i < 150; i++) {
            String key = "k" + i;
            truth.put(key, 1);
            observe(sketches[idx % shards], key, 1);
            idx++;
        }
        for (int i = 0; i < 3; i++) {
            String key = "h" + i;
            truth.put(key, 100);
            observe(sketches[idx % shards], key, 100);
            idx++;
        }

        TopKMerger merger = new TopKMerger(61_440, 131_072, 64);
        long total = 0;
        for (SpaceSaving ss : sketches) {
            merger.add(ss);
            total += ss.observations();
        }
        long e = total / c;

        List<TopKEntry> top = merger.top(10, total, c);
        Set<String> names = new HashSet<>();
        for (TopKEntry entry : top) {
            names.add(entry.name());
            int trueCount = truth.get(entry.name());
            long est = entry.estimatedCount();
            assertTrue(est >= trueCount, "merged estimate must be an upper bound");
            assertTrue(est - trueCount <= e, "merged estimate error must be <= N/C");
            assertTrue(entry.lowerBound() <= trueCount && trueCount <= entry.upperBound());
        }
        assertTrue(names.contains("h0"));
        assertTrue(names.contains("h1"));
        assertTrue(names.contains("h2"));
    }

    @Test
    void crossShardAndBucketMergeContainsTrueFrequencyInInterval() {
        int c = 16;
        int buckets = 12;
        SpaceSaving[] sketches = new SpaceSaving[buckets];
        for (int i = 0; i < buckets; i++) {
            sketches[i] = new SpaceSaving(c, 64);
        }

        String hot = "hot";
        int hotTrue = 0;
        for (SpaceSaving ss : sketches) {
            observe(ss, hot, 50);
            hotTrue += 50;
        }

        int idx = 0;
        for (int i = 0; i < 100; i++) {
            observe(sketches[idx % buckets], "bg" + i, 30);
            idx++;
        }

        TopKMerger merger = new TopKMerger(61_440, 131_072, 64);
        long total = 0;
        for (SpaceSaving ss : sketches) {
            merger.add(ss);
            total += ss.observations();
        }
        long e = total / c;

        List<TopKEntry> top = merger.top(20, total, c);
        boolean found = false;
        for (TopKEntry entry : top) {
            if (entry.name().equals(hot)) {
                found = true;
                assertTrue(entry.lowerBound() <= hotTrue && hotTrue <= entry.upperBound(),
                        "true frequency must fall inside the merged interval");
            }
        }
        assertTrue(found, "hot key must appear in the merged candidate set");
    }

    private static void observe(SpaceSaving ss, String key, int times) {
        byte[] bytes = b(key);
        for (int i = 0; i < times; i++) {
            ss.observe(bytes, 0, bytes.length);
        }
    }
}