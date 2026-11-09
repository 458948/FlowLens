package io.flowlens;

import io.flowlens.sketch.TopKEntry;
import io.flowlens.sketch.TopKMerger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopKMergerTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void emptyWorkspaceHasSmallArena() {
        TopKMerger merger = new TopKMerger(61_440, 131_072, 256);
        assertEquals(256 * 256, merger.arenaCapacity());
    }

    @Test
    void fewCandidatesKeepArenaSmall() {
        TopKMerger merger = new TopKMerger(61_440, 131_072, 256);
        for (int i = 0; i < 100; i++) {
            merger.merge(i, 0, 1, 0, b("n" + i), 0, ("n" + i).length());
        }
        assertEquals(100, merger.size());
        assertTrue(merger.arenaCapacity() < 61_440 * 256);
    }

    @Test
    void arenaGrowsUpToBoundedMaximum() {
        TopKMerger merger = new TopKMerger(61_440, 131_072, 64);
        for (int i = 0; i < 2_000; i++) {
            merger.merge(i, 0, 1, 0, b("n" + i), 0, ("n" + i).length());
        }
        assertEquals(2_000, merger.size());
        assertTrue(merger.arenaCapacity() <= 61_440 * 64);
    }

    @Test
    void candidateLimitIsEnforced() {
        TopKMerger merger = new TopKMerger(4, 8, 64);
        for (int i = 0; i < 4; i++) {
            merger.merge(i, 0, 1, 0, b("n" + i), 0, ("n" + i).length());
        }
        assertThrows(IllegalStateException.class, () -> merger.merge(99, 0, 1, 0, b("n99"), 0, 3));
    }

    @Test
    void hashCollisionKeepsNamesSeparate() {
        TopKMerger merger = new TopKMerger(64, 128, 64);
        merger.merge(42L, 42L, 3, 0, b("a"), 0, 1);
        merger.merge(42L, 42L, 5, 0, b("b"), 0, 1);
        List<TopKEntry> top = merger.top(10, 8, 16);
        assertEquals(2, top.size());
        assertTrue(top.stream().anyMatch(e -> e.name().equals("a") && e.estimatedCount() == 3));
        assertTrue(top.stream().anyMatch(e -> e.name().equals("b") && e.estimatedCount() == 5));
    }

    @Test
    void sameKeySumsAcrossBuckets() {
        TopKMerger merger = new TopKMerger(64, 128, 64);
        byte[] key = b("k");
        merger.merge(7L, 7L, 10, 1, key, 0, 1);
        merger.merge(7L, 7L, 20, 2, key, 0, 1);
        List<TopKEntry> top = merger.top(10, 30, 16);
        assertEquals(1, top.size());
        assertEquals(30, top.get(0).estimatedCount());
        assertTrue(top.get(0).lowerBound() <= 30 && 30 <= top.get(0).upperBound());
    }

    @Test
    void resetClearsCandidatesWithoutLosingArena() {
        TopKMerger merger = new TopKMerger(61_440, 131_072, 64);
        for (int i = 0; i < 500; i++) {
            merger.merge(i, 0, 1, 0, b("n" + i), 0, ("n" + i).length());
        }
        int grown = merger.arenaCapacity();
        merger.reset();
        assertEquals(0, merger.size());
        assertEquals(grown, merger.arenaCapacity());
    }
}