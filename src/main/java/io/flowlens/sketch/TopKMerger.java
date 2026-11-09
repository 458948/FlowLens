package io.flowlens.sketch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Reusable merge workspace for combining SpaceSaving sketches across buckets
 * and shards. Candidate names are stored in a byte arena that grows lazily on
 * demand up to maxCandidates; the arena is kept across queries and reset only
 * clears the table and candidate count.
 */
public final class TopKMerger {

    private static final int INITIAL_CANDIDATES = 256;

    private final int maxCandidates;
    private final int tableSize;
    private final int tableMask;
    private final int maxKeyBytes;

    private final long[] hi;
    private final long[] lo;
    private final long[] count;
    private final long[] error;
    private final int[] nameLen;
    private byte[] arena;
    private final int[] table;

    private int size;

    public TopKMerger(int maxCandidates, int tableSize, int maxKeyBytes) {
        if ((tableSize & (tableSize - 1)) != 0) {
            throw new IllegalArgumentException("tableSize must be a power of two");
        }
        this.maxCandidates = maxCandidates;
        this.tableSize = tableSize;
        this.tableMask = tableSize - 1;
        this.maxKeyBytes = maxKeyBytes;
        this.hi = new long[maxCandidates];
        this.lo = new long[maxCandidates];
        this.count = new long[maxCandidates];
        this.error = new long[maxCandidates];
        this.nameLen = new int[maxCandidates];
        this.arena = new byte[Math.min(maxCandidates, INITIAL_CANDIDATES) * maxKeyBytes];
        this.table = new int[tableSize];
    }

    public void reset() {
        Arrays.fill(table, 0);
        size = 0;
    }

    public int size() {
        return size;
    }

    /** Current arena length in bytes (for tests). */
    public int arenaCapacity() {
        return arena.length;
    }

    public void add(SpaceSaving ss) {
        for (int i = 0; i < ss.size(); i++) {
            merge(ss.fpHi(i), ss.fpLo(i), ss.count(i), ss.error(i),
                    ss.nameBytes(), ss.nameOffset(i), ss.nameLength(i));
        }
    }

    public void merge(long h, long l, long c, long e, byte[] name, int off, int len) {
        if (size >= maxCandidates) {
            throw new IllegalStateException("merge candidate capacity exceeded: " + maxCandidates);
        }
        int slot = hashSlot(h, l);
        while (true) {
            int v = table[slot];
            if (v == 0 || v == -1) {
                int idx = size++;
                hi[idx] = h;
                lo[idx] = l;
                count[idx] = c;
                error[idx] = e;
                nameLen[idx] = len;
                ensureArenaCapacity(idx, len);
                System.arraycopy(name, off, arena, idx * maxKeyBytes, len);
                table[slot] = idx + 1;
                return;
            }
            int idx = v - 1;
            if (hi[idx] == h && lo[idx] == l && bytesEqual(idx, name, off, len)) {
                count[idx] += c;
                error[idx] += e;
                return;
            }
            slot = (slot + 1) & tableMask;
        }
    }

    public List<TopKEntry> top(int k, long totalObservations, int capacity) {
        long e = totalObservations / capacity;
        PriorityQueue<Integer> pq = new PriorityQueue<>(Math.max(1, k), (x, y) -> compare(x, y));
        for (int i = 0; i < size; i++) {
            if (pq.size() < k) {
                pq.add(i);
            } else if (compare(i, pq.peek()) > 0) {
                pq.poll();
                pq.add(i);
            }
        }
        int n = pq.size();
        int[] result = new int[n];
        for (int j = n - 1; j >= 0; j--) {
            result[j] = pq.poll();
        }
        List<TopKEntry> out = new ArrayList<>(n);
        for (int idx : result) {
            String name = new String(arena, idx * maxKeyBytes, nameLen[idx], StandardCharsets.UTF_8);
            long est = count[idx];
            long lower = Math.max(0L, est - e);
            long upper = est + e;
            out.add(new TopKEntry(name, est, lower, upper));
        }
        return out;
    }

    private void ensureArenaCapacity(int candidateIndex, int len) {
        int needed = (candidateIndex + 1) * maxKeyBytes;
        if (needed <= arena.length) {
            return;
        }
        int maxBytes = maxCandidates * maxKeyBytes;
        int newCap = Math.min(maxBytes, Math.max(needed, arena.length * 2));
        arena = Arrays.copyOf(arena, newCap);
    }

    private int compare(int a, int b) {
        int c = Long.compare(count[a], count[b]);
        if (c != 0) {
            return c;
        }
        c = Long.compare(hi[a], hi[b]);
        if (c != 0) {
            return c;
        }
        return Long.compare(lo[a], lo[b]);
    }

    private int hashSlot(long h, long l) {
        long x = h ^ (h >>> 33) ^ l ^ (l >>> 29);
        return (int) (x & tableMask);
    }

    private boolean bytesEqual(int idx, byte[] name, int off, int len) {
        if (nameLen[idx] != len) {
            return false;
        }
        int base = idx * maxKeyBytes;
        for (int i = 0; i < len; i++) {
            if (arena[base + i] != name[off + i]) {
                return false;
            }
        }
        return true;
    }
}