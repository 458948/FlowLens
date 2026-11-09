package io.flowlens.sketch;

import io.flowlens.hash.Fingerprint128;
import io.flowlens.hash.SipHash128;

/**
 * SpaceSaving heavy-hitter sketch with fixed-capacity primitive arrays.
 *
 * Each candidate stores a 128-bit fingerprint, the original UTF-8 bytes, the
 * byte length, a count and an error. Hash collisions are resolved by comparing
 * the original bytes so that different names are never merged.
 *
 * The open-addressing table uses tombstones. All probing loops are bounded by
 * the table length; insertion prefers reusing a tombstone over consuming a
 * fresh empty slot, and throws if the table can no longer accept an entry.
 *
 * The name store is a compact, grow-only byte arena with per-candidate offsets.
 * A replaced candidate reuses its old slot when the new name fits; otherwise
 * the new name is appended and dead bytes are reclaimed on the next compaction
 * pass. This keeps the fixed capacity C=256 and byte-collision safety while
 * avoiding a full maxKeyBytes stride for every candidate.
 *
 * The data structure is single-writer by design. Reads for query merging are
 * performed with a caller-provided snapshot/version protocol.
 */
public final class SpaceSaving {

    private final int capacity;
    private final int maxKeyBytes;
    private final Fingerprint128 fingerprinter;

    private final int tableSize;
    private final int tableMask;
    private final int[] table;      // 0 = empty, -1 = tombstone, otherwise candidate+1

    private final long[] hi;
    private final long[] lo;
    private final long[] count;
    private final long[] error;
    private final byte[] nameLen;
    private final int[] nameOffset;
    private byte[] arena;
    private int arenaUsed;

    private final int[] heap;
    private final int[] heapPos;
    private int heapSize;

    private int size;
    private long observations;

    private final long[] scratch = new long[2];

    public SpaceSaving(int capacity, int maxKeyBytes) {
        this(capacity, maxKeyBytes, SipHash128.INSTANCE);
    }

    public SpaceSaving(int capacity, int maxKeyBytes, Fingerprint128 fingerprinter) {
        if (capacity <= 0 || maxKeyBytes <= 0) {
            throw new IllegalArgumentException("capacity and maxKeyBytes must be positive");
        }
        this.capacity = capacity;
        this.maxKeyBytes = maxKeyBytes;
        this.fingerprinter = fingerprinter;

        int t = capacity * 2;
        int highest = Integer.highestOneBit(t);
        this.tableSize = (highest == t) ? t : highest << 1;
        this.tableMask = tableSize - 1;
        this.table = new int[tableSize];

        this.hi = new long[capacity];
        this.lo = new long[capacity];
        this.count = new long[capacity];
        this.error = new long[capacity];
        this.nameLen = new byte[capacity];
        this.nameOffset = new int[capacity];
        this.arena = new byte[Math.min(capacity, 16) * maxKeyBytes];

        this.heap = new int[capacity];
        this.heapPos = new int[capacity];
        for (int i = 0; i < capacity; i++) {
            heapPos[i] = -1;
        }
    }

    /** Observes one key. The key bytes must not exceed maxKeyBytes. */
    public void observe(byte[] key, int off, int len) {
        if (len < 0 || len > maxKeyBytes) {
            throw new IllegalArgumentException("key length " + len + " exceeds maxKeyBytes " + maxKeyBytes);
        }
        observations++;
        fingerprinter.hash(key, off, len, scratch);
        long h = scratch[0];
        long l = scratch[1];

        int slot = hashSlot(h, l);
        for (int probe = 0; probe < table.length; probe++) {
            int v = table[slot];
            if (v == 0) {
                break;
            }
            if (v != -1) {
                int cand = v - 1;
                if (hi[cand] == h && lo[cand] == l && bytesEqual(cand, key, off, len)) {
                    count[cand]++;
                    siftDown(heapPos[cand]);
                    return;
                }
            }
            slot = (slot + 1) & tableMask;
        }

        if (size < capacity) {
            int cand = size;
            hi[cand] = h;
            lo[cand] = l;
            count[cand] = 1;
            error[cand] = 0;
            writeNewName(cand, key, off, len);
            nameLen[cand] = (byte) len;
            size++;
            insertTable(cand);
            push(cand);
            return;
        }

        int min = heap[0];
        removeTable(min);
        long oldCount = count[min];
        hi[min] = h;
        lo[min] = l;
        writeReplacingName(min, key, off, len);
        nameLen[min] = (byte) len;
        count[min] = oldCount + 1;
        error[min] = oldCount;
        siftDown(0);
        insertTable(min);
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    public int maxKeyBytes() {
        return maxKeyBytes;
    }

    public long observations() {
        return observations;
    }

    public long fpHi(int i) {
        return hi[i];
    }

    public long fpLo(int i) {
        return lo[i];
    }

    public long count(int i) {
        return count[i];
    }

    public long error(int i) {
        return error[i];
    }

    public int nameLength(int i) {
        return nameLen[i] & 0xff;
    }

    public int nameOffset(int i) {
        return nameOffset[i];
    }

    public byte[] nameBytes() {
        return arena;
    }

    /** Current name arena length in bytes (for tests and memory analysis). */
    public int nameArenaLength() {
        return arena.length;
    }

    /** Returns the estimated count for a key, or -1 if the key is absent. */
    public long estimate(byte[] key, int off, int len) {
        fingerprinter.hash(key, off, len, scratch);
        long h = scratch[0];
        long l = scratch[1];
        int slot = hashSlot(h, l);
        for (int probe = 0; probe < table.length; probe++) {
            int v = table[slot];
            if (v == 0) {
                return -1;
            }
            if (v != -1) {
                int cand = v - 1;
                if (hi[cand] == h && lo[cand] == l && bytesEqual(cand, key, off, len)) {
                    return count[cand];
                }
            }
            slot = (slot + 1) & tableMask;
        }
        return -1;
    }

    private void writeNewName(int cand, byte[] key, int off, int len) {
        ensureArenaCapacity(len);
        nameOffset[cand] = arenaUsed;
        System.arraycopy(key, off, arena, arenaUsed, len);
        arenaUsed += len;
    }

    private void writeReplacingName(int cand, byte[] key, int off, int len) {
        int oldLen = nameLen[cand] & 0xff;
        if (len <= oldLen) {
            System.arraycopy(key, off, arena, nameOffset[cand], len);
            return;
        }
        writeNewName(cand, key, off, len);
    }

    private void ensureArenaCapacity(int incomingLen) {
        if (arenaUsed + incomingLen <= arena.length) {
            return;
        }
        compactArena();
        if (arenaUsed + incomingLen <= arena.length) {
            return;
        }
        int maxBytes = capacity * maxKeyBytes;
        int needed = arenaUsed + incomingLen;
        int newCap = Math.min(maxBytes, Math.max(needed, arena.length * 2));
        if (newCap < needed) {
            newCap = Math.min(maxBytes, needed);
        }
        if (newCap < needed) {
            throw new IllegalStateException("SpaceSaving arena cannot grow beyond capacity");
        }
        byte[] grown = new byte[newCap];
        System.arraycopy(arena, 0, grown, 0, arenaUsed);
        arena = grown;
    }

    private void compactArena() {
        int liveBytes = 0;
        for (int i = 0; i < size; i++) {
            liveBytes += nameLen[i] & 0xff;
        }
        byte[] compacted = new byte[Math.max(1, liveBytes)];
        int write = 0;
        for (int i = 0; i < size; i++) {
            int len = nameLen[i] & 0xff;
            System.arraycopy(arena, nameOffset[i], compacted, write, len);
            nameOffset[i] = write;
            write += len;
        }
        arena = compacted;
        arenaUsed = write;
    }

    private int hashSlot(long h, long l) {
        long x = h ^ (h >>> 33) ^ l ^ (l >>> 29);
        return (int) (x & tableMask);
    }

    private boolean bytesEqual(int cand, byte[] key, int off, int len) {
        if ((nameLen[cand] & 0xff) != len) {
            return false;
        }
        int base = nameOffset[cand];
        for (int i = 0; i < len; i++) {
            if (arena[base + i] != key[off + i]) {
                return false;
            }
        }
        return true;
    }

    private void insertTable(int cand) {
        long h = hi[cand];
        long l = lo[cand];
        int slot = hashSlot(h, l);
        int firstTombstone = -1;
        for (int probe = 0; probe < table.length; probe++) {
            int v = table[slot];
            if (v == 0) {
                if (firstTombstone >= 0) {
                    table[firstTombstone] = cand + 1;
                } else {
                    table[slot] = cand + 1;
                }
                return;
            }
            if (v == -1 && firstTombstone < 0) {
                firstTombstone = slot;
            }
            slot = (slot + 1) & tableMask;
        }
        if (firstTombstone >= 0) {
            table[firstTombstone] = cand + 1;
            return;
        }
        throw new IllegalStateException("SpaceSaving table has no free slot");
    }

    private void removeTable(int cand) {
        int slot = hashSlot(hi[cand], lo[cand]);
        for (int probe = 0; probe < table.length; probe++) {
            if (table[slot] == cand + 1) {
                table[slot] = -1;
                return;
            }
            slot = (slot + 1) & tableMask;
        }
        throw new IllegalStateException("candidate not found in table");
    }

    private void push(int cand) {
        heap[heapSize] = cand;
        heapPos[cand] = heapSize;
        heapSize++;
        siftUp(heapPos[cand]);
    }

    private void siftUp(int i) {
        while (i > 0) {
            int parent = (i - 1) >>> 1;
            if (compare(heap[i], heap[parent]) < 0) {
                swap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
    }

    private void siftDown(int i) {
        int half = heapSize >>> 1;
        while (i < half) {
            int left = (i << 1) + 1;
            int right = left + 1;
            int best = left;
            if (right < heapSize && compare(heap[right], heap[left]) < 0) {
                best = right;
            }
            if (compare(heap[best], heap[i]) < 0) {
                swap(i, best);
                i = best;
            } else {
                break;
            }
        }
    }

    private int compare(int a, int b) {
        long ca = count[a];
        long cb = count[b];
        if (ca != cb) {
            return Long.compare(ca, cb);
        }
        return Long.compare(error[a], error[b]);
    }

    private void swap(int i, int j) {
        int a = heap[i];
        int b = heap[j];
        heap[i] = b;
        heap[j] = a;
        heapPos[a] = j;
        heapPos[b] = i;
    }
}