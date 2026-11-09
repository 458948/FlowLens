package io.flowlens.hash;

/**
 * SipHash-2-4 with a 128-bit tag.
 *
 * This is a deterministic fingerprint used by the probabilistic data structures
 * (SpaceSaving, Sliding Bloom Filter, HyperLogLog). It is not intended as a
 * cryptographic MAC.
 */
public final class SipHash128 implements Fingerprint128, Hash64 {

    private static final long C0 = 0x736f6d6570736575L;
    private static final long C1 = 0x646f72616e646f6dL;
    private static final long C2 = 0x6c7967656e657261L;
    private static final long C3 = 0x7465646279746573L;

    private static final long K0 = 0x466c6f774c656e73L;
    private static final long K1 = 0x2d53706948617368L;

    public static final SipHash128 INSTANCE = new SipHash128();

    private SipHash128() {
    }

    @Override
    public void hash(byte[] data, int off, int len, long[] out) {
        hash128(data, off, len, out);
    }

    @Override
    public long hash(byte[] data, int off, int len) {
        return hash64(data, off, len);
    }

    public static long[] hash128(byte[] data, int off, int len) {
        long[] out = new long[2];
        hash128(data, off, len, out);
        return out;
    }

    /** Fills out with {hi, lo}. out.length must be >= 2. */
    public static void hash128(byte[] data, int off, int len, long[] out) {
        long[] s = new long[4];
        s[0] = C0 ^ K0;
        s[1] = C1 ^ K1;
        s[2] = C2 ^ K0;
        s[3] = C3 ^ K1;

        int end = off + (len & ~7);
        int i = off;
        while (i < end) {
            long m = littleEndian64(data, i);
            s[3] ^= m;
            round(s);
            round(s);
            s[0] ^= m;
            i += 8;
        }

        long b = ((long) len) << 56;
        for (int j = i; j < off + len; j++) {
            b |= ((long) (data[j] & 0xff)) << (8 * (j - i));
        }
        s[3] ^= b;
        round(s);
        round(s);
        s[0] ^= b;

        s[2] ^= 0xeeL;
        for (int r = 0; r < 4; r++) round(s);
        long hi = s[0] ^ s[1] ^ s[2] ^ s[3];

        s[1] ^= 0xddL;
        for (int r = 0; r < 4; r++) round(s);
        long lo = s[0] ^ s[1] ^ s[2] ^ s[3];

        out[0] = hi;
        out[1] = lo;
    }

    /** 64-bit hash derived from the 128-bit tag. */
    public static long hash64(byte[] data, int off, int len) {
        long[] tmp = new long[2];
        hash128(data, off, len, tmp);
        return tmp[0] ^ tmp[1];
    }

    private static long littleEndian64(byte[] b, int i) {
        return (b[i] & 0xffL)
                | ((b[i + 1] & 0xffL) << 8)
                | ((b[i + 2] & 0xffL) << 16)
                | ((b[i + 3] & 0xffL) << 24)
                | ((b[i + 4] & 0xffL) << 32)
                | ((b[i + 5] & 0xffL) << 40)
                | ((b[i + 6] & 0xffL) << 48)
                | ((b[i + 7] & 0xffL) << 56);
    }

    private static void round(long[] s) {
        long v0 = s[0];
        long v1 = s[1];
        long v2 = s[2];
        long v3 = s[3];
        v0 += v1;
        v1 = Long.rotateLeft(v1, 13);
        v1 ^= v0;
        v0 = Long.rotateLeft(v0, 32);
        v2 += v3;
        v3 = Long.rotateLeft(v3, 16);
        v3 ^= v2;
        v0 += v3;
        v3 = Long.rotateLeft(v3, 21);
        v3 ^= v0;
        v2 += v1;
        v1 = Long.rotateLeft(v1, 17);
        v1 ^= v2;
        v2 = Long.rotateLeft(v2, 32);
        s[0] = v0;
        s[1] = v1;
        s[2] = v2;
        s[3] = v3;
    }
}