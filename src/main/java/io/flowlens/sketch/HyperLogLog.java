package io.flowlens.sketch;

import io.flowlens.hash.Hash64;
import io.flowlens.hash.SipHash128;

/**
 * HyperLogLog with p=13 (8192 registers), implemented from the original paper.
 * Standard error is approximately 1.04 / sqrt(8192) ~= 1.15%.
 */
public final class HyperLogLog {

    public static final int P = 13;
    public static final int M = 1 << P;

    private static final double ALPHA = 0.7213 / (1.0 + 1.079 / M);

    private final byte[] registers = new byte[M];
    private final Hash64 hasher;

    public HyperLogLog() {
        this(SipHash128.INSTANCE);
    }

    public HyperLogLog(Hash64 hasher) {
        this.hasher = hasher;
    }

    public void add(byte[] data, int off, int len) {
        long h = hasher.hash(data, off, len);
        int idx = (int) (h & (M - 1));
        int rho = Long.numberOfLeadingZeros(h >>> P) - P + 1;
        if ((registers[idx] & 0xff) < rho) {
            registers[idx] = (byte) rho;
        }
    }

    public double estimate() {
        double sum = 0.0;
        int zeros = 0;
        for (int i = 0; i < M; i++) {
            int r = registers[i] & 0xff;
            if (r == 0) {
                zeros++;
            }
            sum += 1.0 / (1L << r);
        }
        double e = ALPHA * M * M / sum;
        if (e <= 2.5 * M && zeros > 0) {
            e = M * Math.log((double) M / zeros);
        }
        return e;
    }

    public byte[] registers() {
        return registers;
    }

    public void mergeInto(HyperLogLog target) {
        for (int i = 0; i < M; i++) {
            int a = registers[i] & 0xff;
            int b = target.registers[i] & 0xff;
            if (a > b) {
                target.registers[i] = registers[i];
            }
        }
    }

    public static HyperLogLog merge(HyperLogLog a, HyperLogLog b) {
        HyperLogLog out = new HyperLogLog();
        a.mergeInto(out);
        b.mergeInto(out);
        return out;
    }
}