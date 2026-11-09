package io.flowlens.hash;

/** 128-bit fingerprint function. Output array must have length >= 2 (out[0]=hi, out[1]=lo). */
@FunctionalInterface
public interface Fingerprint128 {
    void hash(byte[] data, int off, int len, long[] out);
}