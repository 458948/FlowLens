package io.flowlens.hash;

/** 64-bit hash function used by HyperLogLog. */
@FunctionalInterface
public interface Hash64 {
    long hash(byte[] data, int off, int len);
}