package io.flowlens.sketch;

/** One merged Top-K candidate. */
public record TopKEntry(String name, long estimatedCount, long lowerBound, long upperBound) {
}