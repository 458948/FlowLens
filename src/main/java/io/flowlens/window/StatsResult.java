package io.flowlens.window;

public record StatsResult(long effectiveAt, boolean stable, double distinctUsers, double p95Bytes) {
}