package io.flowlens.http;

import io.flowlens.sketch.TopKEntry;
import java.util.List;

/** One immutable SSE snapshot shared by every connected client. */
public record Snapshot(long effectiveAt, long windowMs, long sequence,
                       List<TopKEntry> urlTop, List<TopKEntry> userTop,
                       double distinctUsers, double p95Bytes) {
}