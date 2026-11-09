package io.flowlens.window;

import io.flowlens.sketch.TopKEntry;
import java.util.List;

public record QueryResult(long effectiveAt, boolean stable, List<TopKEntry> items) {
}