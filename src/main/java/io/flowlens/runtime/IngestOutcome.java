package io.flowlens.runtime;

/** Result of a single ingest admission attempt. */
public enum IngestOutcome {
    ACCEPTED,
    DEDUPED,
    OVERLOADED,
    REJECTED,
    CLOSED
}