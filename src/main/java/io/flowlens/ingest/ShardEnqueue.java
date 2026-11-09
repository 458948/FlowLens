package io.flowlens.ingest;

/** Callback used by DedupAdmission to enqueue an event into a specific shard. */
@FunctionalInterface
public interface ShardEnqueue {
    boolean tryEnqueue(int shard);
}