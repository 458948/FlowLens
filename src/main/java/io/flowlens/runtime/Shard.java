package io.flowlens.runtime;

import io.flowlens.ingest.RawEvent;
import io.flowlens.window.RingBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * One shard: a bounded MPSC-style queue, a single writer thread and a
 * RingBuffer. All aggregation state is mutated only by the writer thread.
 */
public final class Shard {

    public static final int QUEUE_CAPACITY = 2048;

    private final RingBuffer ring;
    private final ArrayBlockingQueue<RawEvent> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final LongSupplier nowMs;
    private final EventProcessor processor;
    private final Thread writer;

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong deduped = new AtomicLong();
    private final AtomicLong overloaded = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong processingErrors = new AtomicLong();
    private final AtomicBoolean degraded = new AtomicBoolean();

    private volatile boolean running;

    public Shard(RingBuffer ring) {
        this(ring, System::currentTimeMillis, Shard::defaultProcess);
    }

    public Shard(RingBuffer ring, LongSupplier nowMs, EventProcessor processor) {
        this.ring = ring;
        this.nowMs = nowMs;
        this.processor = processor;
        this.writer = new Thread(this::runLoop, "flowlens-shard-writer");
    }

    public RingBuffer ring() {
        return ring;
    }

    /** Non-blocking enqueue. Returns false when the queue is full. */
    public boolean tryEnqueue(RawEvent event) {
        return queue.offer(event);
    }

    public int queueSize() {
        return queue.size();
    }

    public int queueCapacity() {
        return QUEUE_CAPACITY;
    }

    public void start() {
        running = true;
        writer.start();
    }

    /** Drains the queue and stops the writer. Returns true when fully drained. */
    public boolean close(long timeoutMs) {
        running = false;
        try {
            writer.join(timeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        boolean alive = writer.isAlive();
        if (alive) {
            writer.interrupt();
        }
        return !alive;
    }

    /** Number of enqueued events not yet processed after close. */
    public int unprocessed() {
        return queue.size();
    }

    public boolean isWriterAlive() {
        return writer.isAlive();
    }

    private void runLoop() {
        while (running || !queue.isEmpty()) {
            RawEvent event;
            try {
                event = queue.poll(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (event != null) {
                process(event);
            }
        }
    }

    private void process(RawEvent event) {
        try {
            processor.process(ring, nowMs.getAsLong(), event);
            processed.incrementAndGet();
        } catch (Throwable t) {
            processingErrors.incrementAndGet();
            degraded.set(true);
        }
    }

    public static void defaultProcess(RingBuffer ring, long now, RawEvent event) {
        ring.acquire(event.ts, now).observe(event.ts, event.userId, event.url, event.bytes);
    }

    public void recordAccepted() {
        accepted.incrementAndGet();
    }

    public void recordDeduped() {
        deduped.incrementAndGet();
    }

    public void recordOverloaded() {
        overloaded.incrementAndGet();
    }

    public void recordRejected() {
        rejected.incrementAndGet();
    }

    public long accepted() {
        return accepted.get();
    }

    public long processed() {
        return processed.get();
    }

    public long deduped() {
        return deduped.get();
    }

    public long overloaded() {
        return overloaded.get();
    }

    public long rejected() {
        return rejected.get();
    }

    public long processingErrors() {
        return processingErrors.get();
    }

    public boolean isDegraded() {
        return degraded.get();
    }

    @FunctionalInterface
    public interface EventProcessor {
        void process(RingBuffer ring, long nowMs, RawEvent event);
    }
}