package io.flowlens.http;

import io.flowlens.runtime.FlowLensRuntime;
import io.flowlens.window.QueryResult;
import io.flowlens.window.StatsResult;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Global SSE snapshot publisher. One platform thread computes one global
 * snapshot per interval and broadcasts it to every connected client.
 *
 * Snapshot computation participates in the same query coordination as HTTP
 * Top-K/Stats: it acquires the shared query semaphore non-blockingly and skips
 * a tick when the permit is unavailable. Client registration and shutdown are
 * serialized by a monitor so no client can register after stop().
 */
public final class SnapshotPublisher {

    private final FlowLensRuntime runtime;
    private final long intervalMs;
    private final int windowSeconds;
    private final Semaphore querySemaphore;

    private final CopyOnWriteArrayList<SseClient> clients = new CopyOnWriteArrayList<>();
    private final Object stateLock = new Object();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicInteger computeErrors = new AtomicInteger();
    private final AtomicInteger skippedTicks = new AtomicInteger();

    private volatile boolean stopped;
    private Thread thread;

    public SnapshotPublisher(FlowLensRuntime runtime, long intervalMs, int windowSeconds, Semaphore querySemaphore) {
        this.runtime = runtime;
        this.intervalMs = intervalMs;
        this.windowSeconds = windowSeconds;
        this.querySemaphore = querySemaphore;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        thread = new Thread(this::run, "flowlens-snapshot-publisher");
        thread.start();
    }

    private void run() {
        while (running.get()) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (!querySemaphore.tryAcquire()) {
                skippedTicks.incrementAndGet();
                continue;
            }
            try {
                Snapshot snapshot = compute();
                broadcast(snapshot);
            } catch (Throwable t) {
                computeErrors.incrementAndGet();
            } finally {
                querySemaphore.release();
            }
        }
    }

    private Snapshot compute() {
        long now = System.currentTimeMillis();
        long windowMs = windowSeconds * 1000L;
        QueryResult url = runtime.queryTopK(now, windowMs, true, 10);
        QueryResult user = runtime.queryTopK(now, windowMs, false, 10);
        StatsResult stats = runtime.queryStats(now, windowMs);
        return new Snapshot(stats.effectiveAt(), windowMs, sequence.incrementAndGet(),
                List.copyOf(url.items()), List.copyOf(user.items()),
                stats.distinctUsers(), stats.p95Bytes());
    }

    private void broadcast(Snapshot snapshot) {
        for (SseClient client : clients) {
            client.enqueue(snapshot);
        }
    }

    /** Returns false and closes the client when the publisher is already stopped. */
    boolean register(SseClient client) {
        synchronized (stateLock) {
            if (stopped) {
                client.close();
                return false;
            }
            clients.add(client);
            return true;
        }
    }

    void unregister(SseClient client) {
        synchronized (stateLock) {
            clients.remove(client);
        }
    }

    public void stop() {
        synchronized (stateLock) {
            stopped = true;
            for (SseClient client : clients) {
                client.close();
            }
            clients.clear();
        }
        running.compareAndSet(true, false);
        Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
    }

    public int computeErrors() {
        return computeErrors.get();
    }

    public int skippedTicks() {
        return skippedTicks.get();
    }

    public int clientCount() {
        return clients.size();
    }
}