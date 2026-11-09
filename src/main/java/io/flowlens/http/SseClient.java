package io.flowlens.http;

import com.sun.net.httpserver.HttpExchange;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One SSE connection with a capacity-one snapshot queue. New snapshots replace
 * the previous unsent snapshot; consecutive drops are counted for slow-client
 * detection.
 */
final class SseClient {

    static final int SLOW_DROP_LIMIT = 30;

    private final HttpExchange exchange;
    private final int id;
    private final ArrayBlockingQueue<Snapshot> queue = new ArrayBlockingQueue<>(1);
    private final AtomicInteger consecutiveDrops = new AtomicInteger();

    private volatile boolean closed;
    private volatile Thread handlerThread;

    SseClient(int id) {
        this(null, id);
    }

    SseClient(HttpExchange exchange, int id) {
        this.exchange = exchange;
        this.id = id;
    }

    int id() {
        return id;
    }

    HttpExchange exchange() {
        return exchange;
    }

    void attach(Thread thread) {
        this.handlerThread = thread;
    }

    void enqueue(Snapshot snapshot) {
        if (closed) {
            return;
        }
        Snapshot old = queue.poll();
        if (old != null) {
            consecutiveDrops.incrementAndGet();
        }
        queue.offer(snapshot);
    }

    Snapshot poll(long timeoutMs) throws InterruptedException {
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    int consecutiveDrops() {
        return consecutiveDrops.get();
    }

    void resetDrops() {
        consecutiveDrops.set(0);
    }

    boolean isClosed() {
        return closed;
    }

    void close() {
        closed = true;
        if (exchange != null) {
            exchange.close();
        }
        Thread t = handlerThread;
        if (t != null) {
            t.interrupt();
        }
    }
}