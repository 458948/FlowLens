package io.flowlens.http;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.flowlens.runtime.FlowLensRuntime;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * HTTP server wiring all six endpoints with hard concurrency limits.
 *
 * Close order: atomically mark closing, stop the SnapshotPublisher and close
 * every SSE exchange, call HttpServer.stop(delay) to stop accepting and give
 * normal exchanges time to finish, then interrupt and settle any remaining
 * handlers, then drain the runtime, and finally shut down the virtual-thread
 * executor. Each phase is reported separately and never assumed successful.
 */
public final class FlowLensHttpServer {

    public static final int INGEST_CONCURRENCY = 4;
    public static final int QUERY_CONCURRENCY = 1;
    public static final int SSE_MAX_CONNECTIONS = 16;
    public static final int BACKLOG = 128;
    public static final long DEFAULT_SNAPSHOT_INTERVAL_MS = 1_000L;
    public static final long DEFAULT_CLOSE_GRACE_MS = 5_000L;

    public record CloseResult(boolean drained, boolean httpIncomplete, boolean handlersNotExited,
                              boolean runtimeDrained, long unprocessedEvents) {
    }

    final Semaphore ingestSemaphore = new Semaphore(INGEST_CONCURRENCY);
    final Semaphore querySemaphore = new Semaphore(QUERY_CONCURRENCY);
    final Semaphore sseSemaphore = new Semaphore(SSE_MAX_CONNECTIONS);

    private final ServerState state = new ServerState();
    private final Object admissionLock = new Object();
    private final HttpServer server;
    private final ExecutorService executor;
    private final FlowLensRuntime runtime;
    private final SnapshotPublisher publisher;
    private final long closeGraceMs;

    private volatile CloseResult lastResult;

    public FlowLensHttpServer(FlowLensRuntime runtime, String host, int port) throws IOException {
        this(runtime, host, port, DEFAULT_SNAPSHOT_INTERVAL_MS, DEFAULT_CLOSE_GRACE_MS);
    }

    public FlowLensHttpServer(FlowLensRuntime runtime, String host, int port, long snapshotIntervalMs) throws IOException {
        this(runtime, host, port, snapshotIntervalMs, DEFAULT_CLOSE_GRACE_MS);
    }

    public FlowLensHttpServer(FlowLensRuntime runtime, String host, int port, long snapshotIntervalMs, long closeGraceMs) throws IOException {
        this.runtime = runtime;
        this.closeGraceMs = closeGraceMs;
        this.server = HttpServer.create(new InetSocketAddress(host, port), BACKLOG);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.server.setExecutor(executor);
        this.publisher = new SnapshotPublisher(runtime, snapshotIntervalMs, 60, querySemaphore);

        this.server.createContext("/healthz", tracked(new HealthHandler(runtime)));
        this.server.createContext("/readyz", tracked(new ReadyHandler(runtime)));

        HttpHandler ingestHandler = guarded(new IngestHandler(runtime, ingestSemaphore));
        HttpHandler topkHandler = guarded(new TopKHandler(runtime, querySemaphore));
        HttpHandler statsHandler = guarded(new StatsHandler(runtime, querySemaphore));
        HttpHandler sseHandler = guarded(new SseHandler(publisher, sseSemaphore));

        this.server.createContext("/api/v1/ingest", ingestHandler);
        this.server.createContext("/v1/ingest", ingestHandler);
        this.server.createContext("/api/v1/topk", topkHandler);
        this.server.createContext("/v1/topk", topkHandler);
        this.server.createContext("/api/v1/stats", statsHandler);
        this.server.createContext("/v1/stats", statsHandler);
        this.server.createContext("/api/v1/stream", sseHandler);
        this.server.createContext("/v1/topk/stream", sseHandler);
    }

    public void start() {
        publisher.start();
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public CloseResult close() {
        synchronized (admissionLock) {
            if (!state.closing.compareAndSet(false, true)) {
                return lastResult;
            }
        }
        publisher.stop();

            int delaySeconds = Math.max(1, (int) Math.ceil(closeGraceMs / 1000.0));
            server.stop(delaySeconds);

            boolean httpIncomplete = state.inFlight() > 0;
            boolean handlersNotExited = false;
            if (httpIncomplete) {
                executor.shutdownNow();
                LockSupport.parkNanos(50_000_000L);
                handlersNotExited = state.inFlight() > 0;
            }

            boolean runtimeDrained = runtime.close();

            executor.shutdown();
            try {
                executor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.isTerminated()) {
                executor.shutdownNow();
                handlersNotExited = true;
            }

            long unprocessed = runtime.unprocessedEvents();
            boolean drained = !httpIncomplete && !handlersNotExited && runtimeDrained;
            lastResult = new CloseResult(drained, httpIncomplete, handlersNotExited, runtimeDrained, unprocessed);
        return lastResult;
    }

    private HttpHandler tracked(HttpHandler inner) {
        return exchange -> {
            synchronized (admissionLock) {
                state.inFlight.incrementAndGet();
            }
            try {
                inner.handle(exchange);
            } finally {
                state.inFlight.decrementAndGet();
            }
        };
    }

    private HttpHandler guarded(HttpHandler inner) {
        return exchange -> {
            if (!admit()) {
                HttpJson.error(exchange, 503, "server is closing");
                return;
            }
            try {
                inner.handle(exchange);
            } finally {
                state.inFlight.decrementAndGet();
            }
        };
    }

    /**
     * Atomically checks the closing flag and increments the in-flight counter.
     * close() flips the flag under the same lock, so a handler can never pass
     * the closing check after close() has observed inFlight == 0.
     */
    private boolean admit() {
        synchronized (admissionLock) {
            if (state.isClosing()) {
                return false;
            }
            state.inFlight.incrementAndGet();
            return true;
        }
    }

    public long unprocessedEvents() {
        return runtime.unprocessedEvents();
    }

    public int inFlightCount() {
        return state.inFlight();
    }

    public int publisherClientCount() {
        return publisher.clientCount();
    }

    public int publisherSkippedTicks() {
        return publisher.skippedTicks();
    }
}