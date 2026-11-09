package loadtest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Standalone FlowLens ingest load generator (JDK only). */
public final class LoadTest {

    private static String idPrefix = "lt-" + System.currentTimeMillis() + "-";
    private static final Pattern NUMBER =
            Pattern.compile("\"(accepted|deduped|rejected|overloaded|unprocessed)\":(\\d+|true|false)");

    public static void main(String[] args) throws Exception {
        String url = strArg(args, "--url", "http://127.0.0.1:8080/api/v1/ingest");
        String mode = strArg(args, "--mode", "ingest");
        idPrefix = strArg(args, "--id-prefix", "lt-" + System.currentTimeMillis() + "-");
        int rate = intArg(args, "--rate", 100_000);
        int duration = intArg(args, "--duration", 30);
        int batch = intArg(args, "--batch", 500);
        int concurrency = intArg(args, "--concurrency", 8);
        int requestTimeout = intArg(args, "--request-timeout", 10);

        if (!"ingest".equals(mode)) {
            queryBenchmark(mode, url, duration, concurrency, requestTimeout);
            return;
        }

        RateLimiter limiter = new RateLimiter(rate);
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

        AtomicLong eventSeq = new AtomicLong();
        AtomicLong offered = new AtomicLong();
        AtomicLong accepted = new AtomicLong();
        AtomicLong deduped = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        AtomicLong overloaded = new AtomicLong();
        AtomicLong unprocessed = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        AtomicLong concurrency503 = new AtomicLong();
        ConcurrentHashMap<Integer, Long> statusCounts = new ConcurrentHashMap<>();
        LatencyHistogram successLatency = new LatencyHistogram(10_000);
        LatencyHistogram errorLatency = new LatencyHistogram(10_000);

        long cpuStart = processCpuNanos();
        long startNanos = System.nanoTime();
        long deadline = startNanos + duration * 1_000_000_000L;
        ExecutorService workers = Executors.newFixedThreadPool(concurrency);
        java.util.List<Future<?>> futures = new java.util.ArrayList<>();
        for (int w = 0; w < concurrency; w++) {
            futures.add(workers.submit(() -> {
                while (System.nanoTime() < deadline) {
                    long base = eventSeq.getAndAdd(batch);
                    String body = buildBatch(base, batch);
                    limiter.acquire(batch);
                    offered.addAndGet(batch);
                    long t0 = System.nanoTime();
                    try {
                        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofByteArray(body.getBytes(StandardCharsets.UTF_8)))
                                .timeout(Duration.ofSeconds(requestTimeout))
                                .build();
                        HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
                        statusCounts.merge(resp.statusCode(), 1L, Long::sum);
                        String text = new String(resp.body(), StandardCharsets.UTF_8);
                        if (resp.statusCode() == 503 && text.contains("ingest concurrency")) {
                            concurrency503.incrementAndGet();
                        }
                        Matcher m = NUMBER.matcher(text);
                        while (m.find()) {
                            String name = m.group(1);
                            String value = m.group(2);
                            long v = value.equals("true") ? 1 : value.equals("false") ? 0 : Long.parseLong(value);
                            switch (name) {
                                case "accepted" -> accepted.addAndGet(v);
                                case "deduped" -> deduped.addAndGet(v);
                                case "rejected" -> rejected.addAndGet(v);
                                case "overloaded" -> overloaded.addAndGet(v);
                                case "unprocessed" -> unprocessed.addAndGet(v);
                            }
                        }
                        successLatency.record(t0, System.nanoTime());
                    } catch (Exception e) {
                        errors.incrementAndGet();
                        errorLatency.record(t0, System.nanoTime());
                    }
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        workers.shutdown();
        long elapsedNanos = System.nanoTime() - startNanos;
        long cpuEnd = processCpuNanos();
        double elapsedSec = elapsedNanos / 1e9;

        System.out.printf("offered=%d accepted=%d deduped=%d rejected=%d overloaded=%d unprocessed=%d errors=%d concurrency503=%d%n",
                offered.get(), accepted.get(), deduped.get(), rejected.get(), overloaded.get(), unprocessed.get(), errors.get(), concurrency503.get());
        System.out.printf("offered_rate=%.1f/s accepted_rate=%.1f/s%n", offered.get() / elapsedSec, accepted.get() / elapsedSec);
        System.out.printf("success_latency_ms p50=%.2f p95=%.2f p99=%.2f%n",
                successLatency.percentile(0.50), successLatency.percentile(0.95), successLatency.percentile(0.99));
        System.out.printf("error_latency_ms p50=%.2f p95=%.2f p99=%.2f%n",
                errorLatency.percentile(0.50), errorLatency.percentile(0.95), errorLatency.percentile(0.99));
        System.out.println("status_codes=" + statusCounts);
        double clientCpuPct = (cpuEnd - cpuStart) / 1_000_000.0 / (elapsedSec * 1000.0) * 100.0;
        System.out.printf("client_cpu_pct=%.2f%n", clientCpuPct);
    }

    private static void queryBenchmark(String mode, String url, int duration, int concurrency, int requestTimeout) throws Exception {
        if (!"topk".equals(mode) && !"stats".equals(mode) && !"sse".equals(mode)) {
            throw new IllegalArgumentException("--mode must be ingest, topk, stats or sse");
        }
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        AtomicLong success = new AtomicLong();
        AtomicLong failures = new AtomicLong();
        AtomicLong rateLimited = new AtomicLong();
        AtomicLong events = new AtomicLong();
        ConcurrentHashMap<Integer, Long> statusCounts = new ConcurrentHashMap<>();
        LatencyHistogram successLatency = new LatencyHistogram(10_000);
        LatencyHistogram errorLatency = new LatencyHistogram(10_000);

        long deadline = System.nanoTime() + duration * 1_000_000_000L;
        ExecutorService workers = Executors.newFixedThreadPool(concurrency);
        java.util.List<Future<?>> futures = new java.util.ArrayList<>();
        for (int w = 0; w < concurrency; w++) {
            futures.add(workers.submit(() -> {
                while (System.nanoTime() < deadline) {
                    long t0 = System.nanoTime();
                    try {
                        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(requestTimeout)).GET().build();
                        if ("sse".equals(mode)) {
                            HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());
                            statusCounts.merge(resp.statusCode(), 1L, Long::sum);
                            if (resp.statusCode() == 429) {
                                rateLimited.incrementAndGet();
                                errorLatency.record(t0, System.nanoTime());
                            } else {
                                try (BufferedReader br = new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
                                    long end = System.nanoTime() + 2_000_000_000L;
                                    while (System.nanoTime() < end && System.nanoTime() < deadline) {
                                        String line = br.readLine();
                                        if (line == null) {
                                            break;
                                        }
                                        if (line.startsWith("data:")) {
                                            events.incrementAndGet();
                                        }
                                    }
                                }
                                successLatency.record(t0, System.nanoTime());
                            }
                        } else {
                            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
                            statusCounts.merge(resp.statusCode(), 1L, Long::sum);
                            if (resp.statusCode() == 429) {
                                rateLimited.incrementAndGet();
                                errorLatency.record(t0, System.nanoTime());
                            } else if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                                success.incrementAndGet();
                                successLatency.record(t0, System.nanoTime());
                            } else {
                                failures.incrementAndGet();
                                errorLatency.record(t0, System.nanoTime());
                            }
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                        errorLatency.record(t0, System.nanoTime());
                    }
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        workers.shutdown();
        System.out.printf("mode=%s success=%d failures=%d rate_limited_429=%d sse_events=%d status_codes=%s%n",
                mode, success.get(), failures.get(), rateLimited.get(), events.get(), statusCounts);
        System.out.printf("success_latency_ms p50=%.2f p95=%.2f p99=%.2f%n",
                successLatency.percentile(0.50), successLatency.percentile(0.95), successLatency.percentile(0.99));
        System.out.printf("error_latency_ms p50=%.2f p95=%.2f p99=%.2f%n",
                errorLatency.percentile(0.50), errorLatency.percentile(0.95), errorLatency.percentile(0.99));
    }
    private static String buildBatch(long base, int count) {
        long ts = System.currentTimeMillis() - 1000;
        StringBuilder sb = new StringBuilder(count * 112);
        sb.append('[');
        for (int i = 0; i < count; i++) {
            long seq = base + i;
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"event_id\":\"").append(idPrefix).append(seq)
              .append("\",\"ts\":").append(ts)
              .append(",\"user_id\":\"u").append(seq % 10_000)
              .append("\",\"url\":\"/p/").append(seq % 1_000)
              .append("?x=").append(seq)
              .append("\",\"bytes\":").append(100 + (seq % 900))
              .append('}');
        }
        sb.append(']');
        return sb.toString();
    }

    private static long processCpuNanos() {
        com.sun.management.OperatingSystemMXBean mx =
                (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        return mx.getProcessCpuTime();
    }

    private static String strArg(String[] args, String key, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return def;
    }

    private static int intArg(String[] args, String key, int def) {
        return Integer.parseInt(strArg(args, key, Integer.toString(def)));
    }

    private static final class RateLimiter {
        private final long nanosPerUnit;
        private long nextNanos = System.nanoTime();

        RateLimiter(int unitsPerSecond) {
            this.nanosPerUnit = (long) (1_000_000_000.0 / unitsPerSecond);
        }

        synchronized void acquire(int units) {
            long target = nextNanos + units * nanosPerUnit;
            long now = System.nanoTime();
            nextNanos = Math.max(target, now);
            if (target > now) {
                LockSupport.parkNanos(target - now);
            }
        }
    }

    private static final class LatencyHistogram {
        private final long[] buckets;
        private long total;

        LatencyHistogram(int maxMillis) {
            this.buckets = new long[maxMillis + 1];
        }

        void record(long startNanos, long endNanos) {
            long ms = (endNanos - startNanos) / 1_000_000;
            int idx = (int) Math.min(ms, buckets.length - 1);
            buckets[idx]++;
            total++;
        }

        double percentile(double q) {
            if (total == 0) {
                return 0.0;
            }
            long target = (long) Math.ceil(q * total);
            long cum = 0;
            for (int i = 0; i < buckets.length; i++) {
                cum += buckets[i];
                if (cum >= target) {
                    return i;
                }
            }
            return buckets.length - 1;
        }
    }
}