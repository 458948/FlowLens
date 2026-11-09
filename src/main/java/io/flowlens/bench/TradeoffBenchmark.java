package io.flowlens.bench;

import io.flowlens.ingest.RawEvent;
import io.flowlens.runtime.FlowLensRuntime;
import io.flowlens.runtime.IngestOutcome;
import io.flowlens.runtime.Shard;
import io.flowlens.sketch.SpaceSaving;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/** Local (non-Docker) benchmark for baseline throughput and SpaceSaving recall. */
public final class TradeoffBenchmark {

    public static void main(String[] args) throws Exception {
        System.out.println("== FlowLens local benchmark (Windows, JVM only) ==");
        printConfig();
        ingestThroughput();
        spaceSavingRecall();
    }

    private static void printConfig() {
        System.out.println("config: shards=4, ringBuckets=128, spaceSavingC=256, hllP=13, "
                + "histogramBins=505, bloomSlices=3x30s, bloomFp=1e-6, bloomProbes=20, "
                + "ingestConcurrency=8, queryConcurrency=1, sseConnections=32, queueCapacity=2048");
    }

    private static void ingestThroughput() throws Exception {
        int n = 1_000_000;
        long ts = System.currentTimeMillis() - 1_000;
        FlowLensRuntime runtime = new FlowLensRuntime();

        long offered = 0, accepted = 0, deduped = 0, overloaded = 0, rejected = 0, closed = 0;
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            IngestOutcome o = runtime.ingest(event("ev-" + i, ts, "url-" + (i % 1000), "user-" + (i % 100)));
            offered++;
            switch (o) {
                case ACCEPTED -> accepted++;
                case DEDUPED -> deduped++;
                case OVERLOADED -> overloaded++;
                case REJECTED -> rejected++;
                case CLOSED -> closed++;
            }
        }
        long t1 = System.nanoTime();

        long processed = 0;
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            processed = 0;
            for (Shard s : runtime.shards()) {
                processed += s.processed();
            }
            if (processed >= accepted) {
                break;
            }
            Thread.sleep(5);
        }
        long t2 = System.nanoTime();
        runtime.close();

        double offerSec = (t1 - t0) / 1e9;
        double processSec = (t2 - t0) / 1e9;
        System.out.printf("ingest: offered=%d accepted=%d processed=%d deduped=%d overloaded=%d rejected=%d closed=%d%n",
                offered, accepted, processed, deduped, overloaded, rejected, closed);
        System.out.printf("ingest rates: offered=%.0f/s accepted=%.0f/s processed=%.0f/s (local, not Docker)%n",
                offered / offerSec, accepted / offerSec, processed / processSec);
    }

    private static void spaceSavingRecall() {
        int keys = 1000;
        int n = 500_000;
        for (int c : new int[]{256, 192, 128}) {
            SpaceSaving ss = new SpaceSaving(c, 64);
            long[] exact = new long[keys];
            Random rnd = new Random(42);
            for (int i = 0; i < n; i++) {
                int k = zipf(rnd, keys);
                ss.observe(key(k), 0, keyLen(k));
                exact[k]++;
            }

            int[] trueTop = topIndices(exact, 10);
            int[] sketchTop = topSketch(ss, 10);

            Set<Integer> truthSet = new HashSet<>();
            for (int idx : trueTop) {
                truthSet.add(idx);
            }
            int hits = 0;
            for (int idx : sketchTop) {
                if (truthSet.contains(idx)) {
                    hits++;
                }
            }
            double recall = hits / 10.0;

            long heavy = 0, heavyPresent = 0;
            double sumErr = 0;
            int present = 0;
            long e = n / c;
            for (int k = 0; k < keys; k++) {
                long est = ss.estimate(key(k), 0, keyLen(k));
                if (exact[k] > e) {
                    heavy++;
                    if (est != -1) {
                        heavyPresent++;
                    }
                }
                if (est != -1) {
                    present++;
                    sumErr += Math.abs(est - exact[k]);
                }
            }
            double heavyRecall = heavy == 0 ? 1.0 : (double) heavyPresent / heavy;
            double avgErr = present == 0 ? 0 : sumErr / present;

            System.out.printf("SpaceSaving C=%d N=%d E=%d recall@10=%.2f heavyRecall=%.2f avgAbsErr=%.1f present=%d%n",
                    c, n, e, recall, heavyRecall, avgErr, present);
        }
    }

    private static int[] topIndices(long[] counts, int k) {
        Integer[] idx = new Integer[counts.length];
        for (int i = 0; i < idx.length; i++) {
            idx[i] = i;
        }
        Arrays.sort(idx, (a, b) -> Long.compare(counts[b], counts[a]));
        int[] out = new int[k];
        for (int i = 0; i < k; i++) {
            out[i] = idx[i];
        }
        return out;
    }

    private static int[] topSketch(SpaceSaving ss, int k) {
        Integer[] idx = new Integer[ss.size()];
        for (int i = 0; i < idx.length; i++) {
            idx[i] = i;
        }
        Arrays.sort(idx, (a, b) -> Long.compare(ss.count(b), ss.count(a)));
        int m = Math.min(k, idx.length);
        int[] out = new int[m];
        for (int i = 0; i < m; i++) {
            int cand = idx[i];
            String name = new String(ss.nameBytes(), ss.nameOffset(cand), ss.nameLength(cand), StandardCharsets.UTF_8);
            out[i] = Integer.parseInt(name.substring(1));
        }
        return out;
    }

    private static int zipf(Random rnd, int keys) {
        double u = rnd.nextDouble();
        int k = (int) (keys * Math.pow(u, 4.0));
        return Math.max(0, Math.min(keys - 1, k));
    }

    private static byte[] key(int k) {
        return ("k" + k).getBytes(StandardCharsets.UTF_8);
    }

    private static int keyLen(int k) {
        return ("k" + k).length();
    }

    private static RawEvent event(String id, long ts, String url, String user) {
        return new RawEvent(id.getBytes(StandardCharsets.UTF_8),
                user.getBytes(StandardCharsets.UTF_8),
                url.getBytes(StandardCharsets.UTF_8),
                ts, 1000L);
    }
}