package io.flowlens.sketch;

/**
 * Logarithmic histogram with 505 bins.
 *
 * bin[0] holds bytes == 0. bins 1..504 cover positive values with eight
 * sub-bins per octave. Bin i (i >= 1) covers [2^((i-1)/8), 2^(i/8)).
 */
public final class LogHistogram {

    public static final int BINS = 505;

    private final long[] bins = new long[BINS];

    public void add(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        int index = indexFor(bytes);
        bins[index]++;
    }

    public int indexFor(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        if (bytes == 0) {
            return 0;
        }
        int e = 63 - Long.numberOfLeadingZeros(bytes);
        long normalized = bytes << (63 - e);
        int sub = (int) ((normalized >>> 60) & 7);
        return 1 + 8 * e + sub;
    }

    public long total() {
        long t = 0;
        for (long b : bins) {
            t += b;
        }
        return t;
    }

    public long count(int bin) {
        return bins[bin];
    }

    public long[] bins() {
        return bins;
    }

    /**
     * Returns an interpolated quantile estimate. The bucket that contains the
     * quantile is exact; the position inside the bucket is a geometric
     * (log-linear) interpolation and is therefore only an estimate.
     */
    public double estimateQuantile(double q) {
        if (q < 0.0 || q > 1.0) {
            throw new IllegalArgumentException("q must be in [0,1]");
        }
        long total = total();
        if (total == 0) {
            return 0.0;
        }
        long target = (long) Math.ceil(q * total);
        if (target <= 0) {
            target = 1;
        }
        long cumulative = 0;
        for (int i = 0; i < BINS; i++) {
            long before = cumulative;
            cumulative += bins[i];
            if (cumulative >= target) {
                long count = bins[i];
                if (i == 0 || count == 0) {
                    return lowerBound(i);
                }
                double frac = (double) (target - before) / count;
                return lowerBound(i) * Math.pow(2.0, frac / 8.0);
            }
        }
        return upperBound(BINS - 1);
    }

    public static double lowerBound(int bin) {
        if (bin == 0) {
            return 0.0;
        }
        return Math.pow(2.0, (bin - 1) / 8.0);
    }

    public static double upperBound(int bin) {
        if (bin == 0) {
            return 0.0;
        }
        if (bin == BINS - 1) {
            return Math.pow(2.0, 63.0);
        }
        return Math.pow(2.0, bin / 8.0);
    }

    public void mergeInto(LogHistogram target) {
        for (int i = 0; i < BINS; i++) {
            target.bins[i] += bins[i];
        }
    }

    public static LogHistogram merge(LogHistogram a, LogHistogram b) {
        LogHistogram out = new LogHistogram();
        a.mergeInto(out);
        b.mergeInto(out);
        return out;
    }
}