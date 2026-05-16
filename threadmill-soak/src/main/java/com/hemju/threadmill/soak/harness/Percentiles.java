package com.hemju.threadmill.soak.harness;

import java.util.Arrays;

/**
 * Sort-based percentile utility. A soak run records at most a few hundred
 * thousand samples — sorting is faster and simpler than an approximate
 * histogram for this scale, and avoids pulling in HdrHistogram for one use.
 */
public final class Percentiles {

    private Percentiles() {}

    public record Summary(long p50, long p95, long p99, long max, long count) {

        public static Summary empty() {
            return new Summary(0, 0, 0, 0, 0);
        }
    }

    public static Summary summarise(long[] values) {
        if (values.length == 0) return Summary.empty();
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return new Summary(
                quantileSorted(sorted, 0.50),
                quantileSorted(sorted, 0.95),
                quantileSorted(sorted, 0.99),
                sorted[sorted.length - 1],
                sorted.length);
    }

    public static long percentile(long[] values, double p) {
        if (values.length == 0) return 0L;
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return quantileSorted(sorted, p);
    }

    private static long quantileSorted(long[] sorted, double p) {
        int idx = (int) Math.min(sorted.length - 1, Math.max(0, Math.round(p * (sorted.length - 1))));
        return sorted[idx];
    }
}
