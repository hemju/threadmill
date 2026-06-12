package com.hemju.threadmill.soak.harness;

import java.util.Arrays;

/**
 * Append-only primitive long buffer for percentile aggregation. An endurance
 * run accumulates millions of samples per stage; a boxed {@code List<Long>}
 * at that scale costs an order of magnitude more memory than the primitive
 * array, so the post-run aggregation paths use this instead.
 *
 * <p>Not thread-safe — callers are single-threaded aggregation passes.
 */
final class GrowableLongArray {

    private long[] values = new long[1024];
    private int size;

    void add(long value) {
        if (size == values.length) values = Arrays.copyOf(values, values.length * 2);
        values[size++] = value;
    }

    int size() {
        return size;
    }

    /** The backing array trimmed to size; the buffer must not be used afterwards. */
    long[] toArray() {
        return Arrays.copyOf(values, size);
    }
}
