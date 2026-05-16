package com.hemju.threadmill.core.engine;

import java.util.Objects;

import com.hemju.threadmill.core.Names;

/**
 * A queue-pattern lane definition.
 *
 * <p>Patterns are anchored and support only {@code *} and {@code ?}
 * wildcards. Literal {@code %}, {@code _}, character classes, and double-star
 * patterns are rejected so the store-side discovery shape stays simple.
 */
public final class QueueFamily {

    private final String pattern;
    private final QueueWeights weights;

    public QueueFamily(String pattern, QueueWeights weights) {
        this.pattern = validate(pattern);
        this.weights = Objects.requireNonNull(weights, "weights");
    }

    public String pattern() {
        return pattern;
    }

    public QueueWeights weights() {
        return weights;
    }

    public boolean matches(String queue) {
        Names.requireName("queue", queue);
        return matches(pattern, 0, queue, 0);
    }

    String toPostgresLikePattern() {
        return pattern.replace("*", "%").replace("?", "_");
    }

    private static String validate(String pattern) {
        Names.requireName("queueFamily", pattern);
        if (pattern.contains("**")) {
            throw new IllegalArgumentException("queue-family pattern must not contain double-star");
        }
        if (pattern.indexOf('[') >= 0 || pattern.indexOf(']') >= 0) {
            throw new IllegalArgumentException("queue-family pattern must not contain character classes");
        }
        if (pattern.indexOf('%') >= 0 || pattern.indexOf('_') >= 0) {
            throw new IllegalArgumentException("queue-family pattern must not contain '%' or '_' literals");
        }
        return pattern;
    }

    private static boolean matches(String pattern, int p, String value, int v) {
        if (p == pattern.length()) {
            return v == value.length();
        }
        char pc = pattern.charAt(p);
        if (pc == '*') {
            for (int next = v; next <= value.length(); next++) {
                if (next > v && value.charAt(next - 1) == ':') {
                    break;
                }
                if (matches(pattern, p + 1, value, next)) {
                    return true;
                }
            }
            return false;
        }
        if (v == value.length()) {
            return false;
        }
        if (pc == '?' || pc == value.charAt(v)) {
            return matches(pattern, p + 1, value, v + 1);
        }
        return false;
    }
}
