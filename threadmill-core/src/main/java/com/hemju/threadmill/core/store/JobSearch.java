package com.hemju.threadmill.core.store;

import java.util.Objects;

import com.hemju.threadmill.core.JobState;

/** Bounded dashboard/search query over persisted jobs. */
public record JobSearch(JobState state, String queue, String handlerType, int limit, int offset) {

    public static final int DEFAULT_LIMIT = 50;
    public static final int MAX_LIMIT = 500;

    public JobSearch {
        if (queue != null && queue.isBlank()) queue = null;
        if (handlerType != null && handlerType.isBlank()) handlerType = null;
        if (limit <= 0) limit = DEFAULT_LIMIT;
        if (limit > MAX_LIMIT) limit = MAX_LIMIT;
        if (offset < 0) offset = 0;
    }

    public static JobSearch all() {
        return new JobSearch(null, null, null, DEFAULT_LIMIT, 0);
    }

    public boolean matchesState(JobState candidate) {
        Objects.requireNonNull(candidate, "candidate");
        return state == null || state == candidate;
    }

    public boolean matchesQueue(String candidate) {
        return queue == null || queue.equals(candidate);
    }

    public boolean matchesHandler(String candidate) {
        return handlerType == null || handlerType.equals(candidate);
    }
}
