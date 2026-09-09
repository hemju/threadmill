package com.hemju.threadmill.core.store;

import com.hemju.threadmill.core.JobId;

/**
 * Result of inspecting a bounded page of terminal records for retention.
 *
 * @param deleted records actually deleted, excluding protected or changed jobs
 * @param nextAfter exclusive id cursor for the next page; null when the pass is complete
 */
public record RetentionPage(long deleted, JobId nextAfter) {}
