/**
 * Micrometer integration for Threadmill: per-state gauges sourced from the
 * incrementally-maintained counters, processed/failed counters tagged by
 * cause, and a processing-time timer. Wires in as a
 * {@link com.hemju.threadmill.core.engine.JobInterceptor}.
 */
package com.hemju.threadmill.metrics;
