/**
 * PostgreSQL-backed {@link com.hemju.threadmill.core.store.JobStore} implementation.
 *
 * <p>Uses {@code SELECT ... FOR UPDATE SKIP LOCKED} for the claim path,
 * partial indexes matched to the engine's hot query shapes, an
 * incrementally-maintained per-state counter table fed by a trigger, and
 * deadlock-aware retry around every write.
 */
package com.hemju.threadmill.store.postgres;
