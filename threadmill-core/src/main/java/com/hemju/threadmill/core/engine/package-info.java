/**
 * The processing engine: the per-application-instance
 * {@link com.hemju.threadmill.core.engine.ProcessingNode}, its
 * {@link com.hemju.threadmill.core.engine.Dispatcher} and
 * {@link com.hemju.threadmill.core.engine.MaintenanceCycle} loops, its
 * {@link com.hemju.threadmill.core.engine.NodeRegistry}, and the
 * {@link com.hemju.threadmill.core.engine.JobInterceptor} SPI.
 *
 * <p>Threadmill provides <strong>at-least-once</strong> delivery; handlers
 * must be idempotent. Orphan recovery, timeouts, and thrown exceptions all
 * funnel through one failure code path in
 * {@link com.hemju.threadmill.core.engine.JobRunner} so the
 * {@link com.hemju.threadmill.core.engine.JobInterceptor#onProcessingFailed}
 * hook (and the retry interceptor) is invoked uniformly regardless of cause.
 */
package com.hemju.threadmill.core.engine;
