/**
 * Framework-neutral dashboard model and service contract for Threadmill.
 * Data-first: the dashboard is a <em>consumer</em> of the observability API
 * ({@link com.hemju.threadmill.dashboard.api.EngineSnapshot} and the metrics
 * module) — never the only way to read engine state.
 */
package com.hemju.threadmill.dashboard.api;
