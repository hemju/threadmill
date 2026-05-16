/**
 * Optional monitoring dashboard for Threadmill, mountable into the host
 * application. Data-first: the dashboard is a <em>consumer</em> of the
 * observability API ({@link com.hemju.threadmill.dashboard.EngineSnapshot}
 * and the metrics module) — never the only way to read engine state.
 */
package com.hemju.threadmill.dashboard;
