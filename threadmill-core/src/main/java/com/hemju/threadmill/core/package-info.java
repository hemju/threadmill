/**
 * Threadmill core — the framework-agnostic heart of the library.
 *
 * <p>This package holds the job model, the state machine, the exceptions, and
 * the small value types that the rest of the library is built on. No storage
 * implementation and no framework code may live here.
 *
 * <p>Threadmill provides <strong>at-least-once</strong> delivery: a job may
 * execute more than once (for example after a node crash mid-execution).
 * Job handlers must therefore be idempotent.
 */
package com.hemju.threadmill.core;
