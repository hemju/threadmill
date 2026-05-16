/**
 * Test support shared across every concrete {@code JobStore}.
 *
 * <p>The headline artifact is
 * {@link com.hemju.threadmill.test.AbstractJobStoreContractTest}: the
 * single abstract test class that every {@code JobStore} implementation
 * (in-memory, PostgreSQL, Redis) extends and is held to. There is exactly
 * one contract, and every backend passes the same suite — this is the
 * only thing guaranteeing the stores behave identically.
 */
package com.hemju.threadmill.test;
