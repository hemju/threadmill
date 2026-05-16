/**
 * In-memory {@link com.hemju.threadmill.core.store.JobStore} for tests and
 * local development.
 *
 * <p>This store is held to the same abstract contract as the real backends.
 * It is not a simplified fake — tests across later phases rely on it
 * telling the truth about the SPI's semantics.
 */
package com.hemju.threadmill.store.memory;
