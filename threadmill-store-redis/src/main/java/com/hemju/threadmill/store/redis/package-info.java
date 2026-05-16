/**
 * Redis-backed {@link com.hemju.threadmill.core.store.JobStore} implementation.
 *
 * <p>Uses reliable-fetch claim semantics (the job moves to a per-node
 * processing ZSET as part of the claim — never a destructive pop) and an
 * atomic Lua script for every multi-key state transition. Run Redis with
 * AOF persistence (`appendonly yes`) for the durability a job store needs.
 */
package com.hemju.threadmill.store.redis;
