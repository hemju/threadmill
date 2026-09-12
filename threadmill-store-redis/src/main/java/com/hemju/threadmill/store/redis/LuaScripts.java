package com.hemju.threadmill.store.redis;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads and caches the Lua scripts shipped under
 * {@code com/hemju/threadmill/store/redis/lua/}.
 *
 * <p>Scripts are read once from the classpath at startup. {@code RedisJobStore}
 * evaluates the script body through Lettuce's synchronous scripting API.
 */
public final class LuaScripts {

  private static final String ROOT = "com/hemju/threadmill/store/redis/lua/";
  private static final String NO_KEY_TOKEN = "__THREADMILL_NO_KEY__";
  private static final String INDEX_HELPERS = read("pending_indexes.lua");
  private static final Map<String, String> CACHE = new ConcurrentHashMap<>();

  private LuaScripts() {}

  static String cleanupConcurrency() {
    return load("cleanup_concurrency.lua");
  }

  static String migratePending() {
    return load("migrate_pending.lua");
  }

  public static String insert() {
    return load("insert.lua");
  }

  public static String insertAll() {
    return load("insert_all.lua");
  }

  public static String enqueueIfAbsent() {
    return load("enqueue_if_absent.lua");
  }

  public static String saveAtomic() {
    return load("save_atomic.lua");
  }

  public static String claimCommit() {
    return load("claim_commit.lua");
  }

  public static String leaseAcquire() {
    return load("lease_acquire.lua");
  }

  public static String leaseRelease() {
    return load("lease_release.lua");
  }

  public static String softDelete() {
    return load("soft_delete.lua");
  }

  public static String touchHeartbeat() {
    return load("touch_heartbeat.lua");
  }

  static String touchExecutionHeartbeats() {
    return load("touch_execution_heartbeats.lua");
  }

  public static String dedupDelete() {
    return load("dedup_delete.lua");
  }

  public static String retentionDelete() {
    return load("retention_delete.lua");
  }

  static String retentionCandidates() {
    return load("retention_candidates.lua");
  }

  public static String queuePrune() {
    return load("queue_prune.lua");
  }

  public static String replaceJob() {
    return load("replace_job.lua");
  }

  public static String mutexAcquire() {
    return load("mutex_acquire.lua");
  }

  public static String quarantineUnreadable() {
    return load("quarantine_unreadable.lua");
  }

  private static String load(String name) {
    return CACHE.computeIfAbsent(name, n -> INDEX_HELPERS + "\n" + read(n));
  }

  private static String read(String name) {
    try (InputStream in =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(ROOT + name)) {
      if (in == null) throw new IllegalStateException("Lua script not found: " + ROOT + name);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8)
          .replace(NO_KEY_TOKEN, RedisKeys.NO_KEY)
          .replace("__THREADMILL_STORAGE_FORMAT_KEY__", RedisStorageFormat.KEY)
          .replace("__THREADMILL_STORAGE_FORMAT__", RedisStorageFormat.CURRENT)
          .replace("__THREADMILL_ORDERED_SUFFIX__", RedisKeys.ORDERED_SUFFIX)
          .replace("__THREADMILL_EXCLUSIVE_SUFFIX__", RedisKeys.EXCLUSIVE_SUFFIX)
          .replace("__THREADMILL_READY_SUFFIX__", RedisKeys.READY_SUFFIX)
          .replace("__THREADMILL_IDS_SUFFIX__", RedisKeys.IDS_SUFFIX);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read Lua script: " + ROOT + name, e);
    }
  }
}
