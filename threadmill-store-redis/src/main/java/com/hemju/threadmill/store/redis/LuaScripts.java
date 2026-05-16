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
    private static final Map<String, String> CACHE = new ConcurrentHashMap<>();

    private LuaScripts() {}

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

    public static String replaceJob() {
        return load("replace_job.lua");
    }

    public static String mutexAcquire() {
        return load("mutex_acquire.lua");
    }

    private static String load(String name) {
        return CACHE.computeIfAbsent(name, n -> {
            try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(ROOT + n)) {
                if (in == null) throw new IllegalStateException("Lua script not found: " + ROOT + n);
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read Lua script: " + ROOT + n, e);
            }
        });
    }
}
