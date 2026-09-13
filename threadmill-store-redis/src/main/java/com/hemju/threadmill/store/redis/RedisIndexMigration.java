package com.hemju.threadmill.store.redis;

import java.util.UUID;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;

/**
 * Offline, resumable upgrade from the v0.3 Redis admission indexes.
 *
 * <p>Stop every producer and worker and take a backup before calling this tool.
 * It refuses live registered workers; applications must also stop producers,
 * which have no registration. A failed run can be repeated. The format marker
 * changes only after every state index has been visited. Job payloads, state,
 * versions, concurrency holds, and scheduled times are preserved. Mixed old
 * and new workers and downgrade after this migration are unsupported.
 *
 * <p>Clients remain caller-owned. Connections opened here are closed on exit.
 */
public final class RedisIndexMigration {
  private static final int PAGE_SIZE = 200;
  private static final long LEASE_MILLIS = 60_000;

  private RedisIndexMigration() {}

  /** Upgrade standalone or Sentinel storage; returns the number of visited job records. */
  public static long migrate(RedisClient client) {
    try (var connection = client.connect()) {
      return migrate(connection.sync());
    }
  }

  /** Upgrade Cluster storage; all namespace keys share the same hash slot. */
  public static long migrate(RedisClusterClient client) {
    try (var connection = client.connect()) {
      return migrate(connection.sync());
    }
  }

  private static long migrate(RedisClusterCommands<String, String> commands) {
    var format = commands.get(RedisStorageFormat.KEY);
    if (RedisStorageFormat.CURRENT.equals(format)) return 0;
    if (format != null && !format.equals("1") && !format.equals("migrating:2")) {
      throw new IllegalStateException("Unsupported Redis storage format; refusing to change it");
    }
    for (var node : commands.smembers(RedisKeys.NODES)) {
      if (commands.get(RedisKeys.nodeHeartbeat(NodeId.parse(node))) != null) {
        throw new IllegalStateException(
            "Stop every Threadmill worker and producer before migrating Redis indexes");
      }
    }
    var token = UUID.randomUUID().toString();
    if (!"OK"
        .equals(commands.set(
            RedisStorageFormat.MIGRATION_LOCK, token, SetArgs.Builder.nx().px(LEASE_MILLIS)))) {
      throw new IllegalStateException("Another Redis index migration owns the migration lease");
    }
    Throwable failure = null;
    try {
      Long started = commands.eval(
          """
          if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
          local format = redis.call('GET', KEYS[2])
          if format == ARGV[2] then return 0 end
          if format and format ~= '1' and format ~= 'migrating:2' then return -1 end
          redis.call('SET', KEYS[2], 'migrating:2')
          return 1
          """,
          ScriptOutputType.INTEGER,
          new String[] {RedisStorageFormat.MIGRATION_LOCK, RedisStorageFormat.KEY},
          token,
          RedisStorageFormat.CURRENT);
      if (started != null && started == 0) return 0;
      if (started == null || started != 1)
        throw new IllegalStateException(
            "Redis migration state changed; inspect the storage format before retrying");
      long visited = 0;
      // State indexes avoid SCAN's node-local routing in Redis Cluster. They
      // remain unchanged by this migration, making rank paging restart-safe.
      for (var state : JobState.values()) {
        for (long offset = 0; ; offset += PAGE_SIZE) {
          renew(commands, token);
          var ids = commands.zrange(RedisKeys.byStateTime(state), offset, offset + PAGE_SIZE - 1);
          for (var text : ids) {
            var id = JobId.parse(text);
            var hash = commands.hgetall(RedisKeys.job(id));
            if (hash.isEmpty()) continue;
            var key = hash.get("concurrency_key");
            var actualState = hash.get("state");
            if (key != null
                && !key.isEmpty()
                && (actualState.equals("ENQUEUED")
                    || actualState.equals("SCHEDULED")
                    || actualState.equals("AWAITING"))) {
              var mode = ConcurrencyMode.valueOf(hash.get("concurrency_mode"));
              var root = hash.get("workflow_root_id");
              var queue = hash.get("queue");
              Long changed = commands.eval(
                  LuaScripts.migratePending(),
                  ScriptOutputType.INTEGER,
                  new String[] {
                    RedisStorageFormat.MIGRATION_LOCK,
                    RedisKeys.concurrencyPending(key),
                    RedisKeys.concurrencyPendingRoot(key, root),
                    RedisKeys.queueKeys(queue)
                  },
                  token,
                  mode.name() + ":" + id,
                  RedisKeys.concurrencyPendingMember(mode, id),
                  actualState,
                  hash.get("current_state_at"),
                  key,
                  id.toString().equals(root) ? "0" : "1");
              if (changed == null || changed != 1)
                throw new IllegalStateException(
                    "Redis migration lease expired; rerun the migration");
            }
            Long indexed = commands.eval(
                """
                if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
                redis.call('ZADD', KEYS[2], 0, ARGV[2])
                return 1
                """,
                ScriptOutputType.INTEGER,
                new String[] {
                  RedisStorageFormat.MIGRATION_LOCK,
                  RedisKeys.byStateTime(JobState.valueOf(actualState)) + ":ids"
                },
                token,
                text);
            if (indexed == null || indexed != 1)
              throw new IllegalStateException("Redis migration lease expired; rerun the migration");
            visited++;
          }
          if (ids.size() < PAGE_SIZE) break;
        }
      }
      // Definitions are small compared with jobs, but still page SSCAN for the
      // offline upgrade. Runtime maintenance reads the ordered index directly.
      var cursor = ScanCursor.INITIAL;
      do {
        renew(commands, token);
        var page = commands.sscan(
            RedisJobStore.CRON_TASKS_INDEX, cursor, ScanArgs.Builder.limit(PAGE_SIZE));
        for (var name : page.getValues()) {
          commands.zadd(RedisJobStore.CRON_TASKS_ORDERED, 0, name);
        }
        cursor = page;
      } while (!cursor.isFinished());
      // Route SCAN through a keyed script to the owner of the namespace slot.
      // Scanning the Cluster connection directly would visit an arbitrary node.
      // This offline pass also discovers old counter hashes whose jobs were retained away.
      String counterCursor = "0";
      do {
        renew(commands, token);
        counterCursor = commands.eval(
            """
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then
              return redis.error_reply('Redis migration lease expired')
            end
            local page = redis.call('SCAN', ARGV[2], 'MATCH', ARGV[3], 'COUNT', 200)
            for _, key in ipairs(page[2]) do redis.call('ZADD', KEYS[2], 0, key) end
            return page[1]
            """,
            ScriptOutputType.VALUE,
            new String[] {RedisStorageFormat.MIGRATION_LOCK, RedisKeys.CONCURRENCY_COUNTERS},
            token,
            counterCursor,
            RedisKeys.PREFIX + "concurrency:*:counters");
      } while (!"0".equals(counterCursor));
      Long completed = commands.eval(
          """
          if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
          redis.call('SET', KEYS[2], ARGV[2])
          redis.call('DEL', KEYS[1])
          return 1
          """,
          ScriptOutputType.INTEGER,
          new String[] {RedisStorageFormat.MIGRATION_LOCK, RedisStorageFormat.KEY},
          token,
          RedisStorageFormat.CURRENT);
      if (completed == null || completed != 1)
        throw new IllegalStateException("Redis migration lease expired; rerun the migration");
      return visited;
    } catch (RuntimeException | Error primary) {
      failure = primary;
      throw primary;
    } finally {
      try {
        commands.eval(
            """
          if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
          return 0
          """, ScriptOutputType.INTEGER, new String[] {RedisStorageFormat.MIGRATION_LOCK}, token);
      } catch (RuntimeException cleanup) {
        if (failure != null) failure.addSuppressed(cleanup);
        else throw cleanup;
      }
    }
  }

  private static void renew(RedisClusterCommands<String, String> commands, String token) {
    Long renewed = commands.eval(
        """
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        """,
        ScriptOutputType.INTEGER,
        new String[] {RedisStorageFormat.MIGRATION_LOCK},
        token,
        Long.toString(LEASE_MILLIS));
    if (renewed == null || renewed != 1)
      throw new IllegalStateException("Redis migration lease expired; rerun the migration");
  }
}
