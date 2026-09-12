package com.hemju.threadmill.store.redis;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import com.hemju.threadmill.core.JobEngineFatalException;

/** Version gate for the Redis index layout, independent of job JSON fields. */
final class RedisStorageFormat {
  static final String CURRENT = "2";
  static final String KEY = RedisKeys.PREFIX + "storage_format";
  static final String MIGRATION_LOCK = RedisKeys.PREFIX + "storage_format_migration";

  private RedisStorageFormat() {}

  static void requireCurrent(RedisClusterCommands<String, String> commands) {
    String result = commands.eval(
        """
        local format = redis.call('GET', KEYS[1])
        if format then return format end
        if redis.call('SCARD', KEYS[3]) > 0 then return 'legacy' end
        for _, count in ipairs(redis.call('HVALS', KEYS[2])) do
          if tonumber(count) > 0 then return 'legacy' end
        end
        redis.call('SET', KEYS[1], ARGV[1])
        return ARGV[1]
        """,
        ScriptOutputType.VALUE,
        new String[] {KEY, RedisKeys.COUNTS, RedisJobStore.CRON_TASKS_INDEX},
        CURRENT);
    if (!CURRENT.equals(result)) {
      throw new JobEngineFatalException(
          "Redis storage format requires an offline upgrade. Stop all Threadmill workers "
              + "and producers, back up Redis, then run RedisIndexMigration.migrate(client). See docs/redis-topologies.md.");
    }
  }
}
