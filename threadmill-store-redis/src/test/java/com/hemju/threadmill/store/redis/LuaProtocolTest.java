package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Java and Lua talk through a positional key/arg protocol, so the per-job
 * stride the batched insert script advances by must equal the count the Java
 * side packs. This pins both sides to the one pair of constants so an added
 * key cannot silently shift every later position (PR #120 review).
 */
class LuaProtocolTest {

  @Test
  void insertAllStrideMatchesTheJavaPacking() {
    String lua = LuaScripts.insertAll();
    int keys = RedisJobStore.INSERT_KEYS_PER_JOB;
    int args = RedisJobStore.INSERT_ARGS_PER_JOB;

    assertThat(lua)
        .as("layout header")
        .contains("Layout: " + keys + " keys + " + args + " args per job");
    assertThat(lua)
        .as("key stride")
        .contains("key_offset = key_offset + " + keys)
        .doesNotContain("key_offset = key_offset + " + (keys + 1))
        .doesNotContain("key_offset = key_offset + " + (keys - 1));
    assertThat(lua)
        .as("arg stride")
        .contains("arg_offset = arg_offset + " + args)
        .doesNotContain("arg_offset = arg_offset + " + (args + 1))
        .doesNotContain("arg_offset = arg_offset + " + (args - 1));
    assertThat(lua)
        .as("the last key slot is referenced and nothing beyond it")
        .contains("KEYS[key_offset + " + keys + "]")
        .doesNotContain("KEYS[key_offset + " + (keys + 1) + "]");
  }

  @Test
  void singleInsertDeclaresTheSameKeyCount() {
    String lua = LuaScripts.insert();
    int keys = RedisJobStore.INSERT_KEYS_PER_JOB;
    String keySection = lua.substring(lua.indexOf("-- KEYS:"), lua.indexOf("-- ARGV:"));
    assertThat(keySection)
        .as("insert.lua and insert_all.lua share one key layout")
        .contains("[" + keys + "] queue_enqueued_at ZSET")
        .doesNotContain("[" + (keys + 1) + "]");
    assertThat(lua)
        .as("the last key slot is referenced and nothing beyond it")
        .contains("KEYS[" + keys + "]")
        .doesNotContain("KEYS[" + (keys + 1) + "]");
  }

  @Test
  void priorityRescorePageDeclaresTheJavaPackedPrefix() {
    String lua = LuaScripts.rescoreQueuePriorityPage();
    int prefix = RedisJobStore.RESCORE_QUEUE_PRIORITY_PREFIX_ARGS;

    assertThat(lua)
        .as("fixed argument prefix")
        .contains("ARGV: " + prefix + " fixed args + member-id tail")
        .contains("for i = " + (prefix + 1) + ", #ARGV do")
        .doesNotContain("for i = " + prefix + ", #ARGV do")
        .doesNotContain("for i = " + (prefix + 2) + ", #ARGV do");
  }

  @Test
  void optionalKeySentinelMatchesTheJavaProtocol() {
    var scripts = List.of(
        LuaScripts.insert(),
        LuaScripts.insertAll(),
        LuaScripts.enqueueIfAbsent(),
        LuaScripts.saveAtomic(),
        LuaScripts.claimCommit(),
        LuaScripts.softDelete(),
        LuaScripts.retentionDelete(),
        LuaScripts.replaceJob());

    assertThat(scripts)
        .allSatisfy(script -> assertThat(script).contains("'" + RedisKeys.NO_KEY + "'"));
  }
}
