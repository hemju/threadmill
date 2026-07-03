package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.lettuce.core.cluster.SlotHash;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;

class RedisKeysTest {

    @Test
    void allEngineKeysUseOneClusterSlot() {
        var slot = SlotHash.getSlot(RedisKeys.COUNTS);
        var keys = List.of(
                RedisKeys.COUNTS,
                RedisKeys.SCHEDULED,
                RedisKeys.AWAITING,
                RedisKeys.PROCESSING_ALL,
                RedisKeys.QUEUES,
                RedisKeys.NODES,
                RedisKeys.MAINTENANCE_LEASE,
                RedisKeys.job(JobId.newId()),
                RedisKeys.queue("default"),
                RedisKeys.queue("spaces/slashes:unicode-\uD83D\uDE80"),
                RedisKeys.processingFor(NodeId.newId()),
                RedisKeys.byHandler("com.example.Handler"),
                RedisKeys.byStateTime(JobState.ENQUEUED),
                RedisKeys.nodeHeartbeat(NodeId.newId()),
                RedisKeys.userKey("cron_task", "daily cleanup"),
                RedisKeys.dedup("default", "tenant:42/order/99"),
                RedisKeys.dedupExpiry(),
                RedisKeys.concurrencyClaimLock("project:42"),
                RedisKeys.concurrencyCounters("project:42"),
                RedisKeys.concurrencyPending("project:42"),
                RedisKeys.concurrencyPendingRoot("project:42", JobId.newId().toString()),
                RedisKeys.concurrencyWorkflows("project:42"),
                RedisKeys.concurrencyWorkflowCounts("project:42"),
                RedisKeys.queueKeys("default"),
                RedisKeys.queueUnkeyed("default"));

        assertThat(keys)
                .allSatisfy(key -> assertThat(SlotHash.getSlot(key)).as(key).isEqualTo(slot));
    }

    @Test
    void userSegmentsEncodeUnsafeCharactersWithoutCollisions() {
        assertThat(RedisKeys.queue("a:b")).isNotEqualTo(RedisKeys.queue("a/b"));
        assertThat(RedisKeys.queue("a b")).isNotEqualTo(RedisKeys.queue("a:b"));
        assertThat(RedisKeys.queue("unicode-\uD83D\uDE80")).contains("{threadmill}:queue:");
        assertThat(RedisKeys.dedup("queue:one", "key/two"))
                .doesNotContain("queue:one")
                .doesNotContain("key/two");
    }
}
