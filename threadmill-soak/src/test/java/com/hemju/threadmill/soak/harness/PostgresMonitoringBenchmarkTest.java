package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

/** Opt-in, pooled four-claimer benchmark with concurrent monitoring reads. */
@Tag("monitoring-benchmark")
class PostgresMonitoringBenchmarkTest {
  @Test
  void claimThroughputWithMonitoringAtIncreasingBacklogSizes() throws Exception {
    var output = Path.of("build/soak/postgres-monitoring");
    Files.createDirectories(output);
    var rows =
        new StringBuilder("backlog,monitoring,claims,seconds,jobs_per_second,p95_claim_ms\n");
    try (var postgres = new PostgreSQLContainer("postgres:18-alpine")) {
      postgres.withCreateContainerCmdModifier(command ->
          command.getHostConfig().withNanoCPUs(2_000_000_000L).withMemory(1536L * 1024 * 1024));
      postgres.start();
      var source = new PGSimpleDataSource();
      source.setUrl(postgres.getJdbcUrl());
      source.setUser(postgres.getUsername());
      source.setPassword(postgres.getPassword());
      try (var pool = new HarnessPooledDataSource(source, 8)) {
        new MigrationRunner(pool).migrate();
        var store = new PostgresJobStore(pool);
        for (int count : List.of(10_000, 100_000, 1_000_000)) {
          seed(pool, store, count);
          // Reverse the order for the second pair to expose simple warm-cache bias.
          for (boolean monitoring : List.of(false, true, true, false)) {
            var stop = new AtomicBoolean(false);
            var durations = new ConcurrentLinkedQueue<Long>();
            var claimed = new ConcurrentLinkedQueue<JobId>();
            long started = System.nanoTime();
            long finished;
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
              Future<?> sampler = monitoring
                  ? executor.submit(() -> {
                    while (!stop.get()) {
                      store.countsByState();
                      store.queueDepths();
                      store.oldestEnqueuedAt("default");
                      store.oldestProcessingHeartbeat();
                      for (var state : JobState.values()) store.oldestMaintenanceAt(state);
                      store.searchJobs(new JobSearch(JobState.ENQUEUED, null, null, 20, 0));
                      try {
                        Thread.sleep(100);
                      } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                      }
                    }
                  })
                  : null;
              try {
                var workers = new ArrayList<Future<?>>();
                for (int worker = 0; worker < 4; worker++) {
                  workers.add(executor.submit(() -> {
                    var node = NodeId.newId();
                    for (int batch = 0; batch < 25; batch++) {
                      long before = System.nanoTime();
                      var jobs = store.claimReady(node, "default", 10, Instant.now());
                      durations.add(System.nanoTime() - before);
                      assertThat(jobs).hasSize(10);
                      jobs.forEach(job -> claimed.add(job.id()));
                    }
                  }));
                }
                for (var worker : workers) worker.get(60, TimeUnit.SECONDS);
                finished = System.nanoTime();
              } finally {
                stop.set(true);
              }
              if (sampler != null) sampler.get(10, TimeUnit.SECONDS);
            }
            double seconds = (finished - started) / 1_000_000_000d;
            var sorted = durations.stream().sorted().toList();
            assertThat(claimed).hasSize(1000).doesNotHaveDuplicates();
            rows.append(count)
                .append(',')
                .append(monitoring)
                .append(',')
                .append(claimed.size())
                .append(',')
                .append(seconds)
                .append(',')
                .append(claimed.size() / seconds)
                .append(',')
                .append(sorted.get(94) / 1_000_000d)
                .append('\n');
            Files.writeString(output.resolve("claims.csv"), rows);
          }
          assertThat(store.queueDepths()).containsEntry("default", count - 4000L);
        }
      }
    }
  }

  private static void seed(HarnessPooledDataSource pool, PostgresJobStore store, int count)
      throws Exception {
    var sample = Job.builder().spec(JobSpec.of("benchmark.Handler")).build();
    var body = new JsonJobSerializer().serializeJob(sample.snapshot(), store.capabilities());
    try (var connection = pool.getConnection();
        var statement = connection.createStatement()) {
      statement.execute("TRUNCATE threadmill_jobs,threadmill_dedup_keys,threadmill_queue_counts");
      // Bulk fixture setup is excluded from measurements; restore exact counters
      // and all write triggers before any concurrent claims or monitoring.
      statement.execute("ALTER TABLE threadmill_jobs DISABLE TRIGGER USER");
      try (var insert = connection.prepareStatement("""
          INSERT INTO threadmill_jobs(id,state,queue,handler_signature,current_state_at,version,body,created_at,workflow_root_id)
          SELECT id,'ENQUEUED','default','benchmark.Handler',?,0,replace(?,?,id::text),?,id
          FROM (SELECT uuidv7() id FROM generate_series(1,?)) generated
          """)) {
        insert.setTimestamp(1, Timestamp.from(sample.createdAt()));
        insert.setString(2, body);
        insert.setString(3, sample.id().toString());
        insert.setTimestamp(4, Timestamp.from(sample.createdAt()));
        insert.setInt(5, count);
        insert.executeUpdate();
      } finally {
        statement.execute("ALTER TABLE threadmill_jobs ENABLE TRIGGER USER");
      }
      statement.executeUpdate("UPDATE threadmill_job_counts SET count=0");
      statement.executeUpdate("UPDATE threadmill_job_counts SET count=" + count
          + " WHERE state='ENQUEUED' AND shard=0");
      statement.executeUpdate(
          "INSERT INTO threadmill_queue_counts VALUES ('default',0," + count + ")");
      statement.execute("VACUUM ANALYZE threadmill_jobs");
    }
  }
}
