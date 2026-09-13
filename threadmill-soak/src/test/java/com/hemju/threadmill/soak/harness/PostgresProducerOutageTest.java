package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

/** A real restart must exercise PostgreSQL's refusal of new connections, not just broken sockets. */
@Tag("soak")
class PostgresProducerOutageTest {
  @Test
  @Timeout(60)
  void producerResumesAfterPostgresRejectsNewConnectionsDuringShutdown(@TempDir Path temporary)
      throws Exception {
    // Restart the server inside the container: Docker may reassign an ephemeral published
    // port when a stopped container starts again, which would change the producer's endpoint.
    try (var postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))
        .withCreateContainerCmdModifier(command -> command.withEntrypoint(
            "/bin/sh",
            "-c",
            "while true; do /usr/local/bin/docker-entrypoint.sh postgres; sleep 1; done"))) {
      postgres.start();
      var dataSource = new PGSimpleDataSource();
      dataSource.setURL(postgres.getJdbcUrl());
      dataSource.setUser(postgres.getUsername());
      dataSource.setPassword(postgres.getPassword());
      dataSource.setConnectTimeout(2);
      new MigrationRunner(dataSource).migrate();
      var store = new PostgresJobStore(dataSource);
      var outage = new CountDownLatch(1);
      var job = Job.builder().spec(new JobSpec("soak.TestHandler", List.of())).build();

      // Smart shutdown keeps this session alive while rejecting every new one with 57P03.
      // This makes the short connection-refusal window deterministic on a real server.
      var heldConnection = dataSource.getConnection();
      try (var trace = new SoakTraceWriter(temporary.resolve("trace.jsonl"), event -> {
            if (event.event().equals("producer_outage")) outage.countDown();
          });
          var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        assertThat(postgres
                .execInContainer(
                    "sh", "-c", "kill -TERM \"$(head -n 1 \"$PGDATA/postmaster.pid\")\"")
                .getExitCode())
            .isZero();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
          try (var connection = dataSource.getConnection()) {
            throw new AssertionError("PostgreSQL still accepts new connections: " + connection);
          } catch (SQLException failure) {
            assertThat(failure.getSQLState()).isEqualTo("57P03");
          }
        });
        var producer = executor.submit(
            () -> new RecoveringProducerStore(store, trace, () -> false).insert(job));
        try {
          assertThat(outage.await(5, TimeUnit.SECONDS)).isTrue();
          assertThat(producer).isNotDone();
          heldConnection.close();
          producer.get(20, TimeUnit.SECONDS);
          assertThat(store.findById(job.id())).isPresent();
          assertThat(store.countsByState().get(JobState.ENQUEUED)).isEqualTo(1);
          assertThat(job.version()).isEqualTo(1);
        } finally {
          executor.shutdownNow();
        }
      } finally {
        heldConnection.close();
      }
      assertThat(Files.readString(temporary.resolve("trace.jsonl")))
          .contains("producer_outage", "producer_recovered");
    }
  }
}
