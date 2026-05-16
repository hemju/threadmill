package com.hemju.threadmill.simulation;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Locale;

import javax.sql.DataSource;

import io.lettuce.core.RedisURI;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.simulation.SimulationRunner.SimulationResult;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;

/**
 * Entry point for the simulation suite. Invoke with one of:
 *
 * <ul>
 *   <li>{@code --backend memory} — fast, no Docker dependency.</li>
 *   <li>{@code --backend postgres} — boots a {@code postgres:18-alpine} Testcontainer.</li>
 *   <li>{@code --backend redis} — boots a {@code redis:7-alpine} Testcontainer.</li>
 *   <li>{@code --backend all} (default) — runs all three sequentially.</li>
 * </ul>
 *
 * <p>Writes one JSON-lines trace per backend under {@code build/simulation/}, runs the
 * verifier against each, prints a summary, and exits 0 if every backend drained cleanly
 * and every invariant held — non-zero otherwise.
 */
public final class SimulationMain {

    private static final Logger LOG = LoggerFactory.getLogger(SimulationMain.class);

    private static final DateTimeFormatter FILENAME_TIMESTAMP = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .toFormatter(Locale.ROOT)
            .withZone(ZoneOffset.UTC);

    private SimulationMain() {}

    static void main(String[] args) {
        String backend = "all";
        for (int i = 0; i < args.length - 1; i++) {
            if ("--backend".equals(args[i])) backend = args[i + 1].toLowerCase(Locale.ROOT);
        }

        var config = SimulationConfig.defaults();
        var results = new ArrayList<RunOutcome>();
        try {
            switch (backend) {
                case "memory" -> results.add(runMemory(config));
                case "postgres" -> results.add(runPostgres(config));
                case "redis" -> results.add(runRedis(config));
                case "all" -> {
                    results.add(runMemory(config));
                    results.add(runPostgres(config));
                    results.add(runRedis(config));
                }
                default -> {
                    System.err.println("Unknown backend: " + backend);
                    System.exit(2);
                    return;
                }
            }
        } catch (Exception e) {
            LOG.error("Simulation failed", e);
            System.exit(3);
            return;
        }

        SummaryReporter.print(results);
        boolean ok = results.stream().allMatch(r -> r.result.drained() && r.verifier.isClean());
        System.exit(ok ? 0 : 1);
    }

    private static RunOutcome runMemory(SimulationConfig config) throws IOException, InterruptedException {
        Path trace = traceFile("memory");
        SimulationResult result;
        try (var writer = new TraceWriter(trace)) {
            JobStore store = new InMemoryJobStore();
            var runner = new SimulationRunner(store, config, writer, "memory");
            result = runner.run();
        }
        var verifier = new TraceVerifier(trace).verify();
        return new RunOutcome("memory", trace, result, verifier);
    }

    @SuppressWarnings("resource")
    private static RunOutcome runPostgres(SimulationConfig config) throws IOException, InterruptedException {
        Path trace = traceFile("postgres");
        var container = new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))
                .withDatabaseName("threadmill")
                .withUsername("threadmill")
                .withPassword("threadmill");
        container.start();
        try {
            DataSource ds = postgresDataSource(container);
            new MigrationRunner(ds).migrate();
            SimulationResult result;
            try (var writer = new TraceWriter(trace)) {
                JobStore store = new PostgresJobStore(ds);
                var runner = new SimulationRunner(store, config, writer, "postgres");
                result = runner.run();
            }
            var verifier = new TraceVerifier(trace).verify();
            return new RunOutcome("postgres", trace, result, verifier);
        } finally {
            container.stop();
        }
    }

    @SuppressWarnings("resource")
    private static RunOutcome runRedis(SimulationConfig config) throws IOException, InterruptedException {
        Path trace = traceFile("redis");
        var container = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379)
                .withCommand("redis-server", "--appendonly", "yes")
                .waitingFor(Wait.forListeningPort());
        container.start();
        try {
            RedisURI uri = RedisURI.create("redis://" + container.getHost() + ":" + container.getMappedPort(6379));
            SimulationResult result;
            try (var writer = new TraceWriter(trace)) {
                JobStore store = new RedisJobStore(uri);
                var runner = new SimulationRunner(store, config, writer, "redis");
                result = runner.run();
            }
            var verifier = new TraceVerifier(trace).verify();
            return new RunOutcome("redis", trace, result, verifier);
        } finally {
            container.stop();
        }
    }

    private static DataSource postgresDataSource(PostgreSQLContainer container) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(container.getJdbcUrl());
        ds.setUser(container.getUsername());
        ds.setPassword(container.getPassword());
        return ds;
    }

    private static Path traceFile(String backend) {
        String name = "simulation-" + FILENAME_TIMESTAMP.format(Instant.now()) + "-" + backend + ".jsonl";
        return Path.of("build", "simulation", name);
    }

    record RunOutcome(String backend, Path tracePath, SimulationResult result, TraceVerifier.Result verifier) {}
}
