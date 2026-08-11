package com.hemju.threadmill.simulation.nudge;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import javax.sql.DataSource;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;

/** Real-datastore fixtures and work-row adapters for the nudge simulation. */
final class NudgeSimulationStores {

    private static final String POSTGRES_IMAGE = "postgres:18-alpine";
    private static final String REDIS_IMAGE = "redis:7-alpine";
    private static final String REDIS_DRAIN_SCRIPT = "local values = redis.call('SMEMBERS', KEYS[1]); "
            + "if #values > 0 then redis.call('DEL', KEYS[1]); end; return values";
    private static volatile ConnectionInfo processConnectionInfo;

    private NudgeSimulationStores() {}

    enum Backend {
        POSTGRES,
        REDIS;

        static Backend parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "postgres", "postgresql", "pg" -> POSTGRES;
                case "redis" -> REDIS;
                default -> throw new IllegalArgumentException("backend must be postgres or redis, got: " + value);
            };
        }
    }

    record ConnectionInfo(
            Backend backend, String jdbcUrl, String databaseUser, String databasePassword, String redisUri) {

        void appendArguments(List<String> command) {
            command.add("--backend");
            command.add(backend.name().toLowerCase(Locale.ROOT));
            if (backend == Backend.POSTGRES) {
                command.add("--jdbc-url");
                command.add(jdbcUrl);
                command.add("--db-user");
                command.add(databaseUser);
                command.add("--db-password");
                command.add(databasePassword);
            } else {
                command.add("--redis-uri");
                command.add(redisUri);
            }
        }
    }

    record BackendFixture(ConnectionInfo connectionInfo, AutoCloseable closeAction) implements AutoCloseable {
        @Override
        public void close() {
            closeQuietly(closeAction, "backend fixture");
        }
    }

    record JobStoreHandle(JobStore store, AutoCloseable closeAction) implements AutoCloseable {
        @Override
        public void close() {
            closeQuietly(closeAction, "job store");
        }
    }

    interface WorkStore extends AutoCloseable {
        void prepare();

        void record(int sequence);

        List<Integer> drain();

        boolean isPending(int sequence);

        @Override
        void close();
    }

    @SuppressWarnings("resource")
    static BackendFixture startBackend(Backend backend) {
        return switch (backend) {
            case POSTGRES -> {
                var container = new PostgreSQLContainer(DockerImageName.parse(POSTGRES_IMAGE))
                        .withDatabaseName("threadmill")
                        .withUsername("threadmill")
                        .withPassword("threadmill");
                container.start();
                var info = new ConnectionInfo(
                        backend, container.getJdbcUrl(), container.getUsername(), container.getPassword(), null);
                new MigrationRunner(postgresDataSource(info)).migrate();
                yield new BackendFixture(info, container::stop);
            }
            case REDIS -> {
                var container = new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
                        .withExposedPorts(6379)
                        .withCommand("redis-server", "--appendonly", "yes", "--maxmemory-policy", "noeviction")
                        .waitingFor(Wait.forListeningPort());
                container.start();
                var uri = "redis://" + container.getHost() + ":" + container.getMappedPort(6379);
                yield new BackendFixture(new ConnectionInfo(backend, null, null, null, uri), container::stop);
            }
        };
    }

    static JobStoreHandle openJobStore(ConnectionInfo connectionInfo) {
        return switch (connectionInfo.backend()) {
            case POSTGRES -> new JobStoreHandle(new PostgresJobStore(postgresDataSource(connectionInfo)), null);
            case REDIS -> {
                var store = new RedisJobStore(RedisURI.create(connectionInfo.redisUri()));
                yield new JobStoreHandle(store, store::close);
            }
        };
    }

    static WorkStore openWorkStore(ConnectionInfo connectionInfo, String runId) {
        return switch (connectionInfo.backend()) {
            case POSTGRES -> new PostgresWorkStore(postgresDataSource(connectionInfo), runId);
            case REDIS -> new RedisWorkStore(RedisURI.create(connectionInfo.redisUri()), runId);
        };
    }

    static void configureProcess(ConnectionInfo connectionInfo) {
        processConnectionInfo = Objects.requireNonNull(connectionInfo, "connectionInfo");
    }

    static WorkStore openProcessWorkStore(String runId) {
        var connectionInfo = processConnectionInfo;
        if (connectionInfo == null) {
            throw new IllegalStateException("nudge simulation process connection was not configured");
        }
        return openWorkStore(connectionInfo, runId);
    }

    private static DataSource postgresDataSource(ConnectionInfo connectionInfo) {
        var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(connectionInfo.jdbcUrl());
        dataSource.setUser(connectionInfo.databaseUser());
        dataSource.setPassword(connectionInfo.databasePassword());
        return dataSource;
    }

    private static void closeQuietly(AutoCloseable closeAction, String description) {
        if (closeAction == null) return;
        try {
            closeAction.close();
        } catch (Exception e) {
            throw new IllegalStateException("failed to close nudge simulation " + description, e);
        }
    }

    private static final class PostgresWorkStore implements WorkStore {
        private final DataSource dataSource;
        private final String runId;

        private PostgresWorkStore(DataSource dataSource, String runId) {
            this.dataSource = dataSource;
            this.runId = runId;
        }

        @Override
        public void prepare() {
            transaction(connection -> {
                try (var statement = connection.createStatement()) {
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS threadmill_simulation_nudge_work ("
                            + "run_id text NOT NULL, sequence integer NOT NULL, "
                            + "recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(), drained_at timestamptz, "
                            + "PRIMARY KEY (run_id, sequence))");
                }
                return null;
            });
        }

        @Override
        public void record(int sequence) {
            transaction(connection -> {
                try (var statement = connection.prepareStatement(
                        "INSERT INTO threadmill_simulation_nudge_work (run_id, sequence) VALUES (?, ?)")) {
                    statement.setString(1, runId);
                    statement.setInt(2, sequence);
                    statement.executeUpdate();
                }
                return null;
            });
        }

        @Override
        public List<Integer> drain() {
            return transaction(connection -> {
                var drained = new ArrayList<Integer>();
                try (var statement = connection.prepareStatement(
                        "UPDATE threadmill_simulation_nudge_work SET drained_at = clock_timestamp() "
                                + "WHERE run_id = ? AND drained_at IS NULL RETURNING sequence")) {
                    statement.setString(1, runId);
                    try (var result = statement.executeQuery()) {
                        while (result.next()) drained.add(result.getInt(1));
                    }
                }
                return List.copyOf(drained);
            });
        }

        @Override
        public boolean isPending(int sequence) {
            try (var connection = dataSource.getConnection();
                    var statement = connection.prepareStatement("SELECT 1 FROM threadmill_simulation_nudge_work "
                            + "WHERE run_id = ? AND sequence = ? AND drained_at IS NULL")) {
                statement.setString(1, runId);
                statement.setInt(2, sequence);
                try (var result = statement.executeQuery()) {
                    return result.next();
                }
            } catch (SQLException e) {
                throw new IllegalStateException("failed to inspect Postgres nudge simulation work", e);
            }
        }

        @Override
        public void close() {}

        private <T> T transaction(SqlWork<T> work) {
            try (var connection = dataSource.getConnection()) {
                var previousAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    var result = work.execute(connection);
                    connection.commit();
                    return result;
                } catch (RuntimeException | SQLException e) {
                    connection.rollback();
                    throw e;
                } finally {
                    connection.setAutoCommit(previousAutoCommit);
                }
            } catch (SQLException e) {
                throw new IllegalStateException("Postgres nudge simulation work transaction failed", e);
            }
        }
    }

    @FunctionalInterface
    private interface SqlWork<T> {
        T execute(Connection connection) throws SQLException;
    }

    private static final class RedisWorkStore implements WorkStore {
        private final RedisClient client;
        private final String key;

        private RedisWorkStore(RedisURI redisUri, String runId) {
            this.client = RedisClient.create(redisUri);
            this.key = "{threadmill}:simulation:nudge:" + runId + ":pending";
        }

        @Override
        public void prepare() {
            try (var connection = client.connect()) {
                connection.sync().del(key);
            }
        }

        @Override
        public void record(int sequence) {
            try (var connection = client.connect()) {
                connection.sync().sadd(key, Integer.toString(sequence));
            }
        }

        @Override
        public List<Integer> drain() {
            try (var connection = client.connect()) {
                List<String> values =
                        connection.sync().eval(REDIS_DRAIN_SCRIPT, ScriptOutputType.MULTI, new String[] {key});
                return values.stream().map(Integer::valueOf).sorted().toList();
            }
        }

        @Override
        public boolean isPending(int sequence) {
            try (var connection = client.connect()) {
                return connection.sync().sismember(key, Integer.toString(sequence));
            }
        }

        @Override
        public void close() {
            client.shutdown();
        }
    }
}
