package com.example.threadmill;

import javax.sql.DataSource;

import io.lettuce.core.RedisURI;
import org.postgresql.ds.PGSimpleDataSource;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;

/**
 * Shared store wiring for the example applications.
 */
final class ExampleStores {

    private ExampleStores() {}

    enum Backend {
        POSTGRES,
        REDIS;

        static Backend defaultBackend() {
            return parse(System.getProperty("threadmill.example.backend", "postgres"));
        }

        static Backend parse(String value) {
            return switch (value.toLowerCase()) {
                case "postgres", "postgresql", "pg" -> POSTGRES;
                case "redis" -> REDIS;
                default -> throw new IllegalArgumentException("backend must be postgres or redis, got: " + value);
            };
        }
    }

    record StoreHandle(JobStore store, AutoCloseable closeAction) implements AutoCloseable {
        @Override
        public void close() {
            if (closeAction == null) return;
            try {
                closeAction.close();
            } catch (Exception e) {
                throw new IllegalStateException("failed to close example store", e);
            }
        }
    }

    static DataSource postgresDataSource() {
        var url = System.getenv().getOrDefault("THREADMILL_JDBC_URL", "jdbc:postgresql://localhost:55432/threadmill");
        var user = System.getenv().getOrDefault("THREADMILL_DB_USER", "threadmill");
        var pass = System.getenv().getOrDefault("THREADMILL_DB_PASSWORD", "threadmill");
        var ds = new PGSimpleDataSource();
        ds.setUrl(url);
        ds.setUser(user);
        ds.setPassword(pass);
        return ds;
    }

    static RedisURI redisUri() {
        return RedisURI.create(System.getenv().getOrDefault("THREADMILL_REDIS_URI", "redis://localhost:56379/0"));
    }

    static StoreHandle open(Backend backend) {
        return switch (backend) {
            case POSTGRES -> {
                DataSource ds = postgresDataSource();
                new MigrationRunner(ds).migrate();
                yield new StoreHandle(new PostgresJobStore(ds), null);
            }
            case REDIS -> {
                var store = new RedisJobStore(redisUri());
                yield new StoreHandle(store, store::close);
            }
        };
    }
}
