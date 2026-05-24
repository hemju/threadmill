package com.hemju.threadmill.store.postgres;

import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Strategy that supplies the JDBC transaction boundary for PostgreSQL store writes.
 *
 * <p>The default boundary opens a connection, starts a JDBC transaction, commits
 * successful work, and rolls back failed work. Framework integrations may
 * provide a boundary that joins an already active host transaction without
 * putting framework types in this module.
 */
public interface PostgresTransactionBoundary {

    /**
     * Run one store write unit with a JDBC connection scoped to the desired
     * transaction boundary.
     *
     * @param work the callback to run
     * @param <T> result type
     * @return the callback result
     * @throws SQLException when the connection or callback fails
     */
    <T> T inTransaction(PostgresConnectionWork<T> work) throws SQLException;

    /**
     * Whether this boundary can join an external transaction, such as a Spring
     * transaction bound to the same {@link DataSource}.
     */
    default boolean supportsExternalTransactions() {
        return false;
    }

    /**
     * Whether the current call is already inside an externally managed
     * transaction. Store-level deadlock retry is disabled in this case because a
     * failed PostgreSQL statement aborts the caller's transaction.
     */
    default boolean externallyManagedTransactionActive() {
        return false;
    }

    /** Return the default boundary that owns its own JDBC transactions. */
    static PostgresTransactionBoundary owning(DataSource dataSource) {
        return new OwningPostgresTransactionBoundary(dataSource);
    }
}
