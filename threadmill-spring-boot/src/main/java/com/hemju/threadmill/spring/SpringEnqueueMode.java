package com.hemju.threadmill.spring;

/** How Spring enqueue calls interact with an active Spring transaction. */
public enum SpringEnqueueMode {
    /** Reserve ids immediately, then write jobs in an {@code afterCommit} callback. */
    AFTER_COMMIT,

    /** Write Postgres jobs inside the caller's Spring JDBC transaction. */
    JOIN_TRANSACTION,

    /** Write jobs immediately, regardless of Spring transaction state. */
    IMMEDIATE
}
