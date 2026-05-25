package com.hemju.threadmill.spring;

/** PostgreSQL schema action for Spring Boot auto-configured Threadmill stores. */
public enum PostgresSchemaMode {
    /** Apply every pending Threadmill migration before creating the store. */
    MIGRATE,

    /** Validate that the current schema exactly matches the shipped migration set. */
    VALIDATE,

    /** Do not inspect or modify the schema. */
    NONE,

    /** Drop Threadmill-owned schema objects, then apply all migrations. */
    DROP_AND_MIGRATE
}
