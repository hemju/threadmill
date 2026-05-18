package com.hemju.threadmill.core.serialization;

import com.hemju.threadmill.core.spec.JobArgument;

/**
 * JSON-level migration for a persisted payload whose old type or structure can
 * no longer be deserialized directly.
 */
@FunctionalInterface
public interface PayloadMigration {

    /**
     * Convert the old persisted argument into a new argument. Implementations
     * should parse {@link JobArgument#serialized()} with the application's JSON
     * tooling and return a new type tag plus serialized payload.
     */
    JobArgument migrate(JobArgument oldArgument);
}
