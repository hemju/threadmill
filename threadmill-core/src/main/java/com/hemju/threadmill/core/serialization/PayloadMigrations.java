package com.hemju.threadmill.core.serialization;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.hemju.threadmill.core.spec.JobArgument;

/** Registry of explicit payload migrations keyed by historical type tag. */
public final class PayloadMigrations {

    private static final PayloadMigrations EMPTY = new PayloadMigrations(Map.of());

    private final Map<String, PayloadMigration> migrations;

    private PayloadMigrations(Map<String, PayloadMigration> migrations) {
        this.migrations = Map.copyOf(migrations);
    }

    public static PayloadMigrations empty() {
        return EMPTY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<JobArgument> migrate(JobArgument argument) {
        Objects.requireNonNull(argument, "argument");
        var migration = migrations.get(argument.typeTag());
        return migration == null ? Optional.empty() : Optional.of(migration.migrate(argument));
    }

    /** Builder for {@link PayloadMigrations}. */
    public static final class Builder {
        private final Map<String, PayloadMigration> migrations = new LinkedHashMap<>();

        private Builder() {}

        public Builder migration(String oldTypeTag, PayloadMigration migration) {
            Objects.requireNonNull(oldTypeTag, "oldTypeTag");
            Objects.requireNonNull(migration, "migration");
            if (oldTypeTag.isBlank()) {
                throw new IllegalArgumentException("oldTypeTag must not be blank");
            }
            migrations.put(oldTypeTag, migration);
            return this;
        }

        public PayloadMigrations build() {
            return migrations.isEmpty() ? EMPTY : new PayloadMigrations(migrations);
        }
    }
}
