package com.hemju.threadmill.core.serialization;

import java.util.Objects;

/** Factory methods for built-in {@link JobSerializer} implementations. */
public final class JobSerializers {

    private JobSerializers() {}

    /** Create the default JSON serializer with type aliases and payload migrations. */
    public static JobSerializer json(TypeNameAliases aliases, PayloadMigrations migrations) {
        return new JsonJobSerializer(
                Objects.requireNonNull(aliases, "aliases"), Objects.requireNonNull(migrations, "migrations"));
    }
}
