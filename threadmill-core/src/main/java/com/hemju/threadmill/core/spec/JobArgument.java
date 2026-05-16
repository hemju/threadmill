package com.hemju.threadmill.core.spec;

import java.util.Objects;

/**
 * A single typed argument carried inside a {@link JobSpec}.
 *
 * <p>The {@code typeTag} is a fully-qualified type name describing how the
 * {@code serialized} payload should be interpreted by the active
 * {@code JobSerializer}. {@code null} values are represented by a type tag
 * and an empty serialized form — never as a Java {@code null} field on the
 * record itself.
 *
 * @param typeTag    fully-qualified type name of the argument
 * @param serialized serialized representation of the argument's value
 */
public record JobArgument(String typeTag, String serialized) {

    public JobArgument {
        Objects.requireNonNull(typeTag, "typeTag");
        Objects.requireNonNull(serialized, "serialized");
    }
}
