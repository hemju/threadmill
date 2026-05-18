package com.hemju.threadmill.core.serialization;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Stable mapping from historical persisted type names to their current names.
 *
 * <p>Threadmill persists fully-qualified handler and payload class names. When
 * applications rename or move those classes, aliases let old durable jobs keep
 * running without relying on compatibility shim classes.
 */
public final class TypeNameAliases {

    private static final TypeNameAliases EMPTY = new TypeNameAliases(Map.of());

    private final Map<String, String> aliases;

    private TypeNameAliases(Map<String, String> aliases) {
        this.aliases = Map.copyOf(aliases);
    }

    /** Return an empty alias registry. */
    public static TypeNameAliases empty() {
        return EMPTY;
    }

    /** Start building an alias registry. */
    public static Builder builder() {
        return new Builder();
    }

    /** Resolve {@code typeName} to its current name, following alias chains. */
    public String resolve(String typeName) {
        Objects.requireNonNull(typeName, "typeName");
        String current = typeName;
        for (int i = 0; i < aliases.size(); i++) {
            String next = aliases.get(current);
            if (next == null) {
                return current;
            }
            current = next;
        }
        return current;
    }

    /** Builder for {@link TypeNameAliases}. */
    public static final class Builder {
        private final Map<String, String> aliases = new LinkedHashMap<>();

        private Builder() {}

        public Builder alias(String oldName, String currentName) {
            Objects.requireNonNull(oldName, "oldName");
            Objects.requireNonNull(currentName, "currentName");
            if (oldName.isBlank()) {
                throw new IllegalArgumentException("oldName must not be blank");
            }
            if (currentName.isBlank()) {
                throw new IllegalArgumentException("currentName must not be blank");
            }
            aliases.put(oldName, currentName);
            return this;
        }

        public TypeNameAliases build() {
            return aliases.isEmpty() ? EMPTY : new TypeNameAliases(aliases);
        }
    }
}
