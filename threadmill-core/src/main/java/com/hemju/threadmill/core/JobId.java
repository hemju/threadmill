package com.hemju.threadmill.core;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Strongly-typed wrapper for a job's identifier.
 *
 * <p>New ids are generated as <strong>time-ordered UUIDs (UUIDv7)</strong>:
 * the high 48 bits encode the creation time in milliseconds since the Unix
 * epoch, followed by the standard version (7) and variant nibbles, with the
 * remainder filled from a cryptographically secure RNG. Time-ordered ids keep
 * primary-key inserts roughly sequential, which is important for index
 * locality on the relational store and for predictable iteration order on the
 * key-value store.
 *
 * <p>Equality and hashing are delegated to the wrapped {@link UUID}.
 */
public final class JobId {

    private static final SecureRandom RNG = new SecureRandom();

    private final UUID value;

    private JobId(UUID value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    /** Generates a fresh, time-ordered job id. */
    public static JobId newId() {
        return new JobId(newUuidV7(System.currentTimeMillis()));
    }

    /**
     * Generates a fresh, time-ordered job id whose 48-bit time prefix is the
     * given Unix-epoch millisecond instant instead of a fresh clock read.
     *
     * <p>Used by {@link Job.Builder#build()} so the id's time prefix and the
     * job's {@code createdAt} derive from one clock read: with two separate
     * reads a thread stall between them lets id order contradict the
     * created-time order the engine schedules by.
     */
    public static JobId newId(long unixMillis) {
        return new JobId(newUuidV7(unixMillis));
    }

    /** Wraps an existing {@link UUID} as a {@code JobId}. */
    public static JobId of(UUID value) {
        return new JobId(value);
    }

    /** Parses a string-form {@code JobId} (the wrapped UUID's canonical form). */
    @JsonCreator
    public static JobId parse(String text) {
        return new JobId(UUID.fromString(text));
    }

    public UUID asUuid() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof JobId other && value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    @JsonValue
    public String toString() {
        return value.toString();
    }

    private static UUID newUuidV7(long unixMillis) {
        byte[] randomBytes = new byte[10];
        RNG.nextBytes(randomBytes);

        // High 64 bits: 48 bits of timestamp || 4-bit version (0x7) || 12 random bits.
        long msb = (unixMillis & 0xFFFFFFFFFFFFL) << 16;
        msb |= 0x7000L | (((long) randomBytes[0] & 0x0F) << 8) | ((long) randomBytes[1] & 0xFF);

        // Low 64 bits: 2-bit variant (10) || 62 random bits.
        long lsb = 0L;
        for (int i = 2; i < 10; i++) {
            lsb = (lsb << 8) | ((long) randomBytes[i] & 0xFF);
        }
        lsb &= 0x3FFFFFFFFFFFFFFFL;
        lsb |= 0x8000000000000000L;

        return new UUID(msb, lsb);
    }
}
