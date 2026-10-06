package com.hemju.threadmill.store.oracle;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Standard-JDBC value mapping for the Oracle schema. The store never touches
 * {@code oracle.jdbc} types, so applications choose and supply their own driver.
 *
 * <ul>
 *   <li>UUIDs are {@code RAW(16)} in big-endian byte order, whose unsigned
 *       byte-wise comparison equals {@code JobId}'s canonical natural order.</li>
 *   <li>Instants are UTC {@code TIMESTAMP(9)} values bound as
 *       {@link LocalDateTime}. Binding {@code java.sql.Timestamp} would apply
 *       the JVM's default time zone, so the stored wall time would depend on
 *       the host configuration.</li>
 *   <li>CLOB text is bound through {@link OracleClobs}.</li>
 * </ul>
 */
final class OracleJdbc {

  private OracleJdbc() {}

  /** Largest number of expressions Oracle accepts in one {@code IN} list. */
  static final int MAX_IN_LIST = 1000;

  static byte[] bytes(UUID value) {
    return ByteBuffer.allocate(16)
        .putLong(value.getMostSignificantBits())
        .putLong(value.getLeastSignificantBits())
        .array();
  }

  static UUID uuid(byte[] value) {
    if (value == null) {
      return null;
    }
    if (value.length != 16) {
      throw new IllegalStateException(
          "Expected a 16-byte RAW UUID, got " + value.length + " bytes");
    }
    var buffer = ByteBuffer.wrap(value);
    return new UUID(buffer.getLong(), buffer.getLong());
  }

  static void setUuid(PreparedStatement ps, int index, UUID value) throws SQLException {
    if (value == null) {
      ps.setNull(index, Types.BINARY);
    } else {
      ps.setBytes(index, bytes(value));
    }
  }

  static UUID getUuid(ResultSet rs, int index) throws SQLException {
    return uuid(rs.getBytes(index));
  }

  static UUID getUuid(ResultSet rs, String column) throws SQLException {
    return uuid(rs.getBytes(column));
  }

  static void setInstant(PreparedStatement ps, int index, Instant value) throws SQLException {
    if (value == null) {
      ps.setNull(index, Types.TIMESTAMP);
    } else {
      ps.setObject(index, LocalDateTime.ofInstant(value, ZoneOffset.UTC));
    }
  }

  static Instant getInstant(ResultSet rs, int index) throws SQLException {
    var value = rs.getObject(index, LocalDateTime.class);
    return value == null ? null : value.toInstant(ZoneOffset.UTC);
  }

  static Instant getInstant(ResultSet rs, String column) throws SQLException {
    var value = rs.getObject(column, LocalDateTime.class);
    return value == null ? null : value.toInstant(ZoneOffset.UTC);
  }

  /**
   * Seconds as an exact decimal, for {@code NUMTODSINTERVAL(?, 'SECOND')}.
   * Keeps nanoseconds so a positive sub-millisecond duration never binds zero.
   */
  static void setSeconds(PreparedStatement ps, int index, Duration value) throws SQLException {
    ps.setBigDecimal(
        index, BigDecimal.valueOf(value.getSeconds()).add(BigDecimal.valueOf(value.getNano(), 9)));
  }

  /**
   * Bind-list size for {@code count} values. Sizes are rounded up to a small
   * set of buckets so statements built from variable-length lists share a
   * bounded number of cursors in the shared pool; unused slots are bound to
   * NULL, which matches nothing.
   */
  static int bucket(int count) {
    if (count > MAX_IN_LIST) {
      throw new IllegalArgumentException("At most " + MAX_IN_LIST + " values per list: " + count);
    }
    int size = 8;
    while (size < count) {
      size <<= 1;
    }
    return Math.min(size, MAX_IN_LIST);
  }

  /** {@code ?, ?, ...} with {@code size} placeholders. */
  static String placeholders(int size) {
    return String.join(", ", Collections.nCopies(size, "?"));
  }

  /** Bind strings into {@code size} consecutive slots, padding with NULL. */
  static int bindStrings(PreparedStatement ps, int start, Collection<String> values, int size)
      throws SQLException {
    int index = start;
    for (String value : values) {
      ps.setString(index++, value);
    }
    while (index < start + size) {
      ps.setNull(index++, Types.VARCHAR);
    }
    return index;
  }

  /** Bind UUIDs into {@code size} consecutive slots, padding with NULL. */
  static int bindUuids(PreparedStatement ps, int start, Collection<UUID> values, int size)
      throws SQLException {
    int index = start;
    for (UUID value : values) {
      ps.setBytes(index++, bytes(value));
    }
    while (index < start + size) {
      ps.setNull(index++, Types.BINARY);
    }
    return index;
  }

  /** Split {@code values} into chunks no larger than {@code max}. */
  static <T> List<List<T>> chunks(List<T> values, int max) {
    if (values.size() <= max) {
      return List.of(values);
    }
    var chunks = new ArrayList<List<T>>();
    for (int from = 0; from < values.size(); from += max) {
      chunks.add(values.subList(from, Math.min(values.size(), from + max)));
    }
    return chunks;
  }

  /** Whether {@code e} or a chained/caused exception carries Oracle error {@code code} (ORA-n). */
  static boolean hasErrorCode(SQLException e, int code) {
    for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
      if (cur.getErrorCode() == code) {
        return true;
      }
    }
    for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sql && sql.getErrorCode() == code) {
        return true;
      }
    }
    return false;
  }

  /** ORA-00001: unique constraint violated. */
  static final int UNIQUE_VIOLATION = 1;

  /** ORA-02291: integrity constraint violated, parent key not found. */
  static final int PARENT_KEY_NOT_FOUND = 2291;
}
