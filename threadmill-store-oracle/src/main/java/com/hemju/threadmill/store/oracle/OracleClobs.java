package com.hemju.threadmill.store.oracle;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Binds text to {@code CLOB} or {@code NCLOB} parameters for the statements of
 * one scope and frees the temporary LOBs it created when the scope closes.
 *
 * <p>Short values are bound directly ({@code setString}, or {@code setNString}
 * for {@link OracleTextType#NATIONAL}). Longer values are written through a
 * temporary {@link Clob} or {@link java.sql.NClob}: the Oracle driver streams a
 * long directly bound string in fixed-size chunks and corrupts a supplementary
 * character (a surrogate pair, such as an emoji) that straddles a chunk
 * boundary, while {@link Clob#setString} keeps characters whole. Temporary LOBs
 * live in the session until freed, so a pooled connection would otherwise
 * accumulate them; close this object after the statements executed.
 */
final class OracleClobs implements AutoCloseable {

  /**
   * Longest value bound directly. At most 3 bytes per UTF-16 unit in AL32UTF8
   * and 2 in AL16UTF16, this stays below the driver's 32 KiB direct-bind
   * limit, so it is never chunked.
   */
  static final int DIRECT_BIND_MAX_CHARS = 8_000;

  private final OracleTextType type;
  private final List<Clob> created = new ArrayList<>();

  OracleClobs(OracleTextType type) {
    this.type = Objects.requireNonNull(type, "type");
  }

  void bind(PreparedStatement ps, int index, String value) throws SQLException {
    boolean national = type == OracleTextType.NATIONAL;
    if (value == null) {
      ps.setNull(index, national ? Types.NCLOB : Types.CLOB);
    } else if (value.length() <= DIRECT_BIND_MAX_CHARS) {
      if (national) ps.setNString(index, value);
      else ps.setString(index, value);
    } else if (national) {
      var clob = ps.getConnection().createNClob();
      created.add(clob);
      clob.setString(1, value);
      ps.setNClob(index, clob);
    } else {
      Clob clob = ps.getConnection().createClob();
      created.add(clob);
      clob.setString(1, value);
      ps.setClob(index, clob);
    }
  }

  @Override
  public void close() throws SQLException {
    SQLException failure = null;
    for (Clob clob : created) {
      try {
        clob.free();
      } catch (SQLException e) {
        if (failure == null) failure = e;
        else failure.addSuppressed(e);
      }
    }
    created.clear();
    if (failure != null) throw failure;
  }
}
