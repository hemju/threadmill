package com.hemju.threadmill.store.oracle;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Binds text to {@code CLOB} parameters for the statements of one scope and
 * frees the temporary LOBs it created when the scope closes.
 *
 * <p>Short values are bound directly with {@code setString}. Longer values are
 * written through a temporary {@link Clob}: the Oracle driver streams a long
 * directly bound string in fixed-size chunks and corrupts a supplementary
 * character (a surrogate pair, such as an emoji) that straddles a chunk
 * boundary, while {@link Clob#setString} keeps characters whole. Temporary LOBs
 * live in the session until freed, so a pooled connection would otherwise
 * accumulate them; close this object after the statements executed.
 */
final class OracleClobs implements AutoCloseable {

  /**
   * Longest value bound directly. At most 3 bytes per UTF-16 unit in AL32UTF8,
   * this stays below the driver's 32 KiB direct-bind limit, so it is never
   * chunked.
   */
  static final int DIRECT_BIND_MAX_CHARS = 8_000;

  private final List<Clob> created = new ArrayList<>();

  void bind(PreparedStatement ps, int index, String value) throws SQLException {
    if (value == null) {
      ps.setNull(index, Types.CLOB);
    } else if (value.length() <= DIRECT_BIND_MAX_CHARS) {
      ps.setString(index, value);
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
