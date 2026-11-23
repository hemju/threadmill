package com.hemju.threadmill.store.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Objects;

/**
 * Binds application-supplied text with the form of use its columns were
 * created with ({@link OracleTextType}).
 *
 * <p>Every bind of such text goes through this class. Threadmill's own fixed
 * ASCII values (states, modes, policy names) target {@code VARCHAR2} columns
 * under both text types and stay plain {@code setString} binds.
 */
final class OracleText {

  private final OracleTextType type;

  OracleText(OracleTextType type) {
    this.type = Objects.requireNonNull(type, "type");
  }

  OracleTextType type() {
    return type;
  }

  /** Bind one value, or SQL {@code NULL} of the matching character type. */
  void bind(PreparedStatement ps, int index, String value) throws SQLException {
    if (type == OracleTextType.NATIONAL) {
      if (value == null) ps.setNull(index, Types.NVARCHAR);
      else ps.setNString(index, value);
    } else if (value == null) {
      ps.setNull(index, Types.VARCHAR);
    } else {
      ps.setString(index, value);
    }
  }

  /** Bind {@code values} into {@code size} consecutive slots, padding with NULL. */
  int bindAll(PreparedStatement ps, int start, Collection<String> values, int size)
      throws SQLException {
    int index = start;
    for (String value : values) {
      bind(ps, index++, value);
    }
    while (index < start + size) {
      bind(ps, index++, null);
    }
    return index;
  }

  /** A scope for the character-LOB binds of one or more statements. */
  OracleClobs clobs() {
    return new OracleClobs(type);
  }
}
