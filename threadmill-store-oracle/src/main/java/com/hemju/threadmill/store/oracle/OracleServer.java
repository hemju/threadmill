package com.hemju.threadmill.store.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import com.hemju.threadmill.core.JobEngineFatalException;

/**
 * Startup requirements for the Oracle server Threadmill connects to.
 *
 * <p>Threadmill requires Oracle Database 19c or later, the oldest release still
 * in long-term support, and binary text comparison ({@code NLS_COMP=BINARY},
 * the default). Job bodies and names must round-trip every Unicode character,
 * including 4-byte ones, so the character set holding them must be Unicode:
 * the {@code AL32UTF8} database character set for
 * {@link OracleTextType#STANDARD}, the {@code AL16UTF16} national character set
 * for {@link OracleTextType#NATIONAL}. The check runs once at store
 * construction so a misconfigured host fails fast with an actionable message
 * instead of corrupting text at the first write.
 */
final class OracleServer {

  /** Oldest supported major release. */
  static final int MINIMUM_MAJOR_VERSION = 19;

  /** The only supported database character set for {@link OracleTextType#STANDARD}. */
  static final String REQUIRED_CHARACTER_SET = "AL32UTF8";

  /**
   * The only supported national character set for {@link OracleTextType#NATIONAL}.
   * The other one Oracle allows, {@code UTF8}, is CESU-8: it stores a
   * supplementary character as two 3-byte surrogates.
   */
  static final String REQUIRED_NATIONAL_CHARACTER_SET = "AL16UTF16";

  /**
   * The only supported session text comparison. Keyset predicates and the
   * claim path's key ranges rely on binary comparisons matching index order.
   */
  static final String REQUIRED_COMPARISON = "BINARY";

  private static final Pattern VERSION = Pattern.compile("\\d+(?:\\.\\d+)+");

  private OracleServer() {}

  /**
   * What Threadmill needs to know about the server.
   *
   * @param majorVersion the database major release, for example 19
   * @param version the full release string, for example {@code 19.24.0.0.0}
   * @param characterSet the database character set
   * @param nationalCharacterSet the national character set
   * @param database the database (service) name
   * @param schema the schema that holds the Threadmill objects
   * @param comparison the session's {@code NLS_COMP}
   */
  record Facts(
      int majorVersion,
      String version,
      String characterSet,
      String nationalCharacterSet,
      String database,
      String schema,
      String comparison) {

    String describe() {
      return "Oracle Database " + version + " @ " + database + "/" + schema;
    }
  }

  static Facts read(DataSource dataSource) {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      var meta = conn.getMetaData();
      String product = String.valueOf(meta.getDatabaseProductName());
      if (!product.toLowerCase(Locale.ROOT).contains("oracle")) {
        throw new JobEngineFatalException(
            "Threadmill's OracleJobStore needs an Oracle database, but"
                + " the DataSource points at " + product + ". With Spring Boot, set"
                + " threadmill.store.jdbc-type for the intended store.");
      }
      int major = meta.getDatabaseMajorVersion();
      String release = meta.getDatabaseProductVersion();
      var matcher = VERSION.matcher(release == null ? "" : release);
      String version = matcher.find() ? matcher.group() : Integer.toString(major);
      String characterSet = "unknown";
      String nationalCharacterSet = "unknown";
      try (ResultSet rs = st.executeQuery("SELECT parameter, value FROM nls_database_parameters "
          + "WHERE parameter IN ('NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET')")) {
        while (rs.next()) {
          if ("NLS_CHARACTERSET".equals(rs.getString(1))) characterSet = rs.getString(2);
          else nationalCharacterSet = rs.getString(2);
        }
      }
      String database;
      String schema;
      String comparison;
      try (ResultSet rs = st.executeQuery("SELECT SYS_CONTEXT('USERENV', 'DB_NAME'), "
          + "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'), (SELECT value FROM nls_session_parameters "
          + "WHERE parameter = 'NLS_COMP') FROM dual")) {
        rs.next();
        database = rs.getString(1);
        schema = rs.getString(2);
        comparison = rs.getString(3);
      }
      return new Facts(
          major, version, characterSet, nationalCharacterSet, database, schema, comparison);
    } catch (SQLException e) {
      throw new JobEngineFatalException("Failed to verify the Oracle server version", e);
    }
  }

  /**
   * Refuse unsupported servers.
   *
   * @param facts what the server reported
   * @param textType the column types the schema uses for application-supplied text
   * @throws JobEngineFatalException when the release is older than 19c, the
   *     character set holding application-supplied text is not Unicode
   *     ({@code AL32UTF8} for {@link OracleTextType#STANDARD}, {@code AL16UTF16}
   *     for {@link OracleTextType#NATIONAL}), or the session compares text
   *     linguistically
   */
  static Facts require(Facts facts, OracleTextType textType) {
    if (facts.majorVersion() < MINIMUM_MAJOR_VERSION) {
      throw new JobEngineFatalException("Threadmill requires Oracle Database 19c or later — found "
          + facts.version() + ". Upgrade the server or use a different backend.");
    }
    if (textType == OracleTextType.STANDARD
        && !REQUIRED_CHARACTER_SET.equalsIgnoreCase(facts.characterSet())) {
      throw new JobEngineFatalException("Threadmill's standard Oracle text type stores text in "
          + "the database character set, which must be AL32UTF8 so job bodies and names "
          + "round-trip all Unicode text — found " + facts.characterSet() + ". Use the national "
          + "text type, which stores text in NVARCHAR2/NCLOB columns: "
          + "threadmill.store.oracle.text-type=national with Spring Boot, "
          + "OracleTextType.NATIONAL otherwise.");
    }
    if (textType == OracleTextType.NATIONAL
        && !REQUIRED_NATIONAL_CHARACTER_SET.equalsIgnoreCase(facts.nationalCharacterSet())) {
      throw new JobEngineFatalException("Threadmill's national Oracle text type stores text in "
          + "the national character set, which must be AL16UTF16 so job bodies and names "
          + "round-trip all Unicode text — found " + facts.nationalCharacterSet() + ".");
    }
    if (!REQUIRED_COMPARISON.equalsIgnoreCase(facts.comparison())) {
      throw new JobEngineFatalException("Threadmill requires NLS_COMP=BINARY (the Oracle default) "
          + "so key ranges match index order — found " + facts.comparison()
          + ". Remove the NLS_COMP override for Threadmill's connections.");
    }
    return facts;
  }
}
