package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobEngineFatalException;

/**
 * The startup gate: Oracle 19c or later, binary comparison, and a Unicode
 * character set for the configured text type.
 */
class OracleServerTest {

  @Test
  void refusesToStartAgainstPreNineteenServers() {
    var eighteen =
        new OracleServer.Facts(18, "18.4.0.0.0", "AL32UTF8", "AL16UTF16", "XE", "APP", "BINARY");

    assertThatThrownBy(() -> OracleServer.require(eighteen, OracleTextType.STANDARD))
        .isInstanceOf(JobEngineFatalException.class)
        .hasMessageContaining("19c or later")
        .hasMessageContaining("18.4.0.0.0");
  }

  @Test
  void acceptsNineteenAndLaterReleases() {
    for (int major : new int[] {19, 21, 23}) {
      var facts = new OracleServer.Facts(
          major, major + ".0.0.0.0", "AL32UTF8", "AL16UTF16", "DB", "APP", "BINARY");
      assertThat(OracleServer.require(facts, OracleTextType.STANDARD)).isSameAs(facts);
    }
  }

  @Test
  void refusesNonUnicodeDatabaseCharacterSets() {
    var latin1 = new OracleServer.Facts(
        19, "19.24.0.0.0", "WE8ISO8859P1", "AL16UTF16", "ORCL", "APP", "BINARY");

    assertThatThrownBy(() -> OracleServer.require(latin1, OracleTextType.STANDARD))
        .isInstanceOf(JobEngineFatalException.class)
        .hasMessageContaining("AL32UTF8")
        .hasMessageContaining("WE8ISO8859P1")
        .hasMessageContaining("threadmill.store.oracle.text-type=national")
        .hasMessageContaining("OracleTextType.NATIONAL");
  }

  @Test
  void nationalTextTypeAcceptsANonUnicodeDatabaseCharacterSet() {
    for (String characterSet : new String[] {"WE8MSWIN1252", "WE8ISO8859P1", "AL32UTF8"}) {
      var facts = new OracleServer.Facts(
          19, "19.24.0.0.0", characterSet, "AL16UTF16", "ORCL", "APP", "BINARY");
      assertThat(OracleServer.require(facts, OracleTextType.NATIONAL)).isSameAs(facts);
    }
  }

  @Test
  void nationalTextTypeRefusesTheCesu8NationalCharacterSet() {
    var cesu8 =
        new OracleServer.Facts(19, "19.24.0.0.0", "WE8MSWIN1252", "UTF8", "ORCL", "APP", "BINARY");

    assertThatThrownBy(() -> OracleServer.require(cesu8, OracleTextType.NATIONAL))
        .isInstanceOf(JobEngineFatalException.class)
        .hasMessageContaining("AL16UTF16")
        .hasMessageContaining("found UTF8");
  }

  @Test
  void refusesLinguisticTextComparison() {
    var linguistic = new OracleServer.Facts(
        19, "19.24.0.0.0", "AL32UTF8", "AL16UTF16", "ORCL", "APP", "LINGUISTIC");

    assertThatThrownBy(() -> OracleServer.require(linguistic, OracleTextType.STANDARD))
        .isInstanceOf(JobEngineFatalException.class)
        .hasMessageContaining("NLS_COMP=BINARY")
        .hasMessageContaining("LINGUISTIC");
  }

  @Test
  void describesTheServerWithoutCredentials() {
    var facts = new OracleServer.Facts(
        19, "19.24.0.0.0", "AL32UTF8", "AL16UTF16", "ORCL", "THREADMILL", "BINARY");

    assertThat(facts.describe()).isEqualTo("Oracle Database 19.24.0.0.0 @ ORCL/THREADMILL");
  }
}
