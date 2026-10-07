package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobEngineFatalException;

/** The startup gate: Oracle 19c or later with an AL32UTF8 database character set. */
class OracleServerTest {

  @Test
  void refusesToStartAgainstPreNineteenServers() {
    var eighteen = new OracleServer.Facts(18, "18.4.0.0.0", "AL32UTF8", "XE", "APP", "BINARY");

    assertThatThrownBy(() -> OracleServer.require(eighteen))
        .isInstanceOf(JobEngineFatalException.class)
        .hasMessageContaining("19c or later")
        .hasMessageContaining("18.4.0.0.0");
  }

  @Test
  void acceptsNineteenAndLaterReleases() {
    for (int major : new int[] {19, 21, 23}) {
      var facts =
          new OracleServer.Facts(major, major + ".0.0.0.0", "AL32UTF8", "DB", "APP", "BINARY");
      assertThat(OracleServer.require(facts)).isSameAs(facts);
    }
  }

  @Test
  void refusesNonUnicodeDatabaseCharacterSets() {
    var latin1 = new OracleServer.Facts(19, "19.24.0.0.0", "WE8ISO8859P1", "ORCL", "APP", "BINARY");

    assertThatThrownBy(() -> OracleServer.require(latin1))
        .isInstanceOf(JobEngineFatalException.class)
        .hasMessageContaining("AL32UTF8")
        .hasMessageContaining("WE8ISO8859P1");
  }

  @Test
  void refusesLinguisticTextComparison() {
    var linguistic =
        new OracleServer.Facts(19, "19.24.0.0.0", "AL32UTF8", "ORCL", "APP", "LINGUISTIC");

    assertThatThrownBy(() -> OracleServer.require(linguistic))
        .isInstanceOf(JobEngineFatalException.class)
        .hasMessageContaining("NLS_COMP=BINARY")
        .hasMessageContaining("LINGUISTIC");
  }

  @Test
  void describesTheServerWithoutCredentials() {
    var facts =
        new OracleServer.Facts(19, "19.24.0.0.0", "AL32UTF8", "ORCL", "THREADMILL", "BINARY");

    assertThat(facts.describe()).isEqualTo("Oracle Database 19.24.0.0.0 @ ORCL/THREADMILL");
  }
}
