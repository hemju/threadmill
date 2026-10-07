package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Migration files use SQL*Plus statement terminators: a line holding only {@code /}. */
class OracleStatementSplitTest {

  @Test
  void splitsAtSlashLinesAndDropsLeadingComments() {
    var sql = """
        -- header comment
        CREATE TABLE a (id NUMBER)
        /

        -- explains the trigger
        CREATE TRIGGER t BEFORE INSERT ON a FOR EACH ROW
        BEGIN
            -- inline comment stays
            NULL;
        END;
        /
        """;

    var statements = OracleMigrationRunner.splitStatements("V1__x.sql", sql);

    assertThat(statements).hasSize(2);
    assertThat(statements.get(0)).isEqualTo("CREATE TABLE a (id NUMBER)");
    assertThat(statements.get(1))
        .startsWith("CREATE TRIGGER t")
        .contains("-- inline comment stays")
        .endsWith("END;");
  }

  @Test
  void rejectsTextAfterTheLastTerminator() {
    assertThatThrownBy(() -> OracleMigrationRunner.splitStatements(
            "V2__x.sql", "CREATE TABLE a (id NUMBER)\n/\nCREATE TABLE b (id NUMBER)\n"))
        .isInstanceOf(OracleMigrationRunner.MigrationException.class)
        .hasMessageContaining("V2__x.sql");
  }

  @Test
  void allowsTrailingCommentsAndBlankLines() {
    var statements =
        OracleMigrationRunner.splitStatements("V3__x.sql", "SELECT 1 FROM dual\n/\n\n-- end\n");

    assertThat(statements).containsExactly("SELECT 1 FROM dual");
  }
}
