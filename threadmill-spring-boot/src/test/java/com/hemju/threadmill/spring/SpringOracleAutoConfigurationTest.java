package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.DelegatingDataSource;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.oracle.OracleJobStore;
import com.hemju.threadmill.store.oracle.OracleMigrationRunner;
import com.hemju.threadmill.store.oracle.OracleTextType;

/**
 * Spring Boot wiring of the Oracle store: JDBC store selection with both store
 * modules on the classpath, schema modes, text types, and join-transaction
 * enqueue mode.
 */
@EnabledIf("com.hemju.threadmill.spring.DockerAvailable#check")
class SpringOracleAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(
          ThreadmillRedisAutoConfiguration.class,
          ThreadmillPostgresAutoConfiguration.class,
          ThreadmillOracleAutoConfiguration.class,
          ThreadmillAutoConfiguration.class))
      .withBean(DataSource.class, () -> new DelegatingDataSource(OracleTestDatabase.dataSource()))
      .withPropertyValues("threadmill.enabled=false");

  @BeforeEach
  void dropSchema() {
    new OracleMigrationRunner(OracleTestDatabase.dataSource()).dropThreadmillObjects();
  }

  @Test
  void anOracleDataSourceUrlSelectsTheOracleStoreAndMigrates() {
    contextRunner
        .withPropertyValues("spring.datasource.url=" + OracleTestDatabase.url())
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBean(JobStore.class)).isInstanceOf(OracleJobStore.class);
          assertThat(context.getBean(JobStore.class).describe()).startsWith("Oracle Database");
        });

    new OracleMigrationRunner(OracleTestDatabase.dataSource()).validate();
  }

  @Test
  void explicitJdbcTypeSelectsTheOracleStore() {
    contextRunner.withPropertyValues("threadmill.store.jdbc-type=oracle").run(context -> {
      assertThat(context).hasNotFailed();
      assertThat(context.getBean(JobStore.class)).isInstanceOf(OracleJobStore.class);
    });
  }

  @Test
  void aMisselectedPostgresStoreFailsFastAgainstOracle() {
    // Both modules present and nothing names the database: PostgreSQL wins
    // the tie, and its own server check refuses the Oracle DataSource.
    contextRunner.run(context -> assertThat(context).hasFailed());
  }

  @Test
  void validateSchemaModeFailsOnEmptySchemaAndPassesAfterMigration() {
    var oracle = contextRunner.withPropertyValues(
        "threadmill.store.jdbc-type=oracle", "threadmill.store.oracle.schema-mode=validate");
    oracle.run(context -> {
      assertThat(context).hasFailed();
      assertThat(context.getStartupFailure()).hasMessageContaining("threadmill_schema_history");
    });

    new OracleMigrationRunner(OracleTestDatabase.dataSource()).migrate();
    oracle.run(context -> {
      assertThat(context).hasNotFailed();
      assertThat(context.getBean(JobStore.class)).isInstanceOf(OracleJobStore.class);
    });
  }

  @Test
  void noneSchemaModeDoesNotCreateSchema() throws Exception {
    contextRunner
        .withPropertyValues(
            "threadmill.store.jdbc-type=oracle", "threadmill.store.oracle.schema-mode=none")
        .run(context -> assertThat(context).hasNotFailed());

    assertThat(tableExists("THREADMILL_SCHEMA_HISTORY")).isFalse();
  }

  @Test
  void dropAndMigrateRequiresTheExplicitDestructiveResetFlag() {
    new OracleMigrationRunner(OracleTestDatabase.dataSource()).migrate();
    var dropAndMigrate = contextRunner.withPropertyValues(
        "threadmill.store.jdbc-type=oracle",
        "threadmill.store.oracle.schema-mode=drop-and-migrate");

    dropAndMigrate.run(context -> {
      assertThat(context).hasFailed();
      assertThat(context.getStartupFailure())
          .hasMessageContaining("allow-destructive-schema-reset=true");
    });
    dropAndMigrate
        .withPropertyValues("threadmill.store.oracle.allow-destructive-schema-reset=true")
        .run(context -> assertThat(context).hasNotFailed());
    new OracleMigrationRunner(OracleTestDatabase.dataSource()).validate();
  }

  @Test
  void nationalTextTypeMigratesNationalColumnsAndTheStandardTypeRefusesThatSchema()
      throws Exception {
    try {
      verifyNationalTextType();
    } finally {
      // The shared test database must not keep a national schema: the other
      // Oracle test classes migrate the standard one.
      new OracleMigrationRunner(OracleTestDatabase.dataSource()).dropThreadmillObjects();
    }
  }

  private void verifyNationalTextType() throws Exception {
    contextRunner
        .withPropertyValues(
            "threadmill.store.jdbc-type=oracle", "threadmill.store.oracle.text-type=national")
        .run(context -> {
          assertThat(context).hasNotFailed();
          var store = context.getBean(JobStore.class);
          var job = Job.builder()
              .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"😀\"")))
              .queue("queue 中文 😀")
              .build();
          store.insert(job);
          assertThat(store.findById(job.id()).orElseThrow().queue()).isEqualTo("queue 中文 😀");
        });
    assertThat(columnType("THREADMILL_JOBS", "QUEUE")).isEqualTo("NVARCHAR2");
    assertThat(columnType("THREADMILL_JOBS", "BODY")).isEqualTo("NCLOB");
    new OracleMigrationRunner(OracleTestDatabase.dataSource(), OracleTextType.NATIONAL).validate();

    contextRunner
        .withPropertyValues(
            "threadmill.store.jdbc-type=oracle", "threadmill.store.oracle.schema-mode=validate")
        .run(context -> {
          assertThat(context).hasFailed();
          assertThat(context.getStartupFailure())
              .hasMessageContaining("installed for the national Oracle text type");
        });
  }

  @Test
  void joinTransactionModeUsesTheTransactionJoinedScheduler() {
    contextRunner
        .withPropertyValues(
            "threadmill.store.jdbc-type=oracle", "threadmill.spring.enqueue-mode=join_transaction")
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBean(JobStore.class).supportsExternalTransactions())
              .isTrue();
          assertThat(context.getBean(JobScheduler.class))
              .isInstanceOf(TransactionJoinedJobScheduler.class);
        });
  }

  private static String columnType(String table, String column) throws Exception {
    try (var conn = OracleTestDatabase.dataSource().getConnection();
        var ps = conn.prepareStatement("SELECT data_type FROM user_tab_columns "
            + "WHERE table_name = ? AND column_name = ?")) {
      ps.setString(1, table);
      ps.setString(2, column);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  private static boolean tableExists(String table) throws Exception {
    try (var conn = OracleTestDatabase.dataSource().getConnection();
        var ps = conn.prepareStatement("SELECT COUNT(*) FROM user_tables WHERE table_name = ?")) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }
}
