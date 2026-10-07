package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.mock.env.MockEnvironment;

import com.hemju.threadmill.store.oracle.OracleJobStore;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

/** Which JDBC store owns the application's DataSource when both modules are present. */
class JdbcStoreSelectionTest {

  private static final ClassLoader BOTH = JdbcStoreSelectionTest.class.getClassLoader();

  @Test
  void explicitTypeWinsOverTheDataSourceUrl() {
    var environment = new MockEnvironment()
        .withProperty("threadmill.store.jdbc-type", "postgres")
        .withProperty("spring.datasource.url", "jdbc:oracle:thin:@//db:1521/ORCL");

    assertThat(JdbcStoreSelection.select(environment, BOTH)).isEqualTo(JdbcStoreType.POSTGRES);
    environment.setProperty("threadmill.store.jdbc-type", "ORACLE");
    assertThat(JdbcStoreSelection.select(environment, BOTH)).isEqualTo(JdbcStoreType.ORACLE);
  }

  @Test
  void dataSourceUrlSchemeSelectsTheStore() {
    assertThat(JdbcStoreSelection.select(
            new MockEnvironment()
                .withProperty("spring.datasource.url", "jdbc:oracle:thin:@//db:1521/ORCL"),
            BOTH))
        .isEqualTo(JdbcStoreType.ORACLE);
    assertThat(JdbcStoreSelection.select(
            new MockEnvironment()
                .withProperty("spring.datasource.url", "jdbc:postgresql://db/threadmill"),
            BOTH))
        .isEqualTo(JdbcStoreType.POSTGRES);
  }

  @Test
  void withoutConfigurationThePresentModuleWinsAndPostgresBreaksTies() {
    var environment = new MockEnvironment();

    assertThat(JdbcStoreSelection.select(environment, BOTH)).isEqualTo(JdbcStoreType.POSTGRES);
    assertThat(
            JdbcStoreSelection.select(environment, new FilteredClassLoader(PostgresJobStore.class)))
        .isEqualTo(JdbcStoreType.ORACLE);
    assertThat(
            JdbcStoreSelection.select(environment, new FilteredClassLoader(OracleJobStore.class)))
        .isEqualTo(JdbcStoreType.POSTGRES);
  }

  @Test
  void jdbcTypeBindsAsAStoreProperty() {
    var properties = new ThreadmillProperties();
    properties.getStore().setJdbcType(JdbcStoreType.ORACLE);

    assertThat(properties.getStore().getJdbcType()).isEqualTo(JdbcStoreType.ORACLE);
    assertThat(properties.getStore().getOracle().getSchemaMode())
        .isEqualTo(OracleSchemaMode.MIGRATE);
  }
}
