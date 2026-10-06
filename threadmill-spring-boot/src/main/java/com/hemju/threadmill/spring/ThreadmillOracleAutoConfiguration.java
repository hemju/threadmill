package com.hemju.threadmill.spring;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.store.oracle.OracleJobStore;
import com.hemju.threadmill.store.oracle.OracleMigrationRunner;

/**
 * Oracle-specific Threadmill auto-configuration.
 *
 * <p>Like {@link ThreadmillPostgresAutoConfiguration}, it is separate from
 * {@link ThreadmillAutoConfiguration} and gated by
 * {@code @ConditionalOnClass(OracleJobStore.class)}, so applications without
 * {@code threadmill-store-oracle} never load Oracle types. It applies when a
 * {@link DataSource} bean exists, Redis is not configured, and
 * {@link JdbcStoreSelection} chooses Oracle; it sorts after the real
 * {@code DataSourceAutoConfiguration} so the {@code DataSource} condition sees
 * a property-configured pool.
 */
@AutoConfiguration
@AutoConfigureBefore(ThreadmillAutoConfiguration.class)
@AutoConfigureAfter(
    name = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
@ConditionalOnClass(OracleJobStore.class)
public class ThreadmillOracleAutoConfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(ThreadmillOracleAutoConfiguration.class);

  @Bean
  @ConditionalOnMissingBean(JobStore.class)
  @ConditionalOnBean(DataSource.class)
  @Conditional({OnRedisStoreNotConfigured.class, JdbcStoreSelection.OracleSelected.class})
  public JobStore threadmillJobStore(ThreadmillProperties properties, DataSource dataSource) {
    LOG.info("Threadmill: using Oracle store wired from the application's DataSource");
    OracleJobStore.requireSupportedServer(dataSource);
    applyOracleSchemaMode(dataSource, properties.getStore().getOracle());
    if (properties.getSpring().getEnqueueMode() == SpringEnqueueMode.JOIN_TRANSACTION) {
      return new OracleJobStore(
          dataSource,
          new JsonJobSerializer(),
          JobStoreCapabilities.defaults(),
          new SpringOracleTransactionBoundary(dataSource));
    }
    return new OracleJobStore(dataSource);
  }

  private static void applyOracleSchemaMode(
      DataSource dataSource, ThreadmillProperties.OracleProperties properties) {
    var migrations = new OracleMigrationRunner(dataSource);
    switch (properties.getSchemaMode()) {
      case MIGRATE -> migrations.migrate();
      case VALIDATE -> migrations.validate();
      case NONE -> {}
      case DROP_AND_MIGRATE -> {
        if (!properties.isAllowDestructiveSchemaReset()) {
          throw new IllegalStateException(
              "threadmill.store.oracle.schema-mode=drop-and-migrate requires"
                  + " threadmill.store.oracle.allow-destructive-schema-reset=true");
        }
        migrations.dropThreadmillObjects();
        migrations.migrate();
      }
    }
  }
}
