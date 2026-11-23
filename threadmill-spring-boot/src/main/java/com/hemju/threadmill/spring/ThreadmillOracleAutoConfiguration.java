package com.hemju.threadmill.spring;

import java.util.Locale;

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
import com.hemju.threadmill.store.oracle.OracleTextType;
import com.hemju.threadmill.store.oracle.OracleTransactionBoundary;

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
    var oracle = properties.getStore().getOracle();
    var textType = OracleTextType.valueOf(oracle.getTextType().name());
    LOG.info(
        "Threadmill: using Oracle store ({} text type) wired from the application's DataSource",
        oracle.getTextType().name().toLowerCase(Locale.ROOT));
    OracleJobStore.requireSupportedServer(dataSource, textType);
    applyOracleSchemaMode(dataSource, oracle, textType);
    OracleTransactionBoundary boundary =
        properties.getSpring().getEnqueueMode() == SpringEnqueueMode.JOIN_TRANSACTION
            ? new SpringOracleTransactionBoundary(dataSource)
            : OracleTransactionBoundary.owning(dataSource);
    return new OracleJobStore(
        dataSource, new JsonJobSerializer(), JobStoreCapabilities.defaults(), boundary, textType);
  }

  private static void applyOracleSchemaMode(
      DataSource dataSource,
      ThreadmillProperties.OracleProperties properties,
      OracleTextType textType) {
    var migrations = new OracleMigrationRunner(dataSource, textType);
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
