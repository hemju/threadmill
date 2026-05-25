package com.hemju.threadmill.spring;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

/**
 * Postgres-specific Threadmill auto-configuration.
 *
 * <p>Separated from {@link ThreadmillAutoConfiguration} so that applications
 * that do not have {@code threadmill-store-postgres} on their runtime
 * classpath can still load the core auto-configuration. Spring Boot reads the
 * {@code @ConditionalOnClass} attribute via ASM without triggering JVM class
 * loading, so {@link PostgresJobStore} and its sibling types are never
 * resolved unless the module is present.
 *
 * <p>This class is ordered {@code @AutoConfigureBefore} the core
 * configuration so its {@code @Bean JobStore} declaration is considered
 * first: when the conditions match (Postgres on classpath, a
 * {@link DataSource} bean is present, and Redis is not configured) the
 * Postgres store wins the {@link ConditionalOnMissingBean} race in the core
 * config and Redis precedence is preserved by the
 * {@link OnRedisStoreNotConfigured} gate.
 */
@AutoConfiguration
@AutoConfigureBefore(ThreadmillAutoConfiguration.class)
@ConditionalOnClass(PostgresJobStore.class)
public class ThreadmillPostgresAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillPostgresAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(JobStore.class)
    @ConditionalOnBean(DataSource.class)
    @Conditional(OnRedisStoreNotConfigured.class)
    public JobStore threadmillJobStore(ThreadmillProperties properties, DataSource dataSource) {
        LOG.info("Threadmill: using Postgres store wired from the application's DataSource");
        PostgresJobStore.requireSupportedServer(dataSource);
        applyPostgresSchemaMode(dataSource, properties.getStore().getPostgres());
        if (properties.getSpring().getEnqueueMode() == SpringEnqueueMode.JOIN_TRANSACTION) {
            return new PostgresJobStore(
                    dataSource,
                    new JsonJobSerializer(),
                    JobStoreCapabilities.defaults(),
                    new SpringPostgresTransactionBoundary(dataSource));
        }
        return new PostgresJobStore(dataSource);
    }

    private static void applyPostgresSchemaMode(
            DataSource dataSource, ThreadmillProperties.PostgresProperties properties) {
        var migrations = new MigrationRunner(dataSource);
        switch (properties.getSchemaMode()) {
            case MIGRATE -> migrations.migrate();
            case VALIDATE -> migrations.validate();
            case NONE -> {}
            case DROP_AND_MIGRATE -> {
                if (!properties.isAllowDestructiveSchemaReset()) {
                    throw new IllegalStateException("threadmill.store.postgres.schema-mode=drop-and-migrate requires"
                            + " threadmill.store.postgres.allow-destructive-schema-reset=true");
                }
                migrations.dropThreadmillObjects();
                migrations.migrate();
            }
        }
    }
}
