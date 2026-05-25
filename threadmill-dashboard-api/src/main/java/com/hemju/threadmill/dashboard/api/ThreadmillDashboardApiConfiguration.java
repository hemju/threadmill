package com.hemju.threadmill.dashboard.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;

import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.store.JobStore;

/** Minimal Spring configuration for the Threadmill dashboard API. */
@AutoConfiguration(afterName = "com.hemju.threadmill.spring.ThreadmillAutoConfiguration")
@ConditionalOnBean(JobStore.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableConfigurationProperties(DashboardProperties.class)
public class ThreadmillDashboardApiConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillDashboardApiConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public LocalWakeBus threadmillDashboardWakeBus() {
        return new LocalWakeBus();
    }

    @Bean
    @ConditionalOnMissingBean
    public DashboardOptions threadmillDashboardOptions(DashboardProperties properties) {
        return properties.toOptions();
    }

    @Bean
    @ConditionalOnMissingBean
    public DashboardAuthorizer threadmillDashboardAuthorizer() {
        return new SpringSecurityDashboardAuthorizer();
    }

    @Bean
    @ConditionalOnMissingBean
    public DashboardAuditSink threadmillDashboardAuditSink() {
        return DashboardAuditSink.noop();
    }

    @Bean
    @ConditionalOnMissingBean
    public DashboardApiService threadmillDashboardApiService(JobStore store, LocalWakeBus wakeBus) {
        return new DashboardApiService(store, wakeBus);
    }

    @Bean
    @ConditionalOnMissingBean
    public ThreadmillDashboardApiController threadmillDashboardApiController(
            DashboardApiService service,
            DashboardAuthorizer authorizer,
            DashboardAuditSink auditSink,
            DashboardOptions options) {
        return new ThreadmillDashboardApiController(service, authorizer, auditSink, options);
    }

    @Bean
    @ConditionalOnBean(HttpSecurity.class)
    @ConditionalOnMissingBean(name = "threadmillDashboardSecurityFilterChain")
    @ConditionalOnProperty(
            prefix = "threadmill.dashboard.security",
            name = "auto-configure",
            havingValue = "true",
            matchIfMissing = true)
    @Order(Ordered.HIGHEST_PRECEDENCE + 100)
    public SecurityFilterChain threadmillDashboardSecurityFilterChain(HttpSecurity http, DashboardOptions options)
            throws Exception {
        String matcher = options.apiBasePath() + "/**";
        return http.securityMatcher(matcher)
                .authorizeHttpRequests(authorize -> authorize.anyRequest().authenticated())
                .csrf(csrf -> csrf.csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse()))
                .httpBasic(Customizer.withDefaults())
                .build();
    }

    @Bean
    public SmartInitializingSingleton threadmillDashboardSecurityValidator(
            ListableBeanFactory beanFactory, DashboardOptions options, DashboardAuditSink auditSink) {
        return () -> {
            if (!options.allowUnsafeReadOnlyWithoutAuthentication()
                    && beanFactory.getBeanNamesForType(SecurityFilterChain.class).length == 0) {
                throw new IllegalStateException(
                        "Threadmill dashboard API requires Spring Security or unsafe read-only local mode");
            }
            if (auditSink.isNoop()) {
                LOG.warn(
                        "Threadmill dashboard API is using the default noop audit sink; operator actions will not be persisted");
            }
        };
    }
}
