package com.hemju.threadmill.dashboard.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnResource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.dashboard.api.DashboardApiService;
import com.hemju.threadmill.dashboard.api.DashboardAuditSink;
import com.hemju.threadmill.dashboard.api.DashboardOptions;

/**
 * Minimal Spring configuration for the Threadmill dashboard API.
 *
 * <p>The after-edges to the Spring Security auto-configurations are
 * load-bearing: auto-configs sort alphabetically absent explicit edges
 * ({@code com.hemju...} before {@code org.springframework.boot...}), so in a
 * host that relies on the security starter's auto-configured
 * {@code @EnableWebSecurity} the {@code HttpSecurity} bean definition would
 * not exist yet when {@code @ConditionalOnBean(HttpSecurity.class)} is
 * evaluated — the documented dashboard chain would silently never be created.
 */
@AutoConfiguration(
        afterName = {
            "com.hemju.threadmill.spring.ThreadmillAutoConfiguration",
            "org.springframework.boot.security.autoconfigure.SecurityAutoConfiguration",
            "org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration"
        })
@ConditionalOnBean(JobStore.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableConfigurationProperties(DashboardProperties.class)
public class ThreadmillDashboardApiConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillDashboardApiConfiguration.class);
    static final String UI_INDEX_RESOURCE = "classpath:/META-INF/resources/threadmill/index.html";

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

    /**
     * Secures both the API and the static UI mount. The UI shell lives at
     * the fixed {@code /threadmill/**} path; leaving it outside the matcher
     * would serve the operations-console HTML/JS to unauthenticated clients
     * — asset disclosure of an admin surface even though data calls 401.
     */
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
        String apiMatcher = options.apiBasePath() + "/**";
        return http.securityMatcher(apiMatcher, "/threadmill/**", "/threadmill")
                .authorizeHttpRequests(authorize -> authorize.anyRequest().authenticated())
                .csrf(csrf -> csrf.csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse()))
                .httpBasic(Customizer.withDefaults())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "threadmillDashboardUiWebMvcConfigurer")
    @ConditionalOnResource(resources = UI_INDEX_RESOURCE)
    public WebMvcConfigurer threadmillDashboardUiWebMvcConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addResourceHandlers(ResourceHandlerRegistry registry) {
                registry.addResourceHandler("/threadmill/**")
                        .addResourceLocations("classpath:/META-INF/resources/threadmill/");
            }

            @Override
            public void addViewControllers(ViewControllerRegistry registry) {
                registry.addRedirectViewController("/threadmill", "/threadmill/");
                registry.addViewController("/threadmill/").setViewName("forward:/threadmill/index.html");
            }
        };
    }

    @Bean
    public SmartInitializingSingleton threadmillDashboardSecurityValidator(
            ListableBeanFactory beanFactory, DashboardOptions options, DashboardAuditSink auditSink, Environment env) {
        return () -> {
            // The controller mounts at the raw property; the security chain
            // and validator use DashboardOptions.apiBasePath(). A custom
            // DashboardOptions bean that diverges from the property would
            // scope the security chain to a path with no endpoints — fail
            // fast instead of silently mis-securing.
            String propertyPath = DashboardProperties.normalizeBasePath(
                    env.getProperty("threadmill.dashboard.api.base-path", "/threadmill/api"));
            if (!propertyPath.equals(options.apiBasePath())) {
                throw new IllegalStateException("DashboardOptions.apiBasePath (" + options.apiBasePath()
                        + ") must mirror threadmill.dashboard.api.base-path (" + propertyPath
                        + ") — the controller mounts at the property, the security chain at the options bean");
            }
            boolean anyChain = beanFactory.getBeanNamesForType(SecurityFilterChain.class).length > 0;
            if (!options.allowUnsafeReadOnlyWithoutAuthentication() && !anyChain) {
                throw new IllegalStateException(
                        "Threadmill dashboard API requires Spring Security or unsafe read-only local mode");
            }
            // A host chain existing somewhere does not mean the documented
            // dashboard posture (scoped httpBasic + cookie CSRF) applied. If
            // auto-configure is on but the dashboard chain bean was skipped,
            // the configuration silently degraded — fail loudly instead.
            if (options.autoConfigureSecurity()
                    && anyChain
                    && !options.allowUnsafeReadOnlyWithoutAuthentication()
                    && !beanFactory.containsBean("threadmillDashboardSecurityFilterChain")) {
                throw new IllegalStateException(
                        "threadmill.dashboard.security.auto-configure is on but the threadmillDashboardSecurityFilterChain "
                                + "bean was not created (no HttpSecurity available). Enable Spring Security's web "
                                + "support, provide your own SecurityFilterChain bean named "
                                + "threadmillDashboardSecurityFilterChain, or set "
                                + "threadmill.dashboard.security.auto-configure=false");
            }
            if (auditSink.isNoop()) {
                LOG.warn(
                        "Threadmill dashboard API is using the default noop audit sink; operator actions will not be persisted");
            }
        };
    }
}
