package com.hemju.threadmill.dashboard.spring;

import jakarta.servlet.DispatcherType;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

import com.hemju.threadmill.core.store.JobStore;

/**
 * Preserves authentication for host routes when adding the scoped dashboard chain.
 * Runs before the dashboard declares its chain, so only application-provided
 * chains cause this fallback to back off. Applications with custom chains retain
 * responsibility for their own route coverage, as with Spring Boot itself.
 */
@AutoConfiguration(
    afterName = {
      "com.hemju.threadmill.spring.ThreadmillAutoConfiguration",
      "org.springframework.boot.security.autoconfigure.SecurityAutoConfiguration"
    },
    before = ThreadmillDashboardApiConfiguration.class)
@ConditionalOnBean(JobStore.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnMissingBean(SecurityFilterChain.class)
@ConditionalOnProperty(
    prefix = "threadmill.dashboard.security",
    name = "auto-configure",
    havingValue = "true",
    matchIfMissing = true)
@EnableWebSecurity
public class ThreadmillDashboardHostSecurityConfiguration {
  /** Catch-all authentication with Spring Security's default CSRF protection. */
  @Bean
  @Order(Ordered.LOWEST_PRECEDENCE - 5)
  SecurityFilterChain threadmillHostSecurityFilterChain(HttpSecurity http) throws Exception {
    // Preserve the dashboard's original 401/403 through the container's
    // error dispatch; direct HTTP requests to /error still require authentication.
    return http.authorizeHttpRequests(authorize -> authorize
            .dispatcherTypeMatchers(DispatcherType.ERROR)
            .permitAll()
            .anyRequest()
            .authenticated())
        .formLogin(Customizer.withDefaults())
        .httpBasic(Customizer.withDefaults())
        .build();
  }
}
