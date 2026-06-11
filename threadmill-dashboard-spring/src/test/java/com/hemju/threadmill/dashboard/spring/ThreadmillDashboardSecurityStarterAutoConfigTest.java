package com.hemju.threadmill.dashboard.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/**
 * Regression for the silently-skipped dashboard security chain: the test app
 * deliberately does NOT declare {@code @EnableWebSecurity} and relies on the
 * Spring Security starter's auto-configuration alone. Without the explicit
 * after-edges to the security auto-configurations, auto-configs sort
 * alphabetically ({@code com.hemju...} first), the {@code HttpSecurity} bean
 * definition does not exist when {@code @ConditionalOnBean} is evaluated, and
 * the documented dashboard chain is never created.
 */
@SpringBootTest(
        classes = ThreadmillDashboardSecurityStarterAutoConfigTest.TestApp.class,
        properties = "spring.main.web-application-type=servlet")
class ThreadmillDashboardSecurityStarterAutoConfigTest {

    private final WebApplicationContext context;

    ThreadmillDashboardSecurityStarterAutoConfigTest(WebApplicationContext context) {
        this.context = context;
    }

    @Test
    void dashboardChainIsCreatedWhenTheHostReliesOnSecurityStarterAutoConfiguration() {
        assertThat(context.containsBean("threadmillDashboardSecurityFilterChain"))
                .isTrue();
    }

    @Test
    void documentedSessionAndCsrfBehaviorApplies() throws Exception {
        var mvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(springSecurity())
                .build();

        mvc.perform(get("/threadmill/api/overview")).andExpect(status().isUnauthorized());
        mvc.perform(get("/threadmill/api/session")
                        .with(user("ada").authorities(new SimpleGrantedAuthority("THREADMILL_READ"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.csrf.headerName").exists())
                .andExpect(jsonPath("$.csrf.token").exists());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApp {
        @Bean
        JobStore jobStore() {
            return new InMemoryJobStore();
        }
    }
}
