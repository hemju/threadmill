package com.hemju.threadmill.dashboard.spring;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

@SpringBootTest(
        classes = ThreadmillDashboardSecurityIntegrationTest.TestApp.class,
        properties = "spring.main.web-application-type=servlet")
class ThreadmillDashboardSecurityIntegrationTest {

    private final WebApplicationContext context;
    private MockMvc mvc;

    ThreadmillDashboardSecurityIntegrationTest(WebApplicationContext context) {
        this.context = context;
    }

    @BeforeEach
    void setUpMockMvc() {
        mvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(springSecurity())
                .build();
    }

    @Test
    void dashboardApiRequiresAuthenticationByDefault() throws Exception {
        mvc.perform(get("/threadmill/api/overview")).andExpect(status().isUnauthorized());
    }

    @Test
    void sessionExposesCsrfMetadataWhenAuthenticated() throws Exception {
        mvc.perform(get("/threadmill/api/session").with(user("ada").authorities(authority("THREADMILL_READ"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.csrf.headerName").exists())
                .andExpect(jsonPath("$.csrf.token").exists());
    }

    @Test
    void mutationRequiresCsrfToken() throws Exception {
        mvc.perform(post("/threadmill/api/queues/default/pause")
                        .with(user("ada").authorities(authority("THREADMILL_PAUSE_QUEUE")))
                        .contentType("application/json")
                        .content("{\"reason\":\"test\"}"))
                .andExpect(status().isForbidden());
    }

    @Test
    void mutationAcceptsValidCsrfToken() throws Exception {
        mvc.perform(post("/threadmill/api/queues/default/pause")
                        .with(user("ada").authorities(authority("THREADMILL_PAUSE_QUEUE")))
                        .with(csrf())
                        .contentType("application/json")
                        .content("{\"reason\":\"test\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("paused"));
    }

    private static SimpleGrantedAuthority authority(String value) {
        return new SimpleGrantedAuthority(value);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnableWebSecurity
    static class TestApp {
        @Bean
        JobStore jobStore() {
            return new InMemoryJobStore();
        }
    }
}
