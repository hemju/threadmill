package com.hemju.threadmill.dashboard.spring;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.ZoneId;

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

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
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

    @Test
    void staticUiMountRequiresAuthenticationByDefault() throws Exception {
        // The UI shell is an admin surface; the auto-configured chain must
        // cover the fixed /threadmill/** mount, not just the API subpath.
        mvc.perform(get("/threadmill/")).andExpect(status().isUnauthorized());
        mvc.perform(get("/threadmill/index.html")).andExpect(status().isUnauthorized());
        mvc.perform(get("/threadmill/assets/app.js")).andExpect(status().isUnauthorized());
        // Authenticated requests pass the chain (404 here — no UI assets on
        // this test classpath — but decisively not 401).
        mvc.perform(get("/threadmill/index.html").with(user("ada").authorities(authority("THREADMILL_READ"))))
                .andExpect(status().isNotFound());
    }

    @Test
    void missingScheduleRetryDelayIsABadRequestNotAServerError() throws Exception {
        var store = context.getBean(JobStore.class);
        var failed = Job.builder()
                .spec(JobSpec.of("com.example.Handler"))
                .initialState(JobState.FAILED)
                .build();
        store.insert(failed);

        mvc.perform(post("/threadmill/api/jobs/" + failed.id() + "/schedule-retry")
                        .with(user("ada").authorities(authority("THREADMILL_REQUEUE_JOB")))
                        .with(csrf())
                        .contentType("application/json")
                        .content("{\"expectedVersion\":" + failed.version() + "}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("delay is required"));
    }

    @Test
    void invalidZoneAndIntervalOnRecurringUpdateAreBadRequests() throws Exception {
        var store = context.getBean(JobStore.class);
        store.upsertCronTask(new CronTask(
                "report",
                new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
                "com.example.ReportHandler",
                new JobArgument("com.hemju.threadmill.core.handler.NoPayload", "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true));
        var operator = user("ada").authorities(authority("THREADMILL_UPDATE_RECURRING"));

        mvc.perform(put("/threadmill/api/recurring/report")
                        .with(operator)
                        .with(csrf())
                        .contentType("application/json")
                        .content("{\"zone\":\"Not/AZone\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(put("/threadmill/api/recurring/report")
                        .with(operator)
                        .with(csrf())
                        .contentType("application/json")
                        .content("{\"triggerKind\":\"INTERVAL\",\"triggerValue\":\"5 minutes\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void oversizedJobReplacementIsContentTooLargeNotAServerError() throws Exception {
        var store = context.getBean(JobStore.class);
        var pending = Job.builder().spec(JobSpec.of("com.example.Handler")).build();
        store.insert(pending);
        String hugeArgument = "x".repeat(300_000); // > the 256 KiB default serialized-size cap

        mvc.perform(patch("/threadmill/api/jobs/" + pending.id())
                        .with(user("ada").authorities(authority("THREADMILL_REPLACE_JOB")))
                        .with(csrf())
                        .contentType("application/json")
                        .content("{\"expectedVersion\":" + pending.version()
                                + ",\"handlerType\":\"com.example.Other\",\"arguments\":[{\"typeTag\":\"java.lang.String\",\"serialized\":\""
                                + hugeArgument + "\"}]}"))
                .andExpect(status().is(413))
                .andExpect(jsonPath("$.detail").value(containsString("exceeds limit")));
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
