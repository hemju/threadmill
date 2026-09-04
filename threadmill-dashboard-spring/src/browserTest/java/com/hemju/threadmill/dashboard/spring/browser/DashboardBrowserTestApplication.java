package com.hemju.threadmill.dashboard.spring.browser;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/** Real mounted Spring dashboard used only by the Playwright browser suite. */
@SpringBootConfiguration(proxyBeanMethods = false)
@EnableAutoConfiguration
public final class DashboardBrowserTestApplication {

  static void main(String[] args) {
    var browserPort = System.getenv("THREADMILL_BROWSER_PORT");
    if (browserPort == null || browserPort.isBlank()) {
      throw new IllegalStateException(
          "THREADMILL_BROWSER_PORT is required; run the Gradle browserTest task");
    }
    var application = new SpringApplication(DashboardBrowserTestApplication.class);
    application.setDefaultProperties(Map.of(
        "server.address", "127.0.0.1",
        "server.port", browserPort,
        "spring.main.banner-mode", "off",
        "logging.level.root", "WARN",
        "threadmill.dashboard.api.base-path", "/ops/threadmill/api",
        "threadmill.dashboard.expose-sensitive-details", "true"));
    application.run(args);
  }

  @Bean
  JobStore jobStore() {
    var store = new InMemoryJobStore();
    store.insert(Job.builder()
        .id(JobId.parse("018f0000-0000-7000-8000-000000000101"))
        .createdAt(Instant.parse("2026-01-01T00:00:00Z"))
        .spec(JobSpec.of(
            ReplaceMeHandler.class.getName(),
            new JobArgument(BrowserPayload.class.getName(), "{\"value\":\"before\"}")))
        .initialState(JobState.ENQUEUED)
        .build());
    insertJob(
        store,
        "018f0000-0000-7000-8000-000000000102",
        "com.example.RequeueMeHandler",
        JobState.FAILED);
    insertJob(
        store,
        "018f0000-0000-7000-8000-000000000103",
        "com.example.RetryMeHandler",
        JobState.FAILED);
    insertJob(
        store,
        "018f0000-0000-7000-8000-000000000104",
        "com.example.DeleteMeHandler",
        JobState.SUCCEEDED);
    var sensitive = Job.builder()
        .id(JobId.parse("018f0000-0000-7000-8000-000000000105"))
        .createdAt(Instant.parse("2026-01-01T00:00:05Z"))
        .spec(JobSpec.of("com.example.SensitiveHandler"))
        .metadata("customer-secret", "visible-to-admin")
        .build();
    store.insert(sensitive);

    upsertRecurring(store, "nightly-trigger");
    upsertRecurring(store, "nightly-update");
    upsertRecurring(store, "nightly-delete");
    return store;
  }

  /**
   * Without a serializer the dashboard cannot validate an executable
   * definition and securely answers 501, so a host that offers replacement
   * must supply one. This mounts the dashboard alone, without the Spring Boot
   * starter that would otherwise contribute it.
   */
  @Bean
  JobSerializer jobSerializer() {
    return new JsonJobSerializer();
  }

  @Bean
  UserDetailsService userDetailsService() {
    var admin = User.withUsername("admin")
        .password("{noop}admin")
        .authorities("THREADMILL_ADMIN")
        .build();
    var viewer = User.withUsername("viewer")
        .password("{noop}viewer")
        .authorities("THREADMILL_READ")
        .build();
    return new InMemoryUserDetailsManager(admin, viewer);
  }

  private static void insertJob(
      InMemoryJobStore store, String id, String handlerType, JobState state) {
    var job = Job.builder()
        .id(JobId.parse(id))
        .createdAt(Instant.parse("2026-01-01T00:00:00Z"))
        .spec(JobSpec.of(handlerType))
        .initialState(state)
        .build();
    store.insert(job);
  }

  private static void upsertRecurring(InMemoryJobStore store, String name) {
    store.upsertCronTask(new CronTask(
        name,
        new CronTask.Trigger.Interval(Duration.ofHours(1)),
        "com.example.ReportHandler",
        new JobArgument("java.lang.String", "\"browser-secret\""),
        "default",
        0,
        CronTask.MissedRunPolicy.DROP,
        ZoneId.of("UTC"),
        true));
  }
}
