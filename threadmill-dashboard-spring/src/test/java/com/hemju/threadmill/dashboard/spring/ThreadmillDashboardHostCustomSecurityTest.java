package com.hemju.threadmill.dashboard.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

@SpringBootTest(
    classes = ThreadmillDashboardHostCustomSecurityTest.TestApp.class,
    properties = {
      "spring.main.web-application-type=servlet",
      "threadmill.dashboard.api.base-path=/ops/api"
    })
class ThreadmillDashboardHostCustomSecurityTest {
  private final WebApplicationContext context;

  ThreadmillDashboardHostCustomSecurityTest(WebApplicationContext context) {
    this.context = context;
  }

  @Test
  void customHostChainKeepsItsPolicyAndCustomDashboardPathStaysProtected() throws Exception {
    assertThat(context.containsBean("threadmillHostSecurityFilterChain")).isFalse();
    var mvc =
        MockMvcBuilders.webAppContextSetup(context).apply(springSecurity()).build();
    mvc.perform(get("/host-public")).andExpect(status().isOk());
    mvc.perform(get("/host-private")).andExpect(status().isUnauthorized());
    mvc.perform(get("/ops/api/overview")).andExpect(status().isUnauthorized());
    mvc.perform(get("/threadmill/index.html")).andExpect(status().isUnauthorized());
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class TestApp {
    @Bean
    JobStore jobStore() {
      return new InMemoryJobStore();
    }

    @Bean
    HostController hostController() {
      return new HostController();
    }

    @Bean
    SecurityFilterChain hostChain(HttpSecurity http) throws Exception {
      return http.authorizeHttpRequests(auth ->
              auth.requestMatchers("/host-public").permitAll().anyRequest().authenticated())
          .httpBasic(Customizer.withDefaults())
          .build();
    }
  }

  @RestController
  static class HostController {
    @GetMapping({"/host-public", "/host-private"})
    String endpoint() {
      return "host";
    }
  }
}
