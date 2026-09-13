package com.hemju.threadmill.dashboard.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@SpringBootTest(
    classes = ThreadmillDashboardSecurityStarterAutoConfigTest.TestApp.class,
    properties = {
      "spring.main.web-application-type=servlet",
      "threadmill.dashboard.security.auto-configure=false"
    })
class ThreadmillDashboardSecurityDisabledTest {
  private final WebApplicationContext context;

  ThreadmillDashboardSecurityDisabledTest(WebApplicationContext context) {
    this.context = context;
  }

  @Test
  void disablingDashboardChainsLeavesBootAuthenticationInPlace() throws Exception {
    assertThat(context.containsBean("threadmillHostSecurityFilterChain")).isFalse();
    assertThat(context.containsBean("threadmillDashboardSecurityFilterChain")).isFalse();
    var mvc =
        MockMvcBuilders.webAppContextSetup(context).apply(springSecurity()).build();
    mvc.perform(get("/outside-threadmill").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isUnauthorized());
    mvc.perform(get("/threadmill/api/overview").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isUnauthorized());
  }
}
