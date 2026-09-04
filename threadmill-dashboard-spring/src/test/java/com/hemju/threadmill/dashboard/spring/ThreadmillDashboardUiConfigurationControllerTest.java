package com.hemju.threadmill.dashboard.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.dashboard.api.DashboardOptions;

class ThreadmillDashboardUiConfigurationControllerTest {

  @Test
  void emitsTheConfiguredApiPathAsSafeJavaScript() {
    var options = new DashboardOptions(false, false, "/ops/<dashboard>\"&\n/api", true);
    var controller = new ThreadmillDashboardUiConfigurationController(options);

    assertThat(controller.configuration())
        .isEqualTo("window.__THREADMILL_DASHBOARD_CONFIG__ = Object.freeze({ apiBasePath: "
            + "\"/ops/\\u003cdashboard\\u003e\\\"\\u0026\\n/api\" });\n");
  }
}
