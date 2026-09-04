package com.hemju.threadmill.dashboard.spring;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hemju.threadmill.dashboard.api.DashboardOptions;

/** Serves runtime configuration consumed before the packaged dashboard application starts. */
@Controller
final class ThreadmillDashboardUiConfigurationController {

  static final String CONFIGURATION_PATH = "/threadmill/config.js";
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private final DashboardOptions options;

  ThreadmillDashboardUiConfigurationController(DashboardOptions options) {
    this.options = options;
  }

  @GetMapping(value = CONFIGURATION_PATH, produces = "application/javascript")
  @ResponseBody
  String configuration() {
    return "window.__THREADMILL_DASHBOARD_CONFIG__ = Object.freeze({ apiBasePath: "
        + javascriptString(options.apiBasePath())
        + " });\n";
  }

  private static String javascriptString(String value) {
    var escaped = new StringBuilder(value.length() + 2).append('"');
    for (var index = 0; index < value.length(); index++) {
      var character = value.charAt(index);
      switch (character) {
        case '"' -> escaped.append("\\\"");
        case '\\' -> escaped.append("\\\\");
        case '\b' -> escaped.append("\\b");
        case '\f' -> escaped.append("\\f");
        case '\n' -> escaped.append("\\n");
        case '\r' -> escaped.append("\\r");
        case '\t' -> escaped.append("\\t");
        default -> {
          if (character < 0x20
              || character == '<'
              || character == '>'
              || character == '&'
              || character == '\u2028'
              || character == '\u2029') {
            escaped
                .append("\\u")
                .append(HEX[(character >>> 12) & 0xf])
                .append(HEX[(character >>> 8) & 0xf])
                .append(HEX[(character >>> 4) & 0xf])
                .append(HEX[character & 0xf]);
          } else {
            escaped.append(character);
          }
        }
      }
    }
    return escaped.append('"').toString();
  }
}
