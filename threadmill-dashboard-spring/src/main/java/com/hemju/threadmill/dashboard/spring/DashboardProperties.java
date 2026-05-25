package com.hemju.threadmill.dashboard.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.hemju.threadmill.dashboard.api.DashboardOptions;

/** Spring Boot properties for the dashboard API. */
@ConfigurationProperties(prefix = "threadmill.dashboard")
public class DashboardProperties {

    private final Api api = new Api();
    private final Security security = new Security();
    private boolean allowUnsafeReadOnlyWithoutAuthentication;
    private boolean exposeSensitiveDetails;

    public Api getApi() {
        return api;
    }

    public Security getSecurity() {
        return security;
    }

    public boolean isAllowUnsafeReadOnlyWithoutAuthentication() {
        return allowUnsafeReadOnlyWithoutAuthentication;
    }

    public void setAllowUnsafeReadOnlyWithoutAuthentication(boolean allowUnsafeReadOnlyWithoutAuthentication) {
        this.allowUnsafeReadOnlyWithoutAuthentication = allowUnsafeReadOnlyWithoutAuthentication;
    }

    public boolean isExposeSensitiveDetails() {
        return exposeSensitiveDetails;
    }

    public void setExposeSensitiveDetails(boolean exposeSensitiveDetails) {
        this.exposeSensitiveDetails = exposeSensitiveDetails;
    }

    DashboardOptions toOptions() {
        return new DashboardOptions(
                allowUnsafeReadOnlyWithoutAuthentication,
                exposeSensitiveDetails,
                normalizeBasePath(api.basePath),
                security.autoConfigure);
    }

    private static String normalizeBasePath(String value) {
        if (value == null || value.isBlank()) return "/threadmill/api";
        String normalized = value.trim();
        if (!normalized.startsWith("/")) normalized = "/" + normalized;
        if (normalized.endsWith("/") && normalized.length() > 1) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    public static final class Api {
        private String basePath = "/threadmill/api";

        public String getBasePath() {
            return basePath;
        }

        public void setBasePath(String basePath) {
            this.basePath = basePath;
        }
    }

    public static final class Security {
        private boolean autoConfigure = true;

        public boolean isAutoConfigure() {
            return autoConfigure;
        }

        public void setAutoConfigure(boolean autoConfigure) {
            this.autoConfigure = autoConfigure;
        }
    }
}
