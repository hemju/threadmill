package com.hemju.threadmill.dashboard.api;

/** Dashboard API safety options. */
public record DashboardOptions(
        boolean allowUnsafeReadOnlyWithoutAuthentication,
        boolean exposeSensitiveDetails,
        String apiBasePath,
        boolean autoConfigureSecurity) {

    public DashboardOptions(boolean allowUnsafeReadOnlyWithoutAuthentication, boolean exposeSensitiveDetails) {
        this(allowUnsafeReadOnlyWithoutAuthentication, exposeSensitiveDetails, "/threadmill/api", true);
    }

    public static DashboardOptions secureDefaults() {
        return new DashboardOptions(false, false, "/threadmill/api", true);
    }
}
