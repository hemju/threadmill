package com.hemju.threadmill.dashboard.api;

/** Framework-neutral dashboard API failure. Adapters map the code to transport status. */
public final class DashboardApiException extends RuntimeException {

    private final Code code;

    private DashboardApiException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public Code code() {
        return code;
    }

    public static DashboardApiException badRequest(String message) {
        return new DashboardApiException(Code.BAD_REQUEST, message);
    }

    public static DashboardApiException notFound(String message) {
        return new DashboardApiException(Code.NOT_FOUND, message);
    }

    public static DashboardApiException conflict(String message) {
        return new DashboardApiException(Code.CONFLICT, message);
    }

    public enum Code {
        BAD_REQUEST,
        NOT_FOUND,
        CONFLICT
    }
}
