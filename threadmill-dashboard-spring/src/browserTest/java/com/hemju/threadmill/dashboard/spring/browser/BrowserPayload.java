package com.hemju.threadmill.dashboard.spring.browser;

import com.hemju.threadmill.core.handler.JobPayload;

/** Payload shared by the browser suite's replaceable handlers. */
public record BrowserPayload(String value) implements JobPayload {}
