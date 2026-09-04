package com.hemju.threadmill.dashboard.spring.browser;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;

/** Handler an ADMIN operator selects through the browser suite's replace action. */
public final class ReplacedHandler implements JobHandler<BrowserPayload> {

  @Override
  public void run(BrowserPayload payload, JobExecutionContext ctx) {}
}
