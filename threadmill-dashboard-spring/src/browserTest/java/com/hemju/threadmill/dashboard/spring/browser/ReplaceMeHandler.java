package com.hemju.threadmill.dashboard.spring.browser;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;

/**
 * Original handler of the job the browser suite replaces.
 *
 * <p>The replacement path validates the handler and payload against the real
 * classpath (issue #95), so the browser fixture needs genuine types rather than
 * invented class names.
 */
public final class ReplaceMeHandler implements JobHandler<BrowserPayload> {

  @Override
  public void run(BrowserPayload payload, JobExecutionContext ctx) {}
}
