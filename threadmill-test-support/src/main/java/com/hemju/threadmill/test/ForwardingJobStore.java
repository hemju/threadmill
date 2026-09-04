package com.hemju.threadmill.test;

import com.hemju.threadmill.core.store.JobStore;

/**
 * Compatibility alias for {@link com.hemju.threadmill.core.store.ForwardingJobStore}.
 *
 * <p>The forwarding base moved into {@code threadmill-core} so the production
 * tracing and metrics decorators can share it (issue #131). This subclass adds
 * nothing; it exists only so test code compiled against the previous location
 * keeps working.
 *
 * @deprecated extend {@link com.hemju.threadmill.core.store.ForwardingJobStore} directly.
 */
@Deprecated
// The superclass shares this simple name, so it stays fully qualified at its one use site.
public class ForwardingJobStore extends com.hemju.threadmill.core.store.ForwardingJobStore {

  public ForwardingJobStore(JobStore delegate) {
    super(delegate);
  }
}
