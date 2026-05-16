package com.hemju.threadmill.soak;

import java.util.concurrent.atomic.AtomicInteger;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;

/** Shared payload + counting handler used by every soak class. */
public final class SoakWork {

    private SoakWork() {}

    public static final class P implements JobPayload {
        public int n;

        public P() {}

        public P(int n) {
            this.n = n;
        }
    }

    public static final class CountingHandler implements JobHandler<P> {
        public static final AtomicInteger COUNT = new AtomicInteger();

        @Override
        public void run(P p, JobExecutionContext c) {
            COUNT.incrementAndGet();
        }
    }
}
