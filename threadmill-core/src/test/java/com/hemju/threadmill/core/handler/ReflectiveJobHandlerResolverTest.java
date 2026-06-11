package com.hemju.threadmill.core.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

class ReflectiveJobHandlerResolverTest {

    // Tripwire flag lives OUTSIDE the loaded class so the assertion can read it
    // without itself triggering InitTripwire's static initializer.
    private static final AtomicBoolean TRIPPED = new AtomicBoolean(false);

    /** A non-JobHandler type whose static initializer must never run during resolution. */
    static final class InitTripwire {
        static {
            TRIPPED.set(true);
        }
    }

    @Test
    void resolveDoesNotRunStaticInitializerOfANonHandlerType() {
        TRIPPED.set(false);
        var resolver = new ReflectiveJobHandlerResolver();

        assertThatThrownBy(() -> resolver.resolve(InitTripwire.class.getName()))
                .isInstanceOf(JobHandlerResolver.HandlerResolutionException.class);

        // The class was rejected before its <clinit> could run.
        assertThat(TRIPPED).isFalse();
    }

    @Test
    void resolvesARealHandler() throws Exception {
        var resolver = new ReflectiveJobHandlerResolver();
        assertThat(resolver.resolve(NoopHandler.class.getName())).isInstanceOf(NoopHandler.class);
    }

    static final class NoopHandler implements JobHandler<JobPayload> {
        @Override
        public void run(JobPayload payload, JobExecutionContext ctx) {}
    }
}
