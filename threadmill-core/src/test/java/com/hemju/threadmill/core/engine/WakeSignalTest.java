package com.hemju.threadmill.core.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class WakeSignalTest {

    @Test
    void awaitForExpiresWithoutASignal() throws Exception {
        var signal = new WakeSignal();
        long started = System.nanoTime();
        boolean woke = signal.awaitFor(Duration.ofMillis(50));
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
        assertThat(woke).isFalse();
        assertThat(elapsedMillis).isGreaterThanOrEqualTo(40);
    }

    @Test
    void signalWakesAnAwaitingThread() throws Exception {
        var signal = new WakeSignal();
        var ready = new CountDownLatch(1);
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var fut = executor.submit(() -> {
                ready.countDown();
                return signal.awaitFor(Duration.ofSeconds(5));
            });
            ready.await();
            Thread.sleep(10);
            signal.signal();
            assertThat(fut.get(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void multipleSignalsCollapseIntoOnePermit() throws Exception {
        var signal = new WakeSignal();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var fired = new AtomicInteger();
            for (int i = 0; i < 10; i++) {
                executor.submit(() -> {
                    signal.signal();
                    fired.incrementAndGet();
                });
            }
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.SECONDS);
            assertThat(fired.get()).isEqualTo(10);
            // First await consumes the single coalesced permit.
            assertThat(signal.awaitFor(Duration.ofMillis(10))).isTrue();
            // The next await must time out — there is no second pending permit.
            assertThat(signal.awaitFor(Duration.ofMillis(50))).isFalse();
        } finally {
            executor.shutdownNow();
        }
    }
}
