package com.hemju.threadmill.core.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.store.JobStore.NudgeOutcome;

class NudgeCoalescerTest {

    @Test
    void aBurstAgainstAnInFlightWriteCollapsesIntoOneFollowUpWrite() throws Exception {
        var coalescer = new NudgeCoalescer();
        var writes = new AtomicInteger();
        var firstWriteEntered = new CountDownLatch(1);
        var releaseFirstWrite = new CountDownLatch(1);

        Thread writer = Thread.ofVirtual()
                .start(() -> coalescer.nudge("task", () -> {
                    int n = writes.incrementAndGet();
                    if (n == 1) {
                        firstWriteEntered.countDown();
                        try {
                            releaseFirstWrite.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return NudgeOutcome.ACCEPTED;
                }));
        assertThat(firstWriteEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // Five callers arrive while the first write is in flight. None of
        // them may be satisfied by it — its commit could predate their own
        // triggering commits — so they share ONE follow-up write instead.
        List<Thread> joiners = new ArrayList<>();
        var outcomes = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            joiners.add(Thread.ofVirtual().start(() -> {
                if (coalescer.nudge("task", () -> {
                            writes.incrementAndGet();
                            return NudgeOutcome.ACCEPTED;
                        })
                        == NudgeOutcome.ACCEPTED) {
                    outcomes.incrementAndGet();
                }
            }));
        }
        // Joiners must be parked on the follow-up before the writer resumes,
        // otherwise they would start their own write generations.
        Thread.sleep(Duration.ofMillis(200));
        releaseFirstWrite.countDown();

        writer.join(Duration.ofSeconds(5));
        for (Thread t : joiners) {
            assertThat(t.join(Duration.ofSeconds(5))).isTrue();
        }
        assertThat(outcomes.get()).isEqualTo(5);
        assertThat(writes.get())
                .as("one in-flight write plus exactly one follow-up for the burst")
                .isEqualTo(2);
    }

    @Test
    void theFirstCallerIsNotRetainedToDriveFollowUpGenerations() throws Exception {
        // Under sustained arrivals, follow-up generations run on the
        // coalescer's own virtual thread. A caller must return as soon as
        // its own covering write completes — never be held hostage driving
        // other producers' writes.
        var coalescer = new NudgeCoalescer();
        var writes = new AtomicInteger();
        var gen1Entered = new CountDownLatch(1);
        var gen1Release = new CountDownLatch(1);
        var gen2Entered = new CountDownLatch(1);
        var gen2Release = new CountDownLatch(1);
        Supplier<NudgeOutcome> write = () -> {
            int n = writes.incrementAndGet();
            try {
                if (n == 1) {
                    gen1Entered.countDown();
                    gen1Release.await();
                } else if (n == 2) {
                    gen2Entered.countDown();
                    gen2Release.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return NudgeOutcome.ACCEPTED;
        };

        Thread first = Thread.ofVirtual().start(() -> coalescer.nudge("task", write));
        assertThat(gen1Entered.await(5, TimeUnit.SECONDS)).isTrue();
        Thread second = Thread.ofVirtual().start(() -> coalescer.nudge("task", write));
        Thread.sleep(Duration.ofMillis(200));
        gen1Release.countDown();

        // The first caller returns while the second generation's write is
        // still blocked — the old behavior kept it driving that write.
        assertThat(first.join(Duration.ofSeconds(5)))
                .as("the first caller must not be retained past its own write")
                .isTrue();
        assertThat(gen2Entered.await(5, TimeUnit.SECONDS)).isTrue();

        // A third caller arriving mid-generation-2 joins generation 3 and
        // completes normally once the traffic drains.
        Thread third = Thread.ofVirtual().start(() -> coalescer.nudge("task", write));
        Thread.sleep(Duration.ofMillis(200));
        gen2Release.countDown();
        assertThat(second.join(Duration.ofSeconds(5))).isTrue();
        assertThat(third.join(Duration.ofSeconds(5))).isTrue();
        assertThat(writes.get()).isEqualTo(3);
    }

    @Test
    void sequentialNudgesEachWriteAndTheSlotIsRecycled() {
        var coalescer = new NudgeCoalescer();
        var writes = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            assertThat(coalescer.nudge("task", () -> {
                        writes.incrementAndGet();
                        return NudgeOutcome.ACCEPTED;
                    }))
                    .isEqualTo(NudgeOutcome.ACCEPTED);
        }
        assertThat(writes.get()).isEqualTo(3);
    }

    @Test
    void aFailedWritePropagatesToEveryCallerItCoveredAndTheNextNudgeRecovers() {
        var coalescer = new NudgeCoalescer();
        assertThatThrownBy(() -> coalescer.nudge("task", () -> {
                    throw new IllegalStateException("store down");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("store down");

        // The failed generation must not wedge the slot.
        assertThat(coalescer.nudge("task", () -> NudgeOutcome.ACCEPTED)).isEqualTo(NudgeOutcome.ACCEPTED);
    }

    @Test
    void outcomesArePerTaskNotShared() {
        var coalescer = new NudgeCoalescer();
        assertThat(coalescer.nudge("a", () -> NudgeOutcome.ACCEPTED)).isEqualTo(NudgeOutcome.ACCEPTED);
        assertThat(coalescer.nudge("b", () -> NudgeOutcome.UNKNOWN_TASK)).isEqualTo(NudgeOutcome.UNKNOWN_TASK);
        assertThat(coalescer.nudge("c", () -> NudgeOutcome.DISABLED)).isEqualTo(NudgeOutcome.DISABLED);
    }
}
