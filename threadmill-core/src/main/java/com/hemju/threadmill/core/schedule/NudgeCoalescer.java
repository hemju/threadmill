package com.hemju.threadmill.core.schedule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.hemju.threadmill.core.store.JobStore.NudgeOutcome;

/**
 * Per-task single-flight coalescer for nudge writes.
 *
 * <p>All nudges for one task target one store cell, so under a producer burst
 * the writes are pure duplication. This coalescer bounds the write rate to at
 * most one in-flight store write per task per JVM: the first caller becomes
 * the writer; callers arriving while a write is in flight join a single
 * follow-up write that starts only after the current one completes.
 *
 * <p>The follow-up (never the in-flight write) is what joiners share, and that
 * is load-bearing for the run-after-wake guarantee: an in-flight write may
 * have committed <em>before</em> a joiner's own triggering work committed, so
 * treating it as covering the joiner could lose the follow-up run. A write
 * that <em>starts</em> after the joiner arrived necessarily commits after it.
 *
 * <p>Callers block until their covering write finishes (one or two store
 * round trips) and receive its outcome; a failed write propagates to every
 * caller it covered, so acceptance is never reported for a write that did not
 * commit. Under a sustained burst the writer thread keeps driving follow-up
 * batches until a lull — each iteration serves every caller that arrived
 * during the previous write.
 */
final class NudgeCoalescer {

    private static final class Slot {
        CompletableFuture<NudgeOutcome> inFlight;
        CompletableFuture<NudgeOutcome> next;
        boolean retired;
    }

    private final ConcurrentHashMap<String, Slot> slots = new ConcurrentHashMap<>();

    /**
     * Record one nudge through {@code write}, coalescing with concurrent
     * callers for the same task. Returns the outcome of a write that started
     * at or after this call.
     */
    NudgeOutcome nudge(String taskName, Supplier<NudgeOutcome> write) {
        while (true) {
            Slot slot = slots.computeIfAbsent(taskName, name -> new Slot());
            CompletableFuture<NudgeOutcome> waitOn;
            boolean writer = false;
            synchronized (slot) {
                if (slot.retired) {
                    continue; // raced a retiring writer; grab a fresh slot
                }
                if (slot.inFlight == null) {
                    slot.inFlight = new CompletableFuture<>();
                    waitOn = slot.inFlight;
                    writer = true;
                } else {
                    if (slot.next == null) {
                        slot.next = new CompletableFuture<>();
                    }
                    waitOn = slot.next;
                }
            }
            if (writer) {
                drive(taskName, slot, write);
            }
            try {
                return waitOn.join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof RuntimeException runtime) throw runtime;
                if (e.getCause() instanceof Error error) throw error;
                throw e;
            }
        }
    }

    private void drive(String taskName, Slot slot, Supplier<NudgeOutcome> write) {
        while (true) {
            CompletableFuture<NudgeOutcome> current;
            synchronized (slot) {
                current = slot.inFlight;
            }
            NudgeOutcome outcome = null;
            Throwable failure = null;
            try {
                outcome = write.get();
            } catch (Throwable t) {
                failure = t;
            }
            CompletableFuture<NudgeOutcome> promoted;
            synchronized (slot) {
                promoted = slot.next;
                slot.next = null;
                slot.inFlight = promoted;
                if (promoted == null) {
                    // Retire before removal so a caller that already holds
                    // this slot re-loops instead of joining a dead future.
                    slot.retired = true;
                    slots.remove(taskName, slot);
                }
            }
            if (failure != null) {
                current.completeExceptionally(failure);
            } else {
                current.complete(outcome);
            }
            if (promoted == null) {
                return;
            }
        }
    }
}
