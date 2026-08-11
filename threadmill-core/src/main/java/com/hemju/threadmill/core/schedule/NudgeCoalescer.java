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
 * most one in-flight store write per task per scheduler instance: the first
 * caller becomes the writer; callers arriving while a write is in flight join
 * a single follow-up write that starts only after the current one completes.
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
 * commit. A caller's thread only ever executes its own generation's write:
 * follow-up generations are driven by a dedicated virtual thread, so under
 * sustained arrivals no application request (or Spring {@code afterCommit}
 * callback) is retained beyond its own covering write.
 *
 * <p>Scope is per coalescer instance — one per {@code Scheduler} — not per
 * JVM; an application wiring several schedulers against one store gets one
 * in-flight write per scheduler per task, which only loosens the write-rate
 * bound, never correctness.
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
        if (promoted != null) {
            // Hand the follow-up generation to a dedicated virtual thread:
            // this caller's own future is complete, and holding it hostage
            // to drive other producers' writes would retain it indefinitely
            // under sustained arrivals.
            try {
                Thread.ofVirtual().name("threadmill-nudge-coalescer").start(() -> drive(taskName, slot, write));
            } catch (Throwable cannotStart) {
                // The promoted generation is already installed as inFlight, so
                // if nobody drives it every current and future caller for this
                // task parks forever on an uninterruptible join. Fail the
                // generation and retire the slot instead, so the next nudge
                // starts clean.
                abandon(taskName, slot, promoted, cannotStart);
            }
        }
    }

    private void abandon(String taskName, Slot slot, CompletableFuture<NudgeOutcome> promoted, Throwable cause) {
        CompletableFuture<NudgeOutcome> alsoWaiting;
        synchronized (slot) {
            alsoWaiting = slot.next;
            slot.next = null;
            slot.inFlight = null;
            slot.retired = true;
            slots.remove(taskName, slot);
        }
        promoted.completeExceptionally(cause);
        if (alsoWaiting != null) alsoWaiting.completeExceptionally(cause);
    }
}
