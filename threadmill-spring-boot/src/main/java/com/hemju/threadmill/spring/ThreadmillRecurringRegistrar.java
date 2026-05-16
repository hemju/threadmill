package com.hemju.threadmill.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.handler.NoPayload;
import com.hemju.threadmill.core.schedule.Scheduler;

/**
 * Registers handlers annotated with {@code @Job} and {@code @Recurring}
 * as recurring tasks at startup. Eliminates the per-handler "registrar" boilerplate
 * for the common case of a fixed, parameter-less schedule.
 *
 * <p>Annotation-driven recurring is restricted to {@code JobHandler<NoPayload>}.
 * Handlers with a richer payload need a runtime value per materialization, which
 * an annotation cannot carry; those should use {@link Scheduler#defineRecurring}
 * directly or call {@link JobScheduler#enqueueRecurring} with the desired payload.
 *
 * <p>The registrar is idempotent. {@link Scheduler#defineRecurring} upserts the
 * task definition and preserves existing schedule state across restarts (last
 * run, in-flight job) while recomputing the next-run from the current trigger.
 */
public class ThreadmillRecurringRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillRecurringRegistrar.class);

    private final Scheduler scheduler;
    private final ThreadmillJobRegistry registry;

    public ThreadmillRecurringRegistrar(Scheduler scheduler, ThreadmillJobRegistry registry) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    /** Returns the recurring registrations this registrar would publish. */
    public List<ThreadmillJobRegistry.Registration> recurring() {
        var out = new ArrayList<ThreadmillJobRegistry.Registration>();
        for (var r : registry.registrations()) {
            if (r.isRecurring()) out.add(r);
        }
        return List.copyOf(out);
    }

    /**
     * Idempotently register every annotation-driven recurring task. Called once
     * during context refresh by the auto-configuration; safe to call again.
     */
    public void registerAll() {
        for (var registration : recurring()) {
            if (registration.payloadType() != NoPayload.class) {
                throw new IllegalStateException("Annotation-driven recurring requires JobHandler<NoPayload>; handler "
                        + registration.handlerType().getName()
                        + " declares payload "
                        + registration.payloadType().getName()
                        + ". Use Scheduler.defineRecurring(...) for handlers with a non-trivial payload.");
            }
            var recurring = registration.recurring();
            scheduler.defineRecurring(
                    recurring.name(),
                    recurring.trigger(),
                    NoPayload.INSTANCE,
                    registration.handlerType().getName(),
                    registration.queue(),
                    registration.priority(),
                    recurring.missedRunPolicy());
            LOG.info(
                    "Threadmill: registered recurring task '{}' on queue '{}' for handler {} (trigger={})",
                    recurring.name(),
                    registration.queue(),
                    registration.handlerType().getName(),
                    recurring.trigger());
        }
    }
}
