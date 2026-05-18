package com.hemju.threadmill.spring;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.handler.NoPayload;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;

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
    private final JobSerializer serializer;
    private final String namespace;

    public ThreadmillRecurringRegistrar(Scheduler scheduler, ThreadmillJobRegistry registry) {
        this(scheduler, registry, new JsonJobSerializer(), null);
    }

    public ThreadmillRecurringRegistrar(
            Scheduler scheduler, ThreadmillJobRegistry registry, JobSerializer serializer, String namespace) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.serializer = serializer;
        this.namespace = namespace == null || namespace.isBlank() ? null : namespace;
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
        var desired = new ArrayList<CronTask>();
        for (var registration : recurring()) {
            CronTask task = taskFor(registration);
            desired.add(task);
            if (namespace != null) {
                continue;
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
            logRegistered(task, null);
        }
        if (namespace != null) {
            scheduler.reconcileRecurring(namespace, desired);
            for (CronTask task : desired) {
                logRegistered(task, namespace);
            }
        } else if (!desired.isEmpty()) {
            LOG.warn("Threadmill: recurring cleanup is disabled because no recurring namespace is configured");
        }
    }

    private CronTask taskFor(ThreadmillJobRegistry.Registration registration) {
        if (registration.payloadType() != NoPayload.class) {
            throw new IllegalStateException("Annotation-driven recurring requires JobHandler<NoPayload>; handler "
                    + registration.handlerType().getName()
                    + " declares payload "
                    + registration.payloadType().getName()
                    + ". Use Scheduler.defineRecurring(...) for handlers with a non-trivial payload.");
        }
        if (serializer == null) {
            throw new IllegalStateException("Annotation-driven recurring reconciliation requires a JobSerializer");
        }
        var recurring = registration.recurring();
        return new CronTask(
                recurring.name(),
                recurring.trigger(),
                registration.handlerType().getName(),
                serializer.serializePayload(NoPayload.INSTANCE),
                registration.queue(),
                registration.priority(),
                recurring.missedRunPolicy(),
                ZoneId.systemDefault(),
                true);
    }

    private void logRegistered(CronTask task, String namespace) {
        LOG.info(
                "Threadmill: registered recurring task '{}' on queue '{}' for handler {} (trigger={}, namespace={})",
                task.name(),
                task.queue(),
                task.handlerType(),
                task.trigger(),
                namespace == null ? "<none>" : namespace);
    }
}
