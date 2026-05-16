package com.hemju.threadmill.spring;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.context.ApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;

import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;

/** Registry of annotation-discovered Spring job handlers. */
public class ThreadmillJobRegistry {

    private final Map<Class<? extends JobPayload>, Registration> byPayload;

    /** Test seam: build a registry from a fixed registration list. */
    protected ThreadmillJobRegistry(Registration... registrations) {
        Map<Class<? extends JobPayload>, Registration> map = new LinkedHashMap<>();
        for (Registration r : registrations) map.put(r.payloadType(), r);
        this.byPayload = Map.copyOf(map);
    }

    public ThreadmillJobRegistry(ApplicationContext context, ThreadmillProperties properties) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(properties, "properties");
        Map<Class<? extends JobPayload>, Registration> found = new LinkedHashMap<>();
        for (String beanName : context.getBeanNamesForAnnotation(Job.class)) {
            // Inspect the bean definition WITHOUT triggering instantiation. Calling
            // context.getBean(beanName) here would eagerly create every @Job
            // handler, which deadlocks any handler that constructor-injects
            // JobScheduler (which itself depends on this registry).
            Class<?> rawBeanType = context.getType(beanName);
            if (rawBeanType == null) continue;
            Class<?> beanType = ClassUtils.getUserClass(rawBeanType);
            Job annotation = context.findAnnotationOnBean(beanName, Job.class);
            if (annotation == null) continue;
            if (!JobHandler.class.isAssignableFrom(beanType)) {
                throw new IllegalStateException("@Job bean does not implement JobHandler: " + beanType.getName());
            }
            Class<? extends JobPayload> payloadType = payloadType(beanType);
            Names.requireName("queue", annotation.queue());
            Recurring recurring = context.findAnnotationOnBean(beanName, Recurring.class);
            var registration = new Registration(
                    payloadType,
                    beanType,
                    annotation.queue(),
                    annotation.priority(),
                    annotation.maxRetries() > 0 ? annotation.maxRetries() : properties.getDefaultMaxAttempts(),
                    annotation.timeout().isBlank() ? properties.getJobTimeout() : Duration.parse(annotation.timeout()),
                    recurringSpecFor(beanType, recurring));
            Registration prior = found.putIfAbsent(payloadType, registration);
            if (prior != null) {
                throw new IllegalStateException("Multiple Threadmill handlers for payload "
                        + payloadType.getName()
                        + ": "
                        + prior.handlerType().getName()
                        + " and "
                        + registration.handlerType().getName());
            }
        }
        this.byPayload = Map.copyOf(found);
    }

    public Registration registrationFor(JobPayload payload) {
        Objects.requireNonNull(payload, "payload");
        Registration registration = byPayload.get(payload.getClass());
        if (registration == null) {
            throw new IllegalStateException("No @Job handler registered for payload "
                    + payload.getClass().getName());
        }
        return registration;
    }

    public List<Registration> registrations() {
        var out = new ArrayList<>(byPayload.values());
        out.sort(Comparator.comparing(r -> r.payloadType().getName()));
        return List.copyOf(out);
    }

    private static RecurringSpec recurringSpecFor(Class<?> beanType, Recurring annotation) {
        if (annotation == null) return null;
        boolean hasInterval = !annotation.interval().isBlank();
        boolean hasCron = !annotation.cron().isBlank();
        if (!hasInterval && !hasCron) {
            throw new IllegalStateException(
                    "@Recurring on " + beanType.getName() + " must set either interval or cron");
        }
        if (hasInterval && hasCron) {
            throw new IllegalStateException(
                    "@Recurring on " + beanType.getName() + " sets both interval and cron; pick one");
        }
        String name = annotation.recurringName().isBlank() ? beanType.getName() : annotation.recurringName();
        if (hasInterval) {
            Duration interval;
            try {
                interval = Duration.parse(annotation.interval());
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                        "@Recurring on "
                                + beanType.getName()
                                + " has invalid interval '"
                                + annotation.interval()
                                + "' (expected ISO-8601 duration like PT10S)",
                        e);
            }
            if (interval.isZero() || interval.isNegative()) {
                throw new IllegalStateException("@Recurring on "
                        + beanType.getName()
                        + " has non-positive interval '"
                        + annotation.interval()
                        + "'");
            }
            return new RecurringSpec(name, new CronTask.Trigger.Interval(interval), annotation.missedRunPolicy());
        }
        CronExpression expression;
        try {
            expression = CronExpression.parse(annotation.cron());
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "@Recurring on " + beanType.getName() + " has invalid cron '" + annotation.cron() + "'", e);
        }
        return new RecurringSpec(name, new CronTask.Trigger.CronExpr(expression), annotation.missedRunPolicy());
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends JobPayload> payloadType(Class<?> handlerType) {
        // Reject raw `implements JobHandler` up front: ResolvableType would resolve its
        // missing type parameter to the upper bound (JobPayload), giving a misleading
        // "use JobPayload" payload type. The right answer for a no-payload handler is
        // to implement JobAction (a typed JobHandler<NoPayload>), so point users there.
        if (implementsJobHandlerRaw(handlerType)) {
            throw new IllegalStateException("Handler "
                    + handlerType.getName()
                    + " implements raw JobHandler. Implement JobAction for no-payload handlers, "
                    + "or JobHandler<P> for handlers that need a payload.");
        }
        Class<?> resolved = ResolvableType.forClass(handlerType)
                .as(JobHandler.class)
                .getGeneric(0)
                .resolve();
        if (resolved == null || !JobPayload.class.isAssignableFrom(resolved)) {
            throw new IllegalStateException("Cannot infer JobPayload type for handler " + handlerType.getName());
        }
        return (Class<? extends JobPayload>) resolved;
    }

    /**
     * Returns {@code true} if the handler implements {@link JobHandler} raw — i.e. has
     * a non-parameterised {@code implements JobHandler} somewhere in its type hierarchy.
     * Walks the direct interfaces of the class and each superclass; for the library's
     * intended usage one-level depth is enough, but the walk is cheap and future-proof.
     */
    private static boolean implementsJobHandlerRaw(Class<?> handlerType) {
        Class<?> cursor = handlerType;
        while (cursor != null && cursor != Object.class) {
            for (Type iface : cursor.getGenericInterfaces()) {
                if (iface == JobHandler.class) return true;
                if (iface instanceof ParameterizedType pt && pt.getRawType() == JobHandler.class) {
                    return false;
                }
            }
            cursor = cursor.getSuperclass();
        }
        return false;
    }

    public record Registration(
            Class<? extends JobPayload> payloadType,
            Class<?> handlerType,
            String queue,
            int priority,
            int maxRetries,
            Duration timeout,
            RecurringSpec recurring) {

        /** Whether this handler is registered as a recurring task. */
        public boolean isRecurring() {
            return recurring != null;
        }
    }

    /** Parsed recurring spec for handlers annotated with {@code @Recurring}. */
    public record RecurringSpec(String name, CronTask.Trigger trigger, CronTask.MissedRunPolicy missedRunPolicy) {}
}
