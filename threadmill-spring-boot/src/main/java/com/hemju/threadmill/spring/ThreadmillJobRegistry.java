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
import com.hemju.threadmill.core.handler.NoPayload;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;

/**
 * Registry of annotation-discovered Spring job handlers.
 *
 * <p>Registrations are keyed by handler class — which is always unique within
 * a Spring context — so multiple handlers may legitimately share the same
 * payload type. This is what makes annotation-driven recurring tasks scale
 * beyond a single {@link com.hemju.threadmill.core.handler.JobAction} per
 * application: every {@code JobAction} declares {@code NoPayload} as its
 * payload, and keying by handler class is what lets two of them coexist.
 *
 * <p>The secondary payload-type index is still maintained so that callers
 * holding only a payload (no handler class) — typically tests or
 * generic-resolution code paths — can route to a registration. That lookup
 * remains unambiguous as long as no payload type is shared by more than one
 * handler. {@code NoPayload} is the only payload type expected to be shared
 * in normal use, and {@link #registrationFor(JobPayload)} fails with a clear
 * message in that case directing callers to the handler-class variant.
 */
public class ThreadmillJobRegistry {

    private final Map<Class<?>, Registration> byHandler;
    private final Map<Class<? extends JobPayload>, List<Registration>> byPayload;

    /** Test seam: build a registry from a fixed registration list. */
    protected ThreadmillJobRegistry(Registration... registrations) {
        Map<Class<?>, Registration> handlerIndex = new LinkedHashMap<>();
        Map<Class<? extends JobPayload>, List<Registration>> payloadIndex = new LinkedHashMap<>();
        for (Registration r : registrations) {
            recordRegistration(handlerIndex, payloadIndex, r);
        }
        this.byHandler = Map.copyOf(handlerIndex);
        this.byPayload = copyOfPayloadIndex(payloadIndex);
    }

    public ThreadmillJobRegistry(ApplicationContext context, ThreadmillProperties properties) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(properties, "properties");
        Map<Class<?>, Registration> handlerIndex = new LinkedHashMap<>();
        Map<Class<? extends JobPayload>, List<Registration>> payloadIndex = new LinkedHashMap<>();
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
            recordRegistration(handlerIndex, payloadIndex, registration);
        }
        this.byHandler = Map.copyOf(handlerIndex);
        this.byPayload = copyOfPayloadIndex(payloadIndex);
    }

    /**
     * Look up the registration for {@code handlerType}. The handler-class
     * index is the primary route used by {@link JobScheduler#enqueue} and
     * friends, which always have the handler class in hand.
     */
    public Registration registrationFor(Class<?> handlerType) {
        Objects.requireNonNull(handlerType, "handlerType");
        Registration registration = byHandler.get(handlerType);
        if (registration == null) {
            throw new IllegalStateException("No @Job handler registered for " + handlerType.getName());
        }
        return registration;
    }

    /**
     * Look up the registration solely by payload type. Convenient when only
     * the payload is in scope; rejects with a clear message if more than one
     * handler shares the payload (today only happens for
     * {@link com.hemju.threadmill.core.handler.JobAction}/{@code NoPayload}).
     */
    public Registration registrationFor(JobPayload payload) {
        Objects.requireNonNull(payload, "payload");
        List<Registration> matches = byPayload.get(payload.getClass());
        if (matches == null || matches.isEmpty()) {
            throw new IllegalStateException("No @Job handler registered for payload "
                    + payload.getClass().getName());
        }
        if (matches.size() > 1) {
            var names = new ArrayList<String>(matches.size());
            for (Registration r : matches) names.add(r.handlerType().getName());
            throw new IllegalStateException("Multiple @Job handlers registered for payload "
                    + payload.getClass().getName()
                    + " ("
                    + String.join(", ", names)
                    + "); resolve by handler class via registrationFor(Class).");
        }
        return matches.get(0);
    }

    public List<Registration> registrations() {
        var out = new ArrayList<>(byHandler.values());
        out.sort(Comparator.comparing(r -> r.handlerType().getName()));
        return List.copyOf(out);
    }

    private static void recordRegistration(
            Map<Class<?>, Registration> handlerIndex,
            Map<Class<? extends JobPayload>, List<Registration>> payloadIndex,
            Registration r) {
        Registration prior = handlerIndex.putIfAbsent(r.handlerType(), r);
        if (prior != null) {
            // Spring bean discovery should never see the same handler class twice,
            // but the defensive check keeps the test-seam constructor honest too.
            throw new IllegalStateException(
                    "Duplicate @Job registration for handler " + r.handlerType().getName());
        }
        var existing = payloadIndex.computeIfAbsent(r.payloadType(), k -> new ArrayList<>());
        if (!existing.isEmpty() && r.payloadType() != NoPayload.class) {
            // Two handlers claiming the same non-NoPayload payload type is almost always
            // a bug — pick-the-right-one is impossible from the payload alone. NoPayload
            // is the deliberate exception: every JobAction shares it by construction,
            // and JobScheduler routes by handler class so the ambiguity does not bite.
            throw new IllegalStateException("Multiple Threadmill handlers for payload "
                    + r.payloadType().getName()
                    + ": "
                    + existing.get(0).handlerType().getName()
                    + " and "
                    + r.handlerType().getName());
        }
        existing.add(r);
    }

    private static Map<Class<? extends JobPayload>, List<Registration>> copyOfPayloadIndex(
            Map<Class<? extends JobPayload>, List<Registration>> src) {
        var out = new LinkedHashMap<Class<? extends JobPayload>, List<Registration>>(src.size());
        for (var e : src.entrySet()) out.put(e.getKey(), List.copyOf(e.getValue()));
        return Map.copyOf(out);
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
