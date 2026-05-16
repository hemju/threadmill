package com.hemju.threadmill.spring;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.core.ResolvableType;

import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;

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
        for (String beanName : context.getBeanNamesForAnnotation(ThreadmillJob.class)) {
            Object bean = context.getBean(beanName);
            Class<?> beanType = AopUtils.getTargetClass(bean);
            ThreadmillJob annotation = beanType.getAnnotation(ThreadmillJob.class);
            if (!(bean instanceof JobHandler<?>)) {
                throw new IllegalStateException(
                        "@ThreadmillJob bean does not implement JobHandler: " + beanType.getName());
            }
            Class<? extends JobPayload> payloadType = payloadType(beanType);
            Names.requireName("queue", annotation.queue());
            var registration = new Registration(
                    payloadType,
                    beanType,
                    annotation.queue(),
                    annotation.priority(),
                    annotation.maxRetries() > 0 ? annotation.maxRetries() : properties.getDefaultMaxAttempts(),
                    annotation.timeout().isBlank() ? properties.getJobTimeout() : Duration.parse(annotation.timeout()));
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
            throw new IllegalStateException("No @ThreadmillJob handler registered for payload "
                    + payload.getClass().getName());
        }
        return registration;
    }

    public List<Registration> registrations() {
        var out = new ArrayList<>(byPayload.values());
        out.sort(Comparator.comparing(r -> r.payloadType().getName()));
        return List.copyOf(out);
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends JobPayload> payloadType(Class<?> handlerType) {
        Class<?> resolved = ResolvableType.forClass(handlerType)
                .as(JobHandler.class)
                .getGeneric(0)
                .resolve();
        if (resolved == null || !JobPayload.class.isAssignableFrom(resolved)) {
            throw new IllegalStateException("Cannot infer JobPayload type for handler " + handlerType.getName());
        }
        return (Class<? extends JobPayload>) resolved;
    }

    public record Registration(
            Class<? extends JobPayload> payloadType,
            Class<?> handlerType,
            String queue,
            int priority,
            int maxRetries,
            Duration timeout) {}
}
