package com.hemju.threadmill.core.handler;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple {@link JobHandlerResolver} that constructs handlers from a
 * no-arg constructor via reflection.
 *
 * <p>Adequate for tests and small applications. Production deployments
 * normally wire a DI-backed resolver from the {@code threadmill-spring-boot}
 * module instead, which can reuse the host
 * application's bean container.
 */
public final class ReflectiveJobHandlerResolver implements JobHandlerResolver {

    private final Map<String, JobHandler<?>> cache = new ConcurrentHashMap<>();

    @Override
    public JobHandler<?> resolve(String handlerTypeName) throws HandlerResolutionException {
        Objects.requireNonNull(handlerTypeName, "handlerTypeName");
        JobHandler<?> cached = cache.get(handlerTypeName);
        if (cached != null) return cached;
        try {
            Class<?> klass = Class.forName(handlerTypeName);
            if (!JobHandler.class.isAssignableFrom(klass)) {
                throw new HandlerResolutionException("Type " + handlerTypeName + " does not implement JobHandler");
            }
            Constructor<?> ctor = klass.getDeclaredConstructor();
            ctor.setAccessible(true);
            JobHandler<?> instance = (JobHandler<?>) ctor.newInstance();
            cache.putIfAbsent(handlerTypeName, instance);
            return instance;
        } catch (HandlerResolutionException e) {
            throw e;
        } catch (Exception e) {
            throw new HandlerResolutionException("Cannot resolve handler " + handlerTypeName, e);
        }
    }
}
