package com.hemju.threadmill.core.handler;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.hemju.threadmill.core.serialization.TypeNameAliases;

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
    private final TypeNameAliases aliases;

    public ReflectiveJobHandlerResolver() {
        this(TypeNameAliases.empty());
    }

    public ReflectiveJobHandlerResolver(TypeNameAliases aliases) {
        this.aliases = Objects.requireNonNull(aliases, "aliases");
    }

    @Override
    public JobHandler<?> resolve(String handlerTypeName) throws HandlerResolutionException {
        Objects.requireNonNull(handlerTypeName, "handlerTypeName");
        String resolvedName = aliases.resolve(handlerTypeName);
        JobHandler<?> cached = cache.get(resolvedName);
        if (cached != null) return cached;
        try {
            // initialize=false: never run the named class's static initializer
            // before the JobHandler assignability check. handlerTypeName is
            // producer-controlled persisted data; loading it with the
            // initializing Class.forName(String) would let a job producer trigger
            // an arbitrary classpath class's <clinit> side effects on a worker.
            Class<?> klass = Class.forName(resolvedName, false, getClass().getClassLoader());
            if (!JobHandler.class.isAssignableFrom(klass)) {
                throw new HandlerResolutionException("Type " + resolvedName + " does not implement JobHandler");
            }
            Constructor<?> ctor = klass.getDeclaredConstructor();
            ctor.setAccessible(true);
            JobHandler<?> instance = (JobHandler<?>) ctor.newInstance();
            cache.putIfAbsent(resolvedName, instance);
            return instance;
        } catch (HandlerResolutionException e) {
            throw e;
        } catch (Exception e) {
            throw new HandlerResolutionException("Cannot resolve handler " + handlerTypeName, e);
        }
    }
}
