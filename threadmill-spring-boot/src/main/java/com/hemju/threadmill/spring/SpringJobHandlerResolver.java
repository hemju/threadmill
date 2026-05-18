package com.hemju.threadmill.spring;

import java.util.Objects;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobHandlerResolver;
import com.hemju.threadmill.core.serialization.TypeNameAliases;

/**
 * {@link JobHandlerResolver} backed by the Spring {@link ApplicationContext}.
 *
 * <p>The resolver first tries a bean lookup by type — common for handler beans
 * declared with {@code @Component}. If the type is not a Spring bean, it falls
 * back to constructing the handler with the container's
 * {@code AutowireCapableBeanFactory}, which lets a handler with constructor
 * parameters be created and have its dependencies injected without a
 * surrounding bean definition.
 *
 * <p>If the handler type cannot be located, instantiated, or assigned to
 * {@link JobHandler}, a {@link HandlerResolutionException} is thrown. The
 * engine routes such failures into {@code QUARANTINED}.
 */
public final class SpringJobHandlerResolver implements JobHandlerResolver {

    private final ApplicationContext context;
    private final TypeNameAliases aliases;

    public SpringJobHandlerResolver(ApplicationContext context) {
        this(context, TypeNameAliases.empty());
    }

    public SpringJobHandlerResolver(ApplicationContext context, TypeNameAliases aliases) {
        this.context = Objects.requireNonNull(context, "context");
        this.aliases = Objects.requireNonNull(aliases, "aliases");
    }

    @Override
    public JobHandler<?> resolve(String handlerTypeName) throws HandlerResolutionException {
        Objects.requireNonNull(handlerTypeName, "handlerTypeName");
        String resolvedName = aliases.resolve(handlerTypeName);
        try {
            Class<?> type = Class.forName(resolvedName);
            if (!JobHandler.class.isAssignableFrom(type)) {
                throw new HandlerResolutionException("Type " + resolvedName + " does not implement JobHandler");
            }
            try {
                Object bean = context.getBean(type);
                return (JobHandler<?>) bean;
            } catch (BeansException ignored) {
                // Not a registered bean — fall back to autowire-by-type construction.
                Object instance = context.getAutowireCapableBeanFactory().createBean(type);
                return (JobHandler<?>) instance;
            }
        } catch (ClassNotFoundException cnf) {
            throw new HandlerResolutionException("Handler type not found: " + handlerTypeName, cnf);
        } catch (Exception e) {
            throw new HandlerResolutionException("Cannot resolve handler " + handlerTypeName, e);
        }
    }
}
