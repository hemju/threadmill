package com.hemju.threadmill.spring;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when {@code threadmill.store.redis.*} <em>is</em> configured (uri,
 * sentinel master, or cluster nodes set). The inverse of
 * {@link OnRedisStoreNotConfigured}; used by {@link ThreadmillRedisAutoConfiguration}
 * so the Redis store bean is only contributed when Redis is actually pointed at
 * an instance, keeping the same precedence rule as the rest of the chain.
 */
final class OnRedisStoreConfigured extends SpringBootCondition {

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        var binder = Binder.get(context.getEnvironment());
        var redis = binder.bind("threadmill.store.redis", Bindable.of(ThreadmillProperties.RedisProperties.class))
                .orElseGet(ThreadmillProperties.RedisProperties::new);
        if (redis.isConfigured()) {
            return ConditionOutcome.match("threadmill.store.redis is configured");
        }
        return ConditionOutcome.noMatch("threadmill.store.redis is not configured");
    }
}
