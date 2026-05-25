package com.hemju.threadmill.spring;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when {@code threadmill.store.redis.*} is <em>not</em> configured.
 *
 * <p>Used by the Postgres auto-configuration to keep Redis precedence:
 * Postgres wins only when Redis is not pointed at an actual instance. The
 * binding reuses the same {@link ThreadmillProperties.RedisProperties#isConfigured()}
 * rule (uri, sentinel master, or cluster nodes set) as the precedence chain
 * in the core auto-configuration, so behaviour stays in lockstep.
 */
final class OnRedisStoreNotConfigured extends SpringBootCondition {

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        var binder = Binder.get(context.getEnvironment());
        var redis = binder.bind("threadmill.store.redis", Bindable.of(ThreadmillProperties.RedisProperties.class))
                .orElseGet(ThreadmillProperties.RedisProperties::new);
        if (redis.isConfigured()) {
            return ConditionOutcome.noMatch("threadmill.store.redis is configured; Redis takes precedence");
        }
        return ConditionOutcome.match("threadmill.store.redis is not configured");
    }
}
