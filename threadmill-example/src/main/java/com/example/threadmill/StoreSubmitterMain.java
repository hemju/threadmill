package com.example.threadmill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;

/**
 * Enqueues {@code N} demo jobs (default 50) against a shared Postgres or Redis
 * backend and exits. Workers started via {@link StoreWorkerMain} will then
 * race to claim and process them.
 *
 * <pre>
 *   ./gradlew :threadmill-example:submitPostgres --args="50"
 *   ./gradlew :threadmill-example:submitRedis --args="50"
 * </pre>
 */
public final class StoreSubmitterMain {

    private static final Logger LOG = LoggerFactory.getLogger(StoreSubmitterMain.class);

    private StoreSubmitterMain() {}

    static void main(String[] args) {
        ExampleStores.Backend backend = ExampleStores.Backend.defaultBackend();
        int count = 50;
        if (args.length > 0) {
            if (isBackend(args[0])) {
                backend = ExampleStores.Backend.parse(args[0]);
                if (args.length > 1) count = Integer.parseInt(args[1]);
            } else {
                count = Integer.parseInt(args[0]);
            }
        }

        try (ExampleStores.StoreHandle handle = ExampleStores.open(backend)) {
            var scheduler = new Scheduler(handle.store(), new JsonJobSerializer());
            for (int i = 0; i < count; i++) {
                scheduler.enqueue(new GreetingPayload("world", i), GreetingHandler.class);
            }
        }
        LOG.info("enqueued {} jobs into {}.", count, backend);
    }

    private static boolean isBackend(String value) {
        return value.equalsIgnoreCase("postgres")
                || value.equalsIgnoreCase("postgresql")
                || value.equalsIgnoreCase("pg")
                || value.equalsIgnoreCase("redis");
    }
}
