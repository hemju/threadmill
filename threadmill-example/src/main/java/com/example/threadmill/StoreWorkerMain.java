package com.example.threadmill;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Long-running {@link ProcessingNode} against a shared Postgres or Redis
 * backend. Launch the same command in two or three terminals to see a small
 * Threadmill cluster claim jobs concurrently from one store.
 *
 * <pre>
 *   ./gradlew :threadmill-example:runPostgresWorker --args="alpha"
 *   ./gradlew :threadmill-example:runRedisWorker --args="bravo"
 * </pre>
 *
 * <p>The optional first argument is purely a friendly label for the logs;
 * the engine generates its own UUIDv7 node id.
 */
public final class StoreWorkerMain {

    private static final Logger LOG = LoggerFactory.getLogger(StoreWorkerMain.class);

    private StoreWorkerMain() {}

    static void main(String[] args) throws Exception {
        ExampleStores.Backend backend = ExampleStores.Backend.defaultBackend();
        String label = "node-" + ProcessHandle.current().pid();
        if (args.length > 0) {
            if (isBackend(args[0])) {
                backend = ExampleStores.Backend.parse(args[0]);
                if (args.length > 1) label = args[1];
            } else {
                label = args[0];
            }
        }
        ExampleStores.Backend selectedBackend = backend;
        String selectedLabel = label;

        ProcessingNodeConfig config = ProcessingNodeConfig.builder()
                .workerCount(4)
                .pollInterval(Duration.ofMillis(200))
                .claimHeartbeat(Duration.ofSeconds(2))
                .heartbeatTimeout(Duration.ofSeconds(15))
                .jobTimeout(Duration.ofSeconds(30))
                .defaultMaxAttempts(3)
                .retryInitialBackoff(Duration.ofSeconds(2))
                .storeOutagePollInterval(Duration.ofSeconds(2))
                .claimBatchSize(4)
                .build();

        ExampleStores.StoreHandle handle = ExampleStores.open(selectedBackend);
        JobStore store = handle.store();
        ProcessingNode node = ProcessingNode.builder(store)
                .config(config)
                .lane(new QueueLane("default", 4))
                .build();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            LOG.info("[{}:{}] shutting down (node={})", selectedBackend, selectedLabel, shortId(node));
                            node.close();
                            handle.close();
                        },
                        "threadmill-example-shutdown"));

        LOG.info("[{}:{}] starting worker (node={})", selectedBackend, selectedLabel, shortId(node));
        node.start();

        // Park forever; Ctrl-C triggers the shutdown hook above.
        Thread.currentThread().join();
    }

    private static String shortId(ProcessingNode node) {
        return node.nodeId().asUuid().toString().substring(0, 8);
    }

    private static boolean isBackend(String value) {
        return value.equalsIgnoreCase("postgres")
                || value.equalsIgnoreCase("postgresql")
                || value.equalsIgnoreCase("pg")
                || value.equalsIgnoreCase("redis");
    }
}
