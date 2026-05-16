package com.hemju.threadmill.soak.harness.scenario;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Registry of the eight scenarios shipped with the harness. New scenarios
 * are added by dropping a class into this package and registering it here.
 */
public final class Scenarios {

    private static final Map<String, java.util.function.Supplier<SoakScenario>> REGISTRY = new LinkedHashMap<>();

    static {
        REGISTRY.put("mixed-workload", MixedWorkloadScenario::new);
        REGISTRY.put("rw-lock-stress", RwLockStressScenario::new);
        REGISTRY.put("weighted-queues", WeightedQueuesScenario::new);
        REGISTRY.put("retry-storm", RetryStormScenario::new);
        REGISTRY.put("long-running", LongRunningScenario::new);
        REGISTRY.put("pause-resume", PauseResumeScenario::new);
        REGISTRY.put("bulk-enqueue", BulkEnqueueScenario::new);
        REGISTRY.put("crash-recover", CrashRecoverScenario::new);
    }

    private Scenarios() {}

    public static SoakScenario of(String name) {
        var supplier = REGISTRY.get(name);
        if (supplier == null) {
            throw new IllegalArgumentException("unknown scenario: " + name + " — valid names: " + REGISTRY.keySet());
        }
        return supplier.get();
    }

    public static java.util.Set<String> names() {
        return REGISTRY.keySet();
    }
}
