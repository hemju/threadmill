package com.hemju.threadmill.soak.harness;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point invoked by the {@code soakMemory}, {@code soakPostgres}, and
 * {@code soakRedis} Gradle tasks.
 *
 * <p>Reads {@code --backend X} from the command line; everything else
 * (scenario, duration, rates, output dir) is read from
 * {@code threadmill.soak.*} system properties so the JUnit smoke tests can
 * use exactly the same code path.
 *
 * <p>Exit codes: 0 on {@code passed}, 1 on {@code failed}, 2 on bad input,
 * 3 on harness-internal error.
 */
public final class SoakHarnessMain {

    private static final Logger LOG = LoggerFactory.getLogger(SoakHarnessMain.class);

    private SoakHarnessMain() {}

    static void main(String[] args) {
        String backend = "memory";
        for (int i = 0; i < args.length - 1; i++) {
            if ("--backend".equals(args[i])) {
                backend = args[i + 1].toLowerCase(Locale.ROOT);
            }
        }
        SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties(backend);
        String taskLabel = "soak" + Character.toUpperCase(backend.charAt(0)) + backend.substring(1);

        OutputDir outputDir = new OutputDir(config.outputDir(), config.force());
        try (BackendFixture fixture = openFixture(config)) {
            SoakHarnessRunner runner = new SoakHarnessRunner(config, fixture, outputDir, taskLabel);
            SummaryReport report = runner.run();
            LOG.info("Soak run finished. Output: {} verdict={}", outputDir.root(), report.verdict());
            System.exit("passed".equals(report.verdict()) ? 0 : 1);
        } catch (IllegalArgumentException e) {
            System.err.println("soak harness: " + e.getMessage());
            System.exit(2);
        } catch (Exception e) {
            LOG.error("soak harness failed", e);
            System.exit(3);
        }
    }

    private static BackendFixture openFixture(SoakHarnessConfig config) {
        return switch (config.backend()) {
            case "memory" -> new MemoryHarnessFixture();
            case "postgres" -> new PostgresHarnessFixture(config.postgresUrl());
            case "redis" -> new RedisHarnessFixture(config.redisTopology(), config.redisUrl());
            default -> throw new IllegalArgumentException("unknown backend: " + config.backend());
        };
    }
}
