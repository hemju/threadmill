package com.hemju.threadmill.store.postgres;

import org.testcontainers.DockerClientFactory;

/**
 * JUnit {@code @EnabledIf} predicate: skip tests cleanly when no container
 * runtime is reachable, so a host without Docker can still build the project.
 *
 * <p>CI must always have a runtime — the skip is to keep the build usable on
 * a developer machine where Docker is temporarily down, not to make the
 * real-store integration tests optional.
 */
public final class DockerAvailable {

    private DockerAvailable() {}

    public static boolean check() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }
}
