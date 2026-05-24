package com.hemju.threadmill.spring;

import org.testcontainers.DockerClientFactory;

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
