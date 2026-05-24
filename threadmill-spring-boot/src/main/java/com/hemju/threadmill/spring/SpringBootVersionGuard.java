package com.hemju.threadmill.spring;

import org.springframework.boot.SpringBootVersion;

/**
 * Fails fast when the host application brings in a Spring Boot version that
 * Threadmill does not support.
 *
 * <p>The {@code threadmill-spring-boot} module targets <strong>Spring Boot 4
 * only</strong>. Earlier majors ship an ASM version that cannot parse Java 25
 * class files (major version 69) and crash during {@code @ComponentScan}
 * before any of our own beans run, producing inscrutable
 * {@code IllegalArgumentException} stack traces. Catching the mismatch here
 * surfaces it with an actionable message — "you need Spring Boot 4" — instead.
 */
final class SpringBootVersionGuard {

    /** Spring Boot major required by this module. */
    static final int REQUIRED_MAJOR = 4;

    private SpringBootVersionGuard() {}

    /** Reads the running Spring Boot version and validates it against {@link #REQUIRED_MAJOR}. */
    static void requireSpringBootFour() {
        requireSpringBootFour(SpringBootVersion.getVersion());
    }

    /**
     * Package-private overload that takes the version string directly so tests
     * can exercise the predicate without needing to mock the static getter.
     *
     * @param version the value of {@code SpringBootVersion.getVersion()},
     *                e.g. {@code "4.0.4"}. {@code null} or unparseable values
     *                are treated as unsatisfied — Threadmill will not run on
     *                an environment whose Spring Boot version cannot even be
     *                identified.
     */
    static void requireSpringBootFour(String version) {
        Integer major = parseMajor(version);
        if (major == null) {
            throw new IllegalStateException("Threadmill requires Spring Boot " + REQUIRED_MAJOR
                    + " or newer, but the running Spring Boot version could not be identified"
                    + " (SpringBootVersion.getVersion() returned " + version + "). Upgrade to Spring Boot "
                    + REQUIRED_MAJOR + ".x or remove the Threadmill auto-configuration.");
        }
        if (major < REQUIRED_MAJOR) {
            throw new IllegalStateException("Threadmill requires Spring Boot " + REQUIRED_MAJOR
                    + " or newer, but found Spring Boot " + version + ". Earlier majors are not supported"
                    + " — their ASM cannot parse Java 25 class files. Upgrade to Spring Boot "
                    + REQUIRED_MAJOR + ".x.");
        }
    }

    private static Integer parseMajor(String version) {
        if (version == null || version.isBlank()) return null;
        int dot = version.indexOf('.');
        String head = dot < 0 ? version : version.substring(0, dot);
        try {
            return Integer.parseInt(head.trim());
        } catch (NumberFormatException nfe) {
            return null;
        }
    }
}
