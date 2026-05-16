/**
 * Fixed soak regression checks and load soak harnesses for Threadmill.
 *
 * <p>Lives in its own module so the long-running throughput runs don't gate
 * normal CI. Invoke fixed checks with {@code ./gradlew :threadmill-soak:soakRegression}.
 */
package com.hemju.threadmill.soak;
