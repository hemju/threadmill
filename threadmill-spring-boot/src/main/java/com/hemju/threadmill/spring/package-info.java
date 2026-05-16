/**
 * Spring Boot integration for Threadmill.
 *
 * <p>An auto-configuration wires {@code JobStore} (defaulting to the
 * in-memory store unless the application provides its own), the
 * {@code JobSerializer}, the {@code Scheduler}, a Spring-{@code ApplicationContext}-backed
 * {@code JobHandlerResolver}, and the {@code ProcessingNode}. Lifecycle is
 * tied to the Spring context.
 */
package com.hemju.threadmill.spring;
