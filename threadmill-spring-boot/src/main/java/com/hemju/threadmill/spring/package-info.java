/**
 * Spring Boot integration for Threadmill.
 *
 * <p>An auto-configuration wires {@code JobStore} (durable by default, with an
 * explicitly enabled in-memory option for development and tests), the
 * {@code JobSerializer}, the {@code Scheduler}, a Spring-{@code ApplicationContext}-backed
 * {@code JobHandlerResolver}, and the {@code ProcessingNode}. Lifecycle is
 * tied to the Spring context.
 */
package com.hemju.threadmill.spring;
