package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.support.GenericApplicationContext;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobInterceptors;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.ReflectiveJobHandlerResolver;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/**
 * Regression for resolver class loading under layered classloaders (Spring
 * Boot devtools restart loader, war deployments): handler classes live in a
 * child loader the library's own defining classloader cannot see. A plain
 * {@code Class.forName} there throws {@code ClassNotFoundException} and every
 * job is quarantined; the resolver must go through the application context's
 * classloader instead.
 */
class SpringJobHandlerResolverClassLoaderTest {

  private static final String HANDLER_NAME = "dyn.ChildLoaderHandler";
  private static final String HANDLER_SOURCE = """
            package dyn;

            import com.hemju.threadmill.core.handler.JobHandler;
            import com.hemju.threadmill.core.handler.JobPayload;
            import com.hemju.threadmill.core.handler.JobExecutionContext;

            public class ChildLoaderHandler implements JobHandler<ChildLoaderHandler.Payload> {
                public record Payload(String value) implements JobPayload {}
                @Override
                public void run(Payload payload, JobExecutionContext ctx) {
                    if (!"child".equals(payload.value())) throw new AssertionError("wrong payload");
                }
            }
            """;

  @TempDir
  Path tempDir;

  @Test
  void executesHandlersAndPayloadsVisibleOnlyThroughTheApplicationClassLoader() throws Exception {
    Path sourceFile = tempDir.resolve("dyn").resolve("ChildLoaderHandler.java");
    Files.createDirectories(sourceFile.getParent());
    Files.writeString(sourceFile, HANDLER_SOURCE);
    var compiler = ToolProvider.getSystemJavaCompiler();
    int result = compiler.run(
        null,
        null,
        null,
        "-classpath",
        System.getProperty("java.class.path"),
        "-d",
        tempDir.toString(),
        sourceFile.toString());
    assertThat(result).isZero();

    try (var childLoader = new URLClassLoader(
        new URL[] {tempDir.toUri().toURL()},
        SpringJobHandlerResolverClassLoaderTest.class.getClassLoader())) {
      // Sanity: the handler is invisible to the library's own loader.
      assertThat(canLoadWithLibraryLoader()).isFalse();

      try (var context = new GenericApplicationContext()) {
        context.setClassLoader(childLoader);
        context.refresh();
        var resolver = new SpringJobHandlerResolver(context);

        JobHandler<?> handler = resolver.resolve(HANDLER_NAME);

        assertThat(handler.getClass().getName()).isEqualTo(HANDLER_NAME);
        assertThat(handler.getClass().getClassLoader()).isSameAs(childLoader);
        var original = Thread.currentThread().getContextClassLoader();
        ReflectiveJobHandlerResolver captured;
        try {
          Thread.currentThread().setContextClassLoader(childLoader);
          captured = new ReflectiveJobHandlerResolver();
        } finally {
          Thread.currentThread().setContextClassLoader(original);
        }
        for (var applicationResolver :
            List.of(resolver, new ReflectiveJobHandlerResolver(childLoader), captured)) {
          assertThat(applicationResolver.classLoader()).isSameAs(childLoader);
          var store = new InMemoryJobStore();
          var job = Job.builder()
              .spec(JobSpec.of(
                  HANDLER_NAME,
                  new JobArgument(HANDLER_NAME + "$Payload", "{\"value\":\"child\"}")))
              .build();
          store.insert(job);
          var owner = NodeId.newId();
          var claimed = store.claimReady(owner, "default", 1, Instant.now()).getFirst();
          var runner = new JobRunner(
              store,
              owner,
              applicationResolver,
              new JsonJobSerializer(),
              new JobInterceptors(),
              ProcessingNodeConfig.defaults());
          try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            workers.submit(() -> runner.run(claimed)).get(10, TimeUnit.SECONDS);
          } finally {
            runner.shutdown();
          }
          assertThat(store.findById(job.id()).orElseThrow().currentState())
              .isEqualTo(JobState.SUCCEEDED);
        }
      }
    }
  }

  private static boolean canLoadWithLibraryLoader() {
    try {
      Class.forName(HANDLER_NAME, false, SpringJobHandlerResolver.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException expected) {
      return false;
    }
  }
}
