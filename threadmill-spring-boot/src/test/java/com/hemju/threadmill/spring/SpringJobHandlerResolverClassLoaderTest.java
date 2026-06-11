package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.support.GenericApplicationContext;

import com.hemju.threadmill.core.handler.JobHandler;

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

            public class ChildLoaderHandler
                    implements com.hemju.threadmill.core.handler.JobHandler<com.hemju.threadmill.core.handler.NoPayload> {
                @Override
                public void run(
                        com.hemju.threadmill.core.handler.NoPayload payload,
                        com.hemju.threadmill.core.handler.JobExecutionContext ctx) {}
            }
            """;

    @TempDir
    Path tempDir;

    @Test
    void resolvesHandlerClassesThroughTheContextClassLoader() throws Exception {
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
                new URL[] {tempDir.toUri().toURL()}, SpringJobHandlerResolverClassLoaderTest.class.getClassLoader())) {
            // Sanity: the handler is invisible to the library's own loader.
            assertThat(canLoadWithLibraryLoader()).isFalse();

            try (var context = new GenericApplicationContext()) {
                context.setClassLoader(childLoader);
                context.refresh();
                var resolver = new SpringJobHandlerResolver(context);

                JobHandler<?> handler = resolver.resolve(HANDLER_NAME);

                assertThat(handler.getClass().getName()).isEqualTo(HANDLER_NAME);
                assertThat(handler.getClass().getClassLoader()).isSameAs(childLoader);
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
