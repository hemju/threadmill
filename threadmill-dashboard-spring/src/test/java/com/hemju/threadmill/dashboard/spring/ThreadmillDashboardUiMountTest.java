package com.hemju.threadmill.dashboard.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.dashboard.api.DashboardOptions;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class ThreadmillDashboardUiMountTest {

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ThreadmillDashboardApiConfiguration.class))
            .withBean(JobStore.class, InMemoryJobStore::new)
            .withPropertyValues("threadmill.dashboard.allow-unsafe-read-only-without-authentication=true");

    @Test
    void apiStartsWithoutUiAssets() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(ThreadmillDashboardApiController.class);
            assertThat(context).doesNotHaveBean("threadmillDashboardUiWebMvcConfigurer");
        });
    }

    @Test
    void customOptionsBeanWithDivergentBasePathFailsFast() {
        // The controller mounts at the property; the security chain at the
        // options bean. A divergent custom bean would scope the chain to a
        // path with no endpoints — refuse to start instead.
        runner.withBean(DashboardOptions.class, () -> new DashboardOptions(true, false, "/ops/api", true))
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("apiBasePath");
                });
    }

    @Test
    void customOptionsBeanMatchingThePropertyStarts() {
        runner.withPropertyValues("threadmill.dashboard.api.base-path=/ops/api")
                .withBean(DashboardOptions.class, () -> new DashboardOptions(true, false, "/ops/api", true))
                .run(context -> assertThat(context).hasSingleBean(ThreadmillDashboardApiController.class));
    }

    @Test
    void uiConfigurerIsRegisteredOnlyWhenUiAssetsArePresent() {
        var resource = getClass().getResource("/ui-fixture/threadmill-index.html");
        runner.withClassLoader(new UiResourceClassLoader(getClass().getClassLoader(), resource))
                .run(context -> assertThat(context).hasBean("threadmillDashboardUiWebMvcConfigurer"));
    }

    private static final class UiResourceClassLoader extends ClassLoader {

        private final URL resource;

        private UiResourceClassLoader(ClassLoader parent, URL resource) {
            super(parent);
            this.resource = resource;
        }

        @Override
        public URL getResource(String name) {
            if ("META-INF/resources/threadmill/index.html".equals(name)) {
                return resource;
            }
            return super.getResource(name);
        }
    }
}
