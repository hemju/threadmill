package com.hemju.threadmill.dashboard.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.server.ResponseStatusException;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.ScheduleRetryRequest;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.VersionedActionRequest;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class ThreadmillDashboardApiControllerTest {

    private InMemoryJobStore store;
    private List<DashboardAuditEvent> auditEvents;
    private ThreadmillDashboardApiController secureController;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        auditEvents = new ArrayList<>();
        var service = new DashboardApiService(store, new LocalWakeBus());
        secureController = new ThreadmillDashboardApiController(
                service, new SpringSecurityDashboardAuthorizer(), auditEvents::add, DashboardOptions.secureDefaults());
    }

    @Test
    void unauthenticatedRequestsAreRejected() {
        assertThatThrownBy(() -> secureController.overview(null))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.UNAUTHORIZED));
    }

    @Test
    void missingPermissionReturnsForbidden() {
        var auth = auth("alice", "THREADMILL_READ");

        assertThatThrownBy(() -> secureController.pauseQueue(auth, "default", null))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.FORBIDDEN));
    }

    @Test
    void redactionIsTheDefaultForJobDetails() {
        var job = insertSensitiveJob();

        var detail =
                secureController.job(auth("alice", "THREADMILL_READ"), job.id().toString());

        assertThat(detail.sensitiveDetailsRedacted()).isTrue();
        assertThat(detail.arguments()).isEmpty();
        assertThat(detail.metadata()).isEmpty();
        assertThat(detail.log()).isEmpty();
        assertThat(detail.stateHistory())
                .allSatisfy(entry -> assertThat(entry.message()).isNull());
    }

    @Test
    void sensitiveDetailsRequirePermissionAndExplicitConfiguration() {
        var job = insertSensitiveJob();
        var controller = new ThreadmillDashboardApiController(
                new DashboardApiService(store, new LocalWakeBus()),
                new SpringSecurityDashboardAuthorizer(),
                auditEvents::add,
                new DashboardOptions(false, true));

        var detail = controller.job(
                auth("alice", "THREADMILL_READ", "THREADMILL_VIEW_SENSITIVE_DETAILS"),
                job.id().toString());

        assertThat(detail.sensitiveDetailsRedacted()).isFalse();
        assertThat(detail.arguments()).extracting(JobArgument::serialized).containsExactly("{\"secret\":\"value\"}");
        assertThat(detail.metadata()).containsEntry("token", "secret-token");
        assertThat(detail.log()).extracting(entry -> entry.message()).containsExactly("hidden log");
        assertThat(detail.stateHistory()).extracting(entry -> entry.message()).contains("hidden failure");
    }

    @Test
    void mutationActionsAuditSuccessAndFailure() {
        var failed = Job.builder()
                .spec(JobSpec.of("com.example.Handler"))
                .initialState(JobState.FAILED)
                .build();
        store.insert(failed);
        var auth = auth("operator", "THREADMILL_REQUEUE_JOB");

        var response =
                secureController.requeue(auth, failed.id().toString(), new VersionedActionRequest(failed.version()));
        assertThat(response.status()).isEqualTo("requeued");
        assertThat(auditEvents).extracting(DashboardAuditEvent::outcome).containsExactly("requeued");

        var processing = insertProcessingJob();
        assertThatThrownBy(() -> secureController.requeue(
                        auth, processing.id().toString(), new VersionedActionRequest(processing.version())))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.CONFLICT));
        assertThat(auditEvents).extracting(DashboardAuditEvent::outcome).containsExactly("requeued", "failed");
    }

    @Test
    void retrySchedulingRequiresFailedStateAndVersionMatch() {
        var failed = Job.builder()
                .spec(JobSpec.of("com.example.Handler"))
                .initialState(JobState.FAILED)
                .build();
        store.insert(failed);
        var auth = auth("operator", "THREADMILL_REQUEUE_JOB");

        secureController.scheduleRetry(
                auth, failed.id().toString(), new ScheduleRetryRequest(failed.version(), Duration.ofMinutes(5)));

        var loaded = store.findById(failed.id()).orElseThrow();
        assertThat(loaded.currentState()).isEqualTo(JobState.SCHEDULED);
        assertThat(loaded.scheduledFor())
                .hasValueSatisfying(at -> assertThat(at).isAfter(Instant.now()));
    }

    @Test
    void nonRichStoreSearchRequiresStateOnly() {
        var limitedStore = new InMemoryJobStore(
                new JsonJobSerializer(),
                new JobStoreCapabilities(
                        JobStoreCapabilities.DEFAULT_MAX_SERIALIZED_BYTES,
                        JobStoreCapabilities.DEFAULT_MAX_JOB_LOG_BYTES,
                        JobStoreCapabilities.DEFAULT_MAX_FAILURE_METADATA_BYTES,
                        1000,
                        false,
                        true,
                        true,
                        true));
        var service = new DashboardApiService(limitedStore, new LocalWakeBus());

        assertThatThrownBy(() -> service.jobs(JobSearch.all()))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.BAD_REQUEST));
        assertThatThrownBy(() -> service.jobs(new JobSearch(JobState.ENQUEUED, "default", null, 50, 0)))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void replaceJobReportsMissingStaleAndWrongStateDistinctly() {
        var auth = auth("operator", "THREADMILL_REPLACE_JOB");
        var processing = insertProcessingJob();
        var pending = Job.builder().spec(JobSpec.of("com.example.Handler")).build();
        store.insert(pending);

        assertThatThrownBy(() -> secureController.replaceJob(
                        auth,
                        JobId.newId().toString(),
                        new DashboardPayloads.ReplaceJobRequest(1, null, null, null, "com.example.Other", List.of())))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.NOT_FOUND));
        assertThatThrownBy(() -> secureController.replaceJob(
                        auth,
                        pending.id().toString(),
                        new DashboardPayloads.ReplaceJobRequest(
                                pending.version() + 1, null, null, null, "com.example.Other", List.of())))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.CONFLICT));
        assertThatThrownBy(() -> secureController.replaceJob(
                        auth,
                        processing.id().toString(),
                        new DashboardPayloads.ReplaceJobRequest(
                                processing.version(), null, null, null, "com.example.Other", List.of())))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.CONFLICT));
    }

    @Test
    void pauseQueueRejectsOversizedReasons() {
        var auth = auth("operator", "THREADMILL_PAUSE_QUEUE");
        var reason = "x".repeat(257);

        assertThatThrownBy(() ->
                        secureController.pauseQueue(auth, "default", new DashboardPayloads.PauseQueueRequest(reason)))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(error -> assertThat(((ResponseStatusException) error).getStatusCode())
                        .isEqualTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void unsafeLocalModeAllowsReadOnlyWithoutAuthentication() {
        var controller = new ThreadmillDashboardApiController(
                new DashboardApiService(store, new LocalWakeBus()),
                new SpringSecurityDashboardAuthorizer(),
                DashboardAuditSink.noop(),
                new DashboardOptions(true, false));

        assertThat(controller.overview(null).countsByState()).containsKey(JobState.ENQUEUED);
    }

    private Job insertSensitiveJob() {
        var job = Job.builder()
                .spec(JobSpec.of("com.example.Handler", new JobArgument("example.Payload", "{\"secret\":\"value\"}")))
                .metadata("token", "secret-token")
                .build();
        job.transitionTo(JobState.PROCESSING, Instant.now(), "test.claim", null);
        job.transitionTo(JobState.FAILED, Instant.now(), "test.failure", "hidden failure");
        job.log().info("hidden log");
        store.insert(job);
        return job;
    }

    private Job insertProcessingJob() {
        var job = Job.builder().spec(JobSpec.of("com.example.Handler")).build();
        store.insert(job);
        JobId id = job.id();
        return store.claimReady(NodeId.newId(), "default", 10, Instant.now()).stream()
                .filter(claimed -> claimed.id().equals(id))
                .findFirst()
                .orElseThrow();
    }

    private static Authentication auth(String name, String... authorities) {
        var granted =
                List.of(authorities).stream().map(SimpleGrantedAuthority::new).toList();
        var authentication = new TestingAuthenticationToken(name, "n/a", granted);
        authentication.setAuthenticated(true);
        return authentication;
    }
}
