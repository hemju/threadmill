package com.hemju.threadmill.dashboard.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class DashboardApiServiceTest {

    @Test
    void limitedSearchCapabilitiesFailWithDashboardException() {
        var store = new InMemoryJobStore(
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
        var service = new DashboardApiService(store, new LocalWakeBus());

        assertThatThrownBy(() -> service.jobs(JobSearch.all()))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
        assertThatThrownBy(() -> service.jobs(new JobSearch(JobState.ENQUEUED, "default", null, 50, 0)))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
    }

    @Test
    void validationFailuresStayFrameworkNeutral() {
        var service = new DashboardApiService(new InMemoryJobStore(), new LocalWakeBus());

        assertThatThrownBy(() -> service.pauseQueue("default", "x".repeat(257)))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
        assertThatThrownBy(() -> service.scheduleRetry(JobId.newId(), 1, Duration.ofSeconds(-1)))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
    }
}
