package com.hemju.threadmill.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class JobStateMachineTest {

    @ParameterizedTest
    @CsvSource({
        // legal core flow
        "AWAITING,AWAITING",
        "AWAITING,SCHEDULED",
        "AWAITING,ENQUEUED",
        "AWAITING,DELETED",
        "AWAITING,QUARANTINED",
        "SCHEDULED,AWAITING",
        "SCHEDULED,ENQUEUED",
        "SCHEDULED,SUCCEEDED",
        "SCHEDULED,FAILED",
        "SCHEDULED,DELETED",
        "SCHEDULED,QUARANTINED",
        "ENQUEUED,PROCESSING",
        "ENQUEUED,SCHEDULED",
        "ENQUEUED,AWAITING",
        "ENQUEUED,SUCCEEDED",
        "ENQUEUED,FAILED",
        "ENQUEUED,DELETED",
        "ENQUEUED,QUARANTINED",
        "PROCESSING,SUCCEEDED",
        "PROCESSING,FAILED",
        "PROCESSING,DELETED",
        "PROCESSING,QUARANTINED",
        "FAILED,SCHEDULED",
        "FAILED,ENQUEUED",
        "FAILED,DELETED",
        "FAILED,QUARANTINED",
        "SUCCEEDED,SCHEDULED",
        "SUCCEEDED,ENQUEUED",
        "SUCCEEDED,DELETED",
        "SUCCEEDED,QUARANTINED",
        "DELETED,SCHEDULED",
        "DELETED,ENQUEUED",
        "DELETED,QUARANTINED",
    })
    void everyDocumentedLegalTransitionPasses(JobState from, JobState to) {
        assertThat(JobStateMachine.isLegal(from, to)).as("%s -> %s", from, to).isTrue();
        JobStateMachine.requireLegal(from, to); // does not throw
    }

    @ParameterizedTest
    @CsvSource({
        // critical illegal transitions — orphan recovery never returns to ENQUEUED directly
        "PROCESSING,ENQUEUED",
        // SCHEDULED never goes straight to PROCESSING; it must first become ENQUEUED
        "SCHEDULED,PROCESSING",
        // ENQUEUED never self-loops
        "ENQUEUED,ENQUEUED",
        // QUARANTINED is terminal in v1
        "QUARANTINED,ENQUEUED",
        "QUARANTINED,SCHEDULED",
        // PROCESSED is reserved for the external-jobs feature and has no transitions yet
        "PROCESSED,SUCCEEDED",
        "PROCESSED,FAILED",
    })
    void criticalIllegalTransitionsThrow(JobState from, JobState to) {
        assertThat(JobStateMachine.isLegal(from, to)).as("%s -> %s", from, to).isFalse();
        assertThatThrownBy(() -> JobStateMachine.requireLegal(from, to))
                .isInstanceOf(IllegalJobTransitionException.class);
    }

    @ParameterizedTest
    @EnumSource(JobState.class)
    void illegalTransitionExceptionCarriesEndpoints(JobState from) {
        // Pick a target that is illegal for at least one source state.
        JobState to = JobState.PROCESSED; // PROCESSED is only entered by an explicit reserved path
        if (!JobStateMachine.isLegal(from, to)) {
            try {
                JobStateMachine.requireLegal(from, to);
            } catch (IllegalJobTransitionException e) {
                assertThat(e.from()).isEqualTo(from);
                assertThat(e.to()).isEqualTo(to);
            }
        }
    }

    @Test
    void terminalStatesHaveExpectedSet() {
        assertThat(JobStateMachine.legalSuccessorsOf(JobState.QUARANTINED)).isEmpty();
        assertThat(JobStateMachine.legalSuccessorsOf(JobState.PROCESSED)).isEmpty();
    }

    @Test
    void successorSetsAreReadOnly() {
        Set<JobState> succ = JobStateMachine.legalSuccessorsOf(JobState.PROCESSING);
        assertThatThrownBy(() -> succ.add(JobState.ENQUEUED)).isInstanceOf(UnsupportedOperationException.class);
    }
}
