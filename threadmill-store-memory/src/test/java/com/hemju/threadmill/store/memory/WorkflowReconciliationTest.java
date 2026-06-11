package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.WorkflowInterceptor;
import com.hemju.threadmill.test.Jobs;

/**
 * The maintenance reconciliation recovers workflow children stranded in
 * AWAITING because their predecessor reached a terminal state but the
 * promote/abandon interceptor never ran (a crash in the window between the
 * predecessor's terminal save and the interceptor hook).
 */
class WorkflowReconciliationTest {

    private final InMemoryJobStore store = new InMemoryJobStore();

    private Job driveToTerminal(Job root, JobState terminal) {
        store.insert(root);
        Job claimed =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        long v = claimed.version();
        // Terminal save lands, but the WorkflowInterceptor hook is deliberately
        // NOT fired — this is the crash window.
        claimed.transitionTo(terminal, Instant.now(), "engine.terminal", null);
        claimed.clearOwner();
        store.saveAtomic(claimed, v);
        return claimed;
    }

    @Test
    @DisplayName("a stranded AWAITING child of a SUCCEEDED predecessor is promoted")
    void promotesStrandedChildOfSucceededPredecessor() {
        Job root = Jobs.enqueued("com.example.Root");
        Job child = Jobs.awaitingWorkflowStep("com.example.Child", root);
        store.insert(child);
        driveToTerminal(root, JobState.SUCCEEDED);
        assertThat(store.findById(child.id()).orElseThrow().currentState()).isEqualTo(JobState.AWAITING);

        new WorkflowInterceptor(store).reconcileOrphanedAwaitingChildren(100);

        assertThat(store.findById(child.id()).orElseThrow().currentState()).isEqualTo(JobState.ENQUEUED);
    }

    @Test
    @DisplayName("a stranded AWAITING child of a FAILED predecessor is abandoned")
    void abandonsStrandedChildOfFailedPredecessor() {
        Job root = Jobs.enqueued("com.example.Root");
        Job child = Jobs.awaitingWorkflowStep("com.example.Child", root);
        store.insert(child);
        driveToTerminal(root, JobState.FAILED);

        new WorkflowInterceptor(store).reconcileOrphanedAwaitingChildren(100);

        assertThat(store.findById(child.id()).orElseThrow().currentState()).isEqualTo(JobState.DELETED);
    }

    @Test
    @DisplayName("an AWAITING child of a still-active predecessor is left alone")
    void leavesChildOfActivePredecessorUntouched() {
        Job root = Jobs.enqueued("com.example.Root");
        store.insert(root);
        Job child = Jobs.awaitingWorkflowStep("com.example.Child", root);
        store.insert(child);

        new WorkflowInterceptor(store).reconcileOrphanedAwaitingChildren(100);

        assertThat(store.findById(child.id()).orElseThrow().currentState()).isEqualTo(JobState.AWAITING);
    }
}
