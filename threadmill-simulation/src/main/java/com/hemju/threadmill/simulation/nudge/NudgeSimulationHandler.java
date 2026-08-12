package com.hemju.threadmill.simulation.nudge;

import java.nio.file.Path;
import java.util.Map;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;

/** Recurring outbox-pump handler used by the process-separated nudge simulation. */
public final class NudgeSimulationHandler implements JobHandler<NudgeSimulationPayload> {

    @Override
    public void run(NudgeSimulationPayload payload, JobExecutionContext context) {
        var trace = Path.of(payload.traceFile);
        var pid = ProcessHandle.current().pid();
        var jobId = context.jobId().toString();
        var origin = context.cronOrigin().orElse("unknown");
        NudgeSimulationTrace.append(
                trace,
                "pump-run-start",
                Map.of(
                        "runId", payload.runId,
                        "pid", pid,
                        "nodeId", context.nodeId().toString(),
                        "jobId", jobId,
                        "origin", origin));

        try (var workStore = NudgeSimulationStores.openProcessWorkStore(payload.runId)) {
            for (var sequence : workStore.drain()) {
                NudgeSimulationTrace.append(
                        trace,
                        "work-drained",
                        Map.of(
                                "runId", payload.runId,
                                "sequence", sequence,
                                "pid", pid,
                                "nodeId", context.nodeId().toString(),
                                "jobId", jobId,
                                "origin", origin));
            }
        }

        NudgeSimulationTrace.append(
                trace,
                "pump-run-finish",
                Map.of(
                        "runId", payload.runId,
                        "pid", pid,
                        "nodeId", context.nodeId().toString(),
                        "jobId", jobId,
                        "origin", origin));
    }
}
