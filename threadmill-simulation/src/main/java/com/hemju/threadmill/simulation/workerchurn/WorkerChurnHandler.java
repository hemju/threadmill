package com.hemju.threadmill.simulation.workerchurn;

import java.nio.file.Path;
import java.util.Map;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;

/** Handler used by the worker-churn simulation; it records every attempt to a JSON-lines trace file. */
public final class WorkerChurnHandler implements JobHandler<WorkerChurnPayload> {

    @Override
    public void run(WorkerChurnPayload payload, JobExecutionContext context) throws InterruptedException {
        var trace = Path.of(payload.traceFile);
        WorkerChurnTraceLog.append(
                trace,
                "job-start",
                Map.of(
                        "runId",
                        payload.runId,
                        "sequence",
                        payload.sequence,
                        "jobId",
                        context.jobId(),
                        "nodeId",
                        context.nodeId(),
                        "attempt",
                        context.attempt(),
                        "workMillis",
                        payload.workMillis));
        try {
            Thread.sleep(payload.workMillis);
            WorkerChurnTraceLog.append(
                    trace,
                    "job-finish",
                    Map.of(
                            "runId", payload.runId,
                            "sequence", payload.sequence,
                            "jobId", context.jobId(),
                            "nodeId", context.nodeId(),
                            "attempt", context.attempt()));
        } catch (InterruptedException e) {
            WorkerChurnTraceLog.append(
                    trace,
                    "job-interrupted",
                    Map.of(
                            "runId", payload.runId,
                            "sequence", payload.sequence,
                            "jobId", context.jobId(),
                            "nodeId", context.nodeId(),
                            "attempt", context.attempt()));
            throw e;
        }
    }
}
