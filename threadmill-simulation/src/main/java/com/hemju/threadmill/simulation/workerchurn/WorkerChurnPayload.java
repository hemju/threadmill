package com.hemju.threadmill.simulation.workerchurn;

import com.hemju.threadmill.core.handler.JobPayload;

/** Payload used by the worker-churn simulation. */
public final class WorkerChurnPayload implements JobPayload {

    public String runId;
    public int sequence;
    public long workMillis;
    public String traceFile;

    public WorkerChurnPayload() {}

    public WorkerChurnPayload(String runId, int sequence, long workMillis, String traceFile) {
        this.runId = runId;
        this.sequence = sequence;
        this.workMillis = workMillis;
        this.traceFile = traceFile;
    }
}
