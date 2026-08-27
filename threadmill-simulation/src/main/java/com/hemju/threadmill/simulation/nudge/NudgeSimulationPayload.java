package com.hemju.threadmill.simulation.nudge;

import com.hemju.threadmill.core.handler.JobPayload;

/** Payload for the process-separated nudge pump simulation. */
public final class NudgeSimulationPayload implements JobPayload {

  public String runId;
  public String traceFile;

  public NudgeSimulationPayload() {}

  public NudgeSimulationPayload(String runId, String traceFile) {
    this.runId = runId;
    this.traceFile = traceFile;
  }
}
