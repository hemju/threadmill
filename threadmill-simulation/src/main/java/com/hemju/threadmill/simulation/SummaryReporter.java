package com.hemju.threadmill.simulation;

import java.util.List;

import com.hemju.threadmill.simulation.SimulationMain.RunOutcome;

/** Human-readable summary printed at the end of every simulation invocation. */
final class SummaryReporter {

    private SummaryReporter() {}

    static void print(List<RunOutcome> outcomes) {
        System.out.println();
        System.out.println("================ Threadmill simulation summary ================");
        for (RunOutcome o : outcomes) {
            var r = o.result();
            long total = r.succeeded() + r.failed() + r.timedOut() + r.quarantined();
            double throughput = total / Math.max(1.0, r.duration().toMillis() / 1000.0);
            System.out.printf(
                    "%n  Backend: %-9s  duration=%5dms  enqueued=%d%n",
                    o.backend(), r.duration().toMillis(), r.enqueued());
            System.out.printf(
                    "    succeeded=%d  failed=%d  timed_out=%d  quarantined=%d  stillActive=%d%n",
                    r.succeeded(), r.failed(), r.timedOut(), r.quarantined(), r.stillActive());
            System.out.printf("    throughput=%.1f jobs/sec  drained=%s%n", throughput, r.drained());
            System.out.printf(
                    "    trace=%s  lines=%d  violations=%d%n",
                    o.tracePath(),
                    o.verifier().lineCount(),
                    o.verifier().violations().size());
            if (!r.drained()) {
                System.out.printf("    finalCounts=%s%n", r.finalCounts());
                if (r.activeJobs().isEmpty()) {
                    System.out.println(
                            "    activeJobs=[] (counts reported active jobs, but handler search found none)");
                } else {
                    System.out.println("    activeJobs:");
                    for (var job : r.activeJobs()) {
                        System.out.printf(
                                "      - id=%s state=%s queue=%s attempts=%d version=%d key=%s mode=%s owner=%s"
                                        + " workflowRoot=%s handler=%s%n",
                                job.jobId(),
                                job.state(),
                                job.queue(),
                                job.attempts(),
                                job.version(),
                                job.concurrencyKey(),
                                job.concurrencyMode(),
                                job.ownerNodeId(),
                                job.workflowRootId(),
                                job.handlerType());
                    }
                }
            }
            if (!o.verifier().isClean()) {
                for (String v : o.verifier().violations()) {
                    System.out.println("      ! " + v);
                }
            }
        }
        System.out.println();
        System.out.println("===============================================================");
    }
}
