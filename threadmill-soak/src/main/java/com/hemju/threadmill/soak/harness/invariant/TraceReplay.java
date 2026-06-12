package com.hemju.threadmill.soak.harness.invariant;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Offline verification: streams an on-disk {@code trace.jsonl} through fresh
 * checks, line by line, without ever materialising the corpus in memory.
 *
 * <p>The live harness feeds the same checks as events are written; this path
 * exists for tests and for re-verifying a finished run's artifact after the
 * fact. A check that throws is reported as a failed result naming the
 * exception rather than aborting the replay — one broken checker must not
 * mask the others' findings.
 */
public final class TraceReplay {

    private TraceReplay() {}

    /** Verify {@code traceFile} against fresh checks for every invariant; returns final results in order. */
    public static List<InvariantResult> verify(Path traceFile, List<SoakInvariant> invariants) throws IOException {
        Objects.requireNonNull(traceFile, "traceFile");
        List<StreamingInvariantCheck> checks =
                invariants.stream().map(SoakInvariant::newCheck).toList();
        List<RuntimeException> failures = feed(traceFile, checks);
        List<InvariantResult> results = new ArrayList<>(checks.size());
        for (int i = 0; i < checks.size(); i++) {
            StreamingInvariantCheck check = checks.get(i);
            RuntimeException failure = failures.get(i);
            if (failure != null) {
                results.add(InvariantResult.fail(
                        check.name(),
                        List.of("invariant raised " + failure.getClass().getSimpleName() + ": " + failure.getMessage()),
                        List.of()));
            } else {
                results.add(check.finish());
            }
        }
        return results;
    }

    /**
     * Stream every line of {@code traceFile} into {@code checks}. Returns a
     * parallel list holding the first exception each check raised (or null);
     * a check that throws receives no further events.
     */
    public static List<RuntimeException> feed(Path traceFile, List<StreamingInvariantCheck> checks) throws IOException {
        var mapper = new ObjectMapper();
        var failures = new ArrayList<RuntimeException>(checks.size());
        for (int i = 0; i < checks.size(); i++) failures.add(null);
        try (var lines = Files.lines(traceFile)) {
            lines.forEach(line -> {
                if (line.isBlank()) return;
                TraceEvent event;
                try {
                    event = new TraceEvent(mapper.readTree(line), line);
                } catch (IOException e) {
                    throw new UncheckedIOException("malformed trace line: " + line, e);
                }
                for (int i = 0; i < checks.size(); i++) {
                    if (failures.get(i) != null) continue;
                    try {
                        checks.get(i).onEvent(event);
                    } catch (RuntimeException e) {
                        failures.set(i, e);
                    }
                }
            });
        }
        return failures;
    }
}
