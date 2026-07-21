/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Checks the sweep's human-readable output - the per-queue summary, {@code RETENTION_OK},
 * {@code RETENTION_PARKED}/{@code RETENTION_WOULD_PARK} and the {@code --verbose} trace - which are
 * all emitted at {@code Jvm.debug()} level (not {@code System.out}), so they can be captured and
 * asserted. Kept separate from {@code InternalRollFileCleanupMainTest} because it installs its own
 * recording exception handler (debug is ignored by the standard test tracker) and resets it after.
 */
public class InternalRollFileCleanupMainOutputTest {

    /** Runs {@code action} while capturing debug-level log messages, then restores the handlers. */
    private static List<String> captureDebug(Runnable action) {
        final Map<ExceptionKey, Integer> captured = Jvm.recordExceptions(true);
        try {
            action.run();
            return captured.keySet().stream()
                    .map(k -> k.message).filter(Objects::nonNull).collect(Collectors.toList());
        } finally {
            Jvm.resetExceptionHandlers();
        }
    }

    /** A queue directory with {@code days} daily rolls and optionally one registered named tailer. */
    private static File queueWith(String prefix, int days, String namedTailer) throws Exception {
        File dir = Files.createTempDirectory(prefix).toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(80_000));
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(dir)
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(time).build();
             ExcerptAppender appender = q.createAppender()) {
            for (int d = 0; d < days; d++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("n").int32(d);
                }
                time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
            if (namedTailer != null)
                q.createTailer(namedTailer).close();
        }
        return dir;
    }

    @Test
    public void perQueueSummaryAndRetentionOkAtDebugLevel() throws Exception {
        File dir = queueWith("out-summary", 3, null);
        List<String> log = captureDebug(() ->
                InternalRollFileCleanupMain.sweep(dir, 2, 0, false, Collections.emptyList(), false));
        assertTrue("per-queue summary logged",
                log.stream().anyMatch(m -> m.contains("cycles [") && m.contains("retainable=")));
        assertTrue("RETENTION_OK logged when nothing is wrong",
                log.stream().anyMatch(m -> m.contains("RETENTION_OK")));
    }

    @Test
    public void retentionParkedAtDebugLevel() throws Exception {
        File dir = queueWith("out-parked", 3, "dead");
        List<String> log = captureDebug(() ->
                InternalRollFileCleanupMain.sweep(dir, 2, 0, true, Collections.singletonList("dead"), false));
        assertTrue("RETENTION_PARKED logged when a named tailer is parked",
                log.stream().anyMatch(m -> m.contains("RETENTION_PARKED") && m.contains("dead")));
    }

    @Test
    public void retentionWouldParkInDryRunAtDebugLevel() throws Exception {
        File dir = queueWith("out-would-park", 3, "dead");
        List<String> log = captureDebug(() ->
                InternalRollFileCleanupMain.sweep(dir, 2, 0, false, Collections.singletonList("dead"), false));
        assertTrue("RETENTION_WOULD_PARK logged in dry-run",
                log.stream().anyMatch(m -> m.contains("RETENTION_WOULD_PARK") && m.contains("dead")));
    }

    @Test
    public void verboseTraceIsEmittedAtDebugLevel() throws Exception {
        File dir = queueWith("out-verbose", 3, null);
        List<String> log = captureDebug(() ->
                InternalRollFileCleanupMain.sweep(dir, 2, 0, false, Collections.emptyList(), true));
        // trace-only detail (the summary carries keepFloor/deleteBelow but not oldestTailer)
        assertTrue("verbose per-cycle trace logged",
                log.stream().anyMatch(m -> m.contains("oldestTailer=")));
    }
}
