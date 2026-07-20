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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Checks that {@code --verbose} emits its decision trace at debug level. Kept separate from
 * {@code InternalTailerRetentionMainTest} because it installs its own recording exception handler
 * (to capture debug, which the standard test tracker ignores) and resets the handlers afterwards.
 */
public class InternalTailerRetentionMainVerboseTest {

    @Test
    public void verboseTraceIsEmittedAtDebugLevel() throws Exception {
        File dir = Files.createTempDirectory("main-verbose").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(80_000));
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(dir)
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(time).build();
             ExcerptAppender appender = q.createAppender()) {
            for (int d = 0; d < 3; d++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("n").int32(d);
                }
                time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
        }

        // Debug is ignored by the standard test tracker, so capture it explicitly.
        final Map<ExceptionKey, Integer> captured = Jvm.recordExceptions(true);
        try {
            InternalTailerRetentionMain.sweep(dir, 2, 0, false, Collections.emptyList(), true);
            assertTrue("verbose trace should be logged at debug level",
                    captured.keySet().stream()
                            .anyMatch(k -> k.message != null && k.message.contains("cycles [")));
        } finally {
            Jvm.resetExceptionHandlers();
        }
    }
}
