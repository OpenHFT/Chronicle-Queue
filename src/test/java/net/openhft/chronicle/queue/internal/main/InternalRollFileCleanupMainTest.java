/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the retention CLI surfaces the expected {@code warn}-level conditions, asserted through
 * {@link QueueTestCommon#expectException}. The {@code --verbose} debug trace is captured separately
 * in {@code InternalRollFileCleanupMainVerboseTest} (it swaps exception handlers, which must not
 * clash with this base class's tracker).
 */
public class InternalRollFileCleanupMainTest extends QueueTestCommon {

    private static SingleChronicleQueueBuilder builder(File dir, SetTimeProvider time) {
        return SingleChronicleQueueBuilder.single(dir).rollCycle(TestRollCycles.TEST_DAILY).timeProvider(time);
    }

    private static void writeDaily(File dir, SetTimeProvider time, int days) {
        try (ChronicleQueue q = builder(dir, time).build();
             ExcerptAppender appender = q.createAppender()) {
            for (int d = 0; d < days; d++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("n").int32(d);
                }
                time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
        }
    }

    @Test
    public void warnsWhenATailerLags() throws Exception {
        File dir = Files.createTempDirectory("main-lag").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(60_000));
        writeDaily(dir, time, 5);
        try (SingleChronicleQueue q = builder(dir, time).build();
             ExcerptTailer slow = q.createTailer("slow")) {
            assertTrue(slow.moveToIndex(q.rollCycle().toIndex(q.firstCycle(), 0)));
        }

        expectException("pin rolls below cycle");
        boolean warned = InternalRollFileCleanupMain.sweep(dir, 2, 0, false, Collections.emptyList(), false);
        assertTrue("a lagging tailer must be reported as warned", warned);
    }

    @Test
    public void warnsWhenDiskBelowThreshold() throws Exception {
        File dir = Files.createTempDirectory("main-disk").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(70_000));
        writeDaily(dir, time, 3);

        expectException("below threshold");
        boolean warned = InternalRollFileCleanupMain.sweep(
                dir, 2, Long.MAX_VALUE, false, Collections.emptyList(), false);
        assertTrue("low disk must be reported as warned", warned);
    }

    @Test
    public void dryRunParkDoesNotRetireTailer() throws Exception {
        File dir = Files.createTempDirectory("main-dry-park").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(74_000));
        writeDaily(dir, time, 5);

        long targetIndex;
        SingleChronicleQueue q = builder(dir, time).build();
        try {
            try (ExcerptTailer dead = q.createTailer("dead")) {
                targetIndex = q.rollCycle().toIndex(q.firstCycle(), 0);
                assertTrue(dead.moveToIndex(targetIndex));
            }

            expectException("pin rolls below cycle");
            InternalRollFileCleanupMain.sweep(
                    dir, 2, 0, false, Collections.singletonList("dead"), false, queueDir -> q);
        } finally {
            q.close();
        }

        try (SingleChronicleQueue reopened = builder(dir, time).build()) {
            assertEquals("dry-run --park must not change the committed tailer index",
                    targetIndex, reopened.namedTailerIndexes().get("dead").longValue());
        }
    }

    @Test
    public void failOnWarnThrowsExitCodeFromRun() throws Exception {
        File dir = Files.createTempDirectory("main-fail").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(75_000));
        writeDaily(dir, time, 5);
        createLaggingTailer(dir, time, "slow");

        expectException("pin rolls below cycle");
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--keep", "2", "--fail-on-warn"}));
        assertEquals(3, e.exitCode());
    }

    @Test
    public void failOnWarnThrowsBeforeIntervalSleep() throws Exception {
        File dir = Files.createTempDirectory("main-fail-interval").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(76_000));
        writeDaily(dir, time, 5);
        createLaggingTailer(dir, time, "slow");

        expectException("pin rolls below cycle");
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--keep", "2", "--interval", "3600", "--fail-on-warn"}));
        assertEquals(3, e.exitCode());
    }

    private static void createLaggingTailer(File dir, SetTimeProvider time, String name) {
        try (SingleChronicleQueue q = builder(dir, time).build();
             ExcerptTailer slow = q.createTailer(name)) {
            assertTrue(slow.moveToIndex(q.rollCycle().toIndex(q.firstCycle(), 0)));
        }
    }
}
