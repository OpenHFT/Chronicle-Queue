/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Proves the listener's reap-on-release logic: when a store is released it deletes the rolls that
 * have fallen below the keep-last-N ∧ commitment floor, and keeps those a named tailer still needs.
 * The release call itself is Chronicle-Queue's contract, so it is invoked directly here.
 */
public class TailerPositionRetentionFileListenerTest extends QueueTestCommon {

    private static SingleChronicleQueueBuilder builder(File dir, SetTimeProvider time) {
        return SingleChronicleQueueBuilder.single(dir).rollCycle(TestRollCycles.TEST_DAILY).timeProvider(time);
    }

    private static void writeDailyExcerpts(File dir, SetTimeProvider time, int days) {
        try (ChronicleQueue queue = builder(dir, time).build();
             ExcerptAppender appender = queue.createAppender()) {
            for (int d = 0; d < days; d++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("n").int32(d);
                }
                time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
        }
    }

    private static int rollCount(File dir) {
        File[] files = dir.listFiles((d, name) -> name.endsWith(".cq4"));
        return files == null ? 0 : files.length;
    }

    @Test
    public void reapsBelowKeepWindowOnRelease() throws Exception {
        File dir = Files.createTempDirectory("listener-retain").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(40_000));
        writeDailyExcerpts(dir, time, 6); // 6 daily rolls, no listener

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            TailerPositionRetentionFileListener retention = new TailerPositionRetentionFileListener(2);
            retention.queue(q);

            assertEquals(6, rollCount(dir));
            retention.onReleased(q.lastCycle(), q.fileForCycle(q.lastCycle()));

            // keepLastCycles=2, no tailers -> keep the last 2 cycles, reap the older 4.
            assertEquals(2, rollCount(dir));
        }
    }

    @Test
    public void keepsRollsANamedTailerStillNeeds() throws Exception {
        File dir = Files.createTempDirectory("listener-retain-tailer").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(50_000));
        writeDailyExcerpts(dir, time, 6);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            // A stopped named tailer pinned at cycle f+1.
            final long at = q.rollCycle().toIndex(q.firstCycle() + 1, 0);
            try (ExcerptTailer consumer = q.createTailer("gateway")) {
                assertTrue(consumer.moveToIndex(at));
            }

            TailerPositionRetentionFileListener retention = new TailerPositionRetentionFileListener(2);
            retention.queue(q);
            retention.onReleased(q.lastCycle(), q.fileForCycle(q.lastCycle()));

            // Only cycle f (strictly below the tailer's f+1) is reaped; f+1..f+5 are protected.
            assertEquals(5, rollCount(dir));
        }
    }
}
