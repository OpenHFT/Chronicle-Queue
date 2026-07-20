/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.internal.util.InternalRollFileCleanup;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * Regression coverage for roll-file cleanup helper behaviour.
 */
public class RollFileCleanupRegressionTest extends QueueTestCommon {

    private static void write(ExcerptAppender appender, int value) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().write("n").int32(value);
        }
    }

    @Test
    public void fileForCycleReleasesStoreItAcquires() throws Exception {
        File dir = Files.createTempDirectory("cleanup-regression-listener").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(100));
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger released = new AtomicInteger();
        StoreFileListener listener = new StoreFileListener() {
            @Override
            public void onAcquired(int cycle, File file) {
                acquired.incrementAndGet();
            }

            @Override
            public void onReleased(int cycle, File file) {
                released.incrementAndGet();
            }
        };
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.single(dir)
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(time).storeFileListener(listener).build();
             ExcerptAppender appender = q.createAppender()) {
            for (int day = 0; day < 3; day++) {
                write(appender, day);
                time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
            BackgroundResourceReleaser.releasePendingResources();
            acquired.set(0);
            released.set(0);

            q.fileForCycle(q.firstCycle());
            BackgroundResourceReleaser.releasePendingResources();

            assertEquals("fileForCycle acquired a store", 1, acquired.get());
            assertEquals("fileForCycle must release every store it acquires", acquired.get(), released.get());
        }
    }

    @Test
    public void keepLastCyclesUsesRollCycleNumbersAcrossGaps() throws Exception {
        File dir = Files.createTempDirectory("cleanup-regression-gap").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(300));
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.single(dir)
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(time).build();
             ExcerptAppender appender = q.createAppender()) {
            write(appender, 0);
            time.advanceMillis(TimeUnit.DAYS.toMillis(5));
            write(appender, 1);

            int filesOnDisk = dir.listFiles((d, name) -> name.endsWith(".cq4")).length;
            assertEquals("two present roll files across the gap", 2, filesOnDisk);

            InternalRollFileCleanup.Analysis analysis = InternalRollFileCleanup.analyse(q, 2);
            assertEquals("keepFloor is last-1 by cycle number", q.lastCycle() - 1, analysis.keepFloor());
            assertEquals("one older file is outside the cycle window", 1, analysis.removable().size());
        }
    }
}
