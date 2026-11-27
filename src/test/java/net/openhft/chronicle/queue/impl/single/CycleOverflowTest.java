/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CycleOverflowTest extends QueueTestCommon {

    @Test
    public void extendedYearFilesRetainLogicalBoundsAfterRefreshAndReopen() {
        for (RollCycle rollCycle : new RollCycle[]{TestRollCycles.TEST_DAILY, TestRollCycles.TEST_HOURLY}) {
            File path = getTmpDir();
            AtomicLong time = new AtomicLong();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).testBlockSize()
                    .rollCycle(rollCycle).timeProvider(time::get).build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("low");
                time.set((long) Integer.MAX_VALUE * rollCycle.lengthInMillis());
                appender.writeText("high");
                queue.refreshDirectoryListing();
                assertEquals(0, queue.firstCycle());
                assertEquals(Integer.MAX_VALUE, queue.lastCycle());
            }
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).testBlockSize()
                    .rollCycle(rollCycle).timeProvider(time::get).build();
                 ExcerptTailer tailer = queue.createTailer()) {
                assertEquals(0, queue.firstCycle());
                assertEquals(Integer.MAX_VALUE, queue.lastCycle());
                assertEquals("low", tailer.readText());
                assertTrue(tailer.moveToIndex(rollCycle.toIndex(Integer.MAX_VALUE, 0)));
                assertEquals("high", tailer.readText());
            }
        }
    }

    @Test
    public void overflowingMaxMessagesInCycleShouldThrowException() {
        File path = getTmpDir();
        RollCycle rollCycle = TestRollCycles.TEST_DAILY;
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.set(System.currentTimeMillis());
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().timeProvider(timeProvider).rollCycle(rollCycle).path(path).build(); ExcerptAppender appender = queue.createAppender();) {
            for (int i = 0; i < rollCycle.maxMessagesPerCycle(); i++) {
                appender.writeText(Integer.toString(i));
            }

            assertThrows("Writing beyond the cycle's maximum message count must be rejected",
                    IllegalStateException.class,
                    () -> appender.writeText(Long.toString(rollCycle.maxMessagesPerCycle())));
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

    @Test
    public void maximumUInt31CycleIsNotTreatedAsEmpty() {
        final File path = getTmpDir();
        final RollCycle rollCycle = TestRollCycles.TEST_SECONDLY;
        final SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis((long) Integer.MAX_VALUE * rollCycle.lengthInMillis());
        long firstIndex;
        try {
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                    .timeProvider(timeProvider)
                    .rollCycle(rollCycle)
                    .build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("maximum cycle");
                firstIndex = appender.lastIndexAppended();
            }

            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                    .timeProvider(timeProvider)
                    .rollCycle(rollCycle)
                    .build()) {
                assertEquals(Integer.MAX_VALUE, queue.firstCycle());
                assertEquals(Integer.MAX_VALUE, queue.lastCycle());
                assertEquals(firstIndex, queue.firstIndex());
                assertTrue(queue.dump().contains("maximum cycle"));
                try (ExcerptAppender appender = queue.createAppender()) {
                    appender.writeText("after restart");
                }
                try (ExcerptTailer tailer = queue.createTailer()) {
                    assertEquals("maximum cycle", tailer.readText());
                    assertEquals("after restart", tailer.readText());
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }
}
