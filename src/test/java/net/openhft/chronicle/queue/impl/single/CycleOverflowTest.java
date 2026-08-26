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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CycleOverflowTest extends QueueTestCommon {

    @Test
    public void entriesBeyondSparseIndexCapacityAreFoundByLinearScan() {
        File path = getTmpDir();
        RollCycle rollCycle = TestRollCycles.TEST_DAILY;
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.set(System.currentTimeMillis());
        final long lastIndex;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .timeProvider(timeProvider)
                .rollCycle(rollCycle)
                .indexCount(8)
                .indexSpacing(2)
                .path(path)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            final long indexCapacity = (long) queue.indexCount() * queue.indexCount() * queue.indexSpacing();
            expectException("Sparse index capacity reached");
            for (int i = 0; i < indexCapacity + 3; i++)
                appender.writeText(Integer.toString(i));

            lastIndex = appender.lastIndexAppended();
            assertEquals(indexCapacity + 2, rollCycle.toSequenceNumber(lastIndex));
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .timeProvider(timeProvider)
                .rollCycle(rollCycle)
                .indexCount(8)
                .indexSpacing(2)
                .path(path)
                .build();
             ExcerptTailer tailer = queue.createTailer()) {
            final long firstIndex = rollCycle.toIndex(rollCycle.toCycle(lastIndex), 0);
            assertTrue(tailer.moveToIndex(firstIndex));
            assertEquals("0", tailer.readText());

            assertTrue(tailer.moveToIndex(lastIndex));
            assertEquals(Long.toString(rollCycle.toSequenceNumber(lastIndex)), tailer.readText());
            assertTrue("lookup beyond sparse-index capacity must use a linear scan",
                    ((StoreTailer) tailer).store.indexing.linearScanCount() > 0);
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }
}
