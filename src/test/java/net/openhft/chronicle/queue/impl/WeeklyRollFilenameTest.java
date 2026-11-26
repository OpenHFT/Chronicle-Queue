/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WeeklyRollFilenameTest extends QueueTestCommon {
    @Test
    public void weeklyQueueReopensWithTheSameIndexAndPayload() {
        File path = getTmpDir();
        long time = Instant.parse("2020-12-31T12:00:00Z").toEpochMilli();
        long index;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).testBlockSize()
                .rollCycle(RollCycles.WEEKLY).timeProvider(() -> time).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("before reopen");
            index = appender.lastIndexAppended();
        }
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).testBlockSize()
                .rollCycle(RollCycles.WEEKLY).timeProvider(() -> time).build();
             ExcerptTailer tailer = queue.createTailer()) {
            assertEquals(index, queue.firstIndex());
            assertEquals("before reopen", tailer.readText());
            assertNull(tailer.readText());
        }
    }

    @Test
    public void filenamesRoundTripAcrossWeekBasedYearBoundaries() {
        Locale original = Locale.getDefault(Locale.Category.FORMAT);
        try {
            for (Locale locale : new Locale[]{Locale.US, Locale.UK, Locale.FRANCE}) {
                Locale.setDefault(Locale.Category.FORMAT, locale);
                for (long epoch : new long[]{0, RollCycles.WEEKLY.defaultEpoch(),
                        Instant.parse("2017-03-08T00:00:00Z").toEpochMilli()}) {
                    RollingResourcesCache cache = new RollingResourcesCache(
                            RollCycles.WEEKLY, epoch, File::new, File::getName);
                    int first = Math.toIntExact(Math.floorDiv(
                            Instant.parse("2018-12-16T00:00:00Z").toEpochMilli() - epoch,
                            RollCycles.WEEKLY.lengthInMillis()));
                    for (int cycle = first; cycle < first + 170; cycle++) {
                        String name = cache.resourceFor(cycle).text;
                        assertEquals(locale + " epoch=" + epoch + " filename=" + name,
                                cycle, cache.parseCount(name));
                    }
                }
            }
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, original);
        }
    }
}
