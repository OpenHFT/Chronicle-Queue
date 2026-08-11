/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Pins the resume semantics documented on {@link SingleChronicleQueue#parkNamedTailer(String)}:
 * a parked named tailer (stored index {@code 0}) resumes from {@code firstIndex()} at its next
 * read - the oldest surviving roll - exactly as a freshly created, never-read tailer does, so
 * rolls deleted below that floor are never replayed to it.
 */
public class ParkNamedTailerResumeBehaviourTest extends QueueTestCommon {

    private final SetTimeProvider timeProvider = new SetTimeProvider();

    @Before
    public void useDeterministicSystemTimeProvider() {
        SystemTimeProvider.CLOCK = timeProvider;
    }

    @After
    public void resetSystemTimeProvider() {
        SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
    }

    private SingleChronicleQueueBuilder builder(File dir) {
        return SingleChronicleQueueBuilder.single(dir)
                .rollCycle(TestRollCycles.TEST_DAILY)
                .timeProvider(timeProvider);
    }

    private void writeDailyExcerpts(File dir, int days) {
        try (ChronicleQueue queue = builder(dir).build();
             ExcerptAppender appender = queue.createAppender()) {
            for (int d = 0; d < days; d++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("n").int32(d);
                }
                timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
        }
    }

    private static int readNext(ExcerptTailer tailer, long expectedIndex) {
        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue(dc.isPresent());
            assertEquals(expectedIndex, dc.index());
            return dc.wire().read("n").int32();
        }
    }

    private static void deleteAllButNewestRollFile(File dir) {
        File[] rolls = dir.listFiles((d, name) -> name.endsWith(SingleChronicleQueue.SUFFIX));
        assertNotNull(rolls);
        Arrays.sort(rolls);
        assertTrue(rolls.length > 1);
        for (int i = 0; i < rolls.length - 1; i++)
            assertTrue(rolls[i].delete());
    }

    @Test
    public void parkedTailerResumesFromOldestSurvivingRollAfterRestart() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(70_000));
        writeDailyExcerpts(dir, 3);

        try (SingleChronicleQueue q = builder(dir).build()) {
            try (ExcerptTailer consumer = q.createTailer("consumer")) {
                assertEquals(0, readNext(consumer, q.firstIndex()));
            }
            Long stored = q.namedTailerIndexes().get("consumer");
            assertNotNull(stored);
            assertTrue(stored > 0);

            assertTrue(q.parkNamedTailer("consumer"));
            assertEquals(Long.valueOf(0L), q.namedTailerIndexes().get("consumer"));
        }

        deleteAllButNewestRollFile(dir);

        try (SingleChronicleQueue q = builder(dir).build()) {
            long firstIndex = q.firstIndex();
            assertEquals(q.lastCycle(), q.rollCycle().toCycle(firstIndex));
            try (ExcerptTailer consumer = q.createTailer("consumer")) {
                assertEquals(0, consumer.index());
                assertEquals(2, readNext(consumer, firstIndex));
            }
        }
    }

    @Test
    public void parkedTailerReadsSameFirstEntryAsNeverReadTailer() {
        File dir = getTmpDir();
        timeProvider.currentTimeNanos(TimeUnit.DAYS.toNanos(75_000));
        writeDailyExcerpts(dir, 3);

        try (SingleChronicleQueue q = builder(dir).build()) {
            try (ExcerptTailer consumer = q.createTailer("consumer")) {
                assertEquals(0, readNext(consumer, q.firstIndex()));
            }
            assertTrue(q.parkNamedTailer("consumer"));
        }

        try (SingleChronicleQueue q = builder(dir).build();
             ExcerptTailer parked = q.createTailer("consumer");
             ExcerptTailer fresh = q.createTailer("fresh")) {
            long firstIndex = q.firstIndex();
            assertEquals(0, parked.index());
            assertEquals(0, fresh.index());

            assertEquals(0, readNext(parked, firstIndex));
            assertEquals(0, readNext(fresh, firstIndex));
            assertEquals(parked.lastReadIndex(), fresh.lastReadIndex());
        }
    }
}
