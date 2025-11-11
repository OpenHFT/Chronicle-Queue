/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;

public class NoDataIsSkippedWithInterruptTest extends QueueTestCommon {

    private static final String EXPECTED = "Hello World";

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    public void test() {
        final SetTimeProvider timeProvider = new SetTimeProvider();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.single(DirectoryUtils.tempDir("."))
                .rollCycle(MINUTELY)
                .timeProvider(timeProvider)
                .testBlockSize()
                .build();
             final ExcerptAppender excerptAppender = q.createAppender();
             final ExcerptTailer tailer = q.createTailer()) {

            Thread.currentThread().interrupt();
            excerptAppender.writeText(EXPECTED);
            Assert.assertTrue(Thread.currentThread().isInterrupted());

            timeProvider.advanceMillis(60_000);

            excerptAppender.writeText(EXPECTED);

            Assert.assertEquals(EXPECTED, tailer.readText());
            Assert.assertEquals(EXPECTED, tailer.readText());
        }
    }
}
