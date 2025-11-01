/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.harness.WeeklyRollCycle;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class QueueReadForwardSkippingACycleBiggerThanADayTest extends QueueTestCommon {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File dataDir;
    private SetTimeProvider timeProvider;

    @Before
    public void setup() throws IOException {
        this.dataDir = temporaryFolder.newFolder();
        this.timeProvider = new SetTimeProvider();
    }

    @Test
    public void testReadForwards() {
        RollCycle rollingCycle = WeeklyRollCycle.INSTANCE;
        // Write a message to the queue
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {
            excerptAppender.writeText("0");
        }

        // Wait more than a cycle
        timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(18));

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {
            excerptAppender.writeText("42");
        }

        // Read backwards from the end
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dataDir)
                .timeProvider(timeProvider)
                .rollCycle(rollingCycle)
                .build()) {
            ExcerptTailer tailer = queue.createTailer().toStart().direction(TailerDirection.FORWARD);
            // An exception is thrown here
            assertEquals("0", tailer.readText());
            assertEquals("42", tailer.readText());
            assertNull(tailer.readText());
        }
    }
}
