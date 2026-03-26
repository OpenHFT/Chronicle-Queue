/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.issue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class to verify the behavior of Chronicle Queue when changing Roll Cycles.
 * It checks the compatibility and visibility of data written with one Roll Cycle
 * and accessed with another.
 */
class ChangeRollCycleTest {

    @Test
    void changeRollCycleWithReadOnlyTailer() {
        testChangeRollCycle(true);
    }

    @Test
    void changeRollCycleWithReadWriteTailer() {
        testChangeRollCycle(false);
    }

    /**
     * Test the behavior of Chronicle Queue when switching between roll cycles.
     *
     * @param readOnly whether the tailer should be in read-only mode
     */
    private void testChangeRollCycle(boolean readOnly) {
        // Define the queue path
        String queuePath = OS.getTarget() + "/changeRollCycle-" + System.nanoTime();

        // Step 1: Open a queue with a FAST_DAILY roll cycle and a tailer
        try (ChronicleQueue q1 = ChronicleQueue.singleBuilder(queuePath)
                .rollCycle(RollCycles.FAST_DAILY)
                .readOnly(readOnly)
                .build();
             ExcerptTailer tailer = q1.createTailer()) {

            // Verify the queue is initially empty
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent(), "Queue should be empty initially");
            }

            // Step 2: Reopen the queue with a WEEKLY roll cycle and write data
            try (ChronicleQueue q2 = ChronicleQueue.singleBuilder(queuePath)
                    .rollCycle(RollCycles.WEEKLY)
                    .build();
                 ExcerptAppender appender2 = q2.createAppender()) {

                // Write a messages to the queue
                appender2.writeText("Hello");

                // Step 3: Reopen the queue with a WEEKLY roll cycle and write data
                try (ChronicleQueue q3 = ChronicleQueue.singleBuilder(queuePath)
                        .rollCycle(RollCycles.FAST_HOURLY)
                        .build();
                     ExcerptAppender appender3 = q3.createAppender()) {
                    assertEquals(q2.rollCycle(), q3.rollCycle());

                    // Write two messages to the queue
                    appender3.writeText("World");

                    if (readOnly && !OS.isWindows())
                        assertEquals(RollCycles.WEEKLY, q3.rollCycle(), "Roll cycle should match WEEKLY for read-only mode");
                }

                // If the tailer is read-only, the roll cycle cannot not be changed
                // The read only case assumes there queue is historical and the roll cycle is fixed
                if (readOnly) return;

                // Step 4: Verify the data can be read back correctly
                assertEquals("Hello", tailer.readText(), "First message should match");

                if (readOnly)
                    assertEquals(RollCycles.WEEKLY, q1.rollCycle(), "Roll cycle should match WEEKLY for read-only mode");

                assertEquals("World", tailer.readText(), "Second message should match");

                assertEquals(q2.rollCycle(), q1.rollCycle());

                // Verify there is no extra data in the queue
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertFalse(dc.isPresent(), "No more data should be present in the queue");
                }
            }
        } finally {
            // Clean up the queue directory to avoid leaving test artifacts
            IOTools.deleteDirWithFiles(queuePath, 2);
        }
    }
}
