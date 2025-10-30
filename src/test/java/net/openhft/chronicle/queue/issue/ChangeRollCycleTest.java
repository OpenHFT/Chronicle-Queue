/*
 * Copyright 2016-2025 chronicle.software
 */

package net.openhft.chronicle.queue.issue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test class to verify the behavior of Chronicle Queue when changing Roll Cycles.
 * It checks the compatibility and visibility of data written with one Roll Cycle
 * and accessed with another.
 */
public class ChangeRollCycleTest {

    @Test
    public void changeRollCycleWithReadOnlyTailer() {
        testChangeRollCycle(true);
    }

    @Test
    public void changeRollCycleWithReadWriteTailer() {
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
                assertFalse("Queue should be empty initially", dc.isPresent());
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
                        assertEquals("Roll cycle should match WEEKLY for read-only mode",
                                RollCycles.WEEKLY, q3.rollCycle());
                }

                // If the tailer is read-only, the roll cycle cannot not be changed
                // The read only case assumes there queue is historical and the roll cycle is fixed
                if (readOnly) return;

                // Step 4: Verify the data can be read back correctly
                assertEquals("First message should match", "Hello", tailer.readText());

                if (readOnly)
                    assertEquals("Roll cycle should match WEEKLY for read-only mode",
                            RollCycles.WEEKLY,
                            q1.rollCycle());

                assertEquals("Second message should match", "World", tailer.readText());

                assertEquals(q2.rollCycle(), q1.rollCycle());

                // Verify there is no extra data in the queue
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertFalse("No more data should be present in the queue", dc.isPresent());
                }
            }
        } finally {
            // Clean up the queue directory to avoid leaving test artifacts
            IOTools.deleteDirWithFiles(queuePath, 2);
        }
    }
}
