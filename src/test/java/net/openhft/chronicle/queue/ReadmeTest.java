/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;

public class ReadmeTest extends QueueTestCommon {

    @Test
    public void createAQueue() {
        // Register storage with the shared cleanup, which drains deferred releases and
        // checks deletion. An unchecked immediate delete could leave mappings on Windows.
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(getTmpDir())
                .testBlockSize()
                .rollCycle(TEST_DAILY)
                .build();
             // Obtain an ExcerptAppender
             ExcerptAppender appender = queue.createAppender()) {

            // write - {msg: TestMessage}
            appender.writeDocument(w -> w.write("msg").text("TestMessage"));

            // write - TestMessage
            appender.writeText("TestMessage");

            try (ExcerptTailer tailer = queue.createTailer()) {

                tailer.readDocument(w -> System.out.println("msg: " + w.read("msg").text()));

                assertEquals("TestMessage", tailer.readText());
            }
        }
    }
}
