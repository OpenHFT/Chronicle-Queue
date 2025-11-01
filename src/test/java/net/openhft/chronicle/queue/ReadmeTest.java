/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;

public class ReadmeTest extends QueueTestCommon {

    @Test
    public void createAQueue() {
        final String basePath = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(basePath)
                .testBlockSize()
                .rollCycle(TEST_DAILY)
                .build();
             // Obtain an ExcerptAppender
             ExcerptAppender appender = queue.createAppender()) {

            // write - {msg: TestMessage}
            appender.writeDocument(w -> w.write("msg").text("TestMessage"));

            // System.out.println(queue.dump());
            // write - TestMessage
            appender.writeText("TestMessage");

            ExcerptTailer tailer = queue.createTailer();

            tailer.readDocument(w -> System.out.println("msg: " + w.read("msg").text()));

            assertEquals("TestMessage", tailer.readText());
        } finally {
            IOTools.deleteDirWithFiles(basePath);
        }
    }
}
