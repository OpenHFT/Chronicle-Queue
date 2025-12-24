/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.File;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.jupiter.api.Assertions.*;

@RequiredForClient
public class LastIndexAppendedTest extends QueueTestCommon {

    @Test
    public void testLastIndexAppendedAcrossRestarts() {
        String path = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();

        try {
            for (int i = 0; i < 5; i++) {
                try (ChronicleQueue queue = single(path)
                        .testBlockSize()
                        .rollCycle(TEST_DAILY)
                        .build();
                     ExcerptAppender appender = queue.createAppender()) {

                    try (DocumentContext documentContext = appender.writingDocument()) {
                        int index = (int) documentContext.index();
                        assertEquals(i, index, "document context index should match current iteration number");

                        documentContext.wire().write().text("hello world");
                    }

                    assertEquals(i, (int) appender.lastIndexAppended(), "lastIndexAppended should return most recent write index matching iteration number");
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(path, 2);
        }
    }

    @Test
    public void testTwoAppenders() {
        File path = getTmpDir();
        long appendedIndex;

        try (
                ChronicleQueue appender_queue = single(path)
                        .testBlockSize()
                        .rollCycle(TEST_DAILY)
                        .build();
                ExcerptAppender appender = appender_queue.createAppender()) {
            for (int i = 0; i < 5; i++) {
                appender.writeDocument(wireOut -> wireOut.write("log").marshallable(m ->
                        m.write("msg").text("hello world ")));
            }
            appendedIndex = appender.lastIndexAppended();
        }
        try (ChronicleQueue tailer_queue = single(path)
                .testBlockSize()
                .rollCycle(TEST_DAILY)
                .build()) {
            ExcerptTailer tailer = tailer_queue.createTailer();
            tailer = tailer.toStart();
            long tailIndex;
            tailIndex = doRead(tailer, 5);
            assertEquals(appendedIndex, tailIndex, "tailer index after reading should match last appended index");
            // System.out.println("Continue appending");
            try (ChronicleQueue appender_queue = single(path)
                    .testBlockSize()
                    .rollCycle(TEST_DAILY)
                    //.buffered(false)
                    .build();
                 ExcerptAppender appender = appender_queue.createAppender()) {
                for (int i = 0; i < 5; i++) {
                    appender.writeDocument(wireOut -> wireOut.write("log").marshallable(m ->
                            m.write("msg").text("hello world2 ")));
                }
                appendedIndex = appender.lastIndexAppended();
                assertTrue(appendedIndex > tailIndex, "appendedIndex > tailIndex");
            }
            // if the tailer continues as well it should see the 5 new messages
            tailIndex = doRead(tailer, 5);
            assertEquals(appendedIndex, tailIndex, "tailer index after reading should match last appended index");

            // if the tailer is expecting to read all the message again
            tailer.toStart();
            tailIndex = doRead(tailer, 10);
            assertEquals(appendedIndex, tailIndex, "tailer index after reading should match last appended index");
        } finally {
            IOTools.deleteDirWithFiles(path, 2);
        }
    }

    private long doRead(@NotNull ExcerptTailer tailer, int expected) {
        int[] i = {0};
        long tailIndex = 0;
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                tailIndex = tailer.index();
                dc.wire().read("log").marshallable(m -> {
                    String msg = m.read("msg").text();
                    assertNotNull(msg, "message text read from queue should not be null");
                    i[0]++;
                });
            }
        }
        assertEquals(expected, i[0], "number of messages read should match expected count");
        return tailIndex;
    }
}
