/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RestartableTailerTest extends QueueTestCommon {
    @Test
    @DisplayName("Named tailers resume from stored positions")
    public void restartable() {
        String tmp = OS.getTarget() + "/restartable-" + Time.uniqueId();
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             final ExcerptAppender excerptAppender = cq.createAppender()) {
            for (int i = 0; i < 7; i++) {
                excerptAppender.writeText("test " + i);
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 0", atailer.readText(), "tailer 'a' first read should return 'test 0'");
            assertEquals("test 1", atailer.readText(), "tailer 'a' second read should return 'test 1'");
            assertEquals("test 2", atailer.readText(), "tailer 'a' third read should return 'test 2'");

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 0", btailer.readText(), "tailer 'b' first read should start at 'test 0'");
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 3", atailer.readText(), "tailer 'a' should resume at 'test 3' after reopen");
            assertEquals("test 4", atailer.readText(), "tailer 'a' should continue to 'test 4'");
            assertEquals("test 5", atailer.readText(), "tailer 'a' should continue to 'test 5'");

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 1", btailer.readText(), "tailer 'b' should resume at 'test 1' after reopen");
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 6", atailer.readText(), "tailer 'a' should read final message 'test 6'");
            assertNull(atailer.readText(), "tailer 'a' should return null when queue exhausted");

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 2", btailer.readText(), "tailer 'b' should resume at 'test 2' after reopen");
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertNull(atailer.readText(), "tailer 'a' should remain at end of queue after reopen");

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 3", btailer.readText(), "tailer 'b' should resume at 'test 3' after reopen");
            }
        } finally {
            IOTools.deleteDirWithFiles(tmp);
        }
    }
}
