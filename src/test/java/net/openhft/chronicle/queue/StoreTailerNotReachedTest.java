/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StoreTailerNotReachedTest extends QueueTestCommon {
    @Test
    @DisplayName("Tailer resumes after NOT_REACHED state")
    public void afterNotReached() {
        String path = OS.getTarget() + "/afterNotReached-" + Time.uniqueId();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .build();
             final ExcerptAppender appender = q.createAppender()) {
            appender.writeText("Hello");
            ExcerptTailer tailer = q.createTailer();
            assertEquals("Hello", tailer.readText(), "first read should return 'Hello' from initial append");
            assertNull(tailer.readText(), "second read should return null when queue exhausted");
            appender.writeText("World");
            assertEquals("World", tailer.readText(), "read after new append should return 'World'");
            assertNull(tailer.readText(), "final read should return null when queue exhausted again");
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }
}
