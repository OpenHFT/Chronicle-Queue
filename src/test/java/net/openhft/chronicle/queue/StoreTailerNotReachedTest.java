/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StoreTailerNotReachedTest extends QueueTestCommon {
    @Test
    public void afterNotReached() {
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .testBlockSize()
                .build();
             final ExcerptAppender appender = q.createAppender();
             ExcerptTailer tailer = q.createTailer()) {
            appender.writeText("Hello");
            assertEquals("Hello", tailer.readText());
            assertNull(tailer.readText());
            appender.writeText("World");
            assertEquals("World", tailer.readText());
            assertNull(tailer.readText());
        }
    }
}
