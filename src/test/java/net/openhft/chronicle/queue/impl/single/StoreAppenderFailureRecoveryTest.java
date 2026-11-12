/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.nio.BufferOverflowException;

import static org.junit.Assert.*;

/** A rejected input must not poison the appender or publish a partial document. */
public class StoreAppenderFailureRecoveryTest extends QueueTestCommon {
    @Test
    public void byteStoreOverflowLeavesAppenderReusable() {
        try (SingleChronicleQueue queue = queue(); ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(bytes -> bytes.writeLong(11));
            Bytes<?> oversized = Bytes.allocateDirect(65537);
            try {
                oversized.writeSkip(65537);
                assertThrows(BufferOverflowException.class, () -> appender.writeBytes(oversized));
            } finally {
                oversized.releaseLast();
            }
            appender.writeBytes(bytes -> bytes.writeLong(22));
            assertValues(queue, 11, 22);
        }
    }

    @Test
    public void failedEncodersDoNotPublishTheirPartialOutput() {
        try (SingleChronicleQueue queue = queue(); ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(bytes -> bytes.writeLong(11));
            assertThrows(IllegalStateException.class, () -> appender.writeBytes(bytes -> {
                bytes.writeLong(999);
                throw new IllegalStateException("injected encoder failure");
            }));
            assertThrows(AssertionError.class, () -> appender.writeBytes(bytes -> {
                bytes.writeLong(999);
                throw new AssertionError("injected encoder error");
            }));
            appender.writeBytes(bytes -> bytes.writeLong(22));
            assertValues(queue, 11, 22);
        }
    }

    private SingleChronicleQueue queue() {
        return SingleChronicleQueueBuilder.binary(getTmpDir()).testBlockSize()
                .rollCycle(RollCycles.FAST_DAILY).timeProvider(() -> 86400000).build();
    }

    private static void assertValues(SingleChronicleQueue queue, long... expected) {
        try (ExcerptTailer tailer = queue.createTailer()) {
            for (long value : expected) {
                try (DocumentContext document = tailer.readingDocument()) {
                    assertTrue(document.isPresent());
                    assertEquals(value, document.wire().bytes().readLong());
                    assertEquals(0, document.wire().bytes().readRemaining());
                }
            }
            try (DocumentContext document = tailer.readingDocument()) {
                assertFalse(document.isPresent());
            }
        }
    }
}
