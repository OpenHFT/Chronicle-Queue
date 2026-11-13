/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.nio.BufferOverflowException;

import static org.junit.Assert.*;

/** A rejected input must not poison the appender or publish a partial document. */
public class StoreAppenderFailureRecoveryTest extends QueueTestCommon {
    @Test
    public void failedFirstWriteDoesNotRetainPreviousCycleTouchPosition() {
        SetTimeProvider clock = new SetTimeProvider(1000);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .blockSize(1 << 20).rollCycle(TestRollCycles.TEST_SECONDLY).timeProvider(clock).build();
             StoreAppender appender = (StoreAppender) queue.createAppender()) {
            for (int i = 0; i < 100; i++)
                appender.writeBytes(bytes -> bytes.writeSkip(1024));
            long previousPosition = appender.lastPosition;
            clock.currentTimeMillis(2000);
            assertThrows(IllegalStateException.class, () -> appender.writeBytes(bytes -> {
                throw new IllegalStateException("first encoder failed");
            }));
            assertTrue(appender.lastPosition < previousPosition);
            assertFalse(new MicroToucher(appender).execute());
            appender.writeBytes(bytes -> bytes.writeLong(22));
            try (ExcerptTailer tailer = queue.createTailer()) {
                for (int i = 0; i < 100; i++)
                    try (DocumentContext dc = tailer.readingDocument()) {
                        assertTrue(dc.isPresent());
                        assertEquals(1024, dc.wire().bytes().readRemaining());
                    }
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    assertEquals(22, dc.wire().bytes().readLong());
                }
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertFalse(dc.isPresent());
                }
            }
        }
    }

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
