/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * Successor / regression test for https://github.com/OpenHFT/Chronicle-Queue/issues/1099
 * <p>
 * The reported problem: junk data left in a cycle file by an aborted/crashed longer write could
 * remain in the slot that follows a subsequent shorter, complete write. {@code readDataHeader} then
 * misinterpreted those stray bytes as a valid message header instead of recognising end-of-queue.
 * <p>
 * The fix (in {@code AbstractWire.updateHeader}) explicitly zeroes the next bytes after padding when a
 * write is committed, so any junk left in the following slot is cleared. This test reproduces the
 * layout — planting junk in the slot that will follow a short message — writes the short message, and
 * asserts the tailer reads exactly the written messages and then sees end-of-queue rather than junk.
 */
public class JunkAfterShorterWriteTest extends QueueTestCommon {

    @Test
    public void junkFollowingAShorterWriteIsNotReadAsAHeader() throws Exception {
        assumeFalse(OS.isWindows());

        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(RollCycles.FAST_DAILY)
                .timeProvider(new SetTimeProvider())
                .testBlockSize()
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            // First (complete) message.
            appender.writeText("hi");

            // Start a longer write and roll it back, reproducing the dirty payload
            // left by an incomplete writer before the shorter retry.
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().getValueOut().text("a considerably longer incomplete value");
                dc.rollbackOnClose();
            }

            final int cycle = queue.cycle();
            final long junkSlot;
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().getValueOut().text("x");
                final long committedRecordEnd = dc.wire().bytes().writePosition();
                final long padding = BytesUtil.padOffset(committedRecordEnd);
                assertEquals("the regression requires two bytes of alignment padding", 2, padding);
                junkSlot = committedRecordEnd + padding;

                // Little-endian bytes are 08 00 20 6c. The old four-byte clear starts
                // two bytes before junkSlot, clearing 08 00 but leaving 20 6c. The
                // fixed eight-byte clear removes the complete following header.
                dc.wire().bytes().writeInt(junkSlot, 0x6c20_0008);
                assertTrue("planted following header should be non-zero",
                        dc.wire().bytes().readVolatileInt(junkSlot) != 0);
            }

            try (final SingleChronicleQueueStore store = queue.storeForCycle(cycle, queue.epoch(), false, null);
                 final MappedBytes mb = MappedBytes.mappedBytes(store.file(), OS.pageSize())) {
                assertEquals("commit must clear the complete aligned following header",
                        0, mb.readVolatileInt(junkSlot));
            }

            // A fresh tailer must read exactly the two written messages and then reach end-of-queue,
            // never seeing the planted junk as a message.
            try (final ExcerptTailer tailer = queue.createTailer()) {
                assertEquals("hi", tailer.readText());
                assertEquals("x", tailer.readText());
                try (final DocumentContext dc = tailer.readingDocument()) {
                    assertFalse("junk after the shorter write must not be read as a document", dc.isPresent());
                }
            }
        }
    }
}
