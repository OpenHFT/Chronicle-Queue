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
import net.openhft.chronicle.wire.Wires;
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

            final int cycle = queue.cycle();
            final long junkSlot;
            try (final SingleChronicleQueueStore store = queue.storeForCycle(cycle, queue.epoch(), false, null)) {
                // store.writePosition() is the start of the last (first) record's header.
                final long firstHeader = store.writePosition();
                try (final MappedBytes mb = MappedBytes.mappedBytes(store.file(), OS.pageSize())) {
                    final int header = mb.readVolatileInt(firstHeader);
                    final long firstEnd = firstHeader + Wires.SPB_HEADER_SIZE + Wires.lengthOf(header);
                    // Where the second record's header will go (padding-aligned).
                    final long secondSlot = firstEnd + BytesUtil.padOffset(firstEnd);
                    // A short second message ("x") occupies header(4)+payload(2)+pad = 8 bytes, so the
                    // slot that follows it is 8 bytes on. That is where leftover junk from an aborted
                    // longer write would sit and be misread as the next header.
                    junkSlot = secondSlot + 8;

                    // Plant junk that looks like a small, complete data header.
                    mb.writeInt(junkSlot, 8);
                    // sanity: the junk must be readable as a non-zero header before the shorter write commits
                    assertTrue("planted junk should be non-zero", mb.readVolatileInt(junkSlot) != 0);
                }
            }

            // Second (complete) message, shorter than the aborted write whose tail we simulated.
            // Committing this write must zero the following slot, clearing the planted junk.
            appender.writeText("x");

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
