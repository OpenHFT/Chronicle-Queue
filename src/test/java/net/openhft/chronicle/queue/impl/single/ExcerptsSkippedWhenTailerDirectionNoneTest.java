/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import org.junit.jupiter.api.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.jupiter.api.Assertions.*;

public final class ExcerptsSkippedWhenTailerDirectionNoneTest extends QueueTestCommon {
    @SuppressWarnings("try")
    @Test
    public void shouldNotSkipMessageAtStartOfQueue() {
        final File tmpDir = getTmpDir();
        try (final ChronicleQueue writeQueue =
                     ChronicleQueue.singleBuilder(tmpDir)
                             .testBlockSize()
                             .rollCycle(TEST_DAILY)
                             .build();
             final ExcerptAppender excerptAppender = writeQueue.createAppender()) {
            try (final DocumentContext ctx = excerptAppender.writingDocument()) {
                ctx.wire().getValueOut().object("first");
            }
            try (final DocumentContext ctx = excerptAppender.writingDocument()) {
                ctx.wire().getValueOut().object("second");
            }
        }

        try (final ChronicleQueue readQueue =
                     ChronicleQueue.singleBuilder(tmpDir)
                             .testBlockSize()
                             .rollCycle(TEST_DAILY)
                             .build()) {

            final ExcerptTailer tailer = readQueue.createTailer();
            final RollCycle rollCycle = readQueue.rollCycle();
            assertEquals(0L, rollCycle.toSequenceNumber(tailer.index()), "tailer should start at sequence number 0 before reading any documents");
            try (final DocumentContext ctx = tailer.direction(TailerDirection.NONE).readingDocument()) {
                assertFalse(ctx.isPresent(), "Document shouldn't be readable yet as direction is NONE");
            }
            assertEquals(0L, rollCycle.toSequenceNumber(tailer.index()), "tailer should remain at sequence number 0 after NONE direction read");

            String value;
            try (DocumentContext dc =
                         tailer.direction(TailerDirection.FORWARD).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertEquals(1L, rollCycle.toSequenceNumber(tailer.index()), "tailer should advance to sequence number 1 after FORWARD direction read");

            assertEquals("first", value, "first document should be read after FORWARD direction");

            try (DocumentContext dc =
                         tailer.direction(TailerDirection.NONE).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertEquals(1L, rollCycle.toSequenceNumber(tailer.index()), "tailer should remain at sequence number 1 after first NONE direction read");

            assertEquals("second", value, "second document should be read with NONE direction without advancing index");

            try (DocumentContext dc =
                         tailer.direction(TailerDirection.NONE).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertEquals(1L, rollCycle.toSequenceNumber(tailer.index()), "tailer should remain at sequence number 1 after second NONE direction read");

            assertEquals("second", value, "second document should be read again with NONE direction as index hasn't advanced");
        }
    }
}
