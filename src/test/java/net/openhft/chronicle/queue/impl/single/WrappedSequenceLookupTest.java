/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Test;

import java.io.StreamCorruptedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WrappedSequenceLookupTest extends IndexingTestCommon {
    // HUGE_DAILY_XSPARSE stores 16 position bits in the sequence tracker.
    private static final int POSITION_PERIOD = 1 << 16;

    @Override
    RollCycle rollCycle() {
        return SparseRollCycles.HUGE_DAILY_XSPARSE;
    }

    private void leaveTrackerOnPreviousRecord() {
        appender.writeText("first");
        appender.writeBytes(bytes -> bytes.writeSkip(POSITION_PERIOD - Integer.BYTES));
        long previous = appender.store.writePosition();
        appender.writeText("last");
        assertEquals(POSITION_PERIOD, appender.store.writePosition() - previous);

        // Reproduce a writer stopping after the full write position was published.
        appender.store.indexing.sequence.setSequence(1, previous);
    }

    @Test
    void lastIndexIncludesTheLastCommittedRecord() {
        leaveTrackerOnPreviousRecord();
        assertEquals(appender.lastIndexAppended(), queue.lastIndex());
    }

    @Test
    void lastSequenceWithNoIndexWireStillFindsTheFinalRecord() throws StreamCorruptedException {
        leaveTrackerOnPreviousRecord();
        StoreTailer unpositioned = (StoreTailer) tailer;
        assertNull(unpositioned.wireForIndex());
        assertEquals(2, appender.store.lastSequenceNumber(unpositioned));
    }

    @Test
    void seekReadsTheRequestedRecord() {
        leaveTrackerOnPreviousRecord();
        assertTrue(tailer.moveToIndex(appender.lastIndexAppended() - 1));
        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue(dc.isPresent());
            assertEquals(POSITION_PERIOD - Integer.BYTES, dc.wire().bytes().readRemaining());
        }
    }

    @Test
    void forwardEndPointsPastTheLastCommittedRecord() {
        leaveTrackerOnPreviousRecord();
        assertEquals(appender.lastIndexAppended() + 1, tailer.toEnd().index());
    }

    @Test
    void positionLookupCountsTheFinalRecordOnce() throws StreamCorruptedException {
        leaveTrackerOnPreviousRecord();
        long position = appender.store.writePosition();
        assertEquals(2, appender.store.sequenceForPosition(appender, position, true));
        assertEquals(1, appender.store.sequenceForPosition(appender, position, false));
    }
}
