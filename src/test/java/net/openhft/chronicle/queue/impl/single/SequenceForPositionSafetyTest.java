/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Sequence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The sequence tracker compares only the low {@code 64 - cycleShift} bits of a position, so
 * positions {@code 2^(64 - cycleShift)} bytes apart alias. With {@code HUGE_DAILY_XSPARSE}
 * that period is 65,536 bytes. These tests interleave real publications of the unmodified
 * codec; none of them scripts a {@link Sequence#getSequence(long)} answer.
 */
class SequenceForPositionSafetyTest extends IndexingTestCommon {

    private static final long ALIAS_PERIOD = 65536;

    @Override
    RollCycle rollCycle() {
        return SparseRollCycles.HUGE_DAILY_XSPARSE;
    }

    private long tailLookup(boolean inclusive) throws Exception {
        return appender.store.sequenceForPosition(appender, Long.MAX_VALUE, inclusive);
    }

    /** Sequence 0 at p0, an aligned filler as sequence 1, "last" as sequence 2 one alias period after the filler. */
    private long writeAliasablePattern() {
        appender.writeText("first");
        appender.writeBytes(bytes -> bytes.writeSkip(ALIAS_PERIOD - 4));
        long previousPosition = appender.store.indexing.writePosition.getVolatileValue();
        appender.writeText("last");
        long lastPosition = appender.store.indexing.writePosition.getVolatileValue();
        assertEquals(ALIAS_PERIOD, lastPosition - previousPosition, "the two positions must alias");
        return previousPosition;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void newerPositionStaleTrackerAliasRecoversFromTheIndex(boolean inclusive) throws Exception {
        long previousPosition = writeAliasablePattern();
        SCQIndexing indexing = appender.store.indexing;
        long lastPosition = indexing.writePosition.getVolatileValue();

        // Model a writer that terminated between publishing the full write position and the
        // encoded sequence: the tracker still holds the previous record's pair.
        indexing.sequence.setSequence(1, previousPosition);
        try {
            assertEquals(1, indexing.sequence.getSequence(lastPosition), "the position bits must alias");
            assertEquals(2, tailLookup(inclusive));
        } finally {
            indexing.sequence.setSequence(2, lastPosition);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void olderPositionNewerTrackerAliasRecoversFromTheIndex(boolean inclusive) throws Exception {
        appender.writeText("first");
        appender.writeBytes(bytes -> bytes.writeSkip(ALIAS_PERIOD - 4));
        SCQIndexing indexing = appender.store.indexing;
        long capturedPosition = indexing.writePosition.getVolatileValue();

        // The reader captures the write position, then the writer commits and publishes the next
        // record one alias period later, before the reader's first tracker read.
        Sequence real = indexing.sequence;
        AtomicInteger injections = new AtomicInteger();
        indexing.sequence = new Sequence() {
            @Override
            public long getSequence(long position) {
                if (injections.compareAndSet(0, 1))
                    appender.writeText("last");
                return real.getSequence(position);
            }

            @Override
            public void setSequence(long sequence, long position) {
                real.setSequence(sequence, position);
            }

            @Override
            public long toIndex(long headerNumber, long sequence) {
                return real.toIndex(headerNumber, sequence);
            }

            @Override
            public long toSequenceNumber(long index) {
                return real.toSequenceNumber(index);
            }
        };
        try {
            long result = tailLookup(inclusive);
            assertEquals(1, injections.get());
            assertEquals(ALIAS_PERIOD, indexing.writePosition.getVolatileValue() - capturedPosition,
                    "the captured and republished positions must alias");
            assertEquals(2, real.getSequence(capturedPosition), "the stale position must match the newer tracker");
            assertEquals(2, result, "the suffix must not be counted twice");
        } finally {
            indexing.sequence = real;
        }
    }

    @Test
    void indexedTrackerWithMismatchedPersistedEntryRecoversFromTheIndex() throws Exception {
        writeAliasablePattern();
        SCQIndexing indexing = appender.store.indexing;
        long lastPosition = indexing.writePosition.getVolatileValue();

        // Sequence 0 is indexed, but its persisted entry names the true position of record 0,
        // not the aliased published position this residue claims. The tail check may only
        // trust the pair when the persisted entry matches the full position exactly.
        indexing.sequence.setSequence(0, lastPosition);
        try {
            assertEquals(0, indexing.sequence.getSequence(lastPosition), "the residue must match exactly");
            assertEquals(2, tailLookup(false));
            assertEquals(appender.lastIndexAppended(), queue.lastIndex());
        } finally {
            indexing.sequence.setSequence(2, lastPosition);
        }
    }

    @Test
    void lastIndexWithStaleTrackerAliasRecoversFromTheIndex() throws Exception {
        long previousPosition = writeAliasablePattern();
        SCQIndexing indexing = appender.store.indexing;
        long lastPosition = indexing.writePosition.getVolatileValue();

        indexing.sequence.setSequence(1, previousPosition);
        try {
            assertEquals(1, indexing.sequence.getSequence(lastPosition), "the position bits must alias");
            assertEquals(appender.lastIndexAppended(), queue.lastIndex());
        } finally {
            indexing.sequence.setSequence(2, lastPosition);
        }
    }

    @Test
    void backwardTraversalAcrossRollBoundaryLandsOnTheTrueTail() throws Exception {
        long previousPosition = writeAliasablePattern();
        SCQIndexing indexingA = appender.store.indexing;
        int cycleA = queue.cycle();

        // Leave the previous cycle with stale mid-publication tracker residue, then roll.
        indexingA.sequence.setSequence(1, previousPosition);
        timeProvider.advanceMillis(queue.rollCycle().lengthInMillis());
        appender.writeText("b0");
        int cycleB = queue.cycle();
        assertNotEquals(cycleA, cycleB);

        try (ExcerptTailer backward = queue.createTailer().direction(TailerDirection.BACKWARD).toEnd()) {
            try (DocumentContext dc = backward.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals(queue.rollCycle().toIndex(cycleB, 0), dc.index());
                assertEquals("b0", dc.wire().getValueIn().text());
            }
            try (DocumentContext dc = backward.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals(queue.rollCycle().toIndex(cycleA, 2), dc.index(),
                        "crossing backward into the previous cycle must land on its true last record");
                assertEquals("last", dc.wire().getValueIn().text());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restrictedReadLimitDoesNotHideCompletedRecords(boolean inclusive) throws Exception {
        appender.writeText("first");
        appender.writeText("last");
        appender.wireForIndex().bytes().readPositionRemaining(0, 4);
        assertEquals(1, tailLookup(inclusive));
        assertEquals(appender.lastIndexAppended(), queue.lastIndex());
    }
}
