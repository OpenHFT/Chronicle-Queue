/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.Sequence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SequenceForPositionSafetyTest extends IndexingTestCommon {
    @Override
    RollCycle rollCycle() {
        return SparseRollCycles.HUGE_DAILY_XSPARSE;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aliasedPositionMustRecoverFromTheIndexInBothInclusiveModes(boolean inclusive) throws Exception {
        appender.writeText("first");
        appender.writeBytes(bytes -> bytes.writeSkip(65532));
        SCQIndexing indexing = appender.store.indexing;
        long previousPosition = indexing.writePosition.getVolatileValue();
        appender.writeText("last");
        long lastPosition = indexing.writePosition.getVolatileValue();
        assertEquals(65536, lastPosition - previousPosition);

        // Model termination between publication of the full position and its encoded sequence.
        indexing.sequence.setSequence(1, previousPosition);
        try {
            assertEquals(1, indexing.sequence.getSequence(lastPosition), "the position bits must alias");
            assertEquals(2, indexing.sequenceForPosition(appender, Long.MAX_VALUE, inclusive));
            assertEquals(appender.lastIndexAppended(), queue.lastIndex());
        } finally {
            indexing.sequence.setSequence(2, lastPosition);
        }
    }

    @Test
    void lastSequenceNumberUsesOneAcquisitionBudget() throws Exception {
        appender.writeText("first");
        SCQIndexing indexing = appender.store.indexing;
        Sequence original = indexing.sequence;
        CountingSequence retrying = new CountingSequence(original, Integer.MAX_VALUE);
        indexing.sequence = retrying;
        try {
            assertEquals(0, indexing.lastSequenceNumber(appender));
            assertEquals(129, retrying.calls, "128 acquisition reads and one existing scan read");
        } finally {
            indexing.sequence = original;
        }
    }

    @Test
    void retryThenSuccessScansPastTheVerifiedIndexedRecord() throws Exception {
        appender.writeText("first");
        SCQIndexing indexing = appender.store.indexing;
        Sequence original = indexing.sequence;
        CountingSequence retrying = new CountingSequence(original, 2);
        indexing.sequence = retrying;
        try {
            assertEquals(0, indexing.sequenceForPosition(appender, Long.MAX_VALUE, true));
            assertEquals(4, retrying.calls, "three acquisition reads and the forward scan read");
        } finally {
            indexing.sequence = original;
        }
    }

    @Test
    void restrictedReadLimitDoesNotHideCompletedRecords() throws Exception {
        appender.writeText("first");
        appender.writeText("last");
        appender.wireForIndex().bytes().readPositionRemaining(0, 4);
        assertEquals(1, appender.store.indexing.sequenceForPosition(appender, Long.MAX_VALUE, false));
        assertEquals(appender.lastIndexAppended(), queue.lastIndex());
    }

    private static final class CountingSequence implements Sequence {
        private final Sequence delegate;
        private final int retries;
        private int calls;

        private CountingSequence(Sequence delegate, int retries) {
            this.delegate = delegate;
            this.retries = retries;
        }

        @Override
        public long getSequence(long position) {
            return ++calls <= retries ? NOT_FOUND_RETRY : delegate.getSequence(position);
        }

        @Override
        public void setSequence(long sequence, long position) {
            delegate.setSequence(sequence, position);
        }

        @Override
        public long toIndex(long headerNumber, long sequence) {
            return delegate.toIndex(headerNumber, sequence);
        }

        @Override
        public long toSequenceNumber(long index) {
            return delegate.toSequenceNumber(index);
        }
    }
}
