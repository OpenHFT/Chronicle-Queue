/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Sequence;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.StreamCorruptedException;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

/**
 * Counts actual reads on a small, fixed queue, independently of scan logging and elapsed time.
 */
public class SequenceForPositionFastPathTest extends QueueTestCommon {
    private static final int INDEX_SPACING = 32;
    private static final int LAST_SEQUENCE = 62;
    private final long[] positions = new long[LAST_SEQUENCE + 1];
    private SingleChronicleQueue queue;
    private StoreAppender appender;
    private SingleChronicleQueue readerQueue;
    private SingleChronicleQueueStore readerStore;
    private MappedBytes bytes;
    private RecordingWire wire;
    private SCQIndexing indexing;
    private CountingSequence sequence;

    @Before
    public void createQueue() {
        File path = getTmpDir();
        queue = builder(path).build();
        appender = (StoreAppender) queue.createAppender();
        for (int i = 0; i <= LAST_SEQUENCE; i++) {
            appender.writeText("record-" + i);
            positions[i] = appender.store.writePosition();
        }

        // A separate store starts with cold index-array caches, making index lookup observable too.
        readerQueue = builder(path).build();
        readerStore = readerQueue.storeForCycle(queue.lastCycle(), queue.epoch(), false, null);
        assertNotNull(readerStore);
        indexing = readerStore.indexing;
        bytes = readerStore.bytes();
        wire = new RecordingWire(bytes);
        sequence = new CountingSequence(indexing.sequence);
        indexing.sequence = sequence;
    }

    private SingleChronicleQueueBuilder builder(File path) {
        return SingleChronicleQueueBuilder.binary(path).testBlockSize()
                .timeProvider(new SetTimeProvider()).rollCycle(TEST_SECONDLY)
                .indexCount(8).indexSpacing(INDEX_SPACING);
    }

    @After
    public void closeQueue() {
        if (indexing != null && sequence != null)
            indexing.sequence = sequence.delegate;
        if (bytes != null)
            bytes.releaseLast();
        closeQuietly(readerStore, readerQueue, appender, queue);
    }

    @Test
    public void consistentTrackerSkipsSparseIndex() throws StreamCorruptedException {
        assertEquals(INDEX_SPACING, indexing.indexSpacing());
        assertEquals(LAST_SEQUENCE, indexing.sequenceForPosition(wire, Long.MAX_VALUE, false));
        assertTrackerScan(positions[LAST_SEQUENCE], 1, 0);
        assertEquals(2, sequence.calls);
    }

    @Test
    public void inclusiveEndLookupAlsoUsesTracker() throws StreamCorruptedException {
        assertEquals(LAST_SEQUENCE, indexing.sequenceForPosition(wire, Long.MAX_VALUE, true));
        assertTrackerScan(positions[LAST_SEQUENCE], 1, 0);
    }

    @Test
    public void staleTrackerRecoversDataPastMetadataButNotIncompleteTail() throws StreamCorruptedException {
        try (DocumentContext metadata = appender.writingDocument(true)) {
            metadata.wire().write("metadata").text("not a data record");
        }
        appender.writeText("record-63");
        appender.writeText("record-64");
        long latestPosition = appender.store.writePosition();

        try (DocumentContext incomplete = appender.writingDocument()) {
            incomplete.rollbackOnClose();
            Bytes<?> writeBytes = incomplete.wire().bytes();
            long payloadStart = writeBytes.writePosition();
            incomplete.wire().getValueOut().text("not yet complete");
            // An ordinary open appender leaves a zero header; explicitly simulate a persisted incomplete record.
            int incompleteHeader = Wires.NOT_COMPLETE | (int) (writeBytes.writePosition() - payloadStart);
            writeBytes.writeVolatileInt(payloadStart - Wires.SPB_HEADER_SIZE, incompleteHeader);
            indexing.writePosition.setVolatileValue(positions[35]);
            sequence.setSequence(35, positions[35]);
            try {
                assertEquals(64, indexing.sequenceForPosition(wire, Long.MAX_VALUE, false));
                assertTrackerScan(positions[35], 30, 1);
                assertEquals(1, wire.incompleteHeaders);
            } finally {
                indexing.writePosition.setVolatileValue(latestPosition);
                sequence.setSequence(64, latestPosition);
            }
        }
    }

    @Test
    public void retryThenSuccessUsesTracker() throws StreamCorruptedException {
        sequence.retriesRemaining = 3;
        assertEquals(LAST_SEQUENCE, indexing.sequenceForPosition(wire, Long.MAX_VALUE, false));
        assertTrackerScan(positions[LAST_SEQUENCE], 1, 0);
        // Four attempts to acquire the tracker pair, plus the existing scan's own tracker read.
        assertEquals(5, sequence.calls);
    }

    @Test
    public void lastRetryCanStillUseTracker() throws StreamCorruptedException {
        sequence.retriesRemaining = 127;
        assertEquals(LAST_SEQUENCE, indexing.sequenceForPosition(wire, Long.MAX_VALUE, false));
        assertTrackerScan(positions[LAST_SEQUENCE], 1, 0);
        assertEquals(129, sequence.calls);
    }

    @Test
    public void retryRereadsTheWritePosition() throws StreamCorruptedException {
        indexing.writePosition.setVolatileValue(positions[35]);
        sequence.beforeFirstRead = () -> indexing.writePosition.setVolatileValue(positions[LAST_SEQUENCE]);
        try {
            assertEquals(LAST_SEQUENCE, indexing.sequenceForPosition(wire, Long.MAX_VALUE, false));
            assertTrackerScan(positions[LAST_SEQUENCE], 1, 0);
            // The first captured address mismatches, the next matches, then the scan reads the tracker once.
            assertEquals(3, sequence.calls);
        } finally {
            indexing.writePosition.setVolatileValue(positions[LAST_SEQUENCE]);
        }
    }

    @Test
    public void notFoundUsesIndexedFallbackWithoutRetrying() throws StreamCorruptedException {
        sequence.setSequence(0, 0);
        try {
            assertEquals(LAST_SEQUENCE, indexing.sequenceForPosition(wire, Long.MAX_VALUE, false));
            assertIndexedScan();
            assertEquals(2, sequence.calls);
        } finally {
            sequence.setSequence(LAST_SEQUENCE, positions[LAST_SEQUENCE]);
        }
    }

    @Test
    public void persistentMismatchUsesOneRetryBudgetForDirectLookup() throws StreamCorruptedException {
        assertPersistentMismatch(false);
    }

    @Test
    public void persistentMismatchUsesOneRetryBudgetForLastSequenceNumber() throws StreamCorruptedException {
        assertPersistentMismatch(true);
    }

    private void assertPersistentMismatch(boolean lastSequenceNumber) throws StreamCorruptedException {
        // Leave the encoded sequence at record 62, but put the position back at record 35.
        indexing.writePosition.setVolatileValue(positions[35]);
        try {
            assertEquals(Sequence.NOT_FOUND_RETRY, sequence.delegate.getSequence(positions[35]));
            long result = lastSequenceNumber ? indexing.lastSequenceNumber(wire)
                    : indexing.sequenceForPosition(wire, Long.MAX_VALUE, false);
            assertEquals(LAST_SEQUENCE, result);
            assertIndexedScan();
            // One 128-attempt budget, plus one tracker read inside the indexed recovery scan.
            assertEquals(129, sequence.calls);
        } finally {
            indexing.writePosition.setVolatileValue(positions[LAST_SEQUENCE]);
        }
    }

    @Test
    public void zeroWritePositionStillAllowsDirectIndexedRecovery() throws StreamCorruptedException {
        indexing.writePosition.setVolatileValue(0);
        try {
            assertEquals(LAST_SEQUENCE, indexing.sequenceForPosition(wire, Long.MAX_VALUE, false));
            assertIndexedScan();
            assertEquals(1, sequence.calls);
        } finally {
            indexing.writePosition.setVolatileValue(positions[LAST_SEQUENCE]);
        }
    }

    @Test
    public void ordinaryLastSequenceNumberRetainsItsTailCheck() throws StreamCorruptedException {
        assertEquals(LAST_SEQUENCE, indexing.lastSequenceNumber(wire));
        assertTrackerScan(positions[LAST_SEQUENCE], 1, 0);
        assertEquals(2, sequence.calls);
    }

    @Test
    public void finitePositionKeepsInclusiveSemantics() throws StreamCorruptedException {
        assertEquals(33, indexing.sequenceForPosition(wire, positions[33], true));
        assertEquals(positions[INDEX_SPACING], wire.firstScannedPosition);
        assertEquals(2, wire.dataHeaders);
        assertEquals(1, sequence.calls);

        assertEquals(32, indexing.sequenceForPosition(wire, positions[33], false));
        assertEquals(2, sequence.calls);
    }

    private void assertTrackerScan(long start, int dataHeaders, int metadataHeaders) {
        assertEquals(start, wire.firstScannedPosition);
        assertEquals(dataHeaders, wire.dataHeaders);
        assertEquals(metadataHeaders, wire.metadataHeaders);
        assertEquals(0, wire.indexDocuments);
        assertEquals(0, wire.indexHeaders);
        assertEquals(1, indexing.linearScanByPositionCount());
    }

    private void assertIndexedScan() {
        assertEquals(positions[INDEX_SPACING], wire.firstScannedPosition);
        assertEquals(LAST_SEQUENCE - INDEX_SPACING + 1, wire.dataHeaders);
        assertEquals(1, wire.indexDocuments);
        assertEquals(1, wire.indexHeaders);
        assertEquals(1, indexing.linearScanByPositionCount());
    }

    private static final class RecordingWire extends BinaryWire implements ExcerptContext {
        private long firstScannedPosition = -1;
        private int dataHeaders;
        private int metadataHeaders;
        private int incompleteHeaders;
        private int indexDocuments;
        private int indexHeaders;

        @SuppressWarnings("deprecation")
        RecordingWire(MappedBytes bytes) {
            super(bytes);
            usePadding(true);
        }

        @Override
        public HeaderType readDataHeader(boolean includeMetaData) {
            HeaderType type = super.readDataHeader(includeMetaData);
            if (firstScannedPosition == -1)
                firstScannedPosition = bytes().readPosition();
            if (type == HeaderType.DATA)
                dataHeaders++;
            else if (type == HeaderType.META_DATA)
                metadataHeaders++;
            else if (type == HeaderType.NONE && (bytes().peekVolatileInt() & Wires.NOT_COMPLETE) != 0)
                incompleteHeaders++;
            return type;
        }

        @Override
        public DocumentContext readingDocument(long readLocation) {
            indexDocuments++;
            return super.readingDocument(readLocation);
        }

        @Override
        public void readMetaDataHeader() {
            indexHeaders++;
            super.readMetaDataHeader();
        }

        @Override
        public Wire wire() {
            return this;
        }

        @Override
        public Wire wireForIndex() {
            return this;
        }

        @Override
        public long timeoutMS() {
            return 0;
        }
    }

    private static final class CountingSequence implements Sequence {
        private final Sequence delegate;
        private int calls;
        private int retriesRemaining;
        private Runnable beforeFirstRead;

        CountingSequence(Sequence delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getSequence(long position) {
            calls++;
            if (calls == 1 && beforeFirstRead != null)
                beforeFirstRead.run();
            if (retriesRemaining > 0) {
                retriesRemaining--;
                return NOT_FOUND_RETRY;
            }
            return delegate.getSequence(position);
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
