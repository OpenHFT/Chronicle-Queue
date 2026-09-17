/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Sequence;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

import static org.junit.Assert.*;

public class MaxPositionMutationTest extends QueueTestCommon {
    @Test
    public void maxPositionStartsAtPublishedWritePosition() throws Exception {
        for (boolean inclusive : new boolean[]{false, true}) {
            try (Fixture f = new Fixture(getTmpDir(), 13)) {
                assertEquals(12, f.lookup(Long.MAX_VALUE, inclusive));
                assertEquals("The tail lookup must start at the published record, not the sparse index",
                        f.positions[12], f.firstScanPosition);
                assertEquals("Only the last record and the unwritten header need scanning", 2, f.headersRead);
            }
        }
    }

    @Test
    public void maxPositionAcceptsSequenceZero() throws Exception {
        try (Fixture f = new Fixture(getTmpDir(), 1)) {
            f.scriptSequence(position -> 0);
            assertEquals(0, f.lookup(Long.MAX_VALUE, false));
            assertEquals("The matched tracker must be consumed before the scan", 2, f.sequenceReads);
        }
    }

    @Test
    public void maxPositionScansCommittedSuffixAfterPublishedWritePosition() throws Exception {
        for (boolean inclusive : new boolean[]{false, true}) {
            try (Fixture f = new Fixture(getTmpDir(), 13)) {
                f.scriptPosition(new AtomicLong(f.positions[9]));
                f.scriptSequence(position -> 9);
                assertEquals("Published tracker may lag already committed records", 12,
                        f.lookup(Long.MAX_VALUE, inclusive));
                assertEquals(f.positions[9], f.firstScanPosition);
                assertEquals(5, f.headersRead);
            }
        }
    }

    @Test
    public void maxPositionRetriesTransientTrackerRace() throws Exception {
        try (Fixture f = new Fixture(getTmpDir(), 13)) {
            f.scriptSequence(position -> f.sequenceReads <= 5 ? Sequence.NOT_FOUND_RETRY : 12);
            assertEquals(12, f.lookup(Long.MAX_VALUE, false));
            assertEquals("A transient mismatch must not force a sparse-index scan",
                    f.positions[12], f.firstScanPosition);
            assertEquals(7, f.sequenceReads);
        }
    }

    @Test
    public void maxPositionRefreshesWritePositionAfterRace() throws Exception {
        try (Fixture f = new Fixture(getTmpDir(), 13)) {
            AtomicLong published = new AtomicLong(f.positions[9]);
            f.scriptPosition(published);
            f.scriptSequence(position -> {
                if (f.sequenceReads == 1) {
                    published.set(f.positions[12]);
                    return Sequence.NOT_FOUND_RETRY;
                }
                return position == f.positions[12] ? 12 : Sequence.NOT_FOUND_RETRY;
            });
            assertEquals(12, f.lookup(Long.MAX_VALUE, false));
            assertEquals(f.positions[12], f.firstScanPosition);
            assertEquals(3, f.sequenceReads);
        }
    }

    @Test
    public void maxPositionFallsBackWithoutRetryWhenTrackerAbsent() throws Exception {
        for (boolean inclusive : new boolean[]{false, true}) {
            try (Fixture f = new Fixture(getTmpDir(), 13)) {
                f.scriptSequence(position -> Sequence.NOT_FOUND);
                assertEquals(12, f.lookup(Long.MAX_VALUE, inclusive));
                assertEquals(f.positions[0], f.firstScanPosition);
                assertEquals("An absent tracker needs one attempt, then the ordinary scan", 2, f.sequenceReads);
            }
        }
    }

    @Test(timeout = 5000)
    public void maxPositionFallsBackWithinRetryBudget() throws Exception {
        for (boolean inclusive : new boolean[]{false, true}) {
            try (Fixture f = new Fixture(getTmpDir(), 13)) {
                f.scriptSequence(position -> {
                    if (f.sequenceReads > 129)
                        throw new AssertionError("Tracker loop exceeded the bounded read budget");
                    return Sequence.NOT_FOUND_RETRY;
                });
                assertEquals(12, f.lookup(Long.MAX_VALUE, inclusive));
                assertEquals(f.positions[0], f.firstScanPosition);
                assertTrue("Tracker retries plus the final scan must stay within 129 reads: " + f.sequenceReads,
                        f.sequenceReads <= 129);
            }
        }
    }

    @Test
    public void finitePositionPreservesInclusiveAndExclusiveLookup() throws Exception {
        for (boolean inclusive : new boolean[]{false, true}) {
            try (Fixture f = new Fixture(getTmpDir(), 13)) {
                assertEquals(inclusive ? 11 : 10, f.lookup(f.positions[11], inclusive));
            }
        }
    }

    @Test
    public void emptyStoreReturnsMinusOne() throws Exception {
        try (Fixture f = new Fixture(getTmpDir(), 0)) {
            assertEquals(-1, f.lookup(Long.MAX_VALUE, false));
            assertEquals(-1, f.lookup(Long.MAX_VALUE, true));
        }
    }

    @Test
    public void maxPositionStopsAtRolledFileEof() throws Exception {
        for (boolean inclusive : new boolean[]{false, true}) {
            try (Fixture f = new Fixture(getTmpDir(), 13)) {
                f.clock.set(f.queue.rollCycle().lengthInMillis());
                f.appender.writeText("next-cycle");
                assertEquals(12, f.lookup(Long.MAX_VALUE, inclusive));
                assertEquals(f.positions[12], f.firstScanPosition);
            }
        }
    }

    @Test
    public void maxPositionSkipsTrailingMetadata() throws Exception {
        try (Fixture f = new Fixture(getTmpDir(), 13)) {
            try (DocumentContext dc = f.appender.writingDocument(true)) {
                dc.wire().write("metadata").text("not a data sequence");
            }
            assertEquals(12, f.lookup(Long.MAX_VALUE, false));
            assertEquals(f.positions[12], f.firstScanPosition);
        }
    }

    @Test
    public void maxPositionStopsBeforeUncommittedDocument() throws Exception {
        try (Fixture f = new Fixture(getTmpDir(), 13)) {
            try (DocumentContext dc = f.appender.writingDocument()) {
                dc.wire().write("pending").text("not committed yet");
                assertEquals(12, f.lookup(Long.MAX_VALUE, false));
                dc.rollbackOnClose();
            }
        }
    }

    static final class Fixture implements AutoCloseable, ExcerptContext {
        final SingleChronicleQueue queue;
        final ExcerptAppender appender;
        final StoreTailer tailer;
        final SingleChronicleQueueStore store;
        final Sequence originalSequence;
        final LongValue originalPosition;
        final long[] positions;
        final Wire indexWire;
        final AtomicLong clock = new AtomicLong();
        long firstScanPosition = -1;
        int headersRead;
        int sequenceReads;

        Fixture(File path, int count) {
            queue = SingleChronicleQueueBuilder.binary(path).testBlockSize()
                    .timeProvider(clock::get).indexCount(8).indexSpacing(16).build();
            appender = queue.createAppender();
            store = queue.storeForCycle(queue.cycle(), 0, true, null);
            positions = new long[count];
            long firstIndex = 0;
            for (int i = 0; i < count; i++) {
                appender.writeText("record-" + i);
                if (i == 0)
                    firstIndex = appender.lastIndexAppended();
                positions[i] = store.writePosition();
            }
            tailer = (StoreTailer) queue.createTailer();
            if (count > 0)
                assertTrue(tailer.moveToIndex(firstIndex));
            else
                tailer.moveToIndex(queue.rollCycle().toIndex(queue.cycle(), 0));
            originalSequence = store.indexing.sequence;
            originalPosition = store.indexing.writePosition;
            Wire delegate = tailer.wireForIndex();
            assertNotNull(delegate);
            indexWire = (Wire) Proxy.newProxyInstance(Wire.class.getClassLoader(), new Class<?>[]{Wire.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("readDataHeader")) {
                            if (firstScanPosition < 0)
                                firstScanPosition = delegate.bytes().readPosition();
                            headersRead++;
                        }
                        try {
                            return method.invoke(delegate, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        long lookup(long position, boolean inclusive) throws Exception {
            firstScanPosition = -1;
            headersRead = 0;
            sequenceReads = 0;
            return store.sequenceForPosition(this, position, inclusive);
        }

        void scriptSequence(LongUnaryOperator result) {
            store.indexing.sequence = new Sequence() {
                public long getSequence(long position) {
                    sequenceReads++;
                    return result.applyAsLong(position);
                }
                public void setSequence(long sequence, long position) {
                    originalSequence.setSequence(sequence, position);
                }
                public long toIndex(long header, long sequence) {
                    return originalSequence.toIndex(header, sequence);
                }
                public long toSequenceNumber(long index) {
                    return originalSequence.toSequenceNumber(index);
                }
            };
        }

        void scriptPosition(AtomicLong published) {
            store.indexing.writePosition = (LongValue) Proxy.newProxyInstance(LongValue.class.getClassLoader(),
                    new Class<?>[]{LongValue.class}, (proxy, method, args) -> {
                        if (method.getName().equals("getVolatileValue"))
                            return published.get();
                        try {
                            return method.invoke(originalPosition, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        public Wire wire() { return tailer.wire(); }
        public Wire wireForIndex() { return indexWire; }
        public long timeoutMS() { return 1000; }

        public void close() {
            store.indexing.sequence = originalSequence;
            store.indexing.writePosition = originalPosition;
            tailer.close();
            store.close();
            appender.close();
            queue.close();
        }
    }
}
