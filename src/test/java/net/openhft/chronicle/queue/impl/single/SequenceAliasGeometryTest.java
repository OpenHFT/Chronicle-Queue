/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Sequence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class SequenceAliasGeometryTest extends QueueTestCommon {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void customIndexGeometryMustUseTheLiveCodecAliasPeriod(boolean inclusive) throws Exception {
        try (Fixture f = new Fixture()) {
            assertEquals(2, f.appender.store.indexing.sequenceForPosition(f.appender, Long.MAX_VALUE, inclusive),
                    "The independent indexed lookup must find all three committed records");
            assertEquals(2, f.appender.store.sequenceForPosition(f.appender, Long.MAX_VALUE, inclusive));
        }
    }

    @Test
    void lastSequenceNumberWithCustomIndexGeometryMustUseTheLiveCodecAliasPeriod() throws Exception {
        try (Fixture f = new Fixture()) {
            assertEquals(2, f.appender.store.lastSequenceNumber(f.appender));
            assertEquals(f.appender.lastIndexAppended(), f.queue.lastIndex());
        }
    }

    @Test
    void lastSequenceNumberRejectsSubPeriodCaptureAfterWriterAdvances() throws Exception {
        assertSubPeriodCapture(Lookup.LAST_SEQUENCE, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void maxPositionRejectsSubPeriodCaptureAfterWriterAdvances(boolean inclusive) throws Exception {
        assertSubPeriodCapture(Lookup.MAX_POSITION, inclusive);
    }

    @Test
    void moveToIndexRejectsSubPeriodCaptureAfterWriterAdvances() throws Exception {
        assertSubPeriodCapture(Lookup.INDEX, false);
    }

    @Test
    void finitePositionRejectsSubPeriodCaptureAfterWriterAdvances() throws Exception {
        assertSubPeriodCapture(Lookup.FINITE_POSITION, true);
    }

    @Test
    void forwardToEndRejectsSubPeriodCaptureAfterWriterAdvances() throws Exception {
        assertSubPeriodCapture(Lookup.FORWARD_END, false);
    }

    private enum Lookup { LAST_SEQUENCE, MAX_POSITION, INDEX, FINITE_POSITION, FORWARD_END }

    private void assertSubPeriodCapture(Lookup lookup, boolean inclusive) throws Exception {
        long aliasPeriod = 1L << 24;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .blockSize(32 << 20).timeProvider(() -> 0L).rollCycle(LargeRollCycles.HUGE_DAILY).build();
             StoreAppender appender = (StoreAppender) queue.createAppender();
             StoreTailer tailer = (StoreTailer) queue.createTailer()) {
            appender.writeText("first");
            appender.writeBytes(bytes -> bytes.writeSkip(aliasPeriod - 4));
            tailer.toStart();
            SCQIndexing indexing = lookup == Lookup.INDEX || lookup == Lookup.FORWARD_END
                    ? tailer.store.indexing : appender.store.indexing;
            long capturedPosition = appender.store.writePosition();
            assertTrue(capturedPosition < aliasPeriod, "The captured position must precede the wrap");
            Sequence real = indexing.sequence;
            AtomicBoolean published = new AtomicBoolean();
            indexing.sequence = new Sequence() {
                @Override
                public long getSequence(long position) {
                    if (published.compareAndSet(false, true))
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
                long result;
                switch (lookup) {
                    case LAST_SEQUENCE:
                        result = appender.store.lastSequenceNumber(appender);
                        break;
                    case MAX_POSITION:
                        result = appender.store.sequenceForPosition(appender, Long.MAX_VALUE, inclusive);
                        break;
                    case INDEX:
                        assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(0, 2)));
                        assertEquals("last", tailer.readText());
                        result = queue.rollCycle().toSequenceNumber(tailer.lastReadIndex());
                        break;
                    case FINITE_POSITION:
                        result = appender.store.sequenceForPosition(appender, capturedPosition, inclusive);
                        break;
                    case FORWARD_END:
                        tailer.toEnd();
                        result = queue.rollCycle().toSequenceNumber(tailer.index());
                        assertNull(tailer.readText());
                        break;
                    default:
                        throw new AssertionError(lookup);
                }
                assertTrue(published.get());
                assertEquals(aliasPeriod, appender.store.writePosition() - capturedPosition);
                assertEquals(2, real.getSequence(capturedPosition), "The older position must alias the new tracker");
                assertThreeRecords(queue);
                long expected = lookup == Lookup.FINITE_POSITION ? 1 : lookup == Lookup.FORWARD_END ? 3 : 2;
                assertEquals(expected, result, "The sequence must belong to the physical record");
            } finally {
                indexing.sequence = real;
            }
        }
    }

    private static void assertThreeRecords(SingleChronicleQueue queue) {
        try (ExcerptTailer forward = queue.createTailer()) {
            for (int i = 0; i < 3; i++) {
                try (DocumentContext dc = forward.readingDocument()) {
                    assertTrue(dc.isPresent());
                    assertEquals(queue.rollCycle().toIndex(queue.cycle(), i), dc.index());
                    if (i != 1)
                        assertEquals(i == 0 ? "first" : "last", dc.wire().getValueIn().text());
                }
            }
            try (DocumentContext dc = forward.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
    }

    private final class Fixture implements AutoCloseable {
        final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .blockSize(1 << 20).timeProvider(() -> 0L).rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE)
                .indexCount(8).indexSpacing(16).build();
        final StoreAppender appender = (StoreAppender) queue.createAppender();
        final long lastPosition;

        Fixture() {
            appender.writeText("first");
            appender.writeBytes(bytes -> bytes.writeSkip(65532));
            long previousPosition = appender.store.writePosition();
            appender.writeText("last");
            lastPosition = appender.store.writePosition();
            assertEquals(65536, lastPosition - previousPosition);
            assertEquals(8, appender.store.indexing.indexCount());
            assertEquals(16, appender.store.indexing.indexSpacing());

            // Model the real publication window after the full position but before the tracker.
            appender.store.indexing.sequence.setSequence(1, previousPosition);
            assertEquals(1, appender.store.indexing.sequence.getSequence(lastPosition),
                    "The live codec still uses the roll cycle's 65,536-byte alias period");
            assertThreeRecords(queue);
        }

        @Override
        public void close() {
            appender.store.indexing.sequence.setSequence(2, lastPosition);
            appender.close();
            queue.close();
        }
    }
}
