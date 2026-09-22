/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Sequence;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Real-file counterexamples to inferring an alias bound from one physical record. */
class SequenceAliasGapTest extends QueueTestCommon {
    private static final long PERIOD = 65536;

    @ParameterizedTest
    @CsvSource({"2,false,false", "2,false,true", "1,true,false", "1,true,true", "2,true,false", "2,true,true"})
    void staleTrackerAcrossMultiplePeriodsOrMetadata(int periods, boolean metadata, boolean inclusive) throws Exception {
        try (SingleChronicleQueue queue = queue();
             StoreAppender appender = (StoreAppender) queue.createAppender()) {
            long previous = writePrefix(appender, periods, metadata);
            appender.writeText("last");
            long last = appender.store.writePosition();
            assertEquals(periods * PERIOD, last - previous);
            assertPhysicalRecords(queue, metadata ? 16 : periods * PERIOD - 4);
            Sequence codec = appender.store.indexing.sequence;
            codec.setSequence(1, previous);
            try {
                assertEquals(1, codec.getSequence(last));
                assertEquals(2, appender.store.sequenceForPosition(appender, Long.MAX_VALUE, inclusive));
                // An independently mapped reader has no in-process knowledge of these writes.
                try (SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(queue.file())
                        .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
                     StoreTailer cold = (StoreTailer) reader.createTailer()) {
                    assertNotNull(cold.store);
                    assertEquals(2, cold.store.lastSequenceNumber(cold));
                }
            } finally {
                codec.setSequence(2, last);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"2,false,false", "2,false,true", "1,true,false", "1,true,true", "2,true,false", "2,true,true"})
    void newerTrackerAcrossMultiplePeriodsOrMetadata(int periods, boolean metadata, boolean inclusive) throws Exception {
        try (SingleChronicleQueue queue = queue();
             StoreAppender appender = (StoreAppender) queue.createAppender()) {
            long previous = writePrefix(appender, periods, metadata);
            SCQIndexing indexing = appender.store.indexing;
            Sequence codec = indexing.sequence;
            AtomicBoolean injected = new AtomicBoolean();
            indexing.sequence = new Sequence() {
                public long getSequence(long position) {
                    if (injected.compareAndSet(false, true))
                        appender.writeText("last");
                    return codec.getSequence(position);
                }

                public void setSequence(long sequence, long position) {
                    codec.setSequence(sequence, position);
                }

                public long toIndex(long headerNumber, long sequence) {
                    return codec.toIndex(headerNumber, sequence);
                }

                public long toSequenceNumber(long index) {
                    return codec.toSequenceNumber(index);
                }
            };
            try {
                assertEquals(2, appender.store.sequenceForPosition(appender, Long.MAX_VALUE, inclusive));
                assertTrue(injected.get());
                assertEquals(periods * PERIOD, appender.store.writePosition() - previous);
                assertEquals(2, codec.getSequence(previous));
                assertPhysicalRecords(queue, metadata ? 16 : periods * PERIOD - 4);
            } finally {
                indexing.sequence = codec;
            }
        }
    }

    private long writePrefix(StoreAppender appender, int periods, boolean metadata) {
        appender.writeText("first");
        appender.writeBytes(bytes -> bytes.writeSkip(metadata ? 16 : periods * PERIOD - 4));
        long previous = appender.store.writePosition();
        if (metadata) {
            try (DocumentContext dc = appender.writingDocument(true)) {
                long remaining = previous + periods * PERIOD - dc.wire().bytes().writePosition();
                assertTrue(remaining > 0);
                dc.wire().bytes().writeSkip(remaining);
            }
            assertEquals(previous, appender.store.writePosition(), "metadata must not advance the data tracker");
        }
        return previous;
    }

    private void assertPhysicalRecords(SingleChronicleQueue queue, long fillerSize) {
        try (ExcerptTailer tailer = queue.createTailer()) {
            assertEquals("first", tailer.readText());
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals(queue.rollCycle().toIndex(0, 1), dc.index());
                assertEquals(fillerSize, dc.wire().bytes().readRemaining());
            }
            assertEquals("last", tailer.readText());
            assertEquals(queue.rollCycle().toIndex(0, 2), tailer.lastReadIndex());
            assertNull(tailer.readText());
        }
    }

    private SingleChronicleQueue queue() {
        return SingleChronicleQueueBuilder.binary(getTmpDir()).rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE)
                .blockSize(1 << 20).timeProvider(() -> 0L).build();
    }
}
