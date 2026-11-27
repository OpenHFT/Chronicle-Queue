/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import org.junit.jupiter.api.Test;

import java.io.StreamCorruptedException;

import static net.openhft.chronicle.wire.Sequence.NOT_FOUND;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparseTailScanTest extends IndexingTestCommon {
    @Override
    RollCycle rollCycle() {
        return SparseRollCycles.HUGE_DAILY_XSPARSE;
    }

    @Test
    void sparseTailScanReadsOnlyThePublishedRecordAndItsCommittedSuffix() throws StreamCorruptedException {
        for (int i = 0; i < 63; i++)
            appender.writeText("data");
        long published = appender.store.writePosition();
        // The roll's first index pages already take more than one tracker period.
        assertTrue(published > 1 << 16);
        appender.writeText("committed suffix");
        SCQIndexing indexing = appender.store.indexing;
        indexing.writePosition.setValue(published);
        indexing.sequence.setSequence(62, published);

        assertTailLookup(63, 3); // Published record, committed suffix, unwritten header.
    }

    @Test
    void writerCanIdentifyTheTailMoreThanOnePositionPeriodFromTheIndex() throws StreamCorruptedException {
        appender.writeBytes(bytes -> bytes.writeSkip(1 << 16));
        for (int i = 1; i < 256; i++)
            appender.writeText("data");
        assertTailLookup(255, 2); // Final record and unwritten header.
    }

    @Test
    void reacquiredStoresShareProgressOnlyWithinTheirOwnMapping() throws StreamCorruptedException {
        appender.writeBytes(bytes -> bytes.writeSkip(1 << 16));
        appender.writeText("last");
        long position = appender.store.writePosition();
        try (SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(queue.file())
                .testBlockSize().rollCycle(rollCycle()).timeProvider(timeProvider).build();
             SingleChronicleQueueStore cold = reader.storeForCycle(0, 0, false, null)) {
            assertEquals(NOT_FOUND, cold.indexing.sequence.getSequenceAt(position));

            try (StoreTailer readerTailer = (StoreTailer) reader.createTailer()) {
                assertEquals(2, readerTailer.excerptsInCycle(0));
                appender.writeText("next");
                position = appender.store.writePosition();
                assertEquals(3, readerTailer.excerptsInCycle(0));
            }

            try (SingleChronicleQueueStore reacquired = reader.storeForCycle(0, 0, false, null)) {
                assertNotSame(cold, reacquired);
                assertEquals(2, reacquired.indexing.sequence.getSequenceAt(position));
            }
        }
    }

    private void assertTailLookup(long sequence, int headers) throws StreamCorruptedException {
        class CountingWire extends BinaryWire {
            int headers;

            CountingWire() {
                super(appender.wireForIndex().bytes());
            }

            @Override
            public HeaderType readDataHeader(boolean includeMetaData) {
                headers++;
                return super.readDataHeader(includeMetaData);
            }
        }
        CountingWire counter = new CountingWire();
        Wire wire = counter;
        wire.usePadding(appender.wireForIndex().usePadding());
        ExcerptContext reader = createMock(ExcerptContext.class);
        expect(reader.wireForIndex()).andStubReturn(wire);
        replay(reader);
        assertEquals(sequence, appender.store.sequenceForPosition(reader, Long.MAX_VALUE, true));
        assertEquals(headers, counter.headers);

        counter.headers = 0;
        assertEquals(sequence, appender.store.lastSequenceNumber(reader));
        assertEquals(headers, counter.headers);
    }
}
