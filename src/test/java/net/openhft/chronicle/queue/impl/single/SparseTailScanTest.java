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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        assertEquals(63, appender.store.sequenceForPosition(reader, Long.MAX_VALUE, true));
        assertEquals(3, counter.headers, "published record, committed suffix, unwritten header");

        counter.headers = 0;
        assertEquals(63, appender.store.lastSequenceNumber(reader));
        assertEquals(3, counter.headers, "published record, committed suffix, unwritten header");
    }
}
