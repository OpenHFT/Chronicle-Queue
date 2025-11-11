/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.ExcerptContext;
import org.junit.jupiter.api.Test;

import java.io.StreamCorruptedException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests focussed on {@link Indexing#lastSequenceNumber(ExcerptContext)}.
 */
class IndexingLastSequenceNumberTest extends IndexingTestCommon {

    @Test
    void checkIndexingSpacing() {
        appender.writeText("test");
        assertEquals(rollCycle().defaultIndexSpacing(), indexing(queue).indexSpacing());
    }

    @Test
    void singleCycleOneEntryApproximateLookup() throws StreamCorruptedException {
        appender.writeText("hello");
        Indexing indexing = indexing(queue);
        int linearScanByPositionCountStart = indexing.linearScanByPositionCount();
        assertEquals(0, linearScanByPositionCountStart);
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(0, lastSequenceNumber);
        assertEquals(1, indexing.linearScanByPositionCount());
    }

    @Test
    void singleCycleOneEntryPreciseLookup() throws StreamCorruptedException {
        appender.writeText("hello");
        Indexing indexing = indexing(queue);
        int linearScanByPositionCountStart = indexing.linearScanByPositionCount();
        assertEquals(0, linearScanByPositionCountStart);
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(0, lastSequenceNumber);
        assertEquals(1, indexing.linearScanByPositionCount());
    }

    @Test
    void singleCycleTwoEntries() throws StreamCorruptedException {
        appender.writeText("hello");
        appender.writeText("world");
        Indexing indexing = indexing(queue);
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(1, lastSequenceNumber);
        assertEquals(1, indexing.linearScanByPositionCount());
    }

    @Test
    void multipleCycleFilesFirstEntry() throws StreamCorruptedException {
        appender.writeText("a");
        timeProvider.advanceMillis(1_001);
        appender.writeText("b");
        timeProvider.advanceMillis(1_001);
        appender.writeText("c");
        long lastSequenceNumber = indexing(queue).lastSequenceNumber(appender);
        assertEquals(0, lastSequenceNumber);
    }

    @Test
    void multipleCycleFilesSecondEntry() throws StreamCorruptedException {
        appender.writeText("a");
        timeProvider.advanceMillis(1_001);
        appender.writeText("b");
        timeProvider.advanceMillis(1_001);
        appender.writeText("c");
        appender.writeText("d");
        long lastSequenceNumber = indexing(queue).lastSequenceNumber(appender);
        assertEquals(1, lastSequenceNumber);
    }
}
