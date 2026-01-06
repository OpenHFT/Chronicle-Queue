/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.ExcerptContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StreamCorruptedException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests focussed on {@link Indexing#lastSequenceNumber(ExcerptContext)} behaviour across cycle boundaries and lookup modes.
 */
@SuppressWarnings({"deprecation", "removal"})
class IndexingLastSequenceNumberTest extends IndexingTestCommon {

    @Test
    @DisplayName("Index spacing matches roll cycle defaults")
    void checkIndexingSpacing() {
        appender.writeText("test");
        assertEquals(rollCycle().defaultIndexSpacing(), indexing(queue).indexSpacing(), "index spacing should match roll cycle default");
    }

    @Test
    @DisplayName("Single cycle with one entry uses approximate lookup")
    void singleCycleOneEntryApproximateLookup() throws StreamCorruptedException {
        appender.writeText("hello");
        Indexing indexing = indexing(queue);
        int linearScanByPositionCountStart = indexing.linearScanByPositionCount();
        assertEquals(0, linearScanByPositionCountStart, "linear scan count should be zero before approximate lookup");
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(0, lastSequenceNumber, "last sequence number should be zero for single entry with approximate lookup");
        assertEquals(1, indexing.linearScanByPositionCount(), "linear scan should have been invoked once for approximate lookup");
    }

    @Test
    @DisplayName("Single cycle with one entry uses precise lookup")
    void singleCycleOneEntryPreciseLookup() throws StreamCorruptedException {
        appender.writeText("hello");
        Indexing indexing = indexing(queue);
        int linearScanByPositionCountStart = indexing.linearScanByPositionCount();
        assertEquals(0, linearScanByPositionCountStart, "linear scan count should be zero before precise lookup");
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(0, lastSequenceNumber, "last sequence number should be zero for single entry with precise lookup");
        assertEquals(1, indexing.linearScanByPositionCount(), "linear scan should have been invoked once for precise lookup");
    }

    @Test
    @DisplayName("Single cycle with two entries yields last sequence number 1")
    void singleCycleTwoEntries() throws StreamCorruptedException {
        appender.writeText("hello");
        appender.writeText("world");
        Indexing indexing = indexing(queue);
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(1, lastSequenceNumber, "last sequence number should be 1 when two entries exist");
        assertEquals(1, indexing.linearScanByPositionCount(), "linear scan should have been invoked once for two-entry queue");
    }

    @Test
    @DisplayName("Multiple cycle files report first entry in latest cycle")
    void multipleCycleFilesFirstEntry() throws StreamCorruptedException {
        appender.writeText("a");
        timeProvider.advanceMillis(1_001);
        appender.writeText("b");
        timeProvider.advanceMillis(1_001);
        appender.writeText("c");
        long lastSequenceNumber = indexing(queue).lastSequenceNumber(appender);
        assertEquals(0, lastSequenceNumber, "last sequence number should be 0 for first entry in latest cycle file");
    }

    @Test
    @DisplayName("Multiple cycle files report second entry in latest cycle")
    void multipleCycleFilesSecondEntry() throws StreamCorruptedException {
        appender.writeText("a");
        timeProvider.advanceMillis(1_001);
        appender.writeText("b");
        timeProvider.advanceMillis(1_001);
        appender.writeText("c");
        appender.writeText("d");
        long lastSequenceNumber = indexing(queue).lastSequenceNumber(appender);
        assertEquals(1, lastSequenceNumber, "last sequence number should be 1 for second entry in latest cycle file");
    }
}
