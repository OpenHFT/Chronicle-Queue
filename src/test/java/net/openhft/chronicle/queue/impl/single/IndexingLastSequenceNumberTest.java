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
    @SuppressWarnings("deprecation")
    void checkIndexingSpacing() {
        appender.writeText("test");
        assertEquals(rollCycle().defaultIndexSpacing(), indexing(queue).indexSpacing());
    }

    @Test
    @SuppressWarnings("deprecation")
    void singleCycleOneEntryApproximateLookup() throws StreamCorruptedException {
        appender.writeText("hello");
        Indexing indexing = indexing(queue);
        int linearScanByPositionCountStart = indexing.linearScanByPositionCount();
        assertEquals(0, linearScanByPositionCountStart);
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(0, lastSequenceNumber);
        assertEquals(0, indexing.linearScanByPositionCount());
    }

    @Test
    @SuppressWarnings("deprecation")
    void singleCycleOneEntryPreciseLookup() throws StreamCorruptedException {
        appender.writeText("hello");
        Indexing indexing = indexing(queue);
        int linearScanByPositionCountStart = indexing.linearScanByPositionCount();
        assertEquals(0, linearScanByPositionCountStart);
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(0, lastSequenceNumber);
        assertEquals(0, indexing.linearScanByPositionCount());
    }

    @Test
    @SuppressWarnings("deprecation")
    void singleCycleTwoEntries() throws StreamCorruptedException {
        appender.writeText("hello");
        appender.writeText("world");
        Indexing indexing = indexing(queue);
        long lastSequenceNumber = indexing.lastSequenceNumber(appender);
        assertEquals(1, lastSequenceNumber);
        assertEquals(0, indexing.linearScanByPositionCount());
    }

    @Test
    @SuppressWarnings("deprecation")
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
    @SuppressWarnings("deprecation")
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
