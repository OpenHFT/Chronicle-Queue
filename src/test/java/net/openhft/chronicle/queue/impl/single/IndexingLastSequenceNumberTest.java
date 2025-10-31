/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
