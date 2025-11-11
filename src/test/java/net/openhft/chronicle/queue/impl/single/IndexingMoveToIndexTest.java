/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IndexingMoveToIndexTest extends IndexingTestCommon {

    @Test
    void noDataNegativeIndex() {
        assertFalse(tailer.moveToIndex(-1));
        assertEquals(0, tailer.index());
    }

    @Test
    void someDataNegativeIndex() {
        appender.writeText("a");
        assertFalse(tailer.moveToIndex(-1));
        assertEquals(0, tailer.index());
    }

    @Test
    void noData() {
        assertFalse(tailer.moveToIndex(0));
        assertEquals(0, tailer.index());
    }

    @Test
    void onEntry() {
        appender.writeText("test");
        assertTrue(tailer.moveToIndex(0));
        assertEquals(0, tailer.index());
    }

    /**
     * Surprising results here. Even though the tailer doesn't exist the tailer index is still moved to this position.
     * This should be investigated separately.
     */
    @Test
    void moveNonExistent() {
        appender.writeText("test");
        assertFalse(tailer.moveToIndex(1));
        assertEquals(1, tailer.index());
        assertNull(tailer.readText());
    }

    /**
     * See {@link #moveNonExistent()}. Same applies here.
     */
    @Test
    void moveNonExistentAtEndOfCycle() {
        appender.writeText("a");
        timeProvider.advanceMillis(1_001);
        appender.writeText("b");
        timeProvider.advanceMillis(1_001);
        appender.writeText("c");
        timeProvider.advanceMillis(1_001);
        long nonExistentIndex = queue.rollCycle().toIndex(1, 10);
        assertFalse(tailer.moveToIndex(nonExistentIndex));
        assertEquals(nonExistentIndex, tailer.index());
    }
}
