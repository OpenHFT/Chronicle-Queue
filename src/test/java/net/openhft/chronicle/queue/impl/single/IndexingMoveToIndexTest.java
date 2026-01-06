/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IndexingMoveToIndexTest extends IndexingTestCommon {

    @Test
    @DisplayName("moveToIndex rejects negative index on empty queue")
    void noDataNegativeIndex() {
        assertFalse(tailer.moveToIndex(-1), "tailer should reject moveToIndex(-1) on empty queue");
        assertEquals(0, tailer.index(), "tailer index should remain at 0 after invalid move");
    }

    @Test
    @DisplayName("moveToIndex rejects negative index with data present")
    void someDataNegativeIndex() {
        appender.writeText("a");
        assertFalse(tailer.moveToIndex(-1), "tailer should reject moveToIndex(-1) even with data");
        assertEquals(0, tailer.index(), "tailer index should remain at 0 after negative move");
    }

    @Test
    @DisplayName("moveToIndex(0) fails on empty queue")
    void noData() {
        assertFalse(tailer.moveToIndex(0), "tailer should not move to index 0 on empty queue");
        assertEquals(0, tailer.index(), "tailer index should remain at 0 on empty queue");
    }

    @Test
    @DisplayName("moveToIndex(0) succeeds when entry exists")
    void onEntry() {
        appender.writeText("test");
        assertTrue(tailer.moveToIndex(0), "tailer should move to index 0 when entry exists");
        assertEquals(0, tailer.index(), "tailer index should be 0 after move to first entry");
    }

    /**
     * Surprising results here. Even though the tailer doesn't exist the tailer index is still moved to this position.
     * This should be investigated separately.
     */
    @Test
    @DisplayName("moveToIndex on missing entry updates tailer index")
    void moveNonExistent() {
        appender.writeText("test");
        assertFalse(tailer.moveToIndex(1), "tailer should reject moveToIndex(1) when entry is missing");
        assertEquals(1, tailer.index(), "tailer index should move to requested missing index");
        assertNull(tailer.readText(), "tailer should not read text at missing index");
    }

    /**
     * See {@link #moveNonExistent()} for behaviour when moveToIndex targets a missing entry.
     */
    @Test
    @DisplayName("moveToIndex to missing entry near cycle end leaves tailer index")
    void moveNonExistentAtEndOfCycle() {
        appender.writeText("a");
        timeProvider.advanceMillis(1_001);
        appender.writeText("b");
        timeProvider.advanceMillis(1_001);
        appender.writeText("c");
        timeProvider.advanceMillis(1_001);
        long nonExistentIndex = queue.rollCycle().toIndex(1, 10);
        assertFalse(tailer.moveToIndex(nonExistentIndex), "tailer should reject moveToIndex(nonExistentIndex)");
        assertEquals(nonExistentIndex, tailer.index(), "tailer index should track requested missing index");
    }
}
