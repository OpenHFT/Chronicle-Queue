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
