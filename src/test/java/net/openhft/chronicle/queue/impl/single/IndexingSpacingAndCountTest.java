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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexingSpacingAndCountTest extends IndexingTestCommon {

    @Test
    void firstEntryIndexed() {
        appender.writeText("hello");
        long lastIndexAppended = appender.lastIndexAppended();
        Indexing indexing = indexing(queue);
        assertEquals(0, lastIndexAppended);
        assertTrue(indexing.indexable(lastIndexAppended));
        assertTrue(indexing.indexable(lastIndexAppended));
    }

    @Test
    void everyNthEntryIsIndexable() {
        appender.writeText("start");
        Indexing indexing = indexing(queue);
        for (int i = 0; i < indexing.indexSpacing() * indexing.indexCount(); i++) {
            long lastIndexAppended = appender.lastIndexAppended();
            if (lastIndexAppended % indexing.indexSpacing() == 0) {
                assertTrue(indexing.indexable(lastIndexAppended));
            }
            appender.writeText("<test>");
        }
    }
}
