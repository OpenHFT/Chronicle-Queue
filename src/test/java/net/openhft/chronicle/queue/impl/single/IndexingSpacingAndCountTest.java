/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
