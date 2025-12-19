/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"deprecation", "removal"})
class IndexingSpacingAndCountTest extends IndexingTestCommon {

    @Test
    void firstEntryIndexed() {
        appender.writeText("hello");
        long lastIndexAppended = appender.lastIndexAppended();
        Indexing indexing = indexing(queue);
        assertEquals(0, lastIndexAppended, "first append: lastIndexAppended");
        assertTrue(indexing.indexable(lastIndexAppended), "first append: entry should be indexable");
        assertTrue(indexing.indexable(lastIndexAppended), "first append: entry should be indexable");
    }

    @Test
    void everyNthEntryIsIndexable() {
        appender.writeText("start");
        Indexing indexing = indexing(queue);
        for (int i = 0; i < indexing.indexSpacing() * indexing.indexCount(); i++) {
            long lastIndexAppended = appender.lastIndexAppended();
            if (lastIndexAppended % indexing.indexSpacing() == 0) {
                assertTrue(indexing.indexable(lastIndexAppended), "index spacing: entry should be indexable");
            }
            appender.writeText("<test>");
        }
    }
}
