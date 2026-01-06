/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"deprecation", "removal"})
class IndexingSpacingAndCountTest extends IndexingTestCommon {

    @Test
    @DisplayName("First entry is indexed on initial append")
    void firstEntryIndexed() {
        appender.writeText("hello");
        long lastIndexAppended = appender.lastIndexAppended();
        Indexing indexing = indexing(queue);
        assertEquals(0, lastIndexAppended, "First append should set lastIndexAppended to zero");
        assertTrue(indexing.indexable(lastIndexAppended), "First entry should be indexable");
    }

    @Test
    @DisplayName("Index spacing selects every nth entry")
    void everyNthEntryIsIndexable() {
        appender.writeText("start");
        Indexing indexing = indexing(queue);
        for (int i = 0; i < indexing.indexSpacing() * indexing.indexCount(); i++) {
            long lastIndexAppended = appender.lastIndexAppended();
            if (lastIndexAppended % indexing.indexSpacing() == 0) {
                assertTrue(indexing.indexable(lastIndexAppended),
                        "Entry should be indexable at iteration " + i);
            }
            appender.writeText("<test>");
        }
    }
}
