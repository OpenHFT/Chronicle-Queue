/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class ExcerptTailerTest extends QueueTestCommon {

    private ExcerptTailer excerptTailer;
    private ChronicleQueue queue;

    @BeforeEach
    public void setUp() {
        File dir = new File(System.getProperty("java.io.tmpdir"), "queue-test");
        queue = ChronicleQueue.single(dir.getPath());
        excerptTailer = queue.createTailer();
    }

    @AfterEach
    @Override
    public void tearDown() {
        excerptTailer.close();
        queue.close();
    }

    @Test
    @DisplayName("readingDocument includes metadata when requested from tailer")
    public void testReadingDocumentWithMetaData() {
        try (DocumentContext dc = excerptTailer.readingDocument(true)) {
            assertNotNull(dc, "reading document context should not be null when metadata is included");
        }
    }

    @Test
    @DisplayName("readingDocument excludes metadata when not requested")
    public void testReadingDocumentWithoutMetaData() {
        try (DocumentContext dc = excerptTailer.readingDocument(false)) {
            assertNotNull(dc, "reading document context should not be null when metadata is excluded");
        }
    }

    @Test
    @DisplayName("Index starts at zero or greater")
    public void testIndex() {
        long index = excerptTailer.index();
        assertTrue(index >= 0, "tailer index should be >= 0, index=" + index);
    }

    @Test
    @DisplayName("Last read index is unset before reading")
    public void testLastReadIndex() {
        // The last read index may return -1 or 0 depending on the state
        long lastReadIndex = excerptTailer.lastReadIndex();
        assertTrue(lastReadIndex == -1 || lastReadIndex == 0,
                "lastReadIndex should be -1 or 0 before reads, lastReadIndex=" + lastReadIndex);
    }

    @Test
    @DisplayName("Cycle is unset before any reads")
    public void testCycle() {
        // Since no data has been read, cycle should be Integer.MIN_VALUE as no cycle has been loaded
        int cycle = excerptTailer.cycle();
        assertEquals(Integer.MIN_VALUE, cycle, "tailer cycle should be MIN_VALUE when no data has been read");
    }
}
