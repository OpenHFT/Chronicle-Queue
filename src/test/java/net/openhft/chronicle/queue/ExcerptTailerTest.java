/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class ExcerptTailerTest extends QueueTestCommon {

    private ExcerptTailer excerptTailer;
    private ChronicleQueue queue;

    @Before
    public void setUp() {
        File dir = new File(System.getProperty("java.io.tmpdir"), "queue-test");
        queue = ChronicleQueue.single(dir.getPath());
        excerptTailer = queue.createTailer();
    }

    @After
    public void tearDown() {
        excerptTailer.close();
        queue.close();
    }

    @Test
    public void testReadingDocumentWithMetaData() {
        try (DocumentContext dc = excerptTailer.readingDocument(true)) {
            assertNotNull(dc);
        }
    }

    @Test
    public void testReadingDocumentWithoutMetaData() {
        try (DocumentContext dc = excerptTailer.readingDocument(false)) {
            assertNotNull(dc);
        }
    }

    @Test
    public void testIndex() {
        long index = excerptTailer.index();
        assertTrue(index >= 0);
    }

    @Test
    public void testLastReadIndex() {
        // The last read index may return -1 or 0 depending on the state
        long lastReadIndex = excerptTailer.lastReadIndex();
        assertTrue(lastReadIndex == -1 || lastReadIndex == 0);
    }

    @Test
    public void testCycle() {
        // Since no data has been read, cycle should be Integer.MIN_VALUE as no cycle has been loaded
        int cycle = excerptTailer.cycle();
        assertEquals(Integer.MIN_VALUE, cycle);
    }
}
