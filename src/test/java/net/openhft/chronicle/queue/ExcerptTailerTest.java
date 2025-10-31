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
