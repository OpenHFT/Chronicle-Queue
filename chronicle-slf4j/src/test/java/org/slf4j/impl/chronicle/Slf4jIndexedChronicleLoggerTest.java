/*
 * Copyright 2014 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.slf4j.impl.chronicle;

import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.slf4j.ChronicleLoggingHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class Slf4jIndexedChronicleLoggerTest extends Slf4jChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Before
    public void setUp() {
        System.setProperty(
            "slf4j.chronicle.properties",
            System.getProperty("slf4j.chronicle.indexed.properties")
        );
    }

    @After
    public void tearDown() {
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Ignore
    @Test
    public void testLogging1() throws IOException {
        Thread.currentThread().setName("th-test-logging_1");

        Logger l = LoggerFactory.getLogger("readwrite");
        l.trace("trace");
        l.debug("debug");
        l.info("info");
        l.warn("warn");
        l.error("error");

        IndexedChronicle reader = new IndexedChronicle(BASEPATH_LOGGER_RW);
        ExcerptTailer tailer = reader.createTailer().toStart();

        // debug
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite",tailer.readEnum(String.class));
        assertEquals("debug",tailer.readEnum(String.class));

        // info
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_INFO, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite",tailer.readEnum(String.class));
        assertEquals("info",tailer.readEnum(String.class));

        // warn
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_WARN, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite",tailer.readEnum(String.class));
        assertEquals("warn",tailer.readEnum(String.class));

        // error
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_ERROR, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite",tailer.readEnum(String.class));
        assertEquals("error",tailer.readEnum(String.class));

        assertFalse(tailer.nextIndex());

        tailer.close();
        reader.close();
    }
}
