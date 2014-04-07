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
package net.openhft.chronicle.slf4j;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.slf4j.impl.ChronicleLogWriters;
import net.openhft.lang.io.IOTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 *
 */
public class IndexedChronicleLoggerTest extends ChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Before
    public void setUp() {
        System.setProperty(
                "slf4j.chronicle.properties",
                System.getProperty("slf4j.chronicle.indexed.properties")
        );

        getChronicleLoggerFactory().relaod();
        getChronicleLoggerFactory().warmup();
    }

    @After
    public void tearDown() {
        getChronicleLoggerFactory().shutdown();

        IOTools.deleteDir(basePath(ChronicleLoggingConfig.TYPE_INDEXED));
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLoggerFactory() {
        assertEquals(
                StaticLoggerBinder.getSingleton().getLoggerFactory().getClass(),
                ChronicleLoggerFactory.class);
    }

    @Test
    public void testLogger() {
        Logger l1 = LoggerFactory.getLogger(VanillaChronicleLoggerTest.class);
        Logger l2 = LoggerFactory.getLogger(VanillaChronicleLoggerTest.class);
        Logger l3 = LoggerFactory.getLogger("Logger1");
        Logger l4 = LoggerFactory.getLogger("readwrite");

        assertNotNull(l1);
        assertEquals(l1.getClass(), ChronicleLogger.class);

        assertNotNull(l2);
        assertEquals(l2.getClass(), ChronicleLogger.class);

        assertNotNull(l3);
        assertEquals(l3.getClass(), ChronicleLogger.class);

        assertNotNull(l4);
        assertEquals(l4.getClass(), ChronicleLogger.class);


        assertEquals(l1, l2);
        assertNotEquals(l1, l3);
        assertNotEquals(l3, l4);
        assertNotEquals(l1, l4);

        ChronicleLogger cl1 = (ChronicleLogger) l1;

        assertEquals(cl1.getLevel(), ChronicleLoggingHelper.LOG_LEVEL_DEBUG);
        assertEquals(cl1.getName(), VanillaChronicleLoggerTest.class.getName());
        assertTrue(cl1.getWriter().getChronicle() instanceof IndexedChronicle);
        assertTrue(cl1.getWriter() instanceof ChronicleLogWriters.SynchronizedWriter);

        ChronicleLogger cl2 = (ChronicleLogger) l2;
        assertEquals(cl2.getLevel(), ChronicleLoggingHelper.LOG_LEVEL_DEBUG);
        assertEquals(cl2.getName(), VanillaChronicleLoggerTest.class.getName());
        assertTrue(cl2.getWriter().getChronicle() instanceof IndexedChronicle);
        assertTrue(cl2.getWriter() instanceof ChronicleLogWriters.SynchronizedWriter);

        ChronicleLogger cl3 = (ChronicleLogger) l3;
        assertEquals(cl3.getLevel(), ChronicleLoggingHelper.LOG_LEVEL_INFO);
        assertTrue(cl3.getWriter().getChronicle() instanceof IndexedChronicle);
        assertTrue(cl3.getWriter() instanceof ChronicleLogWriters.SynchronizedWriter);
        assertEquals(cl3.getName(), "Logger1");

        ChronicleLogger cl4 = (ChronicleLogger) l4;
        assertEquals(cl4.getLevel(), ChronicleLoggingHelper.LOG_LEVEL_DEBUG);
        assertTrue(cl4.getWriter().getChronicle() instanceof IndexedChronicle);
        assertTrue(cl4.getWriter() instanceof ChronicleLogWriters.SynchronizedWriter);
        assertEquals(cl4.getName(), "readwrite");
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLogging() throws IOException {
        Thread.currentThread().setName("th-test-logging_1");

        Logger l = LoggerFactory.getLogger("readwrite");
        l.trace("trace");
        l.debug("debug");
        l.info("info");
        l.warn("warn");
        l.error("error");

        Chronicle reader = new IndexedChronicle(basePath(ChronicleLoggingConfig.TYPE_INDEXED, "readwrite"));
        ExcerptTailer tailer = reader.createTailer().toStart();

        // debug
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite", tailer.readEnum(String.class));
        assertEquals("debug", tailer.readEnum(String.class));

        // info
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_INFO, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite", tailer.readEnum(String.class));
        assertEquals("info", tailer.readEnum(String.class));

        // warn
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_WARN, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite", tailer.readEnum(String.class));
        assertEquals("warn", tailer.readEnum(String.class));

        // error
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_ERROR, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite", tailer.readEnum(String.class));
        assertEquals("error", tailer.readEnum(String.class));

        assertFalse(tailer.nextIndex());

        tailer.close();
        reader.close();
    }

    @Test
    public void testTextLogging() throws IOException {
        Logger l = LoggerFactory.getLogger("Text1");
        l.trace("trace");
        l.debug("debug");
        l.info("info");
        l.warn("warn");
        l.error("error");

        Chronicle reader = new IndexedChronicle(basePath(ChronicleLoggingConfig.TYPE_INDEXED, "text_1"));
        ExcerptTailer tailer = reader.createTailer().toStart();

        assertTrue(tailer.nextIndex());
        assertTrue(tailer.readLine().contains("warn"));

        assertTrue(tailer.nextIndex());
        assertTrue(tailer.readLine().contains("error"));

        assertFalse(tailer.nextIndex());

        tailer.close();
        reader.close();
    }
}
