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

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.sandbox.VanillaChronicle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

/**
 *
 */
public class Slf4jChronicleTest {

    private static final String BASEPATH =
        System.getProperty("java.io.tmpdir")
            + File.separator
            + "chronicle"
            + File.separator
            + new SimpleDateFormat("yyyyMMdd").format(new Date())
            + File.separator
            + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
            + File.separator
            + "root";

    private static final String BASEPATH_LOGGER_1 =
        System.getProperty("java.io.tmpdir")
            + File.separator
            + "chronicle"
            + File.separator
            + new SimpleDateFormat("yyyyMMdd").format(new Date())
            + File.separator
            + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
            + File.separator
            + "logger_1";

    // *************************************************************************
    //
    // *************************************************************************

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLoadProperties() {
        ChronicleLoggingConfig cfg = ChronicleLoggingConfig.load();

        assertEquals(
           new File(BASEPATH),
           new File(cfg.getString(ChronicleLoggingConfig.KEY_PATH)));
        assertEquals(
            ChronicleLoggingHelper.LOG_LEVEL_DEBUG_S,
            cfg.getString(ChronicleLoggingConfig.KEY_LEVEL));
        assertEquals(
            ChronicleLoggingHelper.FALSE_S,
            cfg.getString(ChronicleLoggingConfig.KEY_SHORTNAME));
        assertEquals(
            ChronicleLoggingHelper.TRUE_S,
            cfg.getString(ChronicleLoggingConfig.KEY_APPEND));
        //assertEquals(
        //    new File(BASEPATH_LOGGER_1),
        //    new File(cfg.getString("Logger1",ChronicleLoggingConfig.KEY_PATH)));
        //assertEquals(
        //    ChronicleLoggingHelper.LOG_LEVEL_INFO_S,
        //    cfg.getString("Logger1",ChronicleLoggingConfig.KEY_LEVEL));
    }

    @Test
    public void testLoggerFactory() {
        assertEquals(
            StaticLoggerBinder.getSingleton().getLoggerFactory().getClass(),
            ChronicleLoggerFactory.class);
    }

    @Test
    public void testLogger() {
        Logger l1 = LoggerFactory.getLogger(Slf4jChronicleTest.class);
        Logger l2 = LoggerFactory.getLogger(Slf4jChronicleTest.class);

        assertNotNull(l1);
        assertEquals(l1.getClass(),ChronicleLogger.class);

        assertNotNull(l2);
        assertEquals(l2.getClass(),ChronicleLogger.class);

        assertEquals(l1,l2);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLogging1() throws IOException {
        Logger l = LoggerFactory.getLogger(Slf4jChronicleTest.class);
        l.trace("trace");
        l.debug("debug");
        l.info("info");
        l.warn("warn");
        l.error("error");

        VanillaChronicle reader = new VanillaChronicle(BASEPATH);
        ExcerptTailer tailer = reader.createTailer();

        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_DEBUG,tailer.readByte());
        assertEquals(Slf4jChronicleTest.class.getName(),tailer.readEnum(String.class));
        assertEquals("debug",tailer.readEnum(String.class));

        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_INFO,tailer.readByte());
        assertEquals(Slf4jChronicleTest.class.getName(),tailer.readEnum(String.class));
        assertEquals("info",tailer.readEnum(String.class));

        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_WARN,tailer.readByte());
        assertEquals(Slf4jChronicleTest.class.getName(),tailer.readEnum(String.class));
        assertEquals("warn",tailer.readEnum(String.class));

        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_ERROR,tailer.readByte());
        assertEquals(Slf4jChronicleTest.class.getName(),tailer.readEnum(String.class));
        assertEquals("error",tailer.readEnum(String.class));

        assertFalse(tailer.nextIndex());

        tailer.close();
        reader.close();
    }

    @Test
    public void testLoggingPerf1() throws IOException {
        Logger l = LoggerFactory.getLogger(Slf4jChronicleTest.class);

        for(int x=0;x<10;x++) {
            long start = System.nanoTime();

            int items = 10000;
            for (int i = 1; i <= items; i++) {
                l.trace("something to log");
            }

            long end = System.nanoTime();

            System.out.printf("Took an average of %.2f us to write %d items (level disabled)\n",
                (end - start) / items / 1e3,
                items);
        }
    }

    @Test
    public void testLoggingPerf2() throws IOException {
        Logger l = LoggerFactory.getLogger(Slf4jChronicleTest.class);

        for(int x=0;x<10;x++) {
            long start = System.nanoTime();

            int items = 10000;
            for (int i = 1; i <= items; i++) {
                l.debug("something to log");
            }

            long end = System.nanoTime();

            System.out.printf("Took an average of %.2f us to write %d items (level enabled)\n",
                (end - start) / items / 1e3,
                items);
        }
    }
}
