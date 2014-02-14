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
import net.openhft.chronicle.sandbox.VanillaChronicle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class Slf4jChronicleTest {
    private static final String BASEPATH = System.getProperty("java.io.tmpdir") + File.separator + "slf4j-chronicle";

    @Before
    public void setUp() {
        System.setProperty("org.slf4j.logger.chronicle.path",BASEPATH);
        System.setProperty("org.slf4j.logger.chronicle.level",ChronicleHelper.LOG_LEVEL_DEBUG_S);
        System.setProperty("org.slf4j.logger.chronicle.append",ChronicleHelper.FALSE_S);
    }

    @After
    public void tearDown() {
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLoadProperties() {
        Properties properties = ChronicleHelper.loadProperties();

        assertEquals(
           BASEPATH,
           ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_PATH));
        assertEquals(
            ChronicleHelper.LOG_LEVEL_DEBUG_S,
            ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_LEVEL));
        assertEquals(
            ChronicleHelper.TYPE_BINARY,
            ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_TYPE));
        assertEquals(
            ChronicleHelper.FALSE_S,
            ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_SHORTNAME));
        assertEquals(
            ChronicleHelper.FALSE_S,
            ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_APPEND));
        assertEquals(
            System.getProperty("java.io.tmpdir"),
            ChronicleHelper.getStringProperty(properties, "fromSys"));
        assertEquals(
            System.getenv("LOGNAME"),
            ChronicleHelper.getStringProperty(properties, "fromEnv"));
    }

    @Test
    public void testLoggerFactory() {
        assertEquals(
            StaticLoggerBinder.getSingleton().getLoggerFactory().getClass(),
            ChronicleLoggerFactory.class);
    }

    @Test
    public void testLogger() {
        Logger l = LoggerFactory.getLogger("test");

        assertNotNull(l);
        assertEquals(l.getClass(),ChronicleLogger.class);
    }

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
        assertEquals(ChronicleHelper.LOG_LEVEL_DEBUG,tailer.readByte());
        assertEquals(Slf4jChronicleTest.class.getName(),tailer.readEnum(String.class));
        assertEquals("debug",tailer.readEnum(String.class));

        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleHelper.LOG_LEVEL_INFO,tailer.readByte());
        assertEquals(Slf4jChronicleTest.class.getName(),tailer.readEnum(String.class));
        assertEquals("info",tailer.readEnum(String.class));

        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleHelper.LOG_LEVEL_WARN,tailer.readByte());
        assertEquals(Slf4jChronicleTest.class.getName(),tailer.readEnum(String.class));
        assertEquals("warn",tailer.readEnum(String.class));

        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleHelper.LOG_LEVEL_ERROR,tailer.readByte());
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
