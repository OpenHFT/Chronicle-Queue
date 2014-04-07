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
 * TODO: add test case for text-logegrs
 */
public class IndexedChronicleBinaryLoggerTest extends ChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Before
    public void setUp() {
        System.setProperty(
                "slf4j.chronicle.properties",
                System.getProperty("slf4j.chronicle.indexed.binary.properties"));

        getChronicleLoggerFactory().relaod();
        getChronicleLoggerFactory().warmup();
    }

    @After
    public void tearDown() {
        getChronicleLoggerFactory().shutdown();

        IOTools.deleteDir(basePath(ChronicleLoggingConfig.TYPE_VANILLA));
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
        Logger logger = LoggerFactory.getLogger(IndexedChronicleBinaryLoggerTest.class);

        assertNotNull(logger);
        assertEquals(logger.getClass(), ChronicleLogger.class);

        ChronicleLogger cl = (ChronicleLogger) logger;

        assertEquals(cl.getLevel(), ChronicleLoggingHelper.LOG_LEVEL_DEBUG);
        assertEquals(cl.getName(), IndexedChronicleBinaryLoggerTest.class.getName());
        assertTrue(cl.getWriter() instanceof ChronicleLogWriters.SynchronizedWriter);
        assertTrue(cl.getWriter().getChronicle() instanceof IndexedChronicle);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLogging1() throws IOException {
        String theradName = "th-test-binary-logging";
        String loggerName = IndexedChronicleBinaryLoggerTest.class.getName();

        Thread.currentThread().setName(theradName);

        Logger l = LoggerFactory.getLogger(loggerName);
        l.debug("data {}, {}",
                new MySerializableData("a Serializable object"),
                new MyMarshallableData("a Marshallable object")
        );

        Chronicle reader = getIndexedChronicle(ChronicleLoggingConfig.TYPE_INDEXED, "root-binary");
        ExcerptTailer tailer = reader.createTailer();

        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, tailer.readByte());
        assertEquals(Thread.currentThread().getId(), tailer.readLong());
        assertEquals(theradName, tailer.readEnum(String.class));
        assertEquals(loggerName, tailer.readEnum(String.class));
        assertEquals("data {}, {}", tailer.readEnum(String.class));

        int nbObjects = tailer.readInt();
        assertEquals(nbObjects, 2);

        Object serializableObject = tailer.readObject();
        assertNotNull(serializableObject);
        assertTrue(serializableObject instanceof MySerializableData);
        assertEquals(serializableObject.toString(), "a Serializable object");

        Object marshallableObject = tailer.readObject();
        assertNotNull(marshallableObject);
        assertTrue(marshallableObject instanceof MyMarshallableData);
        assertEquals(marshallableObject.toString(), "a Marshallable object");

        tailer.close();
        reader.close();
    }
}
