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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;
import org.slf4j.impl.chronicle.ChronicleHelper;
import org.slf4j.impl.chronicle.ChronicleLogger;
import org.slf4j.impl.chronicle.ChronicleLoggerFactory;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class Slf4jChronicleTest {

    @Test
    public void testLoadProperties1() {
        Properties properties = ChronicleHelper.loadProperties();

        assertEquals(
            ChronicleHelper.LOG_LEVEL_DEBUG_S,
            ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_LEVEL));
        assertEquals(
            ChronicleHelper.FALSE_S,
            ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_SHORTNAME));
        assertEquals(
            System.getProperty("java.io.tmpdir"),
            ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_PATH));
        assertEquals(
            System.getenv("LOGNAME"),
            ChronicleHelper.getStringProperty(properties, "fromEnv"));
    }

    @Test
    public void testLoadProperties2() {
        System.setProperty("org.slf4j.logger.chronicle.path","/tmp/mychronicle");

        Properties properties = ChronicleHelper.loadProperties();

        assertEquals(
           ChronicleHelper.LOG_LEVEL_DEBUG_S,
           ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_LEVEL));
        assertEquals(
           ChronicleHelper.FALSE_S,
           ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_SHORTNAME));
        assertEquals(
           "/tmp/mychronicle",
           ChronicleHelper.getStringProperty(properties, ChronicleHelper.KEY_PATH));
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
}
