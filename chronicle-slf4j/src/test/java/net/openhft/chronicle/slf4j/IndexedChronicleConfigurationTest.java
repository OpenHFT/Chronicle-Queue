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

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class IndexedChronicleConfigurationTest extends ChronicleTestBase {

    @Test
    public void testLoadProperties() {
        String cfgPath = System.getProperty("slf4j.chronicle.indexed.properties");
        ChronicleLoggingConfig cfg = ChronicleLoggingConfig.load(cfgPath);

        assertEquals(
                new File(basePath(ChronicleLoggingConfig.TYPE_INDEXED, "root")),
                new File(cfg.getString(ChronicleLoggingConfig.KEY_PATH)));
        assertEquals(
                ChronicleLoggingConfig.TYPE_INDEXED,
                cfg.getString(ChronicleLoggingConfig.KEY_TYPE));
        assertEquals(
                ChronicleLoggingConfig.BINARY_MODE_FORMATTED,
                cfg.getString(ChronicleLoggingConfig.KEY_BINARY_MODE));
        assertEquals(
                ChronicleLoggingHelper.FALSE_S,
                cfg.getString(ChronicleLoggingConfig.KEY_SYNCHRONOUS));
        assertEquals(
                ChronicleLoggingHelper.LOG_LEVEL_DEBUG_S,
                cfg.getString(ChronicleLoggingConfig.KEY_LEVEL));
        assertEquals(
                ChronicleLoggingHelper.FALSE_S,
                cfg.getString(ChronicleLoggingConfig.KEY_SHORTNAME));
        assertEquals(
                ChronicleLoggingHelper.FALSE_S,
                cfg.getString(ChronicleLoggingConfig.KEY_APPEND));
        assertEquals(
                new File(basePath(ChronicleLoggingConfig.TYPE_INDEXED, "logger_1")),
                new File(cfg.getString("Logger1", ChronicleLoggingConfig.KEY_PATH)));
        assertEquals(
                ChronicleLoggingHelper.LOG_LEVEL_INFO_S,
                cfg.getString("Logger1", ChronicleLoggingConfig.KEY_LEVEL));
        assertEquals(
                new File(basePath(ChronicleLoggingConfig.TYPE_INDEXED, "readwrite")),
                new File(cfg.getString("readwrite", ChronicleLoggingConfig.KEY_PATH)));
        assertEquals(
                ChronicleLoggingHelper.LOG_LEVEL_DEBUG_S,
                cfg.getString("readwrite", ChronicleLoggingConfig.KEY_LEVEL));
    }
}
