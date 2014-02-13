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
package net.openhft.chronicle.sandbox.slf4j;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class Slf4jChronicleTest {

    @Test
    public void testLoadProperties() {
        Properties properties = ChronicleHelper.loadProperties();
        assertEquals(properties.get("org.slf4j.logger.chronicle.level"),"debug");
        assertEquals(properties.get("org.slf4j.logger.chronicle.shortName"),"false");
        assertEquals(properties.get("org.slf4j.logger.chronicle.path"),System.getProperty("java.io.tmpdir"));
        assertEquals(properties.get("org.slf4j.logger.chronicle.fromEnv"),System.getenv("LOGNAME"));
    }
}
