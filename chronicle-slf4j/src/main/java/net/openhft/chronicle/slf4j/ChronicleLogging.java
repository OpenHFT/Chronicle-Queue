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

import org.slf4j.ILoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

/**
 * @author lburgazzoli
 */
public class ChronicleLogging {

    /**
     *
     */
    public static void warmup() {
        ILoggerFactory factory = StaticLoggerBinder.getSingleton().getLoggerFactory();
        if (factory instanceof ChronicleLoggerFactory) {
            ((ChronicleLoggerFactory) factory).warmup();
        }
    }

    /**
     *
     */
    public static void shutdown() {
        ILoggerFactory factory = StaticLoggerBinder.getSingleton().getLoggerFactory();
        if (factory instanceof ChronicleLoggerFactory) {
            ((ChronicleLoggerFactory) factory).shutdown();
        }
    }
}
