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

import org.slf4j.spi.LocationAwareLogger;

/**
 * @author lburgazzoli
 */
public enum ChronicleLoggingLevel {
    TRACE(LocationAwareLogger.TRACE_INT, "TRACE"),
    DEBUG(LocationAwareLogger.DEBUG_INT, "DEBUG"),
    INFO(LocationAwareLogger.INFO_INT, "INFO"),
    WARN(LocationAwareLogger.WARN_INT, "WARN"),
    ERROR(LocationAwareLogger.ERROR_INT, "ERROR");

    private final int level;
    private final String traceName;

    /**
     * @param level
     * @param traceName
     */
    private ChronicleLoggingLevel(int level, String traceName) {
        this.level = level;
        this.traceName = traceName;
    }

    /**
     * @return
     */
    public int level() {
        return this.level;
    }

    /**
     * @return
     */
    public String traceName() {
        return this.traceName;
    }
}
