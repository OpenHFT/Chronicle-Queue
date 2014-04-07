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
 * TODO: tmp class
 */
public class ChronicleLoggingHelper {

    public static final int LOG_LEVEL_TRACE = LocationAwareLogger.TRACE_INT;
    public static final int LOG_LEVEL_DEBUG = LocationAwareLogger.DEBUG_INT;
    public static final int LOG_LEVEL_INFO = LocationAwareLogger.INFO_INT;
    public static final int DEFAULT_LOG_LEVEL = LOG_LEVEL_INFO;
    public static final int LOG_LEVEL_WARN = LocationAwareLogger.WARN_INT;
    public static final int LOG_LEVEL_ERROR = LocationAwareLogger.ERROR_INT;
    public static final String LOG_LEVEL_TRACE_S = "trace";
    public static final String LOG_LEVEL_DEBUG_S = "debug";
    public static final String LOG_LEVEL_INFO_S = "info";
    public static final String DEFAULT_LOG_LEVEL_S = LOG_LEVEL_INFO_S;
    public static final String LOG_LEVEL_WARN_S = "warn";
    public static final String LOG_LEVEL_ERROR_S = "error";
    public static final String TRUE_S = "true";
    public static final String FALSE_S = "false";

    /**
     * @param name
     * @return
     */
    public static String computeShortName(final String name) {
        return name.substring(name.lastIndexOf(".") + 1);
    }

    /**
     * @param levelStr
     * @return
     */
    public static int stringToLevel(String levelStr) {
        if (levelStr != null) {
            if (LOG_LEVEL_TRACE_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_TRACE;
            } else if (LOG_LEVEL_DEBUG_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_DEBUG;
            } else if (LOG_LEVEL_INFO_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_INFO;
            } else if (LOG_LEVEL_WARN_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_WARN;
            } else if (LOG_LEVEL_ERROR_S.equalsIgnoreCase(levelStr)) {
                return LOG_LEVEL_ERROR;
            }
        }

        return DEFAULT_LOG_LEVEL;
    }


    /**
     * @param level
     * @return
     */
    public static String levelToString(int level) {
        switch (level) {
            case LOG_LEVEL_TRACE:
                return LOG_LEVEL_TRACE_S;
            case LOG_LEVEL_DEBUG:
                return LOG_LEVEL_DEBUG_S;
            case LOG_LEVEL_INFO:
                return LOG_LEVEL_INFO_S;
            case LOG_LEVEL_WARN:
                return LOG_LEVEL_WARN_S;
            case LOG_LEVEL_ERROR:
                return LOG_LEVEL_ERROR_S;
        }

        return "<unknown>";
    }
}
