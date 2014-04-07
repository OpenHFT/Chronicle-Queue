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

import org.slf4j.helpers.MarkerIgnoringBase;

/**
 *
 */
public class ChronicleLogger extends MarkerIgnoringBase {

    private final ChronicleLogWriter writer;
    private final int level;

    /**
     * c-tor
     *
     * @param writer
     * @param name
     */
    public ChronicleLogger(final ChronicleLogWriter writer, final String name) {
        this(writer, name, ChronicleLoggingHelper.DEFAULT_LOG_LEVEL);
    }

    /**
     * c-tor
     *
     * @param writer
     * @param name
     * @param level
     */
    public ChronicleLogger(final ChronicleLogWriter writer, final String name, int level) {
        this.writer = writer;
        this.name = name;
        this.level = level;
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * @return
     */
    public int getLevel() {
        return this.level;
    }

    /**
     * @return
     */
    public ChronicleLogWriter getWriter() {
        return this.writer;
    }

    // *************************************************************************
    // TRACE
    // *************************************************************************

    @Override
    public boolean isTraceEnabled() {
        return isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_TRACE);
    }

    @Override
    public void trace(String s) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_TRACE)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_TRACE, this.name, s);
        }
    }

    @Override
    public void trace(String s, Object o) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_TRACE)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_TRACE, this.name, s, o);
        }
    }

    @Override
    public void trace(String s, Object o1, Object o2) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_TRACE)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_TRACE, this.name, s, o1, o2);
        }
    }

    @Override
    public void trace(String s, Object... objects) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_TRACE)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_TRACE, this.name, s, objects);
        }
    }

    @Override
    public void trace(String s, Throwable throwable) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_TRACE)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_TRACE, this.name, s, throwable);
        }
    }

    // *************************************************************************
    // DEBUG
    // *************************************************************************

    @Override
    public boolean isDebugEnabled() {
        return isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_DEBUG);
    }

    @Override
    public void debug(String s) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_DEBUG)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, this.name, s);
        }
    }

    @Override
    public void debug(String s, Object o) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_DEBUG)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, this.name, s, o);
        }
    }

    @Override
    public void debug(String s, Object o1, Object o2) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_DEBUG)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, this.name, s, o1, o2);
        }
    }

    @Override
    public void debug(String s, Object... objects) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_DEBUG)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, this.name, s, objects);
        }
    }

    @Override
    public void debug(String s, Throwable throwable) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_DEBUG)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, this.name, s, throwable);
        }
    }

    // *************************************************************************
    // INFO
    // *************************************************************************

    @Override
    public boolean isInfoEnabled() {
        return isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_INFO);
    }

    @Override
    public void info(String s) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_INFO)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_INFO, this.name, s);
        }
    }

    @Override
    public void info(String s, Object o) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_INFO)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_INFO, this.name, s, o);
        }
    }

    @Override
    public void info(String s, Object o1, Object o2) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_INFO)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_INFO, this.name, s, o1, o2);
        }
    }

    @Override
    public void info(String s, Object... objects) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_INFO)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_INFO, this.name, s, objects);
        }
    }

    @Override
    public void info(String s, Throwable throwable) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_INFO)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_INFO, this.name, s, throwable);
        }
    }

    // *************************************************************************
    // WARN
    // *************************************************************************

    @Override
    public boolean isWarnEnabled() {
        return isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_WARN);
    }

    @Override
    public void warn(String s) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_WARN)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_WARN, this.name, s);
        }
    }

    @Override
    public void warn(String s, Object o) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_WARN)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_WARN, this.name, s, o);
        }
    }

    @Override
    public void warn(String s, Object o1, Object o2) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_WARN)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_WARN, this.name, s, o1, o2);
        }
    }

    @Override
    public void warn(String s, Object... objects) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_WARN)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_WARN, this.name, s, objects);
        }
    }

    @Override
    public void warn(String s, Throwable throwable) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_WARN)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_WARN, this.name, s, throwable);
        }
    }

    // *************************************************************************
    // ERROR
    // *************************************************************************

    @Override
    public boolean isErrorEnabled() {
        return isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_ERROR);
    }

    @Override
    public void error(String s) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_ERROR)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_ERROR, this.name, s);
        }
    }

    @Override
    public void error(String s, Object o) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_ERROR)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_ERROR, this.name, s, o);
        }
    }

    @Override
    public void error(String s, Object o1, Object o2) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_ERROR)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_ERROR, this.name, s, o1, o2);
        }
    }

    @Override
    public void error(String s, Object... objects) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_ERROR)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_ERROR, this.name, s, objects);
        }
    }

    @Override
    public void error(String s, Throwable throwable) {
        if (isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_ERROR)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_ERROR, this.name, s, throwable);
        }
    }

    // *************************************************************************
    // HELPERS
    // *************************************************************************

    /**
     * Is the given slf4j level enabled?
     *
     * @param level is this level enabled?
     */
    private boolean isLevelEnabled(int level) {
        // slf4j level are numerically ordered so can use simple numeric
        // comparison
        return (level >= this.level);
    }
}
