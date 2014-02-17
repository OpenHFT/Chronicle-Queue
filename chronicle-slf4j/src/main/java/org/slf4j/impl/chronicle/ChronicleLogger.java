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

import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

/**
 *
 */
public class ChronicleLogger extends MarkerIgnoringBase {

    private final ChronicleWriter writer;
    private final int level;

    /**
     * c-tor
     *
     * @param writer
     * @param name
     */
    public ChronicleLogger(final ChronicleWriter writer,final String name) {
        this(writer,name, ChronicleLoggingHelper.DEFAULT_LOG_LEVEL);
    }

    /**
     * c-tor
     *
     * @param writer
     * @param name
     * @param level
     */
    public ChronicleLogger(final ChronicleWriter writer,final String name,int level) {
        this.writer = writer;
        this.name = name;
        this.level = level;
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     *
     * @return
     */
    public int getLevel() {
        return this.level;
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
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_TRACE)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_TRACE, this.name, s, null);
        }
    }

    @Override
    public void trace(String s, Object o) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_TRACE, s, o, null);
    }

    @Override
    public void trace(String s, Object o1, Object o2) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_TRACE, s, o1, o2);
    }

    @Override
    public void trace(String s, Object... objects) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_TRACE,s, objects);
    }

    @Override
    public void trace(String s, Throwable throwable) {
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_TRACE)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_TRACE, this.name,s,throwable);
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
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_DEBUG)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, this.name,s, null);
        }
    }

    @Override
    public void debug(String s, Object o) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, s, o, null);
    }

    @Override
    public void debug(String s, Object o1, Object o2) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, s, o1, o2);
    }

    @Override
    public void debug(String s, Object... objects) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_DEBUG,s, objects);
    }

    @Override
    public void debug(String s, Throwable throwable) {
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_DEBUG)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_DEBUG,this.name,s,throwable);
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
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_INFO)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_INFO, this.name,s, null);
        }
    }

    @Override
    public void info(String s, Object o) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_INFO, s, o, null);
    }

    @Override
    public void info(String s, Object o1, Object o2) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_INFO, s, o1, o2);
    }

    @Override
    public void info(String s, Object... objects) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_INFO,s, objects);
    }

    @Override
    public void info(String s, Throwable throwable) {
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_INFO)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_INFO,this.name,s,throwable);
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
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_WARN)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_WARN, this.name,s, null);
        }
    }

    @Override
    public void warn(String s, Object o) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_WARN, s, o, null);
    }

    @Override
    public void warn(String s, Object o1, Object o2) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_WARN, s, o1, o2);
    }

    @Override
    public void warn(String s, Object... objects) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_WARN,s, objects);
    }

    @Override
    public void warn(String s, Throwable throwable) {
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_WARN)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_WARN,this.name,s,throwable);
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
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_ERROR)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_ERROR, this.name,s, null);
        }
    }

    @Override
    public void error(String s, Object o) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_ERROR, s, o, null);
    }

    @Override
    public void error(String s, Object o1, Object o2) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_ERROR, s, o1, o2);
    }

    @Override
    public void error(String s, Object... objects) {
        formatAndLog(ChronicleLoggingHelper.LOG_LEVEL_ERROR,s, objects);
    }

    @Override
    public void error(String s, Throwable throwable) {
        if(isLevelEnabled(ChronicleLoggingHelper.LOG_LEVEL_ERROR)) {
            this.writer.log(ChronicleLoggingHelper.LOG_LEVEL_ERROR,this.name,s,throwable);
        }
    }

    // *************************************************************************
    // HELPERS
    // *************************************************************************

    /**
     * Is the given log level enabled?
     *
     * @param level is this level enabled?
     */
    private boolean isLevelEnabled(int level) {
        // log level are numerically ordered so can use simple numeric
        // comparison
        return (level >= this.level);
    }

    /**
     * For formatted messages, first substitute arguments and then log.
     *
     * @param level
     * @param format
     * @param arg1
     * @param arg2
     */
    private void formatAndLog(int level, String format, Object arg1, Object arg2) {
        if(isLevelEnabled(level)) {
            FormattingTuple tp = MessageFormatter.format(format, arg1, arg2);
            this.writer.log(level, tp.getMessage(), this.name,tp.getThrowable());
        }
    }

    /**
     * For formatted messages, first substitute arguments and then log.
     *
     * @param level
     * @param format
     * @param arguments a list of 3 ore more arguments
     */
    private void formatAndLog(int level, String format, Object... arguments) {
        if(isLevelEnabled(level)) {
            FormattingTuple tp = MessageFormatter.arrayFormat(format, arguments);
            this.writer.log(level, tp.getMessage(), this.name, tp.getThrowable());
        }
    }
}
