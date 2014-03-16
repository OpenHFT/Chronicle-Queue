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
package net.openhft.chronicle.slf4j.rw;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.sandbox.VanillaChronicleConfig;
import net.openhft.chronicle.slf4j.ChronicleLogWriter;
import net.openhft.chronicle.slf4j.ChronicleLoggingConfig;
import net.openhft.chronicle.slf4j.ChronicleLoggingHelper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class TextChronicleLogWriter implements ChronicleLogWriter {

    private final String path;
    private final String dateFormat;
    private final boolean append;
    private final VanillaChronicle chronicle;
    private final ExcerptAppender appender;
    private final ThreadLocal<DateFormat> dateFormatCache;

    /**
     *
     * @param path
     * @param dateFormat
     * @param append
     * @param config
     */
    public TextChronicleLogWriter(String path, String dateFormat, boolean append, VanillaChronicleConfig config) throws IOException {
        this.path = path;
        this.append = append;
        this.dateFormat = dateFormat != null ? dateFormat : ChronicleLoggingConfig.DEFAULT_DATE_FORMAT;
        this.chronicle = new VanillaChronicle(path,config);
        this.appender = this.chronicle.createAppender();
        this.dateFormatCache = new ThreadLocal<DateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(TextChronicleLogWriter.this.dateFormat);
            }
        };

        if(!append) {
            this.chronicle.clear();
        }
    }

    @Override
    public Chronicle getChronicle() {
        return this.chronicle;
    }

    /**
     * This is the internal implementation for logging regular (non-parameterized)
     * slf4j messages.
     *
     * @param level   One of the LOG_LEVEL_XXX constants defining the slf4j level
     * @param name    The logger name
     * @param message The message itself
     * @param t       The exception whose stack trace should be logged
     */
    @Override
    public void log(int level, String name, String message, Throwable t) {
        final Thread currentThread = Thread.currentThread();

        appender.startExcerpt();
        appender.append(this.dateFormatCache.get().format(new Date()));
        appender.append('|');
        appender.append(ChronicleLoggingHelper.levelToString(level));
        appender.append('|');
        appender.append(currentThread.getId());
        appender.append('|');
        appender.append(currentThread.getName());
        appender.append('|');
        appender.append(name);
        appender.append('|');
        appender.append(message);
        appender.append('\n');
        //TODO: append Throwable?
        appender.finish();
    }

    @Override
    public void close() throws IOException {
        if(this.chronicle != null) {
            this.chronicle.close();
        }
    }
}
