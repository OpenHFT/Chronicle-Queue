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

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.sandbox.VanillaChronicleConfig;

import java.io.Closeable;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class TextChronicleWriter implements ChronicleWriter, Closeable {

    private final String path;
    private final String dateFormat;
    private final boolean append;
    private final VanillaChronicle chronicle;
    private final ThreadLocal<DateFormat> dateFormatCache;

    /**
     *
     * @param path
     * @param dateFormat
     * @param append
     * @param config
     */
    public TextChronicleWriter(String path, String dateFormat,boolean append, VanillaChronicleConfig config) {
        this.path = path;
        this.append = append;
        this.dateFormat = dateFormat != null ? dateFormat : ChronicleLoggingConfig.DEFAULT_DATE_FORMAT;
        this.chronicle = new VanillaChronicle(path,config);
        this.dateFormatCache = new ThreadLocal<DateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(TextChronicleWriter.this.dateFormat);
            }
        };

        if(!append) {
            this.chronicle.clear();
        }
    }

    /**
     * This is the internal implementation for logging regular (non-parameterized)
     * log messages.
     *
     * @param level   One of the LOG_LEVEL_XXX constants defining the log level
     * @param name    The logger name
     * @param message The message itself
     * @param t       The exception whose stack trace should be logged
     */
    @Override
    public void log(int level, String name, String message, Throwable t) {
        try {
            ExcerptAppender appender = this.chronicle.createAppender();
            appender.startExcerpt();
            appender.append(this.dateFormatCache.get().format(new Date()));
            appender.append('|');
            appender.append(level);
            appender.append('|');
            appender.append(name);
            appender.append('|');
            appender.append(message);
            appender.append('\n');
            //TODO: append Throwable
            appender.finish();
        } catch(Exception e) {
            //TODO
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if(this.chronicle != null) {
            this.chronicle.close();
        }
    }
}
