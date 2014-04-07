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
package net.openhft.chronicle.slf4j.impl;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.slf4j.ChronicleLogWriter;
import net.openhft.chronicle.slf4j.ChronicleLoggingConfig;
import net.openhft.chronicle.slf4j.ChronicleLoggingHelper;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 *
 */
public class ChronicleLogWriters {

    public static final Object[] NULL_ARGS = new Object[]{};

    // *************************************************************************
    //
    // *************************************************************************

    /**
     *
     */
    public static final class BinaryWriter extends AbstractChronicleLogWriter {

        public BinaryWriter(Chronicle chronicle) throws IOException {
            super(chronicle);
        }

        /**
         * This is the internal implementation for logging slf4j messages.
         *
         * @param level   One of the LOG_LEVEL_XXX constants defining the slf4j level
         * @param name    The logger name
         * @param message The message
         * @param message The message arguments
         */
        @Override
        public void log(int level, String name, String message, Object... args) {
            final Thread currentThread = Thread.currentThread();

            this.appender.startExcerpt();
            this.appender.writeLong(System.currentTimeMillis());
            this.appender.writeByte(level);
            this.appender.writeLong(currentThread.getId());
            this.appender.writeEnum(currentThread.getName());
            this.appender.writeEnum(name);
            this.appender.writeEnum(message);
            this.appender.writeInt(args.length);
            for (Object arg : args) {
                this.appender.writeObject(arg);
            }

            this.appender.finish();
        }
    }

    /**
     *
     */
    public static final class BinaryFormattingWriter extends AbstractChronicleLogWriter {

        public BinaryFormattingWriter(Chronicle chronicle) throws IOException {
            super(chronicle);
        }

        /**
         * This is the internal implementation for logging slf4j messages.
         *
         * @param level   One of the LOG_LEVEL_XXX constants defining the slf4j level
         * @param name    The logger name
         * @param message The message
         * @param message The message arguments
         */
        @Override
        public void log(int level, String name, String message, Object... args) {
            final Thread currentThread = Thread.currentThread();
            final FormattingTuple tp = MessageFormatter.format(message, args);

            this.appender.startExcerpt();
            this.appender.writeLong(System.currentTimeMillis());
            this.appender.writeByte(level);
            this.appender.writeLong(currentThread.getId());
            this.appender.writeEnum(currentThread.getName());
            this.appender.writeEnum(name);

            if (tp.getThrowable() == null) {
                this.appender.writeEnum(tp.getMessage());
            } else {
                appender.writeEnum(tp.getMessage() + " " + tp.getThrowable().toString());
            }

            this.appender.writeInt(0);
            this.appender.finish();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     *
     */
    public static final class TextWriter extends AbstractChronicleLogWriter {

        private final String dateFormat;
        private final ThreadLocal<DateFormat> dateFormatCache;

        /**
         * c-tor
         *
         * @param chronicle
         * @param dateFormat
         * @throws IOException
         */
        public TextWriter(Chronicle chronicle, String dateFormat) throws IOException {
            super(chronicle);
            this.dateFormat = dateFormat != null ? dateFormat : ChronicleLoggingConfig.DEFAULT_DATE_FORMAT;
            this.dateFormatCache = new ThreadLocal<DateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat(TextWriter.this.dateFormat);
                }
            };
        }

        /**
         * This is the internal implementation for logging slf4j messages.
         *
         * @param level   One of the LOG_LEVEL_XXX constants defining the slf4j level
         * @param name    The logger name
         * @param message The message
         * @param message The message arguments
         */
        @Override
        public void log(int level, String name, String message, Object... args) {
            final Thread currentThread = Thread.currentThread();
            final FormattingTuple tp = MessageFormatter.format(message, args);

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
            appender.append(tp.getMessage());

            if (tp.getThrowable() != null) {
                appender.append('|');
                appender.append(tp.getThrowable().toString());
            }

            appender.append('\n');
            appender.finish();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     *
     */
    public static final class SynchronizedWriter implements ChronicleLogWriter, Closeable {
        private final ChronicleLogWriter writer;
        private final Object sync;

        /**
         * @param writer
         */
        public SynchronizedWriter(final ChronicleLogWriter writer) {
            this.writer = writer;
            this.sync = new Object();
        }

        @Override
        public Chronicle getChronicle() {
            return this.writer.getChronicle();
        }

        @Override
        public void log(int level, String name, String message, Object... args) {
            synchronized (this.sync) {
                this.writer.log(level, name, message, args);
            }

        }

        @Override
        public void close() throws IOException {
            this.writer.close();
        }
    }
}
