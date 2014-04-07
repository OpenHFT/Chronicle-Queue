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
package net.openhft.chronicle.slf4j.tools;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.slf4j.ChronicleLogProcessor;
import net.openhft.chronicle.slf4j.ChronicleLogReader;
import net.openhft.chronicle.slf4j.ChronicleLoggingConfig;
import net.openhft.chronicle.slf4j.ChronicleLoggingHelper;
import net.openhft.chronicle.slf4j.impl.AbstractBinaryChronicleLogReader;
import net.openhft.chronicle.slf4j.impl.AbstractTextChronicleLogReader;
import org.jetbrains.annotations.NotNull;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class ChroniTool {

    public static final DateFormat DF =
            new SimpleDateFormat(ChronicleLoggingConfig.DEFAULT_DATE_FORMAT);

    // *************************************************************************
    //
    // *************************************************************************
    public static final ChronicleLogReader READER_BINARY = new AbstractBinaryChronicleLogReader() {
        @Override
        public void process(Date timestamp, int level, long threadId, String threadName, String name, String message, Object... args) {
            System.out.println(asString(timestamp, level, threadId, threadName, name, message, args));
        }
    };
    public static final ChronicleLogReader READER_TEXT = new AbstractTextChronicleLogReader() {
        @Override
        public void process(String message) {
            System.out.println(message);
        }
    };

    /**
     * @param timestamp
     * @param level
     * @param threadId
     * @param threadName
     * @param name
     * @param message
     * @param args
     * @return
     */
    public static String asString(Date timestamp, int level, long threadId, String threadName, String name, String message, Object... args) {
        if (args != null && args.length > 0) {
            final FormattingTuple tp = MessageFormatter.format(message, args);

            if (tp.getThrowable() != null) {
                return String.format("%s|%s|%d|%s|%s|%s",
                        DF.format(timestamp),
                        ChronicleLoggingHelper.levelToString(level),
                        threadId,
                        threadName,
                        name,
                        tp.getMessage());
            } else {
                return String.format("%s|%s|%d|%s|%s|%s|%s",
                        DF.format(timestamp),
                        ChronicleLoggingHelper.levelToString(level),
                        threadId,
                        threadName,
                        name,
                        tp.getMessage(),
                        tp.getThrowable());
            }
        } else {
            return String.format("%s|%s|%d|%s|%s|%s",
                    DF.format(timestamp),
                    ChronicleLoggingHelper.levelToString(level),
                    threadId,
                    threadName,
                    name,
                    message);
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static ChronicleLogReader binaryReader(final ChronicleLogProcessor processor) {
        return new AbstractBinaryChronicleLogReader() {
            @Override
            public void process(Date timestamp, int level, long threadId, String threadName, String name, String message, Object... args) {
                processor.process(timestamp, level, threadId, threadName, name, message, args);
            }
        };
    }

    public static ChronicleLogReader textReader(final ChronicleLogProcessor processor) {
        return new AbstractTextChronicleLogReader() {
            @Override
            public void process(String message) {
                processor.process(message);
            }
        };
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static void process(
            @NotNull final Chronicle chronicle,
            @NotNull final ChronicleLogReader reader,
            boolean waitForData,
            boolean fromEnd) throws IOException {

        ExcerptTailer tailer = null;

        try {
            tailer = fromEnd
                    ? chronicle.createTailer().toEnd()
                    : chronicle.createTailer();

            while (true) {
                if (tailer.nextIndex()) {
                    reader.read(tailer);
                    tailer.finish();
                } else {
                    if (waitForData) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            // Ignore
                        }
                    } else {
                        break;
                    }
                }
            }
        } finally {
            tailer.close();
            chronicle.close();
        }
    }
}
