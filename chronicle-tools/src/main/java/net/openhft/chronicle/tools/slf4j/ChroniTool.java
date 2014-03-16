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
package net.openhft.chronicle.tools.slf4j;

import net.openhft.chronicle.slf4j.*;
import net.openhft.chronicle.slf4j.impl.AbstractBinaryChronicleLogReader;
import net.openhft.chronicle.slf4j.impl.AbstractTextChronicleLogReader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class ChroniTool {

    protected static final DateFormat DF =
            new SimpleDateFormat(ChronicleLoggingConfig.DEFAULT_DATE_FORMAT);

    // *************************************************************************
    //
    // *************************************************************************

    protected static final ChronicleLogReader BINARY = new AbstractBinaryChronicleLogReader() {
        @Override
        public void read(Date timestamp, int level, long threadId, String threadName, String name, String message, Throwable t) {
            System.out.format("%s|%s|%d|%s|%s|%s\n",
                    DF.format(timestamp),
                    ChronicleLoggingHelper.levelToString(level),
                    threadId,
                    threadName,
                    name,
                    message);
        }
    };

    protected static final ChronicleLogReader TEXT = new AbstractTextChronicleLogReader() {
        @Override
        public void read(Date timestamp, int level, long threadId, String threadName, String name, String message, Throwable t) {
            System.out.format("%s|%s|%d|%s|%s|%s\n",
                    DF.format(timestamp),
                    ChronicleLoggingHelper.levelToString(level),
                    threadId,
                    threadName,
                    name,
                    message);
        }
    };
}
