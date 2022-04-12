/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.onoes.Slf4jExceptionHandler;
import net.openhft.chronicle.core.util.Time;
import org.jetbrains.annotations.NotNull;

@RequiredForClient
public class LongRunTestMain {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        final TLogEntry entry = new TLogEntry();
        entry.setSessionId(321234L);
        entry.setLogLevel(77);
        entry.setSecurityLevel(1234);
        entry.setPosixTimestamp(6141234321L);
        entry.setMessage("This is a test message for the system................................ A");

        final LogEntryOutput output = new LogEntryOutput(1024);
        output.setMarshallable(entry);

        final ChronicleQueue queue = ChronicleQueue.singleBuilder(
                OS.getTarget() + "/test-" + Time.uniqueId())
                .rollCycle(RollCycles.HOURLY)
                .build();
        final ExcerptAppender appender = queue.acquireAppender();
        Jvm.setExceptionHandlers(Slf4jExceptionHandler.ERROR, Slf4jExceptionHandler.WARN, Slf4jExceptionHandler.WARN);
        for (int j = 0; j < 100; ++j) {
            for (int i = 0; i < 100000; ++i) {
                appender.writeBytes(output);
            }

           // System.out.println((j + 1) * 100000);
            // Jvm.pause(100L);
        }

        queue.close();
       // System.out.println("took " + (System.currentTimeMillis() - start) / 1e3);
    }

    static class TLogEntry {

        private long sessionId;
        private int logLevel;
        private int securityLevel;
        private long posixTimestamp;
        private CharSequence message;

        public long getSessionId() {
            return sessionId;
        }

        public void setSessionId(long sessionId) {
            this.sessionId = sessionId;
        }

        public int getLogLevel() {
            return logLevel;
        }

        public void setLogLevel(int logLevel) {
            this.logLevel = logLevel;
        }

        public int getSecurityLevel() {
            return securityLevel;
        }

        public void setSecurityLevel(int securityLevel) {
            this.securityLevel = securityLevel;
        }

        public long getPosixTimestamp() {
            return posixTimestamp;
        }

        public void setPosixTimestamp(long posixTimestamp) {
            this.posixTimestamp = posixTimestamp;
        }

        public CharSequence getMessage() {
            return message;
        }

        public void setMessage(CharSequence message) {
            this.message = message;
        }
    }

    static class LogEntryOutput implements WriteBytesMarshallable {
        private final int maxMessageSize;
        private TLogEntry logEntry;

        LogEntryOutput(final int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
        }

        public void setMarshallable(final TLogEntry logEntry) {
            this.logEntry = logEntry;
        }

        @Override
        public void writeMarshallable(@NotNull final BytesOut<?> bytes) {
            bytes.writeLong(this.logEntry.getSessionId());
            bytes.writeInt(this.logEntry.getLogLevel());
            bytes.writeInt(this.logEntry.getSecurityLevel());
            bytes.writeLong(this.logEntry.getPosixTimestamp());

            // Limit size of string messages.
            final int messageSize = Math.min(this.logEntry.getMessage().length(), this.maxMessageSize);

            // Write message length
            bytes.writeStopBit((long) messageSize);

            // Write message bytes.
            bytes.write(this.logEntry.getMessage(), 0, messageSize);
        }
    }
}
