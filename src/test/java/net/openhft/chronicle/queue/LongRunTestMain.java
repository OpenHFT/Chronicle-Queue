/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;

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

        try (final ChronicleQueue queue = ChronicleQueue.singleBuilder(
                        OS.getTarget() + "/test-" + Time.uniqueId())
                .rollCycle(HOURLY)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {
            Jvm.setExceptionHandlers(Slf4jExceptionHandler.ERROR, Slf4jExceptionHandler.WARN, Slf4jExceptionHandler.WARN);
            for (int j = 0; j < 100; ++j) {
                for (int i = 0; i < 100000; ++i) {
                    appender.writeBytes(output);
                }

                // System.out.println((j + 1) * 100000);
                // Jvm.pause(100L);
            }
        }
        // System.out.println("took " + (System.currentTimeMillis() - start) / 1e3);
    }

    static class TLogEntry {

        private long sessionId;
        private int logLevel;
        private int securityLevel;
        private long posixTimestamp;
        private CharSequence message;

        long getSessionId() {
            return sessionId;
        }

        void setSessionId(long sessionId) {
            this.sessionId = sessionId;
        }

        int getLogLevel() {
            return logLevel;
        }

        void setLogLevel(int logLevel) {
            this.logLevel = logLevel;
        }

        int getSecurityLevel() {
            return securityLevel;
        }

        void setSecurityLevel(int securityLevel) {
            this.securityLevel = securityLevel;
        }

        long getPosixTimestamp() {
            return posixTimestamp;
        }

        void setPosixTimestamp(long posixTimestamp) {
            this.posixTimestamp = posixTimestamp;
        }

        CharSequence getMessage() {
            return message;
        }

        void setMessage(CharSequence message) {
            this.message = message;
        }
    }

    static class LogEntryOutput implements WriteBytesMarshallable {
        private final int maxMessageSize;
        private TLogEntry logEntry;

        LogEntryOutput(final int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
        }

        void setMarshallable(final TLogEntry logEntry) {
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
