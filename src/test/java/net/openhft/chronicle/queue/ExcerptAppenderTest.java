/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.Wire;
import org.junit.AfterClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * Unit tests for ExcerptAppender interface implementations.
 */
public class ExcerptAppenderTest extends QueueTestCommon {

    static final String TEST_QUEUE = OS.getTarget() + "/ExcerptAppenderTest";

    class ExcerptAppenderImpl implements ExcerptAppender {

        private long lastIndexAppended = 0;
        private int currentCycle = 1;
        private Wire wire;

        @Override
        public void writeBytes(BytesStore<?, ?> bytes) {
            // Assume the bytes are written successfully
            lastIndexAppended++;
        }

        @Override
        public long lastIndexAppended() {
            return lastIndexAppended;
        }

        @Override
        public int cycle() {
            return currentCycle;
        }

        @Override
        public Wire wire() {
            return wire;
        }

        @Override
        public int sourceId() {
            return 0;
        }

        @Override
        public ChronicleQueue queue() {
            return ChronicleQueue.single(TEST_QUEUE);
        }

        @Override
        public void close() {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void singleThreadedCheckReset() {

        }

        @Override
        public void singleThreadedCheckDisabled(boolean singleThreadedCheckDisabled) {

        }

        @Override
        public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
            return null;
        }

        @Override
        public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
            return null;
        }
    }

    @AfterClass
    public static void cleanup() {
        IOTools.deleteDirWithFiles(TEST_QUEUE);
    }

    @Test
    public void testWriteBytes() {
        ExcerptAppenderImpl appender = new ExcerptAppenderImpl();
        Bytes<byte[]> bytes = Bytes.wrapForRead("Test data".getBytes(StandardCharsets.UTF_8));

        appender.writeBytes(bytes);

        assertEquals(1, appender.lastIndexAppended());
    }

    @Test
    public void testLastIndexAppended() {
        ExcerptAppenderImpl appender = new ExcerptAppenderImpl();

        assertEquals(0, appender.lastIndexAppended());

        Bytes<byte[]> bytes = Bytes.wrapForRead("Test data".getBytes(StandardCharsets.UTF_8));
        appender.writeBytes(bytes);

        assertEquals(1, appender.lastIndexAppended());
    }

    @Test
    public void testCycle() {
        ExcerptAppenderImpl appender = new ExcerptAppenderImpl();

        assertEquals(1, appender.cycle());
    }

    @Test
    public void testWire() {
        ExcerptAppenderImpl appender = new ExcerptAppenderImpl();

        assertNull(appender.wire());  // As wire is not set in this example
    }

    @Test
    public void testPretouch() {
        ExcerptAppenderImpl appender = new ExcerptAppenderImpl();
        long before = appender.lastIndexAppended();
        appender.pretouch();
        // Verify no side-effect on default no-op implementation
        assertEquals(before, appender.lastIndexAppended());
    }

    @Test
    public void testNormaliseEOFs() {
        ExcerptAppenderImpl appender = new ExcerptAppenderImpl();
        long before = appender.lastIndexAppended();
        appender.normaliseEOFs();
        // Verify no side-effect on default no-op implementation
        assertEquals(before, appender.lastIndexAppended());
    }
}
