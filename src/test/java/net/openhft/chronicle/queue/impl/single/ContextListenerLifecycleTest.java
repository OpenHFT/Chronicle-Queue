/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

/**
 * Verifies that {@link StoreAppender} delegates its listener-related entry points to one lifecycle
 * object. The recording implementation deliberately avoids listener mechanics so these tests fail
 * when a write path bypasses the lifecycle abstraction.
 */
public class ContextListenerLifecycleTest extends QueueTestCommon {

    @Test
    public void defaultBuilderHasNonNullSharedConfiguration() {
        SingleChronicleQueueBuilder first = SingleChronicleQueueBuilder.binary(getTmpDir());
        SingleChronicleQueueBuilder second = SingleChronicleQueueBuilder.binary(getTmpDir());

        assertNotNull(first.contextListenerConfiguration());
        assertSame(ContextListenerConfiguration.NONE, first.contextListenerConfiguration());
        assertSame(first.contextListenerConfiguration(), second.contextListenerConfiguration());
        assertFalse(first.contextListenerConfiguration().configured());
    }

    @Test
    public void documentWritesUseInjectedLifecycle() {
        finishedNormally = false;
        RecordingLifecycle lifecycle = new RecordingLifecycle();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).testBlockSize().build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appender.contextListenerLifecycle(lifecycle);

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("message").text("one");
            }

            assertEquals(1, lifecycle.writeAttempts);
            assertEquals(1, lifecycle.documentCalls);
            assertFalse(lifecycle.lastMetaData);
            assertEquals(0, lifecycle.rawDocumentCalls);

            appender.close();
            assertEquals(1, lifecycle.closeCalls);
        }
    }

    @Test
    public void rawWritesUseInjectedLifecycle() {
        finishedNormally = false;
        RecordingLifecycle lifecycle = new RecordingLifecycle();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).testBlockSize().build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appender.contextListenerLifecycle(lifecycle);

            Bytes<?> payload = binaryPayload("raw");
            try {
                appender.writeBytes(payload);
            } finally {
                payload.releaseLast();
            }

            assertEquals(1, lifecycle.writeAttempts);
            assertEquals(0, lifecycle.documentCalls);
            assertEquals(1, lifecycle.rawDocumentCalls);
        }
    }

    @Test
    public void indexedWritesStartButDoNotNotifyLifecycle() {
        finishedNormally = false;
        RecordingLifecycle lifecycle = new RecordingLifecycle();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).testBlockSize().build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appender.contextListenerLifecycle(lifecycle);
            int cycle = ((SingleChronicleQueue) queue).cycle();
            long index = queue.rollCycle().toIndex(cycle, 0);

            Bytes<?> payload = binaryPayload("indexed");
            try {
                appender.writeBytes(index, payload);
            } finally {
                payload.releaseLast();
            }

            assertEquals(1, lifecycle.writeAttempts);
            assertEquals(0, lifecycle.documentCalls);
            assertEquals(0, lifecycle.rawDocumentCalls);
        }
    }

    private static Bytes<?> binaryPayload(String value) {
        Bytes<?> payload = Bytes.allocateElasticOnHeap();
        Wire wire = WireType.BINARY.apply(payload);
        wire.write("message").text(value);
        return payload;
    }

    /**
     * Test-only lifecycle implementation that records every appender entry point without creating
     * a method writer or writing a synthetic context document.
     */
    private static final class RecordingLifecycle implements ContextListenerLifecycle {
        private int writeAttempts;
        private int documentCalls;
        private int rawDocumentCalls;
        private int closeCalls;
        private boolean lastMetaData;

        @Override
        public boolean started() {
            return writeAttempts != 0;
        }

        @Override
        public void onWriteAttempt() {
            writeAttempts++;
        }

        @Override
        public boolean beforeDocument(boolean metaData, long safeLength) {
            documentCalls++;
            lastMetaData = metaData;
            return false;
        }

        @Override
        public boolean beforeRawDocument(long safeLength) {
            rawDocumentCalls++;
            return false;
        }

        @Override
        public ContextListenerLifecycle configure(
                Class<?> writerType, MarshallableOut.ContextListener<?> listener) {
            return this;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }
}
