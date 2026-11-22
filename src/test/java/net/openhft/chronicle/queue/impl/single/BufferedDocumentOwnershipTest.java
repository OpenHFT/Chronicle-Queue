/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class BufferedDocumentOwnershipTest extends QueueTestCommon {
    @Test
    public void bufferedRollbackDoesNotUnlockAnotherAppender() throws Exception {
        File path = getTmpDir();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (BufferedDocumentTestResources resources = new BufferedDocumentTestResources(worker)) {
            SingleChronicleQueue queue = resources.own(builder(path).build());
            ExcerptAppender owner = resources.own(queue.createAppender());
            ExcerptAppender buffered = worker.submit(() -> resources.own(queue.createAppender())).get(5, TimeUnit.SECONDS);
            try (DocumentContext held = owner.writingDocument()) {
                held.wire().getValueOut().text("owner");
                worker.submit(() -> {
                    try (DocumentContext document = buffered.writingDocument()) {
                        document.wire().getValueOut().text("discard");
                        document.rollbackOnClose();
                    }
                }).get(5, TimeUnit.SECONDS);
                assertTrue("a private buffer never owns the other appender's lock", queue.writeLock().locked());
            }
            assertFalse(queue.writeLock().locked());
            try (ExcerptTailer tailer = queue.createTailer()) {
                assertEquals("owner", tailer.readText());
                assertNull(tailer.readText());
            }
        }
        assertReopened(path, "owner");
    }

    @Test
    public void failedBufferedFlushDoesNotLeakPayloadIntoNextDocument() throws Exception {
        File path = getTmpDir();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (BufferedDocumentTestResources resources = new BufferedDocumentTestResources(worker)) {
            SingleChronicleQueue queue = resources.own(builder(path).build());
            ExcerptAppender owner = resources.own(queue.createAppender());
            ExcerptAppender buffered = worker.submit(() -> resources.own(queue.createAppender())).get(5, TimeUnit.SECONDS);
            DocumentContext first = bufferWhileOwnerWrites(owner, buffered, worker, "failed payload");
            queue.appendLock().lock();
            try {
                worker.submit(() -> assertThrows(IllegalStateException.class, first::close)).get(5, TimeUnit.SECONDS);
            } finally {
                queue.appendLock().unlock();
            }
            assertFalse(queue.writeLock().locked());
            DocumentContext second = bufferWhileOwnerWrites(owner, buffered, worker, "fresh payload");
            worker.submit(second::close).get(5, TimeUnit.SECONDS);
            assertFalse(queue.writeLock().locked());
            try (ExcerptTailer tailer = queue.createTailer()) {
                assertEquals("owner", tailer.readText());
                assertEquals("owner", tailer.readText());
                assertEquals("fresh payload", tailer.readText());
                assertNull(tailer.readText());
            }
            assertFalse(queue.dump().contains("failed payload"));
        }
        assertReopened(path, "owner", "owner", "fresh payload");
    }

    private DocumentContext bufferWhileOwnerWrites(ExcerptAppender owner, ExcerptAppender buffered,
                                                  ExecutorService worker, String payload) throws Exception {
        try (DocumentContext held = owner.writingDocument()) {
            held.wire().getValueOut().text("owner");
            return worker.submit(() -> {
                DocumentContext document = buffered.writingDocument();
                document.wire().getValueOut().text(payload);
                return document;
            }).get(5, TimeUnit.SECONDS);
        }
    }

    private void assertReopened(File path, String... expected) {
        try (SingleChronicleQueue queue = builder(path).build(); ExcerptTailer tailer = queue.createTailer()) {
            for (String message : expected)
                assertEquals(message, tailer.readText());
            assertNull(tailer.readText());
            assertFalse(queue.dump().contains("failed payload"));
        }
    }

    private SingleChronicleQueueBuilder builder(File path) {
        return SingleChronicleQueueBuilder.binary(path).testBlockSize().doubleBuffer(true)
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(() -> 0);
    }
}
