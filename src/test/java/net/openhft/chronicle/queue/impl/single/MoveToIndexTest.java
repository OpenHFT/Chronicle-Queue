/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class MoveToIndexTest extends QueueTestCommon {
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void shouldMoveToPreviousIndexAfterDocumentIsConsumed() throws IOException {
        File queuePath = tmpFolder.newFolder("cq");

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(queuePath).build();
             ExcerptAppender appender = queue.createAppender()) {
            for (int i = 1; i < 10; ++i) {
                appender.writeText("id" + i);
            }

            ExcerptTailer tailer = queue.createTailer();
            assertNext(tailer, "id1");
            long index = tailer.index();
            assertNext(tailer, "id2");
            tailer.moveToIndex(index);
            assertNext(tailer, "id2");
            tailer.moveToIndex(index);
            assertNext(tailer, "id2");
        }
    }

    // https://github.com/OpenHFT/Chronicle-Queue/issues/401
    @Test
    public void testRandomMove() throws IOException {
        final Map<Long, String> messageByIndex = new HashMap<>();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.
                binary(tmpFolder.newFolder()).build();
             final ExcerptAppender appender = queue.createAppender()) {
            // create a queue and add some excerpts
            for (int i = 0; i < 10; i++) {
                final String message = "msg" + i;
                appender.writeDocument(w -> w.write("message").object(message));
                final long appendIndex = appender.lastIndexAppended();
                messageByIndex.put(appendIndex, message);
            }

            final Random random = new Random(1510298038000L);
            final List<Long> indices = new ArrayList<>(messageByIndex.keySet());
            final ExcerptTailer tailer = queue.createTailer();
            final AtomicReference<String> capturedMessage = new AtomicReference<>();
            for (int i = 0; i < 100; i++) {
                final long randomIndex = indices.get(random.nextInt(messageByIndex.keySet().size()));
                tailer.moveToIndex(randomIndex);
                tailer.readDocument(w -> capturedMessage.set((String) w.read("message").object()));
                assertEquals(messageByIndex.get(randomIndex), capturedMessage.get());
                tailer.readDocument(w -> w.read("message").object());
            }
        }
    }

    /**
     * Performs series of moveToIndex in order to bring tailer in NOT_REACHED_IN_CYCLE state
     * and check that it's correctly handled.
     * https://github.com/OpenHFT/Chronicle-Queue/issues/781
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotReachedInCycle() throws Exception {
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(tmpFolder.newFolder()).build();
             ExcerptAppender appender = q.createAppender()) {
            ExcerptTailer tailer = q.createTailer();

            final int versionByte = 7;

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().getValueOut().writeByte((byte) versionByte);
                    dc.wire().write("contentKey").text("contentValue");
                }
            }

            long index;
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.wire().getValueIn().readByte() != versionByte)
                    throw new IllegalStateException("Illegal version bytes: " + dc.wire().bytes().readSkip(-1).toDebugString());

                index = dc.index();
            }

            int cycle = q.rollCycle().toCycle(index);
            long seq = q.rollCycle().toSequenceNumber(index);

            index = q.rollCycle().toIndex(cycle + 1, seq);
            assertFalse(tailer.moveToIndex(index));

            index = q.rollCycle().toIndex(cycle, seq + 6);
            assertFalse(tailer.moveToIndex(index));

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().getValueOut().writeByte((byte) versionByte);
                    dc.wire().write("contentKey").text("contentValue");
                }
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.wire().getValueIn().readByte() != versionByte)
                    throw new IllegalStateException("Illegal version bytes: " + dc.wire().bytes().readSkip(-1).toDebugString());
            }
        }
    }

    private void assertNext(ExcerptTailer tailer, String expected) {
        String next = tailer.readText();
        assertEquals(expected, next);
    }
}
