/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class CheckIndicesTest extends QueueTestCommon {

    private static final int BATCH_SIZE = 10;
    private ChronicleQueue queue0;

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    @DisplayName("Manual stress test checks index consistency under concurrent writes")
    @Disabled("Stress test to run manually for index checking")
    public void test() throws ExecutionException, InterruptedException {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).epoch(System.currentTimeMillis()).build()) {
            Assertions.assertNotNull(queue, "check-indices: queue should be created");
            queue0 = queue;
            newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::appendToQueue, 0, 1, TimeUnit.MICROSECONDS);
            Future<Callable<Void>> f = newSingleThreadScheduledExecutor().submit(this::checkIndices);
            Future<Callable<Void>> f2 = newSingleThreadScheduledExecutor().submit(this::checkIndices);

            for (; ; ) {
                if (f.isDone())
                    f.get();
                if (f2.isDone())
                    f2.get();
                Thread.sleep(500);
            }
        }
    }

    private Callable<Void> checkIndices() {
        ExcerptTailer tailer = queue0.createTailer();

        long index = 0;

        boolean movetoIndex = true;
        for (int i = 0; i < 10_000_000; i++) {

            if (movetoIndex)
                if (!tailer.moveToIndex(index))
                    continue;
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    index += ((long) (Math.random() * 10) - 2);
                    movetoIndex = true;
                    continue;
                }
                if (index != dc.index())
                    throw new AssertionError("Index mismatch, expected " + index + " but read " + dc.index());
                long expectedSeq = queue0.rollCycle().toSequenceNumber(index);
                long actualSeq = dc.wire().read("value").readLong();
                if (expectedSeq != actualSeq)
                    throw new AssertionError("Sequence mismatch at index " + index + ": expected " + expectedSeq + ", actual " + actualSeq);
            }
            movetoIndex = false;
            index += 1;

        }
        return null;

    }

    private void appendToQueue() {
        try (ExcerptAppender appender = queue0.createAppender()) {

            for (int i = 0; i < BATCH_SIZE; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    long seq = appender.queue().rollCycle().toSequenceNumber(dc.index());
                    dc.wire().write("value").writeLong(seq);
                }
            }
        } catch (Exception e) {
            throw new AssertionError("Appender failed while writing batch entries", e);
        }
    }
}
