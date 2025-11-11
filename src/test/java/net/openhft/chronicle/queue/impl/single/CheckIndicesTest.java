/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class CheckIndicesTest extends QueueTestCommon {

    private static final int BATCH_SIZE = 10;
    private ChronicleQueue queue0;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Ignore("stress test to run manually")
    @Test
    public void test() throws ExecutionException, InterruptedException {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).epoch(System.currentTimeMillis()).build()) {
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
                    throw new AssertionError();
                // System.out.println("reading index=" + Long.toHexString(index));
                if (queue0.rollCycle().toSequenceNumber(index) != dc.wire().read("value").readLong())
                    throw new AssertionError();
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
                    // System.out.println("write=" + Long.toHexString(dc.index()));
                    dc.wire().write("value").writeLong(seq);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
