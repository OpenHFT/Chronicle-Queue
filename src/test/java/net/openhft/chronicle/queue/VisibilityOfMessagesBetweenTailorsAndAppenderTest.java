/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;
import static org.junit.Assert.fail;

@RequiredForClient
public class VisibilityOfMessagesBetweenTailorsAndAppenderTest extends QueueTestCommon {

    @SuppressWarnings("PMD.UnusedAssignment") // written by appender thread, consumed by tailer thread
    private volatile long lastWrittenIndex = Long.MIN_VALUE;
    private volatile boolean stop = false;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    /**
     * check if a message is written with an appender its visible to the tailor, without locks etc.
     */
    @Test
    public void test() throws InterruptedException, ExecutionException {

        try (ChronicleQueue x = SingleChronicleQueueBuilder
                .binary(getTmpDir())
                .rollCycle(MINUTELY)
                .build()) {

            ExecutorService e1 = newSingleThreadExecutor(new NamedThreadFactory("e1"));

            e1.submit(() -> {
                try (ExcerptAppender excerptAppender = x.createAppender()) {
                    for (long i = 0; i < 1_000_000; i++) {
                        try (DocumentContext dc = excerptAppender.writingDocument()) {
                            dc.wire().write("data").int64(i);
                        }
                        lastWrittenIndex = excerptAppender.lastIndexAppended();
                        if (Thread.currentThread().isInterrupted())
                            return null;
                    }
                    stop = true;
                }
                return null;
            });

            long start = System.currentTimeMillis();
            try (ExcerptTailer tailer = x.createTailer()) {

                for (; ; ) {
                    boolean stop = this.stop;
                    long i = lastWrittenIndex;
                    if (i != Long.MIN_VALUE) {
                        if (!tailer.moveToIndex(i))
                            throw new ExecutionException("non atomic, index=" + Long.toHexString(i), null);
                        if (stop)
                            break;
                    }
                    if (Thread.currentThread().isInterrupted())
                        break;
                    if (System.currentTimeMillis() - start > 10_000)
                        fail("Timeout waiting for tailer to see index " + Long.toHexString(i));
                }
            }

            e1.shutdown();

            if (!e1.awaitTermination(1, TimeUnit.SECONDS)) {
                e1.shutdownNow();
            }
        }
    }
}
