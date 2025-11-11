/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.WireType;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.*;

public class ThreadedQueueTest extends QueueTestCommon {

    private static final int REQUIRED_COUNT = 10;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test(timeout = 10000)
    public void testMultipleThreads() throws InterruptedException, ExecutionException, TimeoutException {

        final File path = getTmpDir();

        final AtomicInteger counter = new AtomicInteger();

        ExecutorService tailerES = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("tailer"));
        Future<?> tf = tailerES.submit(() -> {
            try (final ChronicleQueue rqueue = ChronicleQueue.singleBuilder(path)
                    .testBlockSize()
                    .build()) {

                final ExcerptTailer tailer = rqueue.createTailer();
                final Bytes<?> bytes = Bytes.elasticByteBuffer();

                while (counter.get() < REQUIRED_COUNT && !Thread.interrupted()) {
                    bytes.clear();
                    if (tailer.readBytes(bytes))
                        counter.incrementAndGet();
                }

                bytes.releaseLast();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });

        ExecutorService appenderES = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("appender"));
        Future<?> af = appenderES.submit(() -> {
            try (final ChronicleQueue wqueue = ChronicleQueue.singleBuilder(path)
                    .testBlockSize()
                    .build();
                 final ExcerptAppender appender = wqueue.createAppender()) {

                final Bytes<?> message = Bytes.elasticByteBuffer();
                for (int i = 0; i < REQUIRED_COUNT; i++) {
                    message.clear();
                    message.append(i);
                    appender.writeBytes(message);
                }
                message.releaseLast();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });

        appenderES.shutdown();
        tailerES.shutdown();

        long end = System.currentTimeMillis() + 9000;
        af.get(9000, TimeUnit.MILLISECONDS);
        tf.get(end - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        assertEquals(REQUIRED_COUNT, counter.get());
    }

    @Test
    public void testTailerReadingEmptyQueue() {
        final File path = getTmpDir();

        try (final ChronicleQueue rqueue = SingleChronicleQueueBuilder.builder(path, WireType.FIELDLESS_BINARY)
                .testBlockSize()
                .rollCycle(TEST_DAILY)
                .build()) {

            final ExcerptTailer tailer = rqueue.createTailer();

            try (final ChronicleQueue wqueue = SingleChronicleQueueBuilder.builder(path, WireType.FIELDLESS_BINARY)
                    .testBlockSize()
                    .rollCycle(TEST_DAILY)
                    .build();
                 final ExcerptAppender appender = wqueue.createAppender()) {

                Bytes<?> bytes = Bytes.elasticByteBuffer();
                assertFalse(tailer.readBytes(bytes));

                appender.writeBytes(Bytes.wrapForRead("Hello World".getBytes(ISO_8859_1)));

                bytes.clear();
                boolean condition = tailer.readBytes(bytes);
                // NOTE: Something in the cache for directory isn't being updated.
                if (!condition) {
                    Jvm.pause(1);
                    condition = tailer.readBytes(bytes);
                }
                assertTrue(condition);
                assertEquals("Hello World", bytes.toString());

                bytes.releaseLast();
            }
        }
    }
}
