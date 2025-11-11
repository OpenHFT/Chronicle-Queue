/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.rollcycles.SparseRollCycles.SMALL_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RequiredForClient
public class ChronicleQueueTwoThreadsTest extends QueueTestCommon {

    private static final int BYTES_LENGTH = 256;
    private static final long INTERVAL_US = 10;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Ignore("long running test")
    @Test(timeout = 60000)
    public void testUnbuffered() throws InterruptedException {
        doTest(false, 50_000);
    }

    @Test
    public void testConcurrentShortRun() throws InterruptedException {
        doTest(false, 1_000);
    }

    @Test
    public void testBufferedShortRun() throws InterruptedException {
        assumeBufferingAvailable();
        doTest(BufferMode.Asynchronous, false, false, 1_000);
    }

    @Test
    public void testBufferedHeapBytes() throws InterruptedException {
        assumeBufferingAvailable();
        doTest(BufferMode.Asynchronous, true, true, 512);
    }

    private void doTest(boolean buffered, long runs) throws InterruptedException {
        doTest(buffered ? BufferMode.Asynchronous : BufferMode.None, false, false, runs);
    }

    private void doTest(@NotNull BufferMode bufferMode,
                        boolean tailerHeapBytes,
                        boolean appenderHeapBytes,
                        long runs) throws InterruptedException {
        File name = getTmpDir();

        AtomicLong counter = new AtomicLong();
        Thread tailerThread = new Thread(() -> {
            AffinityLock rlock = AffinityLock.acquireLock();
            Bytes<?> bytes = tailerHeapBytes
                    ? Bytes.allocateElasticOnHeap(BYTES_LENGTH)
                    : NativeBytes.nativeBytes(BYTES_LENGTH).unchecked(true);
            try (ChronicleQueue rqueue = buildQueue(name, bufferMode)) {

                ExcerptTailer tailer = rqueue.createTailer();

                while (!Thread.interrupted()) {
                    bytes.clear();
                    if (tailer.readBytes(bytes)) {
                        counter.incrementAndGet();
                    }
                }
            } finally {
                bytes.releaseLast();
                if (rlock != null) {
                    rlock.release();
                }
                // System.out.printf("Read %,d messages", counter.intValue());
            }
        }, "tailer thread");

        Thread appenderThread = new Thread(() -> {
            AffinityLock wlock = AffinityLock.acquireLock();
            Bytes<?> bytes = appenderHeapBytes
                    ? Bytes.allocateElasticOnHeap(BYTES_LENGTH)
                    : Bytes.allocateDirect(BYTES_LENGTH).unchecked(true);
            try (ChronicleQueue wqueue = buildQueue(name, bufferMode);
                 ExcerptAppender appender = wqueue.createAppender()) {

                long next = System.nanoTime() + INTERVAL_US * 1000;
                for (int i = 0; i < runs; i++) {
                    while (System.nanoTime() < next)
                        /* busy wait*/ ;
                    long start = next;
                    bytes.readPositionRemaining(0, BYTES_LENGTH);
                    bytes.writeLong(0L, start);

                    appender.writeBytes(bytes);
                    next += INTERVAL_US * 1000;
                }
            } finally {
                bytes.releaseLast();
                if (wlock != null) {
                    wlock.release();
                }
            }
        }, "appender thread");

        tailerThread.start();
        Jvm.pause(100);

        appenderThread.start();
        appenderThread.join();

        //Pause to allow tailer to catch up (if needed)
        for (int i = 0; i < 10; i++) {
            if (runs != counter.get())
                Jvm.pause(Jvm.isDebug() ? 10000 : 100);
        }

        for (int i = 0; i < 10; i++) {
            tailerThread.interrupt();
            tailerThread.join(100);
        }

        assertEquals(runs, counter.get());

    }

    private ChronicleQueue buildQueue(File path, boolean buffered) {
        return buildQueue(path, buffered ? BufferMode.Asynchronous : BufferMode.None);
    }

    private ChronicleQueue buildQueue(File path, BufferMode bufferMode) {
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.builder(path, WireType.FIELDLESS_BINARY)
                .rollCycle(SMALL_DAILY)
                .testBlockSize()
                .writeBufferMode(bufferMode);
        try {
            return builder.build();
        } catch (IllegalStateException ise) {
            if (bufferMode == BufferMode.Asynchronous && ise.getMessage() != null
                    && ise.getMessage().contains("Chronicle Queue Enterprise")) {
                return builder.writeBufferMode(BufferMode.None).build();
            }
            throw ise;
        }
    }

    private static void assumeBufferingAvailable() {
        assumeTrue("BufferMode.Asynchronous requires Chronicle Queue Enterprise",
                SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable());
    }
}
