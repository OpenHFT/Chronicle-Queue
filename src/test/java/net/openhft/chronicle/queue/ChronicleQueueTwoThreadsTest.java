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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.rollcycles.SparseRollCycles.SMALL_DAILY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

@RequiredForClient
public class ChronicleQueueTwoThreadsTest extends QueueTestCommon {

    private static final int BYTES_LENGTH = 256;
    private static final long INTERVAL_US = 10;

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    @DisplayName("Unbuffered two-thread run completes expected reads")
    @Disabled("Long running test disabled for standard runs")
    @Timeout(value = 60000, unit = TimeUnit.MILLISECONDS)
    public void testUnbuffered() throws InterruptedException {
        long reads = doTest(false, 50_000);
        assertEquals(50_000, reads, "unbuffered run should read expected number of messages");
    }

    @Test
    @DisplayName("Concurrent short run matches expected read count")
    public void testConcurrentShortRun() throws InterruptedException {
        long reads = doTest(false, 1_000);
        assertEquals(1_000, reads, "concurrent short run should read expected number of messages");
    }

    @Test
    @DisplayName("Buffered short run matches expected read count")
    public void testBufferedShortRun() throws InterruptedException {
        assumeBufferingAvailable();
        long reads = doTest(BufferMode.Asynchronous, false, false, 1_000);
        assertEquals(1_000, reads, "buffered short run should read expected number of messages");
    }

    @Test
    @DisplayName("Buffered heap bytes run matches expected read count")
    public void testBufferedHeapBytes() throws InterruptedException {
        assumeBufferingAvailable();
        long reads = doTest(BufferMode.Asynchronous, true, true, 512);
        assertEquals(512, reads, "buffered heap bytes run should read expected number of messages");
    }

    private long doTest(boolean buffered, long runs) throws InterruptedException {
        return doTest(buffered ? BufferMode.Asynchronous : BufferMode.None, false, false, runs);
    }

    private long doTest(@NotNull BufferMode bufferMode,
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
                        Jvm.nanoPause();
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

        return counter.get();
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
        assumeTrue(SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable(), "BufferMode.Asynchronous requires Chronicle Queue Enterprise");
    }
}
