/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.rollcycles.SparseRollCycles.SMALL_DAILY;
import static org.junit.Assert.assertEquals;

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
        doTest(false);
    }

    private void doTest(boolean buffered) throws InterruptedException {
        File name = getTmpDir();

        AtomicLong counter = new AtomicLong();
        Thread tailerThread = new Thread(() -> {
            AffinityLock rlock = AffinityLock.acquireLock();
            Bytes<?> bytes = NativeBytes.nativeBytes(BYTES_LENGTH).unchecked(true);
            try (ChronicleQueue rqueue = SingleChronicleQueueBuilder.builder(name, WireType.FIELDLESS_BINARY)
                    .rollCycle(SMALL_DAILY)
                    .testBlockSize()
                    .build()) {

                ExcerptTailer tailer = rqueue.createTailer();

                while (!Thread.interrupted()) {
                    bytes.clear();
                    if (tailer.readBytes(bytes)) {
                        counter.incrementAndGet();
                    }
                }
            } finally {
                if (rlock != null) {
                    rlock.release();
                }
                // System.out.printf("Read %,d messages", counter.intValue());
            }
        }, "tailer thread");

        long runs = 50_000;

        Thread appenderThread = new Thread(() -> {
            AffinityLock wlock = AffinityLock.acquireLock();
            try (ChronicleQueue wqueue = SingleChronicleQueueBuilder.builder(name, WireType.FIELDLESS_BINARY)
                    .rollCycle(SMALL_DAILY)
                    .testBlockSize()
                    .writeBufferMode(buffered ? BufferMode.Asynchronous : BufferMode.None)
                    .build();
                 ExcerptAppender appender = wqueue.createAppender()) {

                Bytes<?> bytes = Bytes.allocateDirect(BYTES_LENGTH).unchecked(true);

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
}
