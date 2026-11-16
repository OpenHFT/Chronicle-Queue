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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.queue.rollcycles.SparseRollCycles.SMALL_DAILY;
import static org.junit.Assert.*;
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
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
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

        captureWorkerFailure(tailerThread, workerFailure);
        captureWorkerFailure(appenderThread, workerFailure);

        tailerThread.start();
        Jvm.pause(100);

        appenderThread.start();
        try {
            appenderThread.join();

            //Pause to allow tailer to catch up (if needed)
            for (int i = 0; i < 10; i++) {
                if (runs != counter.get())
                    Jvm.pause(Jvm.isDebug() ? 10000 : 100);
            }
        } finally {
            stopAndJoin(tailerThread);
        }

        rethrowWorkerFailure(workerFailure);
        assertEquals(runs, counter.get());

    }

    private static void stopAndJoin(Thread worker) throws InterruptedException {
        //! FIX-292: the reader consumes the stop interrupt before closing its queue.
        //! Repeating it in each join slice interrupts resource release and records a
        //! warning. Keep the existing one-second bound and require actual thread exit;
        //! ChronicleQueueTwoThreadsTest#cleanupIsNotInterruptedTwice and
        //! ChronicleQueueTwoThreadsTest#stalledCleanupStillFails cover both outcomes.
        worker.interrupt();
        worker.join(1000);
        assertFalse("Worker did not stop: " + worker.getName(), worker.isAlive());
    }

    //! FIX-292: uncaught writer/reader failures otherwise disappear on background
    //! threads and can look like successful cleanup. Preserve them for JUnit;
    //! ChronicleQueueTwoThreadsTest#workerFailureIsReported checks this shared
    //! capture path and the original cause propagated by rethrowWorkerFailure.
    private static void captureWorkerFailure(Thread worker, AtomicReference<Throwable> failure) {
        worker.setUncaughtExceptionHandler((thread, cause) -> failure.compareAndSet(null, cause));
    }

    private static void rethrowWorkerFailure(AtomicReference<Throwable> failure) {
        if (failure.get() != null)
            throw new AssertionError("Queue worker failed", failure.get());
    }

    @Test
    public void cleanupIsNotInterruptedTwice() throws Exception {
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch cleanup = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch secondInterrupt = new CountDownLatch(1);
        AtomicReference<Throwable> controllerFailure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            reading.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException stopping) {
                cleanup.countDown();
                try {
                    release.await();
                } catch (InterruptedException interruptedCleanup) {
                    secondInterrupt.countDown();
                }
            }
        }, "owned-cleanup-control");
        Thread controller = new Thread(() -> {
            try {
                assertTrue(cleanup.await(5, TimeUnit.SECONDS));
                secondInterrupt.await(150, TimeUnit.MILLISECONDS);
            } catch (Throwable e) {
                controllerFailure.set(e);
            } finally {
                release.countDown();
            }
        }, "owned-cleanup-controller");
        worker.start();
        controller.start();
        try {
            assertTrue(reading.await(5, TimeUnit.SECONDS));
            stopAndJoin(worker);
            assertEquals("Cleanup received another stop interrupt", 1, secondInterrupt.getCount());
        } finally {
            release.countDown();
            worker.interrupt();
            worker.join(1000);
            controller.join(1000);
            assertFalse(worker.isAlive());
            assertFalse(controller.isAlive());
            assertNull(controllerFailure.get());
        }
    }

    @Test
    public void stalledCleanupStillFails() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread worker = new Thread(() -> {
            started.countDown();
            boolean interrupted = false;
            while (release.getCount() != 0) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }, "owned-stalled-cleanup");
        worker.start();
        try {
            assertTrue(started.await(5, TimeUnit.SECONDS));
            try {
                stopAndJoin(worker);
                fail("Stalled cleanup was accepted");
            } catch (AssertionError expected) {
                assertEquals("Worker did not stop: owned-stalled-cleanup", expected.getMessage());
                assertTrue(worker.isAlive());
            }
        } finally {
            release.countDown();
            worker.join(1000);
            assertFalse(worker.isAlive());
        }
    }

    @Test
    public void workerFailureIsReported() throws Exception {
        RuntimeException original = new RuntimeException("controlled worker failure");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> { throw original; }, "owned-failed-worker");
        captureWorkerFailure(worker, failure);
        worker.start();
        worker.join(1000);
        assertFalse(worker.isAlive());
        AssertionError reported = null;
        try {
            rethrowWorkerFailure(failure);
        } catch (AssertionError expected) {
            reported = expected;
        }
        assertNotNull("Worker failure was lost", reported);
        assertSame(original, reported.getCause());
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
