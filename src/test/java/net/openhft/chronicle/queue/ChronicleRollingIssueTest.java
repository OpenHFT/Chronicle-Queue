/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

@RequiredForClient
public class ChronicleRollingIssueTest extends QueueTestCommon {
    private File path;
    private volatile Thread fixtureThread;

    // JUnit's timeout reports before the interrupted body finishes finally/After.
    // Join outside the inherited Timeout rule so its exception-handler restoration
    // cannot overlap the next test in this reused JVM.
    @Rule(order = Integer.MIN_VALUE)
    public final TestRule awaitFixtureCleanup = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            Throwable original = null;
            try {
                base.evaluate();
            } catch (Throwable failure) {
                original = failure;
                throw failure;
            } finally {
                Thread body = fixtureThread;
                if (body != null && body != Thread.currentThread() && body.isAlive()) {
                    try {
                        stopAndJoin(Collections.singletonList(body));
                    } catch (AssertionError cleanupFailure) {
                        if (original == null)
                            throw cleanupFailure;
                        original.addSuppressed(cleanupFailure);
                    }
                }
            }
        }
    };

    @Override
    @Before
    public void threadDump() {
        fixtureThread = Thread.currentThread();
        super.threadDump();
        path = getTmpDir();
    }

    @Test
    public void test() throws InterruptedException {
        runRolling(Math.min(64, Runtime.getRuntime().availableProcessors() * 4) - 1, 100, () -> { });
    }

    @Test
    public void writerFailureReachesTheReaderAndClosesWorkers() {
        AssertionError original = new AssertionError("Deliberate appender setup failure");
        AssertionError actual = assertThrows(AssertionError.class,
                () -> runRolling(2, 1, () -> { throw original; }));
        assertSame(original, actual.getCause());
        assertEquals("Writer cleanup must complete without secondary failures", 0, actual.getSuppressed().length);
    }

    @Test
    public void interruptedCleanupJoinsEveryWriter() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(2);
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Thread worker = new Thread(() -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException expected) {
                    Thread.currentThread().interrupt();
                }
            }, "rolling-cleanup-control-" + i);
            workers.add(worker);
            worker.start();
        }
        try {
            assertTrue(started.await(5, TimeUnit.SECONDS));
            Thread.currentThread().interrupt();
            stopAndJoin(workers);
            assertTrue("Cleanup must retain the caller's interruption", Thread.currentThread().isInterrupted());
            for (Thread worker : workers)
                assertFalse("Cleanup must join every writer", worker.isAlive());
        } finally {
            Thread.interrupted();
            stopAndJoin(workers);
        }
    }

    private void runRolling(int writers, int messages, Runnable beforeReady) throws InterruptedException {
        AtomicInteger written = new AtomicInteger();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        AtomicBoolean stopping = new AtomicBoolean();
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch startWriting = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        Throwable original = null;
        try {
            for (int i = 0; i < writers; i++) {
                Thread thread = new Thread(() -> {
                    try (ChronicleQueue queue = ChronicleQueue.singleBuilder(path)
                            .testBlockSize().rollCycle(TEST_SECONDLY).build();
                         ExcerptAppender appender = queue.createAppender()) {
                        beforeReady.run();
                        // Construction also takes the write lock. Prepare each appender on
                        // its owner before the unchanged concurrent rolling workload starts.
                        ready.countDown();
                        startWriting.await();
                        for (int message = 0; message < messages && !stopping.get(); message++) {
                            if (Thread.currentThread().isInterrupted())
                                throw new InterruptedException("Rolling writer interrupted");
                            long millis = System.currentTimeMillis() % 100;
                            if (millis > 1 && millis < 99)
                                Jvm.pause(99 - millis);
                            Map<String, Object> map = new HashMap<>();
                            map.put("key", Thread.currentThread().getName() + " - " + message);
                            appender.writeMap(map);
                            written.incrementAndGet();
                        }
                    } catch (Throwable failure) {
                        if (!stopping.get() || !(failure instanceof InterruptedException
                                || failure instanceof InterruptedRuntimeException))
                            workerFailure.compareAndSet(null, failure);
                    }
                }, "appender-" + i);
                threads.add(thread);
                thread.start();
            }
            while (!ready.await(10, TimeUnit.MILLISECONDS))
                checkWriterFailure(workerFailure, written.get(), 0, 0);
            checkWriterFailure(workerFailure, written.get(), 0, 0);
            startWriting.countDown();

            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(path)
                    .testBlockSize().rollCycle(TEST_SECONDLY).build();
                 ExcerptTailer tailer = queue.createTailer()) {
                int read = 0;
                long started = System.nanoTime();
                while (read < writers * messages) {
                    checkWriterFailure(workerFailure, written.get(), read, tailer.index());
                    if (Thread.currentThread().isInterrupted())
                        throw new InterruptedException("Rolling reader interrupted; written=" + written + ", read=" + read);
                    if (tailer.readMap() != null)
                        read++;
                    else
                        Thread.yield();
                    assertTrue("Rolling deadline; written=" + written + ", read=" + read
                                    + ", index=" + Long.toHexString(tailer.index()),
                            System.nanoTime() - started < TimeUnit.SECONDS.toNanos(60));
                }
                assertEquals(writers * messages, read);
            }
        } catch (InterruptedException | RuntimeException | Error failure) {
            original = failure;
            throw failure;
        } finally {
            stopping.set(true);
            startWriting.countDown();
            try {
                stopAndJoin(threads);
                if (original == null)
                    checkWriterFailure(workerFailure, written.get(), writers * messages, 0);
            } catch (AssertionError cleanupFailure) {
                if (original == null)
                    throw cleanupFailure;
                original.addSuppressed(cleanupFailure);
            }
        }
    }

    private static void checkWriterFailure(AtomicReference<Throwable> failure, int written, int read, long index) {
        if (failure.get() != null)
            throw new AssertionError("Appender failed; written=" + written + ", read=" + read
                    + ", index=" + Long.toHexString(index), failure.get());
    }

    private static void stopAndJoin(List<Thread> threads) {
        // Interrupt every owner before waiting; interruptions must not abandon later
        // writers, and per-writer timeouts would multiply the cleanup budget.
        threads.forEach(Thread::interrupt);
        boolean interrupted = Thread.interrupted();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try {
            for (Thread thread : threads) {
                while (thread.isAlive() && System.nanoTime() < deadline) {
                    try {
                        TimeUnit.NANOSECONDS.timedJoin(thread, Math.max(1, deadline - System.nanoTime()));
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
            }
            for (Thread thread : threads)
                assertFalse("Rolling fixture thread remains alive: " + thread + ", state=" + thread.getState(), thread.isAlive());
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
