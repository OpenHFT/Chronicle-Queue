/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WriteAfterEOFException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StoreAppenderTest extends QueueTestCommon {

    private static final String TEST_TEXT = "Some text some text some text";
    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);

    @Rule
    public final TemporaryFolder queueDirectory = new TemporaryFolder();

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void writingDocumentAcquisitionWorksAfterInterruptedAttempt() throws InterruptedException, IOException {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder()).testBlockSize().build();
             BlockedWriter blockedWriter = new BlockedWriter(queue);
             BlockingWriter blockingWriter = new BlockingWriter(queue)) {

            writeSomeText(queue, 5);
            blockedWriter.makeSuccessfulWrite();
            writeSomeText(queue, 5);

            expectTestText(queue, 11);

            blockingWriter.blockWrites();
            blockedWriter.makeInterruptedAttemptToWrite();
            blockingWriter.unblockWrites();
            writeSomeText(queue, 5);

            blockedWriter.makePostInterruptAttemptToWrite();

            expectTestText(queue, 16);
        }
    }

    @Test
    public void failureClosesBothWritersBeforeTheirQueue() throws Exception {
        AssertionError bodyFailure = new AssertionError("Deliberate failure with both writers owned");
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder())
                .testBlockSize().build()) {
            BlockedWriter blockedWriter = new BlockedWriter(queue);
            BlockingWriter blockingWriter = new BlockingWriter(queue);
            try (BlockedWriter ownedBlocked = blockedWriter;
                 BlockingWriter ownedBlocking = blockingWriter) {
                ownedBlocked.makeSuccessfulWrite();
                ownedBlocking.blockWrites();
                throw bodyFailure;
            } catch (AssertionError actual) {
                assertSame("Cleanup must preserve the original body failure", bodyFailure, actual);
                assertEquals("Cleanup must finish without secondary failures", 0, actual.getSuppressed().length);
            }
            assertFalse(blockedWriter.t.isAlive());
            assertFalse(blockingWriter.t != null && blockingWriter.t.isAlive());
            assertFalse("The writers must terminate before the queue closes", queue.isClosed());
        }
    }

    @Test
    public void testCanWriteAfterWriteAfterEOFExceptionIsThrown() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

        clock.addAndGet(-clock.get() % ONE_DAY);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .testBlockSize()
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            // Create an 'old' roll-cycle, then wait a day:
            appender.writingDocument().close();
            clock.addAndGet(ONE_DAY);

            // Write to a new cycle:
            appender.writingDocument().close();

            // The code now throws WriteAfterEOFException for the old cycle:
            clock.addAndGet(-1); // One millisecond earlier

            assertThrows(WriteAfterEOFException.class, // is this a race?
                    () -> appender.writingDocument().close());

            // advance back to the latest cycle and write
            clock.addAndGet(2);
            appender.writingDocument().close();

            assertEquals(3, queue.entryCount());
        }
    }

    private void expectTestText(ChronicleQueue chronicleQueue, int times) {
        try (final ExcerptTailer tailer = chronicleQueue.createTailer()) {
            for (int i = 0; i < times; i++) {
                assertEquals(TEST_TEXT, tailer.readText());
            }
        }
    }

    private void writeSomeText(ChronicleQueue chronicleQueue, int times) {
        try (final ExcerptAppender appender = chronicleQueue.createAppender()) {
            for (int i = 0; i < times; i++) {
                appender.writeText(TEST_TEXT);
            }
        }
    }

    static class BlockedWriter implements AutoCloseable {

        private Thread t;
        private final SingleChronicleQueue queue;
        private Semaphore waitingToAcquire;
        private Semaphore waitingAfterInterrupt;
        private final CountDownLatch attemptingWrite = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        BlockedWriter(SingleChronicleQueue queue) {
            this.queue = queue;
        }

        void makeSuccessfulWrite() {
            waitingToAcquire = new Semaphore(0);
            waitingAfterInterrupt = new Semaphore(0);
            t = new Thread(() -> {
                try {
                    makeInterruptedWriteAttemptThenTryAgain();
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    attemptingWrite.countDown();
                }
            });
            t.setName("blocked-writer");
            t.start();
            waitForThreads(waitingToAcquire, t, failure);
        }

        void makeInterruptedAttemptToWrite() throws InterruptedException {
            waitingToAcquire.release(1);
            // The worker must leave Semaphore.acquire before receiving the test interrupt.
            // A fixed sleep races with that handoff on slower or heavily loaded JVMs.
            assertTrue("Writer did not reach its queue acquisition", attemptingWrite.await(5, TimeUnit.SECONDS));
            assertWorkerHealthy(t, failure);
            t.interrupt();
            waitForThreads(waitingAfterInterrupt, t, failure);
        }

        void makePostInterruptAttemptToWrite() throws InterruptedException {
            waitingAfterInterrupt.release();
            joinWriter(t);
            if (failure.get() != null)
                throw new AssertionError("Interrupted writer failed", failure.get());
        }

        private void makeInterruptedWriteAttemptThenTryAgain() {
            try (final ExcerptAppender appender = queue.createAppender()) {
                appender.writeText(TEST_TEXT);
                acquire(waitingToAcquire);
                attemptingWrite.countDown();
                try (final DocumentContext documentContext = appender.writingDocument()) {
                    throw new AssertionError("We shouldn't get here " + documentContext);
                } catch (InterruptedRuntimeException e) {
                    // This is expected, we should get interrupted, clear the interrupt
                    Thread.interrupted();
                }
                acquire(waitingAfterInterrupt);
                appender.writeText(TEST_TEXT);
            }
        }

        @Override
        public void close() {
            if (t != null && t.isAlive()) {
                waitingToAcquire.release();
                waitingAfterInterrupt.release();
                t.interrupt();
                joinWriter(t);
            }
        }
    }

    static class BlockingWriter implements AutoCloseable {

        private Thread t;
        private final SingleChronicleQueue queue;
        private final Semaphore inWritingDocument = new Semaphore(0);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        BlockingWriter(SingleChronicleQueue queue) {
            this.queue = queue;
        }

        void blockWrites() {
            t = new Thread(() -> {
                try {
                    acquireWritingDocumentThenBlock();
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            });
            t.setName("blocking-writer");
            t.start();
            waitForThreads(inWritingDocument, t, failure);
        }

        void unblockWrites() {
            inWritingDocument.release(1);
            joinWriter(t);
            if (failure.get() != null)
                throw new AssertionError("Blocking writer failed", failure.get());
            t = null;
        }

        private void acquireWritingDocumentThenBlock() {
            try (final ExcerptAppender appender = queue.createAppender()) {
                try (final DocumentContext documentContext = appender.writingDocument()) {
                    acquire(inWritingDocument);
                    documentContext.rollbackOnClose();
                }
            }
        }

        @Override
        public void close() {
            if (t != null && t.isAlive())
                unblockWrites();
        }
    }

    private static void acquire(Semaphore semaphore) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new AssertionError("Unexpected interrupt before the queue acquisition", e);
        }
    }

    private static void waitForThreads(Semaphore semaphore, Thread worker, AtomicReference<Throwable> failure) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!semaphore.hasQueuedThreads()) {
            assertWorkerHealthy(worker, failure);
            assertTrue("Writer did not reach its handoff: " + worker + ", state=" + worker.getState(),
                    System.nanoTime() < deadline);
            Jvm.pause(1);
        }
    }

    private static void assertWorkerHealthy(Thread worker, AtomicReference<Throwable> failure) {
        if (failure.get() != null)
            throw new AssertionError("Writer failed before its handoff", failure.get());
        assertTrue("Writer exited before its handoff: " + worker, worker.isAlive());
    }

    private static void joinWriter(Thread worker) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        boolean interrupted = Thread.interrupted();
        try {
            while (worker.isAlive() && System.nanoTime() < deadline) {
                try {
                    TimeUnit.NANOSECONDS.timedJoin(worker, Math.max(1, deadline - System.nanoTime()));
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            assertFalse("Writer still alive: " + worker + ", state=" + worker.getState(), worker.isAlive());
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
