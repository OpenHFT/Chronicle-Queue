/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;

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
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder()).build()) {
            final BlockingWriter blockingWriter = new BlockingWriter(queue);
            final BlockedWriter blockedWriter = new BlockedWriter(queue);

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
    public void writingDocumentIgnoresClockRollback() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

        clock.addAndGet(-clock.get() % ONE_DAY);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            // Create an 'old' roll-cycle, then wait a day:
            appender.writingDocument().close();
            clock.addAndGet(ONE_DAY);

            // Write to a new cycle:
            appender.writingDocument().close();

            final int latestCycle = queue.rollCycle().toCycle(appender.lastIndexAppended());

            // A backwards clock must not move an ordinary writer into the sealed old cycle.
            clock.addAndGet(-1); // One millisecond earlier

            appender.writingDocument().close();
            assertEquals(latestCycle, queue.rollCycle().toCycle(appender.lastIndexAppended()));

            // advance back to the latest cycle and write
            clock.addAndGet(2);
            appender.writingDocument().close();

            assertEquals(4, queue.entryCount());
        }
    }

    @Test
    public void sequentialWriteBytesIgnoresClockRollback() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        clock.addAndGet(-clock.get() % ONE_DAY);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("old-cycle"));
            clock.addAndGet(ONE_DAY);
            appender.writeBytes(Bytes.from("new-cycle"));

            final int latestCycle = queue.rollCycle().toCycle(appender.lastIndexAppended());

            clock.addAndGet(-1);
            appender.writeBytes(Bytes.from("must-not-reopen"));
            assertEquals(latestCycle, queue.rollCycle().toCycle(appender.lastIndexAppended()));

            clock.addAndGet(2);
            appender.writeBytes(Bytes.from("still-writable"));
            assertEquals(4, queue.entryCount());
        }
    }

    @Test
    public void stalledWriterFollowsAnotherWriterToLaterCycle() throws IOException {
        final AtomicLong advancingClock = new AtomicLong(System.currentTimeMillis());
        advancingClock.addAndGet(-advancingClock.get() % ONE_DAY);
        final AtomicLong stalledClock = new AtomicLong(advancingClock.get());
        final File directory = queueDirectory.newFolder();

        try (SingleChronicleQueue advancingQueue = SingleChronicleQueueBuilder.single(directory)
                .timeProvider(advancingClock::get)
                .build();
             SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.single(directory)
                     .timeProvider(stalledClock::get)
                     .build();
             ExcerptAppender advancingWriter = advancingQueue.createAppender();
             ExcerptAppender stalledWriter = stalledQueue.createAppender()) {
            stalledWriter.writeText("initial");

            advancingClock.addAndGet(3 * ONE_DAY);
            advancingWriter.writeText("advanced");
            final int latestCycle = advancingQueue.rollCycle().toCycle(advancingWriter.lastIndexAppended());

            try (DocumentContext document = stalledWriter.writingDocument()) {
                document.wire().write("message").text("followed-document");
            }
            assertEquals(latestCycle, stalledQueue.rollCycle().toCycle(stalledWriter.lastIndexAppended()));

            stalledWriter.writeBytes(Bytes.from("followed-bytes"));
            assertEquals(latestCycle, stalledQueue.rollCycle().toCycle(stalledWriter.lastIndexAppended()));
            assertEquals(4, stalledQueue.entryCount());
        }
    }

    @Test
    public void ordinaryAppendUsesPublishedCycleWithoutRefreshingDirectoryListing() throws IOException {
        final AtomicLong clock = new AtomicLong();
        final File directory = queueDirectory.newFolder();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(clock::get)
                .rollCycle(TEST4_DAILY)
                .forceDirectoryListingRefreshIntervalMs(1)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("initial");
            assertEquals(0, appender.cycle());

            // A valid later filename makes lastCycle() perform a directory refresh and publish 5.
            // The append hot path must use only cycles published by cooperating writers, so it
            // neither scans the directory nor jumps to an externally planted empty file.
            assertTrue(new File(directory, "19700106T4" + SingleChronicleQueue.SUFFIX).createNewFile());
            clock.incrementAndGet();

            appender.writeText("still-cycle-zero");
            assertEquals("ordinary append must not refresh the directory listing", 0, appender.cycle());
            assertEquals("the planted filename must be discoverable by an explicit refresh",
                    5, queue.lastCycle());
        }
    }

    @Test
    public void stalledWriterRollsWithoutRefreshingDirectoryListing() throws IOException {
        final AtomicLong advancingClock = new AtomicLong();
        final AtomicLong stalledClock = new AtomicLong();
        final File directory = queueDirectory.newFolder();

        try (SingleChronicleQueue advancingQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(advancingClock::get)
                .rollCycle(TEST4_DAILY)
                .forceDirectoryListingRefreshIntervalMs(1)
                .build();
             SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.binary(directory)
                     .timeProvider(stalledClock::get)
                     .rollCycle(TEST4_DAILY)
                     .forceDirectoryListingRefreshIntervalMs(1)
                     .build();
             ExcerptAppender advancingWriter = advancingQueue.createAppender();
             ExcerptAppender stalledWriter = stalledQueue.createAppender()) {
            stalledWriter.writeText("initial");

            advancingClock.set(3L * TEST4_DAILY.lengthInMillis());
            advancingWriter.writeText("published-cycle-three");
            assertEquals(3, stalledQueue.lastPublishedCycle());

            // If the stalled appender calls lastCycle() while rolling, the elapsed refresh interval
            // makes this external file visible and changes the published directory state to 5.
            assertTrue(new File(directory, "19700106T4" + SingleChronicleQueue.SUFFIX).createNewFile());
            stalledClock.set(2);

            stalledWriter.writeText("follow-published-cycle");

            assertEquals(3, stalledWriter.cycle());
            assertEquals("rolling an initialised appender must not refresh the directory listing",
                    3, stalledQueue.lastPublishedCycle());
            assertEquals("an explicit refresh must still discover the externally planted file",
                    5, stalledQueue.lastCycle());
        }
    }

    @Test
    public void ordinaryWritingDocumentRollsForwardPastSealedCurrentCycle() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        clock.addAndGet(-clock.get() % ONE_DAY);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("before seal");
            final int sealedCycle = appender.cycle();
            sealCurrentCycle(appender);

            appender.writeText("after seal");

            assertEquals(sealedCycle + 1, appender.cycle());
            assertEquals(2, queue.entryCount());
        }
    }

    @Test
    public void sequentialWriteBytesRollsForwardPastSealedCurrentCycle() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        clock.addAndGet(-clock.get() % ONE_DAY);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("before seal"));
            final int sealedCycle = appender.cycle();
            sealCurrentCycle(appender);

            appender.writeBytes(Bytes.from("after seal"));

            assertEquals(sealedCycle + 1, appender.cycle());
            assertEquals(2, queue.entryCount());
        }
    }

    @Test
    public void ordinaryEofRollJumpsToLatestPublishedCycleAcrossUnusedCycles() throws IOException {
        final AtomicLong stalledClock = new AtomicLong(System.currentTimeMillis());
        stalledClock.addAndGet(-stalledClock.get() % ONE_DAY);
        final AtomicLong advancingClock = new AtomicLong(stalledClock.get());
        final File directory = queueDirectory.newFolder();

        try (SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.single(directory)
                .timeProvider(stalledClock::get)
                .build();
             SingleChronicleQueue advancingQueue = SingleChronicleQueueBuilder.single(directory)
                     .timeProvider(advancingClock::get)
                     .build();
             ExcerptAppender stalledWriter = stalledQueue.createAppender();
             ExcerptAppender advancingWriter = advancingQueue.createAppender()) {
            final StoreAppender stalledAppender = (StoreAppender) stalledWriter;
            stalledWriter.writeText("before seal");
            final int sealedCycle = stalledAppender.cycle();
            sealCurrentCycle(stalledAppender);

            advancingClock.addAndGet(3 * ONE_DAY);
            advancingWriter.writeText("advanced across unused cycles");
            final int latestCycle = advancingWriter.cycle();
            assertEquals(sealedCycle + 3, latestCycle);

            stalledQueue.writeLock().lock();
            try {
                stalledAppender.rollForwardAfterEOF();
            } finally {
                stalledQueue.writeLock().unlock();
            }

            assertEquals("must join the latest published cycle, not create cycle N + 1",
                    latestCycle, stalledAppender.cycle());
            stalledWriter.writeText("joined latest cycle");
            assertEquals(latestCycle, stalledWriter.cycle());
            assertEquals(3, stalledQueue.entryCount());
        }
    }

    private static void sealCurrentCycle(StoreAppender appender) {
        final SingleChronicleQueueStore store = appender.store;
        if (store == null)
            throw new AssertionError("Appender has no current store");

        try (MappedBytes bytes = store.bytes()) {
            final Wire wire = appender.queue().wireType().apply(bytes);
            wire.usePadding(store.dataVersion() > 0);
            assertTrue("test precondition: current roll must be sealed",
                    store.writeEOF(wire, appender.queue().timeoutMS));
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

    static class BlockedWriter {

        private Thread t;
        private final SingleChronicleQueue queue;
        private Semaphore waitingToAcquire;
        private Semaphore waitingAfterInterrupt;

        BlockedWriter(SingleChronicleQueue queue) {
            this.queue = queue;
        }

        void makeSuccessfulWrite() {
            waitingToAcquire = new Semaphore(0);
            waitingAfterInterrupt = new Semaphore(0);
            t = new Thread(this::makeInterruptedWriteAttemptThenTryAgain);
            t.setName("blocked-writer");
            t.start();
            waitForThreads(waitingToAcquire);
        }

        void makeInterruptedAttemptToWrite() {
            waitingToAcquire.release(1);
            // Wait till the lock() call has been made
            Jvm.pause(10);
            t.interrupt();
            waitForThreads(waitingAfterInterrupt);
        }

        void makePostInterruptAttemptToWrite() throws InterruptedException {
            waitingAfterInterrupt.release();
            t.join();
        }

        private void makeInterruptedWriteAttemptThenTryAgain() {
            try (final ExcerptAppender appender = queue.createAppender()) {
                appender.writeText(TEST_TEXT);
                acquire(waitingToAcquire);
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
    }

    static class BlockingWriter {

        private Thread t;
        private final SingleChronicleQueue queue;
        private final Semaphore inWritingDocument = new Semaphore(0);

        BlockingWriter(SingleChronicleQueue queue) {
            this.queue = queue;
        }

        void blockWrites() {
            t = new Thread(this::acquireWritingDocumentThenBlock);
            t.setName("blocking-writer");
            t.start();
            waitForThreads(inWritingDocument);
        }

        void unblockWrites() throws InterruptedException {
            inWritingDocument.release(1);
            t.join();
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
    }

    private static void acquire(Semaphore semaphore) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new AssertionError("This shouldn't happen");
        }
    }

    private static void waitForThreads(Semaphore semaphore) {
        while (!semaphore.hasQueuedThreads()) {
            Jvm.pause(10);
        }
    }
}
