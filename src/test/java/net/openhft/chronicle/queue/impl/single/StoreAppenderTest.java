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

            advancingClock.addAndGet(ONE_DAY);
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
    public void ordinaryAppendPathsRollPastSealedCycle() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        clock.addAndGet(-clock.get() % ONE_DAY);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("initial");

            int expectedCycle = appender.cycle();
            sealCurrentCycle(appender);
            try (DocumentContext document = appender.writingDocument()) {
                document.wire().write("message").text("document-path");
            }
            assertEquals(++expectedCycle, appender.cycle());

            sealCurrentCycle(appender);
            appender.writeBytes(Bytes.from("direct-bytes"));
            assertEquals(++expectedCycle, appender.cycle());
            assertEquals(3, queue.entryCount());
        }
    }

    @Test
    public void ordinaryAppenderRollsForwardAfterIndexedRecoveryWithUnchangedClock() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        clock.addAndGet(-clock.get() % ONE_DAY);
        final long unchangedTime = clock.get();
        final Bytes<?> result = Bytes.elasticHeapByteBuffer();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender();
             ExcerptAppender secondAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("initial"));

            final int sealedCycle = appender.cycle();
            final long recoveredIndex = appender.lastIndexAppended() + 1;
            sealCurrentCycle(appender);

            expectException("queue=" + queue.fileAbsolutePath() + ", cycle=" + sealedCycle
                    + ", index=0x" + Long.toHexString(recoveredIndex));
            ((InternalAppender) appender).writeBytes(recoveredIndex, Bytes.from("recovered"));
            assertEquals("exact-index recovery belongs to the sealed cycle",
                    sealedCycle, appender.cycle());

            appender.writeBytes(Bytes.from("following ordinary entry"));
            assertEquals("ordinary append must roll past the restored EOF",
                    sealedCycle + 1, appender.cycle());
            final long firstIndexInNewCycle = appender.lastIndexAppended();
            assertEquals("the clock must remain unchanged throughout the test",
                    unchangedTime, clock.get());

            final long laterRecoveredIndex = recoveredIndex + 1;
            expectException("queue=" + queue.fileAbsolutePath() + ", cycle=" + sealedCycle
                    + ", index=0x" + Long.toHexString(laterRecoveredIndex));
            ((InternalAppender) appender).writeBytes(laterRecoveredIndex, Bytes.from("later recovered"));

            secondAppender.writeBytes(Bytes.from("second appender entry"));
            assertEquals("the appender created before the roll must join the latest cycle",
                    sealedCycle + 1, secondAppender.cycle());
            assertEquals("the second appender must follow the existing new-cycle entry",
                    firstIndexInNewCycle + 1, secondAppender.lastIndexAppended());

            try (ExcerptTailer tailer = queue.createTailer()) {
                assertNextBytes(tailer, result, "initial");
                assertNextBytes(tailer, result, "recovered");
                assertNextBytes(tailer, result, "later recovered");
                assertNextBytes(tailer, result, "following ordinary entry");
                assertNextBytes(tailer, result, "second appender entry");
            }
        }
    }

    private static void assertNextBytes(ExcerptTailer tailer, Bytes<?> result, String expected) {
        result.clear();
        assertTrue(tailer.readBytes(result));
        assertEquals(expected, result.toString());
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
