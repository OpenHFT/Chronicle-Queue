/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Long.toHexString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

@SuppressWarnings("this-escape")
public class TestDeleteQueueFile extends QueueTestCommon {

    private static final int NUM_REPEATS = 10;
    private static final int CYCLES_TO_DELETE_PER_ITERATION = 20;
    private final Path tempQueueDir = getTmpDir().toPath();
    private QueueTailerRace activeRace;

    public TestDeleteQueueFile() {
        globalTimeout = Timeout.seconds(180);
    }

    @Override
    protected void preAfter() {
        //! cleanupBoundsIncompleteHeaderAcquisition: fixture cleanup must also stop a reader left by a failed test body.
        if (activeRace != null)
            activeRace.close();
    }

    @Override
    protected void tearDown() {
        assertReadersStopped();
        super.tearDown();
    }

    @After
    @Override
    public void deleteTargetDirTestArtifacts() {
        assertReadersStopped();
        super.deleteTargetDirTestArtifacts();
    }

    private void assertReadersStopped() {
        //! teardownPreservesFilesWhileAReaderIsStillRunning covers ordinary and hugetlbfs artifact cleanup.
        //! Neither cleanup path may unlink a live reader's files after a failed shutdown.
        assertTrue("Deletion-race readers are still using their files",
                activeRace == null || activeRace.workers.isTerminated());
    }

    @Test
    public void testRefreshDirectoryListingWillUpdateFirstAndLastIndicesCorrectly() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            // delete the first and last files
            Files.delete(Paths.get(queueWithCycleDetails.rollCycles.get(0).filename));
            Files.delete(Paths.get(queueWithCycleDetails.rollCycles.get(2).filename));

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            queue.refreshDirectoryListing();

            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(queue.firstIndex()));
            assertEquals(toHexString(secondCycle.lastIndex), toHexString(queue.lastIndex()));

            // and create a tailer it should only read data in second file
            ExcerptTailer excerptTailer2 = queue.createTailer();
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(excerptTailer2.index()));
            readText(excerptTailer2, "test2");
        }
    }

    @Test
    public void tailerToStartWorksInFaceOfDeletedStoreFile() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // should be at correct index
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(tailer.toStart().index()));
        }
    }

    @Ignore("https://github.com/OpenHFT/Chronicle-Queue/issues/1151")
    @Test
    public void tailerToStartFromStartWorksInFaceOfDeletedStoreFile() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // should be at correct index
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(tailer.toStart().index()));
        }
    }

    @Test
    public void tailerToEndWorksInFaceOfDeletedStoreFile() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));

            // delete the last store
            Files.delete(Paths.get(thirdCycle.filename));

            // should be at correct index
            assertEquals(toHexString(secondCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));
        }
    }

    @Ignore("https://github.com/OpenHFT/Chronicle-Queue/issues/1151")
    @Test
    public void tailerToEndFromEndWorksInFaceOfDeletedStoreFile() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));

            // delete the last store
            Files.delete(Paths.get(thirdCycle.filename));

            // should be at correct index
            assertEquals(toHexString(secondCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));
        }
    }

    @Test
    public void firstAndLastIndexAreRefreshedAfterForceRefreshInterval() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, builder -> builder.forceDirectoryListingRefreshIntervalMs(250))) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // using old cached value
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(queue.firstIndex()));
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(queue.firstIndex()));

            // wait for cache to expire
            ((SetTimeProvider) queue.time()).advanceMillis(260);

            // using correct value
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(queue.firstIndex()));
        }
    }

    @Test
    public void tailingThroughDeletedCyclesWillRefreshThenRetry_Writable() throws IOException {
        tailingThroughDeletedCyclesWillRefreshThenRetry(qwcd -> qwcd.queue);
    }

    @Test
    public void tailingThroughDeletedCyclesWillRefreshThenRetry_ReadOnly() throws IOException {
        tailingThroughDeletedCyclesWillRefreshThenRetry(qwcd -> SingleChronicleQueueBuilder.binary(qwcd.queue.fileAbsolutePath())
                .rollCycle(RollCycles.FAST_DAILY)
                .readOnly(true)
                .build());
    }

    @Test
    public void tailingThroughDeletedCyclesWillRecoverToLaterCycle_Writable() throws IOException {
        tailingThroughDeletedCyclesWillRecoverToLaterCycle(qwcd -> qwcd.queue);
    }

    @Test
    public void tailingThroughDeletedCyclesWillRecoverToLaterCycle_ReadOnly() throws IOException {
        tailingThroughDeletedCyclesWillRecoverToLaterCycle(qwcd -> SingleChronicleQueueBuilder.binary(qwcd.queue.fileAbsolutePath())
                .rollCycle(RollCycles.FAST_DAILY)
                .readOnly(true)
                .build());
    }

    @Test
    public void deletingOldFilesChaosTest() throws Exception {
        // Like the other unlink-under-tailer cases, this requires deleting a mapped file.
        assumeFalse(OS.isWindows());
        ignoreException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");
        try (QueueTailerRace race = new QueueTailerRace(300)) {
            race.awaitReaders();
            progressivelyTruncateOldRollCycles(race.details);
        }
    }

    @Test
    public void deleteFileFromUnderTailerTest_StartOfRange() throws IOException {
        deleteFileFromUnderTailerTest(10, 0);
    }

    @Test
    public void deleteFileFromUnderTailerTest_MiddleOfRange() throws IOException {
        deleteFileFromUnderTailerTest(10, 5);
    }

    @Test
    public void deleteFileFromUnderTailerTest_EndOfRange() throws IOException {
        deleteFileFromUnderTailerTest(10, 8);
    }

    private void deleteFileFromUnderTailerTest(int numberOfCycles, int currentCycleIndex) throws IOException {
        assumeFalse(OS.isWindows());
        ignoreException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");
        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(numberOfCycles, null)) {
            try (final ExcerptTailer tailer = queueWithCycleDetails.queue.createTailer()) {
                final RollCycleDetails rollCycleBeingRead = queueWithCycleDetails.rollCycles.get(currentCycleIndex);
                final RollCycleDetails nextRollCycleToBeRead = queueWithCycleDetails.rollCycles.get(currentCycleIndex + 1);
                final long expectedLastIndexRead = queueWithCycleDetails.rollCycles.stream()
                        .filter(rc -> rc != nextRollCycleToBeRead)
                        .mapToLong(rc -> rc.lastIndex)
                        .reduce(-1, Long::max);
                tailer.moveToIndex(rollCycleBeingRead.firstIndex);
                long lastIndexRead;
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    // just read the record to ensure we've loaded the store
                    lastIndexRead = documentContext.index();
                }
                Jvm.startup().on(TestDeleteQueueFile.class, "First index read was " + toHexString(lastIndexRead));
                // Need to delete current and next cycle to trigger failure
                Files.delete(Paths.get(rollCycleBeingRead.filename));
                Files.delete(Paths.get(nextRollCycleToBeRead.filename));
                queueWithCycleDetails.queue.refreshDirectoryListing();
                // Now read through the rest of the queue
                while (true) {
                    try (final DocumentContext documentContext = tailer.readingDocument()) {
                        if (!documentContext.isPresent()) {
                            break;
                        }
                        lastIndexRead = documentContext.index();
                        // Do nothing
                    }
                }
                Jvm.startup().on(TestDeleteQueueFile.class, "Last index read was " + toHexString(lastIndexRead));
                assertEquals(toHexString(expectedLastIndexRead), toHexString(lastIndexRead));
            }
        }
    }

    private void progressivelyTruncateOldRollCycles(QueueWithCycleDetails queueWithCycleDetails) {
        try {
            int deletedUpTo = 0;
            // previously used for debug output; removed to avoid commented code smell
            while (!queueWithCycleDetails.rollCycles.isEmpty()) {
                Jvm.startup().on(TestDeleteQueueFile.class, "Deleting from " + deletedUpTo + " to " + (deletedUpTo + CYCLES_TO_DELETE_PER_ITERATION));
                for (int i = 0; i < CYCLES_TO_DELETE_PER_ITERATION; i++) {
                    final RollCycleDetails rollCycleDetails = queueWithCycleDetails.rollCycles.remove(0);
                    // debug trace removed; keep deletion behaviour unchanged
                    Files.delete(Paths.get(rollCycleDetails.filename));
                    deletedUpTo++;
                }
                queueWithCycleDetails.queue.refreshDirectoryListing();
                Jvm.pause(1_000);
            }
        } catch (IOException e) {
            Jvm.error().on(TestDeleteQueueFile.class, "Error occurred", e);
        }
    }

    private void deleteAllRollCyclesInRandomOrder(QueueWithCycleDetails queueWithCycleDetails) {
        try {
            int numberOfCycles = queueWithCycleDetails.rollCycles.size();
            int deleted = 0;
            while (queueWithCycleDetails.rollCycles.size() > 1) {
                // Don't delete the first cycle, we can't deal with that yet
                final int index = ThreadLocalRandom.current().nextInt(1, queueWithCycleDetails.rollCycles.size());
                final RollCycleDetails rollCycleDetails = queueWithCycleDetails.rollCycles.remove(index);
                deleted++;
                Jvm.startup().on(TestDeleteQueueFile.class, "Deleting " + rollCycleDetails.rollCycle + ": " + rollCycleDetails.filename + " (" + deleted + "/" + numberOfCycles + "), firstIndex=" + toHexString(rollCycleDetails.firstIndex) + ", lastIndex=" + toHexString(rollCycleDetails.lastIndex));
                Files.delete(Paths.get(rollCycleDetails.filename));
                queueWithCycleDetails.queue.refreshDirectoryListing();
                Jvm.pause(20);
            }
        } catch (IOException e) {
            Jvm.error().on(TestDeleteQueueFile.class, "Error occurred", e);
        }
    }

    @Test
    public void deletingRandomRollCyclesChaosTest() throws Exception {
        assumeFalse(OS.isWindows());
        ignoreException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");
        try (QueueTailerRace race = new QueueTailerRace(300)) {
            race.awaitReaders();
            deleteAllRollCyclesInRandomOrder(race.details);
        }
    }

    @Test
    public void failedDeletionStopsReadersBeforeQueueClose() throws Exception {
        final QueueTailerRace race = new QueueTailerRace(3);
        final QueueWithCycleDetails details = race.details;
        final IllegalStateException original = new IllegalStateException("controlled deletion failure");
        try (QueueTailerRace owned = race) {
            owned.awaitReaders();
            throw original;
        } catch (IllegalStateException failure) {
            assertSame(original, failure);
        }
        assertTrue("Both readers must stop before closing their queue", race.workers.isTerminated());
        assertTrue("The readers' queue must close on failure", details.queue.isClosed());
    }

    @Test
    public void interruptedCleanupStopsReadersAndPreservesInterrupt() throws Exception {
        final QueueTailerRace race = new QueueTailerRace(3);
        final QueueWithCycleDetails details = race.details;
        try {
            try (QueueTailerRace owned = race) {
                owned.awaitReaders();
                Thread.currentThread().interrupt();
            }
            assertTrue("Cleanup must restore the caller's interrupt", Thread.currentThread().isInterrupted());
            assertTrue("Both readers must stop before closing their queue", race.workers.isTerminated());
            assertTrue("The readers' queue must close on interruption", details.queue.isClosed());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @SuppressWarnings("try") // Restore the header used to clean up the negative control.
    public void cleanupBoundsIncompleteHeaderAcquisition() throws Exception {
        try (QueueTailerRace race = new QueueTailerRace(3);
             CycleHeaderOverride ignored = new CycleHeaderOverride(race.details.rollCycles.get(1), 0)) {
            CountDownLatch acquiring = new CountDownLatch(1);
            race.readers.add(race.workers.submit(() -> {
                acquiring.countDown();
                final SingleChronicleQueueStore store = race.details.queue.storeForCycle(
                        race.details.rollCycles.get(1).rollCycle, 0, false, null);
                try {
                    assertNull("An incomplete header must not produce a store", store);
                } finally {
                    if (store != null)
                        race.details.queue.closeStore(store);
                }
            }));
            assertTrue("The acquisition task must start", acquiring.await(5, TimeUnit.SECONDS));

            preAfter();

            assertTrue("Header acquisition must stop before cleanup returns", race.workers.isTerminated());
            assertTrue("The readers' queue must close", race.details.queue.isClosed());
        }
    }

    @Test
    @SuppressWarnings("try") // Restore the malformed header after the controlled worker failure.
    public void readerFailureIsReportedOnceAndQueueIsClosed() throws Exception {
        try (QueueTailerRace race = new QueueTailerRace(3);
             CycleHeaderOverride ignored = new CycleHeaderOverride(race.details.rollCycles.get(1), 1)) {
            Future<?> reader = race.workers.submit(() -> {
                SingleChronicleQueueStore store = race.details.queue.storeForCycle(
                        race.details.rollCycles.get(1).rollCycle, 0, false, null);
                if (store != null)
                    race.details.queue.closeStore(store);
            });
            race.readers.add(reader);
            ExecutionException original = assertThrows(ExecutionException.class, () -> reader.get(5, TimeUnit.SECONDS));
            assertTrue("The real malformed file must cause the failure", original.getCause() instanceof StreamCorruptedException);

            AssertionError reported = assertThrows(AssertionError.class, race::close);
            assertSame(original.getCause(), reported.getCause().getCause());
            assertTrue("A failed reader must terminate before queue closure", race.workers.isTerminated());
            assertTrue("A failed reader must not leak its queue", race.details.queue.isClosed());
            preAfter();
        }
    }

    @Test
    public void teardownPreservesFilesWhileAReaderIsStillRunning() throws Exception {
        try (QueueTailerRace race = new QueueTailerRace(3)) {
            race.awaitReaders();
            try {
                assertThrows(AssertionError.class, this::tearDown);
                assertThrows(AssertionError.class, this::deleteTargetDirTestArtifacts);
                assertTrue("A live reader's directory must remain intact", Files.isDirectory(tempQueueDir));
                assertTrue("A live reader's file must remain intact",
                        Files.exists(Paths.get(race.details.rollCycles.get(1).filename)));
            } finally {
                race.running.set(false);
            }
        }
    }

    private static final class CycleHeaderOverride implements AutoCloseable {
        private final MappedBytes bytes;
        private final int originalHeader;

        CycleHeaderOverride(RollCycleDetails cycle, int replacementHeader) throws IOException {
            bytes = MappedBytes.mappedBytes(new File(cycle.filename), OS.pageSize());
            originalHeader = bytes.readVolatileInt(0);
            bytes.writeVolatileInt(0, replacementHeader);
        }

        @Override
        public void close() {
            // Publish all four bytes atomically: a reader must not see a partially restored header.
            try {
                bytes.writeVolatileInt(0, originalHeader);
            } finally {
                bytes.close();
            }
        }
    }

    private final class QueueTailerRace implements AutoCloseable {
        private final QueueWithCycleDetails details;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final CountDownLatch readersStarted = new CountDownLatch(2);
        private final List<Thread> readerThreads = new ArrayList<>();
        private final ExecutorService workers = Executors.newFixedThreadPool(2, task -> {
            Thread thread = Executors.defaultThreadFactory().newThread(task);
            thread.setName("deletion-reader-" + readerThreads.size());
            readerThreads.add(thread);
            return thread;
        });
        private final List<Future<?>> readers = new ArrayList<>();
        private boolean started;

        QueueTailerRace(int cycles) {
            //! cleanupBoundsIncompleteHeaderAcquisition: all fixture records are committed before readers start.
            //! An incomplete file left by deletion has no publisher to wait for; the default 10s wait exceeds shutdown's 5s.
            details = createQueueWithNRollCycles(cycles, builder -> builder.timeoutMS(0));
            activeRace = this;
        }

        void awaitReaders() throws Exception {
            if (!started) {
                started = true;
                readers.add(workers.submit(new QueueTailer(running, details, TailerDirection.FORWARD, readersStarted)));
                readers.add(workers.submit(new QueueTailer(running, details, TailerDirection.BACKWARD, readersStarted)));
            }
            if (!readersStarted.await(5, TimeUnit.SECONDS)) {
                // A failed reader should report its cause, not just a readiness timeout.
                for (Future<?> reader : readers)
                    if (reader.isDone())
                        reader.get();
                fail("Both deletion-race readers must read a document before deletion starts");
            }
        }

        @Override
        public void close() {
            //! readerFailureIsReportedOnceAndQueueIsClosed: fixture cleanup must not rethrow an already-observed worker failure.
            if (workers.isTerminated() && details.isClosed())
                return;
            running.set(false);
            workers.shutdown();
            boolean interrupted = Thread.interrupted();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            try {
                while (!workers.isTerminated()) {
                    try {
                        final long remaining = deadline - System.nanoTime();
                        if (remaining <= 0 || !workers.awaitTermination(remaining, TimeUnit.NANOSECONDS))
                            throw readersStillRunning();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                Exception failure = null;
                for (Future<?> reader : readers) {
                    try {
                        reader.get();
                    } catch (Exception e) {
                        if (failure == null)
                            failure = e;
                        else
                            failure.addSuppressed(e);
                    }
                }
                try {
                    details.close();
                } catch (RuntimeException e) {
                    if (failure == null)
                        throw e;
                    failure.addSuppressed(e);
                }
                if (failure != null)
                    throw new AssertionError("Deletion-race reader failed", failure);
            } finally {
                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        }

        private AssertionError readersStillRunning() {
            StringBuilder message = new StringBuilder("Deletion-race readers did not stop; their queue and files remain open");
            for (Thread thread : readerThreads) {
                @SuppressWarnings("deprecation") // Thread.threadId() is unavailable on Java 8.
                long threadId = thread.getId();
                ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(threadId, Integer.MAX_VALUE);
                message.append('\n').append(thread.getName()).append(": ");
                if (info == null) {
                    message.append("terminated");
                    continue;
                }
                message.append(info.getThreadState()).append(", lock=").append(info.getLockInfo())
                        .append(", owner=").append(info.getLockOwnerName()).append(" id=").append(info.getLockOwnerId());
                for (StackTraceElement frame : info.getStackTrace())
                    message.append("\n  at ").append(frame);
            }
            return new AssertionError(message.toString());
        }
    }

    private static class QueueTailer implements Runnable {

        private final AtomicBoolean running;
        private final QueueWithCycleDetails queueWithCycleDetails;
        private final TailerDirection direction;
        private final CountDownLatch readersStarted;

        QueueTailer(AtomicBoolean running, QueueWithCycleDetails queueWithCycleDetails, TailerDirection direction, CountDownLatch readersStarted) {
            this.running = running;
            this.queueWithCycleDetails = queueWithCycleDetails;
            this.direction = direction;
            this.readersStarted = readersStarted;
        }

        @Override
        public void run() {
            boolean firstRead = true;
            try {
                while (running.get()) {
                    try (final ExcerptTailer tailer = queueWithCycleDetails.queue.createTailer().direction(direction)) {
                        if (direction == TailerDirection.BACKWARD) {
                            tailer.toEnd();
                        } else {
                            tailer.toStart();
                        }
                        Jvm.startup().on(TestDeleteQueueFile.class, direction + " Tailer starting at index=" + toHexString(tailer.index()) + ", cycle=" + tailer.cycle());
                        int cyclesRead = 0;
                        long lastReadIndex = -5;
                        int currentCycle = -1;
                        while (running.get()) {
                            try (final DocumentContext documentContext = readingDocumentWithRetries(tailer)) {
                                if (!documentContext.isPresent()) {
                                    logIterationResult(direction, tailer, cyclesRead, lastReadIndex);
                                    break;
                                }
                                if (firstRead) {
                                    readersStarted.countDown();
                                    firstRead = false;
                                }
                                lastReadIndex = documentContext.index();
                                final int cycle = queueWithCycleDetails.queue.rollCycle().toCycle(lastReadIndex);
                                if (cycle != currentCycle) {
                                    Jvm.startup().on(TestDeleteQueueFile.class, direction + " reading cycle " + cycle);
                                    currentCycle = cycle;
                                    cyclesRead++;
                                }
                            } catch (RuntimeException e) {
                                Jvm.error().on(TestDeleteQueueFile.class, "Failed after reading " + lastReadIndex);
                                throw e;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new AssertionError("Deletion-race reader failed: " + direction, e);
            }
            Jvm.startup().on(TestDeleteQueueFile.class, "Tailer thread terminated: " + direction);
        }

        private DocumentContext readingDocumentWithRetries(ExcerptTailer excerptTailer) {
            DocumentContext documentContext = null;
            for (int i = 0; i < 2; i++) {
                documentContext = excerptTailer.readingDocument();
                if (documentContext.isPresent()) {
                    break;
                }
            }
            return documentContext;
        }

        private OptionalLong lastAvailableIndex() {
            return queueWithCycleDetails.rollCycles.stream().mapToLong(rc -> rc.lastIndex).reduce(Long::max);
        }

        private OptionalLong firstAvailableIndex() {
            return queueWithCycleDetails.rollCycles.stream().mapToLong(rc -> rc.firstIndex).findFirst();
        }

        private int remainingCycles() {
            return queueWithCycleDetails.rollCycles.size();
        }

        private void logIterationResult(TailerDirection direction, ExcerptTailer tailer, int cyclesRead, long lastReadIndex) {
            final int remainingCycles = remainingCycles();
            // Check we read at least the number of cycles remaining now
            if (cyclesRead < remainingCycles) {
                Jvm.error().on(TestDeleteQueueFile.class, direction + " didn't read all remaining cycles cyclesRead=" + cyclesRead + ", cyclesRemaining=" + remainingCycles);
            }
            // Check we got to the end if we're moving forward
            if (direction == TailerDirection.FORWARD) {
                lastAvailableIndex().ifPresent(lastIndex -> {
                    if (lastReadIndex < lastIndex) {
                        logError(tailer, lastIndex, cyclesRead);
                    }
                });
                return;
            }
            // Check we got to the start if we're moving backwards
            if (direction == TailerDirection.BACKWARD) {
                firstAvailableIndex().ifPresent(firstIndex -> {
                    if (lastReadIndex > firstIndex) {
                        logError(tailer, firstIndex, cyclesRead);
                    }
                });
                return;
            }
            // Otherwise report what we read
            Jvm.startup().on(TestDeleteQueueFile.class, direction + " Tailer read " + cyclesRead + " cycles of " + remainingCycles + " remaining (read should always be >= remaining)");
        }

        private void logError(ExcerptTailer tailer, long lastIndex, int cyclesRead) {
            String firstLast = direction == TailerDirection.BACKWARD ? "first" : "last";
            String error = String.format("Didn't get to %s. lastReadIndex=%x, lastReadCycle=%d, %sIndex=%x, %sCycle=%d, cyclesRead=%d",
                    direction == TailerDirection.BACKWARD ? "start" : "end",
                    tailer.lastReadIndex(),
                    queueWithCycleDetails.queue.rollCycle().toCycle(tailer.lastReadIndex()),
                    firstLast,
                    lastIndex,
                    firstLast,
                    queueWithCycleDetails.queue.rollCycle().toCycle(lastIndex),
                    cyclesRead);
            Jvm.error().on(TestDeleteQueueFile.class, error);
        }
    }

    private void tailingThroughDeletedCyclesWillRefreshThenRetry(Function<QueueWithCycleDetails, SingleChronicleQueue> queueCreator) throws IOException {
        assumeFalse(OS.isWindows());
        expectException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null);
             SingleChronicleQueue queue = queueCreator.apply(queueWithCycleDetails)
        ) {
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // delete the store files
            Files.delete(Paths.get(firstCycle.filename));
            Files.delete(Paths.get(secondCycle.filename));
            Files.delete(Paths.get(thirdCycle.filename));

            int counter = 0;
            while (true) {
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    if (!documentContext.isPresent()) {
                        break;
                    }
                    counter++;
                }
            }
            assertEquals(10, counter); // we still get 10 because the current store is in memory
        }
    }

    private void tailingThroughDeletedCyclesWillRecoverToLaterCycle(Function<QueueWithCycleDetails, SingleChronicleQueue> queueCreator) throws IOException {
        assumeFalse(OS.isWindows());
        expectException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null);
             SingleChronicleQueue queue = queueCreator.apply(queueWithCycleDetails)
        ) {
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            List<String> observedText = new ArrayList<>();
            List<Long> observedIndexes = new ArrayList<>();
            try (ExcerptTailer tailer = queue.createTailer()) {
                String firstText = tailer.readText();
                assertEquals("test1", firstText);
                observedText.add(firstText);
                observedIndexes.add(tailer.lastReadIndex());

                // Delete the mapped current cycle and the next cycle. The third cycle remains on disk,
                // so recovery must scan past both deleted files and continue from that later cycle.
                Files.delete(Paths.get(firstCycle.filename));
                Files.delete(Paths.get(secondCycle.filename));

                for (int i = 1; i < NUM_REPEATS * 2; i++) {
                    observedText.add(tailer.readText());
                    observedIndexes.add(tailer.lastReadIndex());
                }
                assertNull("recovery must end after the surviving messages", tailer.readText());

                assertEquals(thirdCycle.lastIndex, tailer.lastReadIndex());
            }

            List<String> expectedText = new ArrayList<>();
            expectedText.addAll(Collections.nCopies(NUM_REPEATS, "test1"));
            expectedText.addAll(Collections.nCopies(NUM_REPEATS, "test3"));
            assertEquals(expectedText, observedText);
            List<Long> expectedIndexes = new ArrayList<>();
            for (RollCycleDetails cycle : new RollCycleDetails[]{firstCycle, thirdCycle}) {
                for (long index = cycle.firstIndex; index <= cycle.lastIndex; index++)
                    expectedIndexes.add(index);
            }
            assertEquals(expectedIndexes, observedIndexes);
        }
    }

    /**
     * Create a queue with N roll cycles
     *
     * @param numberOfCycles  The number of cycles to create
     * @param builderConsumer A consumer that can optionally modify the queue settings
     * @return The queue and the details of the roll cycles created
     */
    private QueueWithCycleDetails createQueueWithNRollCycles(int numberOfCycles, Consumer<SingleChronicleQueueBuilder> builderConsumer) {
        SetTimeProvider timeProvider = new SetTimeProvider();
        QueueStoreFileListener listener = new QueueStoreFileListener();
        final SingleChronicleQueueBuilder queueBuilder = SingleChronicleQueueBuilder.binary(tempQueueDir.resolve("unitTestQueue"))
                .rollCycle(RollCycles.FAST_DAILY)
                .timeProvider(timeProvider)
                .testBlockSize()
                .storeFileListener(listener);
        if (builderConsumer != null) {
            builderConsumer.accept(queueBuilder);
        }
        SingleChronicleQueue queue = queueBuilder
                .build();
        List<RollCycleDetails> rollCycleDetails;
        try (ExcerptAppender appender = queue.createAppender()) {

            assertEquals(Long.MAX_VALUE, queue.firstIndex());

            rollCycleDetails = IntStream.range(0, numberOfCycles)
                    .mapToObj(i -> {
                        long firstIndexInCycle = writeTextAndReturnFirstIndex(appender, "test" + (i + 1));
                        long lastIndexInCycle = appender.lastIndexAppended();
                        timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(1));
                        BackgroundResourceReleaser.releasePendingResources();
                        return new RollCycleDetails(
                                queue.rollCycle().toCycle(firstIndexInCycle),
                                firstIndexInCycle,
                                lastIndexInCycle,
                                listener.lastFileAcquired.getAbsolutePath());
                    }).collect(Collectors.toList());
        }

        // There should be 3 acquired files, for roll cycles 1, 2, 3
        assertEquals(numberOfCycles, rollCycleDetails.size());

        // now let's create one tailer which will read all content
        try (ExcerptTailer excerptTailer = queue.createTailer()) {
            for (int i = 0; i < numberOfCycles; i++) {
                readText(excerptTailer, "test" + (i + 1));
            }
        }

        return new QueueWithCycleDetails(queue, new CopyOnWriteArrayList<>(rollCycleDetails));
    }

    static class QueueWithCycleDetails extends AbstractCloseable {
        final SingleChronicleQueue queue;
        final List<RollCycleDetails> rollCycles;

        QueueWithCycleDetails(SingleChronicleQueue queue, List<RollCycleDetails> rollCycles) {
            this.queue = queue;
            this.rollCycles = rollCycles;
        }

        @Override
        protected void performClose() throws IllegalStateException {
            Closeable.closeQuietly(queue);
        }
    }

    static class RollCycleDetails {
        final int rollCycle;
        final long firstIndex;
        final long lastIndex;
        final String filename;

        RollCycleDetails(int rollCycle, long firstIndex, long lastIndex, String filename) {
            this.rollCycle = rollCycle;
            this.firstIndex = firstIndex;
            this.lastIndex = lastIndex;
            this.filename = filename;
        }
    }

    /**
     * Write the specified text the specified number of times, return the index of the first entry written
     */
    private long writeTextAndReturnFirstIndex(ExcerptAppender appender, String text) {
        long firstIndex = -1;
        for (int i = 0; i < NUM_REPEATS; i++) {
            appender.writeText(text);
            if (firstIndex < 0) {
                firstIndex = appender.lastIndexAppended();
            }
        }
        return firstIndex;
    }

    /**
     * Read the specified text the specified number of times
     */
    private void readText(ExcerptTailer tailer, String text) {
        for (int i = 0; i < NUM_REPEATS; i++) {
            assertEquals(text, tailer.readText());
        }
    }

    static final class QueueStoreFileListener implements StoreFileListener {

        private File lastFileAcquired;

        @Override
        public void onReleased(int cycle, File file) {
        }

        @Override
        public void onAcquired(int cycle, File file) {
            Jvm.debug().on(TestDeleteQueueFile.class, "onAcquired called cycle: " + cycle + ", file: " + file);
            lastFileAcquired = file;
        }
    }
}
