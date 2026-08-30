/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteAfterEOFException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;

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
    public void deletingOldestHistoricalRollPreservesPublishedMaximum() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong();
        final File oldestRoll;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("cycle-0"));
            oldestRoll = appender.currentFile();
            time.set(TEST_DAILY.lengthInMillis());
            appender.writeBytes(Bytes.from("cycle-1"));
            time.set(2L * TEST_DAILY.lengthInMillis());
            appender.writeBytes(Bytes.from("cycle-2"));

            assertTrue("test precondition: oldest historical roll must be removable", oldestRoll.delete());
            queue.refreshDirectoryListing();
            time.set(0);
            appender.writeBytes(Bytes.from("existing-appender"));
            assertEquals(2, appender.cycle());
        }

        try (SingleChronicleQueue reopened = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = reopened.createAppender()) {
            appender.writeBytes(Bytes.from("reopened-appender"));
            assertEquals(2, appender.cycle());
        }
    }

    @Test
    public void stalledAppenderDoesNotRecreateDeletedPublishedMaximum() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong stalledClock = new AtomicLong();
        final AtomicLong advancingClock = new AtomicLong();

        try (SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(stalledClock::get)
                .rollCycle(TEST_DAILY)
                .build();
             SingleChronicleQueue advancingQueue = SingleChronicleQueueBuilder.binary(directory)
                     .timeProvider(advancingClock::get)
                     .rollCycle(TEST_DAILY)
                     .build();
             ExcerptAppender stalledWriter = stalledQueue.createAppender()) {
            stalledWriter.writeBytes(Bytes.from("cycle-0"));

            advancingClock.set(TEST_DAILY.lengthInMillis());
            final File publishedFile;
            try (ExcerptAppender advancingWriter = advancingQueue.createAppender()) {
                advancingWriter.writeBytes(Bytes.from("cycle-1"));
                publishedFile = advancingWriter.currentFile();
            }

            assertTrue("test precondition: published maximum must be removed", publishedFile.delete());
            final long stalledWritePositionBefore = ((StoreAppender) stalledWriter).store.writePosition();

            // When wall time equals neither a newer nor the published cycle, an ordinary append
            // must still open the published maximum as existing-only and fail if it disappeared.
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> stalledWriter.writeBytes(Bytes.from("must-fail")));
            assertTrue(failure.getMessage().contains("Highest/current roll 1 disappeared"));
            assertFalse("failed append must not recreate the removed generation", publishedFile.exists());
            assertEquals("failure must precede sealing the stalled roll",
                    stalledWritePositionBefore, ((StoreAppender) stalledWriter).store.writePosition());
        }
    }

    @Test
    public void unusedAppenderDoesNotCreateDeletedPublishedMaximum() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong stalledClock = new AtomicLong();
        final AtomicLong advancingClock = new AtomicLong(TEST_DAILY.lengthInMillis());

        try (SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(stalledClock::get)
                .rollCycle(TEST_DAILY)
                .build();
             SingleChronicleQueue advancingQueue = SingleChronicleQueueBuilder.binary(directory)
                     .timeProvider(advancingClock::get)
                     .rollCycle(TEST_DAILY)
                     .build();
             ExcerptAppender unusedAppender = stalledQueue.createAppender()) {
            final File publishedFile;
            try (ExcerptAppender advancingWriter = advancingQueue.createAppender()) {
                advancingWriter.writeBytes(Bytes.from("cycle-1"));
                publishedFile = advancingWriter.currentFile();
            }

            assertEquals(1, stalledQueue.lastPublishedCycle());
            assertTrue("test precondition: published maximum must be removed", publishedFile.delete());

            // A first ordinary write follows the same existing-only rule as an already-live
            // appender when its target is the published maximum.
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> unusedAppender.writeBytes(Bytes.from("must-fail")));
            assertTrue(failure.getMessage().contains("Highest/current roll 1 disappeared"));
            assertFalse("failed append must not recreate the removed generation", publishedFile.exists());
        }
    }

    @Test(timeout = 10_000)
    public void incompletePublishedCycleIsReinitialised() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .timeoutMS(100)
                .build()) {
            try (ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("cycle-0");
                time.set(TEST_DAILY.lengthInMillis());
                appender.writeText("cycle-1");
            }

            final SingleChronicleQueueStore published = queue.storeForCycle(1, queue.epoch(), false, null);
            final MappedBytes bytes = published.bytes();
            bytes.writeInt(0, Wires.NOT_COMPLETE);
            bytes.releaseLast();
            queue.closeStore(published);

            expectException("Renamed un-acquirable segment file to");
            try (ExcerptAppender replacement = queue.createAppender()) {
                replacement.writeText("recovered-cycle-1");
                assertEquals(1, replacement.cycle());
            }
        }
    }

    @Test(timeout = 10_000)
    public void stalledAppenderReinitialisesIncompletePublishedCycle() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong stalledTime = new AtomicLong();
        final AtomicLong advancingTime = new AtomicLong(TEST_DAILY.lengthInMillis());

        try (SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(stalledTime::get)
                .rollCycle(TEST_DAILY)
                .timeoutMS(100)
                .build();
             SingleChronicleQueue advancingQueue = SingleChronicleQueueBuilder.binary(directory)
                     .timeProvider(advancingTime::get)
                     .rollCycle(TEST_DAILY)
                     .timeoutMS(100)
                     .build();
             ExcerptAppender stalledAppender = stalledQueue.createAppender()) {
            stalledAppender.writeText("cycle-0");
            try (ExcerptAppender advancingAppender = advancingQueue.createAppender()) {
                advancingAppender.writeText("cycle-1");
            }

            final SingleChronicleQueueStore published = advancingQueue.storeForCycle(
                    1, advancingQueue.epoch(), false, null);
            final MappedBytes bytes = published.bytes();
            bytes.writeInt(0, Wires.NOT_COMPLETE);
            bytes.releaseLast();
            advancingQueue.closeStore(published);

            expectException("Renamed un-acquirable segment file to");
            stalledAppender.writeText("recovered-cycle-1");
            assertEquals(1, stalledAppender.cycle());
        }
    }

    @Test(timeout = 10_000)
    public void steadyStateAppenderAndTailerReusePublishedCycleStore() throws IOException {
        final int messageCount = 2_000_000;
        final AtomicInteger acquisitions = new AtomicInteger();
        final StoreFileListener listener = new StoreFileListener() {
            @Override
            public void onAcquired(int cycle, File file) {
                acquisitions.incrementAndGet();
            }

            @Override
            public void onReleased(int cycle, File file) {
                // No action required: this regression counts unnecessary acquisitions.
            }
        };

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDirectory.newFolder())
                .storeFileListener(listener)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("warm-up");
            BackgroundResourceReleaser.releasePendingResources();
            acquisitions.set(0);

            // Approximately one second on the reference review host: long enough to exercise the
            // steady-state path repeatedly without making the assertion depend on wall-clock timing.
            for (int i = 0; i < messageCount; i++)
                appender.writeText("message-" + i);

            BackgroundResourceReleaser.releasePendingResources();
            assertEquals("steady-state appends must reuse the appender's open store", 0, acquisitions.get());

            try (ExcerptTailer tailer = queue.createTailer()) {
                try (DocumentContext warmUp = tailer.readingDocument()) {
                    assertTrue("test precondition: the first document must be readable", warmUp.isPresent());
                }
                BackgroundResourceReleaser.releasePendingResources();
                acquisitions.set(0);

                int read = 1;
                while (true) {
                    try (DocumentContext document = tailer.readingDocument()) {
                        if (!document.isPresent())
                            break;
                        read++;
                    }
                }

                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(messageCount + 1, read);
                assertEquals("steady-state tailing must reuse the tailer's open store", 0, acquisitions.get());
            }
        }
    }

    @Test
    public void deletingHighestRollWithMetadataFailsClosed() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong();
        final File highestRoll;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("cycle-0"));
            time.set(TEST_DAILY.lengthInMillis());
            appender.writeBytes(Bytes.from("cycle-1"));
            highestRoll = appender.currentFile();
            assertEquals(1, queue.lastPublishedCycle());
        }

        assertTrue("test precondition: highest roll must be removed after every handle is closed",
                highestRoll.delete());
        time.set(0);

        final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
            try (SingleChronicleQueue unexpectedlyOpened = SingleChronicleQueueBuilder.binary(directory)
                    .timeProvider(time::get)
                    .rollCycle(TEST_DAILY)
                    .build()) {
                assertFalse("the constructor must reject the missing current generation",
                        unexpectedlyOpened.isClosed());
            }
        });
        assertTrue(failure.getMessage().contains("Highest/current roll 1 disappeared"));
        assertFalse("failed reopen must not recreate the removed current roll", highestRoll.exists());
    }

    @Test
    public void deletingWholeQueueOfflineAllowsClockSelectedInitialCycle() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong(3L * TEST_DAILY.lengthInMillis());

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("cycle-3"));
            assertEquals(3, appender.cycle());
        }

        assertTrue("offline deletion must remove rolls and Queue metadata", IOTools.deleteDirWithFiles(directory));
        assertTrue("test precondition: recreate the now-new Queue directory", directory.mkdirs());
        time.set(0);

        try (SingleChronicleQueue newQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = newQueue.createAppender()) {
            appender.writeBytes(Bytes.from("new-queue"));
            assertEquals(0, appender.cycle());
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

    @Test(timeout = 15_000)
    public void stalledWriterSeesCyclePublishedByAnotherJvmWithoutRefreshingDirectoryListing()
            throws IOException, InterruptedException {
        final AtomicLong stalledClock = new AtomicLong();
        final File directory = queueDirectory.newFolder();

        try (SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(stalledClock::get)
                .rollCycle(TEST4_DAILY)
                .forceDirectoryListingRefreshIntervalMs(1)
                .build();
             ExcerptAppender stalledWriter = stalledQueue.createAppender()) {
            stalledWriter.writeText("initial");

            final Process publisher = JavaProcessBuilder.create(PublishLaterCycle.class)
                    .withProgramArguments(directory.getAbsolutePath(), "3")
                    .start();
            assertTrue("publisher process did not exit",
                    publisher.waitFor(10, TimeUnit.SECONDS));
            assertEquals("publisher process failed", 0, publisher.exitValue());
            assertEquals("table-store publication must be visible across JVMs",
                    3, stalledQueue.lastPublishedCycle());

            // Make a filesystem refresh observable. The stalled writer must follow the cycle
            // published through directory-listing.cq4t, without discovering this planted file.
            assertTrue(new File(directory, "19700106T4" + SingleChronicleQueue.SUFFIX).createNewFile());
            stalledClock.set(2);
            stalledWriter.writeText("follow-cross-process-publication");

            assertEquals(3, stalledWriter.cycle());
            assertEquals("the append path must not refresh the filesystem listing",
                    3, stalledQueue.lastPublishedCycle());
            assertEquals("an explicit refresh must still discover the planted file",
                    5, stalledQueue.lastCycle());
        }
    }

    public static final class PublishLaterCycle {
        public static void main(String[] args) {
            final File directory = new File(args[0]);
            final long clock = Long.parseLong(args[1]) * TEST4_DAILY.lengthInMillis();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                    .timeProvider(() -> clock)
                    .rollCycle(TEST4_DAILY)
                    .build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("published-by-child");
            }
        }
    }

    /// A document write that meets a seal continues in the next roll.
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

    /// The same guarantee for sequential byte writes.
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

    /// Only one advance per attempt: a second seal is reported, and the appender remains usable afterwards.
    @Test
    public void secondConsecutiveEofIsPropagated() throws IOException {
        final AtomicLong clock = new AtomicLong();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("before seals");
            final int sealedCycle = appender.cycle();
            sealCurrentCycle(appender);
            sealCycle(queue, sealedCycle + 1);

            // Keep the first sealed roll as the selected active cycle. The ordinary append may
            // advance once, but the unexpected second EOF must escape instead of looping.
            queue.tableStoreAcquire("listing.highestCycle", sealedCycle)
                    .setVolatileValue(sealedCycle);
            queue.tableStoreAcquire("listing.highestCycleWriteFloor", sealedCycle)
                    .setVolatileValue(sealedCycle);

            assertThrows(WriteAfterEOFException.class,
                    () -> appender.writeText("must fail at second EOF"));
            assertEquals(sealedCycle + 1, appender.cycle());
            assertEquals(1, queue.entryCount());

            // The failed acquisition must not poison the mapped Wire retained by the appender.
            appender.writeText("after propagated EOF");
            assertEquals(sealedCycle + 2, appender.cycle());
            assertEquals(2, queue.entryCount());
        }
    }

    /// At the last representable cycle the advance is refused before anything is created or published.
    @Test
    public void eofAdvanceRejectsCycleOverflowBeforeMutation() throws IOException {
        final long clock = (long) Integer.MAX_VALUE * TEST_DAILY.lengthInMillis();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDirectory.newFolder())
                .timeProvider(() -> clock)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("last cycle");
            assertEquals(Integer.MAX_VALUE, appender.cycle());
            sealCurrentCycle(appender);

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> appender.writeText("must not wrap"));
            assertTrue(failure.getMessage().contains("Cannot advance ordinary append"));
            assertEquals(Integer.MAX_VALUE, appender.cycle());
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

    private static void sealCycle(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, queue.epoch(), true, null);
             MappedBytes bytes = store.bytes()) {
            final Wire wire = queue.wireType().apply(bytes);
            wire.usePadding(store.dataVersion() > 0);
            assertTrue("test precondition: next roll must be sealed",
                    store.writeEOF(wire, queue.timeoutMS));
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
