/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.ExceptionHandler;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.core.values.TwoLongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueSystemProperties;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.rollcycles.RollCycleArithmetic;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteAfterEOFException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_HOURLY;
import static net.openhft.chronicle.wire.MarshallableOut.UNSET_CONTEXT;
import static net.openhft.chronicle.wire.Wires.*;

public class StoreAppenderTest extends QueueTestCommon {

    private static final String TEST_TEXT = "Some text some text some text";
    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);
    private static final RollCycle ALIASING_POSITION_ROLL_CYCLE =
            new ConfigurableRollCycle("yyyyMMdd'A48'", (int) ONE_DAY, 512, 1 << 30);
    private static final RollCycle TWO_DAY_DAILY_ROLL_CYCLE =
            new ConfigurableRollCycle("yyyyMMdd'T2D'", (int) (2 * ONE_DAY), 8, 1);

    @Rule
    public final TemporaryFolder queueDirectory = new TemporaryFolder();

    private boolean originalCheckIndex;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
        originalCheckIndex = QueueSystemProperties.CHECK_INDEX;
    }

    @After
    public void restoreIndexChecks() {
        QueueSystemProperties.CHECK_INDEX = originalCheckIndex;
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
            assertTrue(failure.toString(),
                    failure.getMessage().contains("Highest/current roll 1 disappeared"));
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
            assertTrue(failure.toString(),
                    failure.getMessage().contains("Highest/current roll 1 disappeared"));
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
        final int messageCount = 100;
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

            // Multiple iterations catch per-document reacquisition without turning this correctness
            // regression into a throughput or wall-clock test.
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
    public void stalledWriterAdvancesOnceFromPublishedSealedCycle() throws IOException {
        final AtomicLong publishingClock = new AtomicLong();
        final AtomicLong stalledClock = new AtomicLong();
        final File directory = queueDirectory.newFolder();

        try (SingleChronicleQueue publishingQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(publishingClock::get)
                .rollCycle(TEST_DAILY)
                .build();
             SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.binary(directory)
                     .timeProvider(stalledClock::get)
                     .rollCycle(TEST_DAILY)
                     .build();
             ExcerptAppender publishingExcerpt = publishingQueue.createAppender();
             ExcerptAppender stalledExcerpt = stalledQueue.createAppender()) {
            final StoreAppender publishingWriter = (StoreAppender) publishingExcerpt;
            final StoreAppender stalledWriter = (StoreAppender) stalledExcerpt;

            stalledWriter.writeText("cycle-0");
            final File cycleZeroFile = stalledWriter.currentFile();
            assertEquals(0, stalledWriter.cycle());

            publishingClock.set(3L * TEST_DAILY.lengthInMillis());
            publishingWriter.writeText("cycle-3");
            final File cycleThreeFile = publishingWriter.currentFile();
            assertEquals(3, publishingWriter.cycle());
            sealCurrentCycle(publishingWriter);

            stalledWriter.writeText("cycle-4");
            final File cycleFourFile = stalledWriter.currentFile();

            assertEquals("the stalled clock must not select an intermediate cycle", 4, stalledWriter.cycle());
            assertEquals(4, stalledQueue.lastPublishedCycle());
            assertEquals(3, stalledQueue.entryCount());
            assertTrue(cycleZeroFile.exists());
            assertTrue(cycleThreeFile.exists());
            assertTrue(cycleFourFile.exists());
            assertEquals("cycles 1 and 2 must not be created", 3, cycleFileNames(directory).length);
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

    @Test
    public void exactRecoveryRejectsLegacyUnpaddedStore() throws Exception {
        ignoreException("reading control code as text");
        ignoreException("Unable to copy TimedStoreRecovery safely");
        ignoreException("Unexpected field lastAcknowledgedIndexReplicated");

        final File directory = queueDirectory.newFolder("legacy-unpadded");
        final Path source = Paths.get(StoreAppenderTest.class
                .getResource("/tr2/20170320.cq4").toURI());
        final Path legacyFile = directory.toPath().resolve(source.getFileName());
        Files.copy(source, legacyFile, StandardCopyOption.REPLACE_EXISTING);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            final int legacyCycle = queue.lastCycle();
            final long requestedIndex = queue.rollCycle().toIndex(legacyCycle, 0);
            try (SingleChronicleQueueStore store =
                         queue.storeForCycle(legacyCycle, queue.epoch(), false, null)) {
                assertEquals("test fixture must be a legacy unpadded store", 0, store.dataVersion());
            }

            final UnsupportedOperationException failure = org.junit.Assert.assertThrows(
                    UnsupportedOperationException.class,
                    () -> ((InternalAppender) appender).writeBytes(
                            requestedIndex, Bytes.from("must-not-be-written")));

            assertTrue(failure.getMessage().contains("legacy unpadded queue stores"));
        }
    }

    @Test
    public void crossCycleExactRecoveryRejectsLegacyTargetBeforeRollover() throws Exception {
        ignoreException("reading control code as text");
        ignoreException("Unable to copy TimedStoreRecovery safely");
        ignoreException("Unexpected field lastAcknowledgedIndexReplicated");

        final File directory = queueDirectory.newFolder("legacy-target");
        final Path source = Paths.get(StoreAppenderTest.class
                .getResource("/tr2/20170320.cq4").toURI());
        Files.copy(source, directory.toPath().resolve(source.getFileName()),
                StandardCopyOption.REPLACE_EXISTING);

        final int legacyCycle;
        final long nextCycleTime;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .testBlockSize()
                .build()) {
            legacyCycle = queue.lastCycle();
            nextCycleTime = queue.epoch()
                    + ((long) legacyCycle + 1) * queue.rollCycle().lengthInMillis();
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> nextCycleTime)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("padded current roll");
            final int currentCycle = appender.cycle();
            assertTrue(currentCycle > legacyCycle);
            assertFalse(hasEOF(queue, currentCycle));

            final long legacyIndex = queue.rollCycle().toIndex(legacyCycle, 0);
            final UnsupportedOperationException failure = org.junit.Assert.assertThrows(
                    UnsupportedOperationException.class,
                    () -> ((InternalAppender) appender).writeBytes(
                            legacyIndex, Bytes.from("must-not-be-written")));

            assertTrue(failure.getMessage().contains("legacy unpadded queue stores"));
            assertEquals("rejection must not move the appender", currentCycle, appender.cycle());
            assertFalse("rejection must not seal the current roll", hasEOF(queue, currentCycle));
        }
    }

    @Test
    public void writeEofReplacementUsesStorePadding() throws IOException {
        final Bytes<?> unavailableBytes = Bytes.allocateElasticOnHeap();
        final Wire unavailableWire = WireType.BINARY.apply(unavailableBytes);
        unavailableBytes.releaseLast();

        try (MappedBytes mappedBytes = MappedBytes.mappedBytes(queueDirectory.newFile(), 64 << 10);
             PaddingRecordingStore store = new PaddingRecordingStore(mappedBytes)) {
            assertTrue(store.writeEOF(unavailableWire, 1));
            assertTrue("replacement Wire must inherit the padded store format",
                    store.replacementUsesPadding);
        }
    }

    @Test
    public void exactRecoveryRejectsUnsupportedSequenceBeforeCreatingRoll() throws IOException {
        final File directory = queueDirectory.newFolder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            final long unsupportedIndex = queue.rollCycle().toIndex(
                    0, queue.rollCycle().maxMessagesPerCycle());

            final IllegalArgumentException rejection = org.junit.Assert.assertThrows(IllegalArgumentException.class,
                    () -> ((InternalAppender) appender).writeBytes(
                            unsupportedIndex, Bytes.from("unsupported")));
            assertTrue(rejection.getMessage(), rejection.getMessage().contains("maxMessagesPerCycle"));

            assertEquals("unsupported exact recovery must not commit a payload", 0, queue.entryCount());
            assertEquals("unsupported exact recovery must not create a roll", 0, countQueueFiles(directory));
        }
    }

    @Test
    public void ordinaryAppenderRollsForwardAfterHistoricalIndexedRecovery() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        clock.addAndGet(-clock.get() % ONE_DAY);
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
            assertFalse("the recovered cycle stays open for the rest of the backfill",
                    hasEOF(queue, sealedCycle));
            clock.addAndGet(ONE_DAY);
            appender.normaliseEOFs();

            appender.writeBytes(Bytes.from("following ordinary entry"));
            assertEquals("ordinary append must roll past completion's restored EOF",
                    sealedCycle + 1, appender.cycle());
            final long firstIndexInNewCycle = appender.lastIndexAppended();

            final long laterRecoveredIndex = recoveredIndex + 1;
            expectException("queue=" + queue.fileAbsolutePath() + ", cycle=" + sealedCycle
                    + ", index=0x" + Long.toHexString(laterRecoveredIndex));
            ((InternalAppender) appender).writeBytes(laterRecoveredIndex, Bytes.from("later recovered"));
            assertFalse(hasEOF(queue, sealedCycle));
            appender.normaliseEOFs();

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

    @Test
    public void exactWriteAdoptsReadyFirstRecordRetryBeforeWritingNext() throws IOException {
        final File directory = queueDirectory.newFolder();
        final long firstIndex;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("first"));
            firstIndex = appender.lastIndexAppended();

            // Simulate termination after publishing the ready data header but before publishing
            // the store write position. Sequence zero uses zero as its write-position sentinel.
            forceWritePosition(appender.store, 0);
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            expectException("Exact-index duplicate differs from ready content and was ignored");
            ((InternalAppender) appender).writeBytes(firstIndex, Bytes.from("retry-not-overwrite"));
            assertEquals(firstIndex, appender.lastIndexAppended());
            assertEquals(1, queue.entryCount());

            ((InternalAppender) appender).writeBytes(firstIndex + 1, Bytes.from("second"));
            assertEquals(firstIndex + 1, appender.lastIndexAppended());
            assertEquals(2, queue.entryCount());

            final Bytes<?> result = Bytes.elasticHeapByteBuffer();
            assertNextBytes(tailer, result, "first");
            assertNextBytes(tailer, result, "second");
            assertFalse(tailer.readBytes(result.clear()));
        }
    }

    @Test
    public void readyCrashDuplicatesCompareMatchingAndDifferentPayloads() throws IOException {
        for (boolean publishedPrefix : new boolean[]{false, true}) {
            assertReadyCrashDuplicateDiagnostic(publishedPrefix, true, true);
            assertReadyCrashDuplicateDiagnostic(publishedPrefix, false, true);
        }
    }

    @Test
    public void readyCrashDuplicateEqualityIsSilentWhenDebugDisabled() throws IOException {
        assertReadyCrashDuplicateDiagnostic(false, true, false);
        assertReadyCrashDuplicateDiagnostic(false, false, false);
    }

    private void assertReadyCrashDuplicateDiagnostic(boolean publishedPrefix, boolean matching, boolean debugEnabled) throws IOException {
        // As in restartPublishesReadyRecordLeftBeyondWritePosition, model interrupted publication rather than
        // the fully published state assumed by the optional acquisition-time index consistency diagnostic.
        QueueSystemProperties.CHECK_INDEX = false;
        final File directory = queueDirectory.newFolder();
        final long requestedIndex = TEST4_DAILY.toIndex(0, publishedPrefix ? 1 : 0);
        final long requestedPosition;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0).rollCycle(TEST4_DAILY).testBlockSize().build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            if (publishedPrefix) {
                appender.writeBytes(Bytes.from("published prefix"));
            } else {
                try (SingleChronicleQueueStore empty = queue.storeForCycle(0, queue.epoch(), true, null)) {
                    assertNotNull(empty);
                }
            }
            requestedPosition = commitRecordWithoutPublishing(queue, 0, Bytes.from("hello world"));
            commitRecordWithoutPublishing(queue, 0, Bytes.from("later ready record"));
        }

        final List<String> debugMessages = new ArrayList<>();
        final List<String> warningMessages = new ArrayList<>();
        final ExceptionHandler debugHandler = new ExceptionHandler() {
            @Override
            public void on(Logger logger, String message, Throwable thrown) {
                debugMessages.add(message);
            }

            @Override
            public boolean isEnabled(Class<?> type) {
                return debugEnabled;
            }
        };
        final Bytes<?> supplied = Bytes.from(matching ? "hello world" : "HELLO WORLD");
        final Bytes<?> result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0).rollCycle(TEST4_DAILY).testBlockSize().build();
             ExcerptAppender excerptAppender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            final long readPosition = supplied.readPosition();
            final long readLimit = supplied.readLimit();
            final long writePosition = supplied.writePosition();
            final long writeLimit = supplied.writeLimit();
            Jvm.setThreadLocalExceptionHandlers(null,
                    (logger, message, thrown) -> warningMessages.add(message), debugHandler);
            try {
                appender.writeBytes(requestedIndex, supplied);
            } finally {
                Jvm.setThreadLocalExceptionHandlers(null, null, null);
            }
            assertEquals(readPosition, supplied.readPosition());
            assertEquals(readLimit, supplied.readLimit());
            assertEquals(writePosition, supplied.writePosition());
            assertEquals(writeLimit, supplied.writeLimit());
            assertEquals("only the requested ready record is adopted", requestedPosition, appender.store.writePosition());
            assertEquals(requestedIndex, appender.lastIndexAppended());
            assertTrue(tailer.moveToIndex(requestedIndex));
            assertNextBytes(tailer, result, "hello world");
            assertNextBytes(tailer, result, "later ready record");
        } finally {
            supplied.releaseLast();
            result.releaseLast();
        }

        debugMessages.removeIf(message -> !message.contains("Exact-index duplicate"));
        assertEquals(matching && debugEnabled ? 1 : 0, debugMessages.size());
        assertEquals(matching ? 0 : 1, warningMessages.size());
        if (matching && debugEnabled)
            assertTrue(debugMessages.get(0).contains("duplicate matches ready content and was ignored"));
        if (!matching) {
            final String warning = warningMessages.get(0);
            assertTrue(warning.contains("duplicate differs from ready content and was ignored"));
            assertTrue(warning.contains("\nexisting:\n"));
            assertTrue(warning.contains("\nsupplied:\n"));
            final String normalisedHex = warning.replaceAll("\\s+", " ");
            assertTrue(normalisedHex.contains("68 65 6c 6c 6f 20 77 6f"));
            assertTrue(normalisedHex.contains("48 45 4c 4c 4f 20 57 4f"));
        }
    }

    @Test
    public void exactPreflightScansPublishedBoundaryWhenEncodedSequenceWasLost() throws IOException {
        final File directory = queueDirectory.newFolder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("first"));
            final long secondIndex = appender.lastIndexAppended() + 1;
            clearEncodedSequence(appender.store);

            ((InternalAppender) appender).writeBytes(secondIndex, Bytes.from("second"));

            assertEquals(secondIndex, appender.lastIndexAppended());
            assertQueueContents(queue, secondIndex - 1, "first", "second");
        }
    }

    @Test
    public void exactPreflightRejectsStaleAliasingEncodedSequence() throws IOException {
        final File directory = queueDirectory.newFolder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(ALIASING_POSITION_ROLL_CYCLE)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.wrapForRead(new byte[(1 << 16) - SPB_HEADER_SIZE]));
            final long firstPosition = appender.store.writePosition();
            final TwoLongValue writePositionAndSequence = writePositionAndSequence(appender.store);
            final long encodedFirstSequence = writePositionAndSequence.getVolatileValue2();

            appender.writeBytes(Bytes.from("second"));
            final long secondPosition = appender.store.writePosition();
            assertEquals("test positions must alias in the paired encoding",
                    1L << 16, secondPosition - firstPosition);
            writePositionAndSequence.setValue2(encodedFirstSequence);
            assertEquals("the stale pair must falsely identify the later position as sequence zero",
                    0, new RollCycleEncodeSequence(writePositionAndSequence,
                            ALIASING_POSITION_ROLL_CYCLE.defaultIndexCount(),
                            ALIASING_POSITION_ROLL_CYCLE.defaultIndexSpacing()).getSequence(secondPosition));

            final long sparseIndexBefore = appender.store.indexing.nextEntryToBeIndexed();
            try (MappedBytes bytes = appender.store.bytes()) {
                final Wire preflightWire = queue.wireType().apply(bytes);
                preflightWire.usePadding(appender.store.dataVersion() > 0);
                assertEquals("full-position preflight must recover the true published sequence",
                        1, appender.store.lastPublishedSequenceNumber(preflightWire));
            }
            assertEquals("read-only preflight must retain the stale pair",
                    encodedFirstSequence, writePositionAndSequence.getVolatileValue2());
            assertEquals("read-only preflight must not repair the sparse index",
                    sparseIndexBefore, appender.store.indexing.nextEntryToBeIndexed());

            final long thirdIndex = ALIASING_POSITION_ROLL_CYCLE.toIndex(0, 2);
            ((InternalAppender) appender).writeBytes(thirdIndex, Bytes.from("third"));

            assertEquals(thirdIndex, appender.lastIndexAppended());
            assertEquals(3, queue.entryCount());
        }
    }

    @Test
    public void restartScansCommittedRecordAfterSparseIndexPublicationWasLost() throws IOException {
        final File directory = queueDirectory.newFolder();
        final long lastCommittedIndex;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            final int indexedSequence = queue.indexSpacing();
            for (int i = 0; i <= indexedSequence; i++)
                appender.writeBytes(Bytes.from("entry-" + i));
            lastCommittedIndex = appender.lastIndexAppended();

            // recordCommittedData publishes writePosition before the sparse index entry. Clearing
            // that entry models a crash in that window without changing the committed record.
            assertTrue(appender.store.writePosition() > 0);
            appender.store.indexing.setPositionForSequenceNumber(
                    appender, indexedSequence, 0);
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            assertEquals("restart must scan forward from the last published sparse index",
                    lastCommittedIndex, queue.lastIndex());
            ((InternalAppender) appender).writeBytes(
                    lastCommittedIndex + 1, Bytes.from("after-crash"));
            assertEquals(lastCommittedIndex + 1, queue.lastIndex());
        }
    }

    @Test
    public void eosOnlyRestartPreservesReadySequenceZeroBeforeOrdinaryAppend() throws IOException {
        QueueSystemProperties.CHECK_INDEX = true;
        final File directory = queueDirectory.newFolder();
        final long firstIndex;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("first"));
            firstIndex = appender.lastIndexAppended();
            forceWritePosition(appender.store, 0);
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;

            // CQE may receive EOS without another exact-index event. Normalisation deliberately
            // leaves the active cycle open; appender restart must still discover its ready record.
            appender.normaliseEOFs();
            appender.writeBytes(Bytes.from("second"));

            assertEquals(firstIndex + 1, appender.lastIndexAppended());
            assertQueueContents(queue, firstIndex, "first", "second");
        }
    }

    @Test
    public void historicalWriteAfterReadyInterruptedRecordNormalisesAtCompletion() throws IOException {
        final File directory = queueDirectory.newFolder();
        final SetTimeProvider time = new SetTimeProvider();
        final long recoveredIndex;
        final int recoveredCycle;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time)
                .rollCycle(TEST_HOURLY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("recovered"));
            recoveredIndex = appender.lastIndexAppended();
            recoveredCycle = appender.cycle();

            // Simulate an already-advanced cursor, then a current-cycle sequence-zero recovery
            // which publishes its ready header but crashes before its write position or EOF.
            queue.tableStoreAcquire("normalisedEOFsTo", recoveredCycle + 1)
                    .setValue(recoveredCycle + 1);
            sealCurrentCycle(appender);
            removeEndOfData(queue, recoveredCycle);
            forceWritePosition(appender.store, 0);

            assertTrue(queue.tableStoreGet("normalisedEOFsTo") > recoveredCycle);
            assertFalse(hasEOF(queue, recoveredCycle));
        }

        time.advanceMillis(TimeUnit.MINUTES.toMillis(65));
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time)
                .rollCycle(TEST_HOURLY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            ((InternalAppender) appender).writeBytes(recoveredIndex + 1, Bytes.from("following"));

            assertTrue("adopting an interrupted ready record must lower the historical EOF cursor",
                    queue.tableStoreGet("normalisedEOFsTo") <= recoveredCycle);
            assertFalse("the adopted historical recovery stays open until completion",
                    hasEOF(queue, recoveredCycle));
            appender.normaliseEOFs();
            assertTrue("completion must reseal the adopted historical recovery",
                    hasEOF(queue, recoveredCycle));
            assertQueueContents(queue, recoveredIndex, "recovered", "following");
        }
    }

    @Test
    public void futureExactWriteDoesNotAdvanceEofCursorAcrossTimestampCurrentCycle() throws IOException {
        final SetTimeProvider time = new SetTimeProvider();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDirectory.newFolder())
                .timeProvider(time)
                .rollCycle(TEST_HOURLY)
                .testBlockSize()
                .build()) {
            final int currentCycle;
            try (ExcerptAppender writer = queue.createAppender()) {
                writer.writeBytes(Bytes.from("timestamp-current"));
                currentCycle = writer.cycle();
            }
            final int futureCycle = currentCycle + 2;

            // A freshly created appender has released the store selected by its construction-time
            // scan, so exact positioning reaches the private setWireIfNull normalization path.
            try (ExcerptAppender freshAppender = queue.createAppender()) {
                ((InternalAppender) freshAppender).writeBytes(
                        queue.rollCycle().toIndex(futureCycle, 0), Bytes.from("future"));
            }

            assertTrue("positioning on a future cycle must not advance the EOF cursor past the "
                            + "timestamp-current cycle",
                    queue.tableStoreGet("normalisedEOFsTo") <= currentCycle);
        }
    }

    @Test
    public void backfillBelowRolledBackHighWaterIsNormalisedAtCompletion() throws IOException {
        final SetTimeProvider time = new SetTimeProvider();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDirectory.newFolder())
                .timeProvider(time)
                .rollCycle(TEST_HOURLY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("cycle-0"));
            final long missingIndex = appender.lastIndexAppended() + 1;
            final int recoveredCycle = queue.rollCycle().toCycle(missingIndex);

            time.advanceMillis(TimeUnit.HOURS.toMillis(1));
            appender.writeBytes(Bytes.from("cycle-1"));
            appender.normaliseEOFs();
            assertTrue(hasEOF(queue, recoveredCycle));
            assertTrue(queue.tableStoreGet("normalisedEOFsTo") > recoveredCycle);

            time.advanceMillis(-TimeUnit.HOURS.toMillis(1));
            assertEquals(recoveredCycle, queue.cycle());
            removeEndOfData(queue, recoveredCycle);
            ((InternalAppender) appender).writeBytes(missingIndex, Bytes.from("recovered"));
            assertFalse("backfill remains open even below the published high-water",
                    hasEOF(queue, recoveredCycle));
            appender.normaliseEOFs();
            assertTrue("completion uses the published high-water to seal the recovered cycle",
                    hasEOF(queue, recoveredCycle));
        }
    }

    @Test
    public void dailyNonZeroEpochEnumeratesCycleZero() throws IOException {
        assertNonZeroEpochCycleZero(TEST_DAILY, ONE_DAY);
    }

    @Test
    public void hourlyNonZeroEpochEnumeratesCycleZero() throws IOException {
        assertNonZeroEpochCycleZero(TEST_HOURLY, ONE_DAY);
    }

    @Test
    public void weeklyNonZeroEpochEnumeratesCycleZero() throws IOException {
        final long epoch = (long) RollCycles.WEEKLY.defaultEpoch()
                + RollCycles.WEEKLY.lengthInMillis();
        assertNonZeroEpochCycleZero(RollCycles.WEEKLY, epoch);
    }

    private void assertNonZeroEpochCycleZero(RollCycle rollCycle, long epoch) throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong(epoch);
        final TreeSet<Long> cycleZero = new TreeSet<>(Arrays.asList(0L));

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .epoch(epoch)
                .rollCycle(rollCycle)
                .testBlockSize()
                .build();
             ExcerptAppender first = queue.createAppender()) {
            first.writeText("first");
            assertEquals(cycleZero, queue.listCyclesBetween(0, 0));

            try (ExcerptAppender second = queue.createAppender()) {
                second.normaliseEOFs();
                second.writeText("second");
                assertEquals(0, second.cycle());
            }
        }

        try (SingleChronicleQueue reopened = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .epoch(epoch)
                .rollCycle(rollCycle)
                .testBlockSize()
                .build();
             ExcerptAppender appender = reopened.createAppender()) {
            assertEquals(cycleZero, reopened.listCyclesBetween(0, 0));
            appender.normaliseEOFs();
            appender.writeText("after restart");
            assertEquals(0, appender.cycle());
            assertEquals(3, reopened.entryCount());
        }
    }

    @Test(timeout = 20_000)
    public void sparseRollsAreTraversedByExistingCycle() throws IOException {
        final int farCycle = 1_000_000;
        final File directory = queueDirectory.newFolder();
        final SetTimeProvider time = new SetTimeProvider();
        final long recoveredIndex;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time)
                .rollCycle(TEST_HOURLY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("cycle-zero"));
            recoveredIndex = appender.lastIndexAppended() + 1;

            time.currentTimeMillis((long) farCycle * TEST_HOURLY.lengthInMillis());
            appender.writeBytes(Bytes.from("far-cycle"));
            assertEquals(farCycle, appender.cycle());
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time)
                .rollCycle(TEST_HOURLY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            expectException("queue=" + queue.fileAbsolutePath() + ", cycle=0, index=0x"
                    + Long.toHexString(recoveredIndex));
            ((InternalAppender) appender).writeBytes(recoveredIndex, Bytes.from("backfilled"));
            assertFalse(hasEOF(queue, 0));

            appender.normaliseEOFs();

            assertTrue(hasEOF(queue, 0));
            assertEquals("completion must advance across the proven-empty sparse range",
                    farCycle, queue.tableStoreGet("normalisedEOFsTo"));
            final File[] rollFiles = directory.listFiles(
                    ignored -> ignored.getName().endsWith(SingleChronicleQueue.SUFFIX));
            assertEquals("normalisation must not create files for sparse gaps", 2,
                    rollFiles == null ? 0 : rollFiles.length);
        }
    }

    @Test
    public void malformedPhysicalRollNameFailsNormalisationWithoutAdvancingCursor() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("cycle-zero");
            time.set(TEST_DAILY.lengthInMillis());
            appender.writeText("cycle-one");
            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", 0);
            cursor.setValue(0);

            assertTrue(new File(directory, "not-a-roll" + SingleChronicleQueue.SUFFIX).createNewFile());
            final IllegalStateException failure = assertThrows(
                    IllegalStateException.class, appender::normaliseEOFs);

            assertTrue("the physical filename parse failure must remain visible",
                    containsCause(failure, DateTimeException.class));
            assertEquals("failed enumeration must not advance completion", 0,
                    cursor.getVolatileValue());
        }
    }

    @Test
    public void cachedCycleTreeDoesNotHideMalformedPhysicalRoll() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("cycle-zero");
            time.set(TEST_DAILY.lengthInMillis());
            appender.writeText("cycle-one");
            assertEquals(2, queue.listCyclesBetween(0, 1).size());

            final File malformed = new File(directory, "not-a-roll" + SingleChronicleQueue.SUFFIX);
            assertTrue(malformed.createNewFile());
            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", 0);
            cursor.setValue(0);
            final long currentWritePositionBefore = appender.store.writePosition();
            final long historicalWritePositionBefore = writePosition(queue, 0);
            final long listingModCountBefore = queue.tableStoreGet("listing.modCount");
            final String[] cycleFileNamesBefore = cycleFileNames(directory);

            final IllegalStateException failure = assertThrows(
                    IllegalStateException.class, appender::normaliseEOFs);

            assertTrue("the fresh scan must retain the filename parse failure",
                    containsCause(failure, DateTimeException.class));
            assertEquals(0, cursor.getVolatileValue());
            assertEquals(currentWritePositionBefore, appender.store.writePosition());
            assertEquals(historicalWritePositionBefore, writePosition(queue, 0));
            assertEquals(listingModCountBefore, queue.tableStoreGet("listing.modCount"));
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
        }
    }

    @Test
    public void cachedCycleEnumerationRejectsDeletedBoundaries() throws IOException {
        assumeFalse(OS.isWindows());
        for (int deletedCycle : new int[]{0, 1}) {
            final File directory = queueDirectory.newFolder();
            final AtomicLong clock = new AtomicLong();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                    .timeProvider(clock::get).rollCycle(TEST4_DAILY).testBlockSize().build();
                 ExcerptAppender excerptAppender = queue.createAppender()) {
                final StoreAppender appender = (StoreAppender) excerptAppender;
                appender.writeText("cycle-zero");
                final File cycleZero = appender.store.file();
                clock.set(TEST4_DAILY.lengthInMillis());
                appender.writeText("cycle-one");
                final File boundary = deletedCycle == 0 ? cycleZero : appender.store.file();
                assertEquals(2, queue.listCyclesBetween(0, 1).size());
                final long modCount = queue.tableStoreGet("listing.modCount");
                Files.delete(boundary.toPath());

                final IllegalStateException failure = assertThrows(IllegalStateException.class,
                        () -> queue.listCyclesBetween(0, 1));

                assertTrue(failure.getMessage().contains("file not found"));
                assertFalse(boundary.exists());
                assertEquals(modCount, queue.tableStoreGet("listing.modCount"));
                assertEquals(1, queue.lastPublishedCycle());
            }
        }
    }

    @Test
    public void duplicateLogicalCycleFilenameFailsNormalisationWithoutMutation() throws IOException {
        final File directory = queueDirectory.newFolder();
        final long epoch = java.time.Instant.parse("1970-01-02T00:00:00Z").toEpochMilli();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> epoch)
                .epoch(epoch)
                .rollCycle(TWO_DAY_DAILY_ROLL_CYCLE)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("canonical-cycle-zero");
            assertTrue("test precondition: canonical cycle zero must exist", queue.cycleFileExists(0));
            final File alias = new File(directory, "19700103T2D" + SingleChronicleQueue.SUFFIX);
            assertTrue("test precondition: a second date must alias cycle zero", alias.createNewFile());

            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", 0);
            cursor.setValue(0);
            final long writePositionBefore = appender.store.writePosition();
            final int nextWordBefore = readWordAfterLastEntry(appender.store);
            final long listingModCountBefore = queue.tableStoreGet("listing.modCount");
            final String[] cycleFileNamesBefore = cycleFileNames(directory);

            final IllegalStateException failure = assertThrows(
                    IllegalStateException.class, appender::normaliseEOFs);

            assertTrue("the non-canonical logical-cycle alias must remain visible",
                    containsCause(failure, DateTimeException.class));
            assertTrue(containsMessage(failure, "Non-canonical roll name 19700103T2D"));
            assertTrue(containsMessage(failure, "cycle 0 is 19700102T2D"));
            assertEquals(0, cursor.getVolatileValue());
            assertEquals(writePositionBefore, appender.store.writePosition());
            assertEquals(nextWordBefore, readWordAfterLastEntry(appender.store));
            assertEquals(listingModCountBefore, queue.tableStoreGet("listing.modCount"));
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
        }
    }

    @Test
    public void nonCanonicalWeeklyRollFailsNormalisationWithoutAdvancingCursor() throws IOException {
        final Locale originalFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
        final long epoch = java.time.Instant.parse("2022-01-03T00:00:00Z").toEpochMilli();
        try {
            for (Locale locale : new Locale[]{Locale.US, Locale.UK}) {
                Locale.setDefault(Locale.Category.FORMAT, locale);
                final File directory = queueDirectory.newFolder();
                try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                        .timeProvider(() -> epoch)
                        .epoch(epoch)
                        .rollCycle(RollCycles.WEEKLY)
                        .testBlockSize()
                        .build();
                     ExcerptAppender excerptAppender = queue.createAppender()) {
                    final StoreAppender appender = (StoreAppender) excerptAppender;
                    appender.writeText("cycle-zero");
                    final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", 0);
                    cursor.setValue(0);
                    assertTrue(new File(directory, "2021W53" + SingleChronicleQueue.SUFFIX).createNewFile());

                    final IllegalStateException failure = assertThrows(
                            IllegalStateException.class, appender::normaliseEOFs);

                    assertTrue(locale + " non-canonical week must remain visible",
                            containsCause(failure, DateTimeException.class));
                    assertEquals("failed weekly enumeration must not advance completion",
                            0, cursor.getVolatileValue());
                }
            }
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, originalFormatLocale);
        }
    }

    @Test
    public void corruptedEofNormalisationCursorFailsBeforeMutation() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("cycle-zero");
            final int historicalCycle = appender.cycle();
            time.set(TEST_DAILY.lengthInMillis());
            appender.writeText("cycle-one");
            removeEndOfData(queue, historicalCycle);

            final long corruptCursor = 0x1_0000_0005L;
            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", historicalCycle);
            cursor.setValue(corruptCursor);
            final long writePositionBefore = appender.store.writePosition();
            final long historicalWritePositionBefore = writePosition(queue, historicalCycle);
            final long listingModCountBefore = queue.tableStoreGet("listing.modCount");
            final String[] cycleFileNamesBefore = cycleFileNames(directory);

            final IllegalStateException failure = assertThrows(
                    IllegalStateException.class, appender::normaliseEOFs);

            assertTrue(failure.getMessage().contains("Invalid EOF normalisation cycle"));
            assertEquals(corruptCursor, cursor.getVolatileValue());
            assertFalse("an invalid cursor must not reseal a historical roll",
                    hasEOF(queue, historicalCycle));
            assertEquals(writePositionBefore, appender.store.writePosition());
            assertEquals(historicalWritePositionBefore, writePosition(queue, historicalCycle));
            assertEquals(listingModCountBefore, queue.tableStoreGet("listing.modCount"));
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
        }
    }

    @Test
    public void generationDisappearingAfterEnumerationDoesNotAdvanceCursor() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("cycle-zero");
            time.set(TEST_DAILY.lengthInMillis());
            appender.writeText("cycle-one");
            final File cycleOneFile = appender.currentFile();
            time.set(2L * TEST_DAILY.lengthInMillis());
            appender.writeText("cycle-two");
            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", 0);
            cursor.setValue(0);

            final IllegalStateException failure = assertThrows(
                    IllegalStateException.class, () -> appender.normaliseEOFs(() ->
                            assertTrue("test precondition: cycle one must be removed", cycleOneFile.delete())));

            assertTrue(failure.getMessage().contains("Roll generation disappeared"));
            assertEquals("a vanished generation must abort before cursor publication", 0,
                    cursor.getVolatileValue());
        }
    }

    @Test
    public void currentMappedGenerationDisappearingAfterEnumerationDoesNotAdvanceCursor() throws IOException {
        assumeFalse("Windows cannot unlink the appender's open mapping", OS.isWindows());
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("cycle-zero"));
            final long recoveredIndex = appender.lastIndexAppended() + 1;
            time.set(TEST_DAILY.lengthInMillis());
            appender.writeBytes(Bytes.from("cycle-one"));
            expectException("queue=" + queue.fileAbsolutePath() + ", cycle=0, index=0x"
                    + Long.toHexString(recoveredIndex));
            ((InternalAppender) appender).writeBytes(recoveredIndex, Bytes.from("recovered-cycle-zero"));
            assertEquals("test precondition: recovery must retain the historical mapping", 0, appender.cycle());
            assertFalse("test precondition: recovered history remains open", hasEOF(queue, 0));

            final File mappedHistoricalFile = appender.currentFile();
            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", 0);
            final long cursorBefore = cursor.getVolatileValue();
            final long writePositionBefore = appender.store.writePosition();
            final int nextWordBefore = readWordAfterLastEntry(appender.store);
            final long headerNumberBefore = appender.wire().headerNumber();
            final long lastIndexBefore = appender.lastIndexAppended();
            final long listingModCountBefore = queue.tableStoreGet("listing.modCount");

            final IllegalStateException failure = assertThrows(
                    IllegalStateException.class, () -> appender.normaliseEOFs(() -> assertTrue(
                            "test precondition: mapped history must be unlinked", mappedHistoricalFile.delete())));

            assertTrue(failure.getMessage().contains("Roll generation disappeared"));
            assertFalse(mappedHistoricalFile.exists());
            assertEquals(cursorBefore, cursor.getVolatileValue());
            assertEquals(writePositionBefore, appender.store.writePosition());
            assertEquals(nextWordBefore, readWordAfterLastEntry(appender.store));
            assertEquals(0, appender.cycle());
            assertEquals(headerNumberBefore, appender.wire().headerNumber());
            assertEquals(lastIndexBefore, appender.lastIndexAppended());
            assertEquals(listingModCountBefore, queue.tableStoreGet("listing.modCount"));
        }
    }

    @Test
    public void refreshedEmptyPhysicalRangeFailsClosed() {
        final IllegalStateException initialFailure = new IllegalStateException("stale initial bounds");
        final StoreAppender.ExistingCycles empty = new StoreAppender.ExistingCycles(
                UNSET_CONTEXT, UNSET_CONTEXT, new TreeSet<>());

        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> StoreAppender.requireConsistentRefreshedRange(
                        true, empty, "test-queue", initialFailure));

        assertTrue(failure.getMessage().contains("remained inconsistent"));
        assertEquals(initialFailure, failure.getSuppressed()[0]);
    }

    @Test(timeout = 20_000)
    public void restartRetriesIncompleteHistoricalRecoveryAndNormalisesEof()
            throws IOException, InterruptedException {
        QueueSystemProperties.CHECK_INDEX = false;
        final File directory = queueDirectory.newFolder();
        final Process failedRecovery = JavaProcessBuilder.create(CrashDuringHistoricalRecovery.class)
                .withProgramArguments(directory.getAbsolutePath())
                .start();
        assertTrue("recovery process did not exit",
                failedRecovery.waitFor(10, TimeUnit.SECONDS));
        assertEquals("child must stop without closing its Queue",
                CrashDuringHistoricalRecovery.EXIT_CODE, failedRecovery.exitValue());

        final SetTimeProvider time = new SetTimeProvider();
        time.currentTimeMillis(TEST_HOURLY.lengthInMillis());
        final long missingIndex = TEST_HOURLY.toIndex(0, 1);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time)
                .rollCycle(TEST_HOURLY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            expectException("Exact-index recovery replaced incomplete header");
            ((InternalAppender) appender).writeBytes(missingIndex, Bytes.from("recovered-after-restart"));

            assertFalse("the successful historical retry must stay open until completion",
                    hasEOF(queue, 0));
            appender.normaliseEOFs();
            assertTrue("completion must restore every historical EOF",
                    hasEOF(queue, 0));

            assertEquals(3, queue.entryCount());
            final Bytes<?> result = Bytes.elasticHeapByteBuffer();
            try (ExcerptTailer tailer = queue.createTailer()) {
                assertNextBytes(tailer, result, "first");
                assertNextBytes(tailer, result, "recovered-after-restart");
                assertNextBytes(tailer, result, "current-cycle");
                assertFalse(tailer.readBytes(result.clear()));
            }
        }
    }

    @Test(timeout = 20_000)
    public void restartPublishesReadyRecordLeftBeyondWritePosition()
            throws IOException, InterruptedException {
        QueueSystemProperties.CHECK_INDEX = false;
        final File directory = queueDirectory.newFolder();
        final Process interruptedPublisher = JavaProcessBuilder.create(CrashAfterReadyHeader.class)
                .withProgramArguments(directory.getAbsolutePath())
                .start();
        assertTrue("publisher process did not exit",
                interruptedPublisher.waitFor(10, TimeUnit.SECONDS));
        assertEquals("child must stop before publishing its write position",
                CrashAfterReadyHeader.EXIT_CODE, interruptedPublisher.exitValue());

        final long firstIndex = TEST_HOURLY.toIndex(0, 0);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST_HOURLY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            assertEquals("the physical scan must retain the ready first-writer record",
                    firstIndex + 1, queue.lastIndex());
            ((InternalAppender) appender).writeBytes(firstIndex + 2, Bytes.from("third"));
            assertQueueContents(queue, firstIndex, "first", "second", "third");
        }
    }

    public static final class CrashDuringHistoricalRecovery {
        private static final int EXIT_CODE = 17;

        public static void main(String[] args) {
            QueueSystemProperties.CHECK_INDEX = false;
            final SetTimeProvider time = new SetTimeProvider();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(new File(args[0]))
                    .timeProvider(time)
                    .rollCycle(TEST_HOURLY)
                    .testBlockSize()
                    .build()) {
                final ExcerptAppender appender = queue.createAppender();
                appender.writeBytes(Bytes.from("first"));
                time.currentTimeMillis(TEST_HOURLY.lengthInMillis());
                appender.writeBytes(Bytes.from("current-cycle"));

                // Persist the same state produced by termination after the shared lower bound,
                // EOF CAS and a partial payload, but before the data header and EOF are committed.
                queue.tableStoreAcquire("normalisedEOFsTo", 0).setValue(0);
                leaveIncompleteRecordAtEof(queue, 0, Bytes.from("partial"));
                Runtime.getRuntime().halt(EXIT_CODE);
            }
        }
    }

    public static final class CrashAfterReadyHeader {
        private static final int EXIT_CODE = 18;

        public static void main(String[] args) {
            QueueSystemProperties.CHECK_INDEX = false;
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(new File(args[0]))
                    .timeProvider(() -> 0)
                    .rollCycle(TEST_HOURLY)
                    .testBlockSize()
                    .build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeBytes(Bytes.from("first"));
                commitRecordWithoutPublishing(queue, 0, Bytes.from("second"));
                Runtime.getRuntime().halt(EXIT_CODE);
            }
        }
    }

    @Test
    public void eofRestorationFailureIsNotReportedAsSuccess() throws IOException {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder()).build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("current unsealed cycle"));
            final SingleChronicleQueueStore realStore = appender.store;

            try (MappedBytes fakeBytes = MappedBytes.mappedBytes(queueDirectory.newFile(), 64 << 10);
                 SingleChronicleQueueStore failingStore = new FailingEofStore(fakeBytes)) {
                appender.store = failingStore;
                queue.writeLock().lock();
                try {
                    final IllegalStateException failure = org.junit.Assert.assertThrows(
                            IllegalStateException.class,
                            () -> appender.ensureEndOfData("simulated EOF restoration failure"));
                    assertEquals("simulated EOF restoration failure", failure.getMessage());
                } finally {
                    queue.writeLock().unlock();
                }
            } finally {
                appender.store = realStore;
            }
        }
    }

    @Test
    public void eofAdvanceRejectsCycleOverflowBeforeMutation() throws IOException {
        final long clock = (long) Integer.MAX_VALUE * TEST_DAILY.lengthInMillis();
        final File directory = queueDirectory.newFolder();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> clock)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("last cycle");
            assertEquals(Integer.MAX_VALUE, appender.cycle());
            sealCurrentCycle(appender);

            final long publishedCycleBefore = queue.lastPublishedCycle();
            final long listingModCountBefore = queue.tableStoreGet("listing.modCount");
            final String[] cycleFileNamesBefore = cycleFileNames(directory);
            final long sourceWritePositionBefore = appender.store.writePosition();
            final int eofWordBefore = readWordAfterLastEntry(appender.store);
            assertEquals(Wires.END_OF_DATA, eofWordBefore);

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> appender.writeText("must not wrap"));
            assertTrue(failure.getMessage().contains("Cannot advance ordinary append"));
            assertEquals(Integer.MAX_VALUE, appender.cycle());
            assertEquals(publishedCycleBefore, queue.lastPublishedCycle());
            assertEquals(listingModCountBefore, queue.tableStoreGet("listing.modCount"));
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
            assertEquals(sourceWritePositionBefore, appender.store.writePosition());
            assertEquals(eofWordBefore, readWordAfterLastEntry(appender.store));
        }
    }

    @Test
    public void exactEofRecoveryAtMaximumCycleIsRejectedBeforeMutation() throws IOException {
        final long clock = (long) Integer.MAX_VALUE * TEST_DAILY.lengthInMillis();
        final File directory = queueDirectory.newFolder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> clock)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            final long lastSupportedSequence = TEST_DAILY.maxMessagesPerCycle() - 1;
            for (long sequence = 0; sequence < lastSupportedSequence; sequence++)
                appender.writeText("entry-" + sequence);
            assertEquals(Integer.MAX_VALUE, appender.cycle());
            assertEquals(lastSupportedSequence - 1,
                    queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
            sealCurrentCycle(appender);

            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", Integer.MAX_VALUE);
            final long writePositionBefore = appender.store.writePosition();
            final long cursorBefore = cursor.getVolatileValue();
            final long headerNumberBefore = appender.wire().headerNumber();
            final long lastIndexBefore = appender.lastIndexAppended();
            final long listingModCountBefore = queue.tableStoreGet("listing.modCount");
            final String[] cycleFileNamesBefore = cycleFileNames(directory);
            final long lastSupportedIndex = queue.rollCycle().toIndex(
                    Integer.MAX_VALUE, lastSupportedSequence);

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(
                            lastSupportedIndex, Bytes.from("must remain sealed")));

            assertTrue(failure.getMessage().contains("Cannot reopen end-of-data in the final UInt31 cycle"));
            assertEquals(writePositionBefore, appender.store.writePosition());
            assertEquals(cursorBefore, cursor.getVolatileValue());
            assertEquals(headerNumberBefore, appender.wire().headerNumber());
            assertEquals(lastIndexBefore, appender.lastIndexAppended());
            assertEquals(listingModCountBefore, queue.tableStoreGet("listing.modCount"));
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
            assertEquals(END_OF_DATA, readWordAfterLastEntry(appender.store));

            appender.normaliseEOFs();
            assertEquals("completion must retain the final cycle's existing seal",
                    END_OF_DATA, readWordAfterLastEntry(appender.store));
            assertEquals(Integer.MAX_VALUE, queue.firstCycle());
            assertEquals(Integer.MAX_VALUE, queue.lastCycle());
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
        }
    }

    private static String[] cycleFileNames(File directory) {
        final String[] names = directory.list((ignored, name) -> name.endsWith(SingleChronicleQueue.SUFFIX));
        assertNotNull(names);
        Arrays.sort(names);
        return names;
    }

    private static int readWordAfterLastEntry(SingleChronicleQueueStore store) {
        try (MappedBytes bytes = store.bytes()) {
            final long lastEntryPosition = store.writePosition();
            final int lastEntryHeader = bytes.readVolatileInt(lastEntryPosition);
            long nextHeaderPosition = lastEntryPosition + Wires.lengthOf(lastEntryHeader) + Wires.SPB_HEADER_SIZE;
            if (store.dataVersion() > 0)
                nextHeaderPosition += BytesUtil.padOffset(nextHeaderPosition);
            return bytes.readVolatileInt(nextHeaderPosition);
        }
    }

    private static long writePosition(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, queue.epoch(), false, null)) {
            assertNotNull(store);
            return store.writePosition();
        }
    }

    @Test
    public void exactRecoveryFailsIfTheEofMarkerChangesBeforeCas() {
        final Bytes<?> bytes = Bytes.allocateElasticDirect();
        try {
            bytes.writeInt(0, Wires.NOT_INITIALIZED);

            final IllegalStateException changed = org.junit.Assert.assertThrows(
                    IllegalStateException.class,
                    () -> StoreAppender.replaceEndOfDataMarkerForRecovery(bytes, 0));

            assertEquals("End-of-data changed while starting exact-index recovery at 0",
                    changed.getMessage());
            assertEquals("failed CAS must not change the word", Wires.NOT_INITIALIZED,
                    bytes.readVolatileInt(0));
        } finally {
            bytes.releaseLast();
        }
    }

    private static void assertNextBytes(ExcerptTailer tailer, Bytes<?> result, String expected) {
        result.clear();
        assertTrue(tailer.readBytes(result));
        assertEquals(expected, result.toString());
    }

    private static long commitRecordWithoutPublishing(SingleChronicleQueue queue,
                                                       int cycle,
                                                       Bytes<?> payload) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, 0, false, null);
             MappedBytes bytes = store.bytes()) {
            long position = store.writePosition();
            position += lengthOf(bytes.readVolatileInt(position)) + SPB_HEADER_SIZE;
            for (; ; ) {
                position += BytesUtil.padOffset(position);
                final int header = bytes.readVolatileInt(position);
                if (header == NOT_INITIALIZED)
                    break;
                assertTrue("expected a ready record before the free slot at " + position,
                        isReadyData(header) || isReadyMetaData(header));
                position += SPB_HEADER_SIZE + lengthOf(header);
            }
            final long length = payload.readRemaining();
            bytes.write(position + SPB_HEADER_SIZE, payload, payload.readPosition(), length);
            bytes.writeOrderedInt(position, (int) length);
            assertTrue(isReadyData(bytes.readVolatileInt(position)));
            return position;
        }
    }

    private static void assertQueueContents(SingleChronicleQueue queue,
                                            long firstIndex,
                                            String... expected) {
        assertEquals(expected.length, queue.entryCount());
        final Bytes<?> result = Bytes.elasticHeapByteBuffer();
        try (ExcerptTailer tailer = queue.createTailer()) {
            for (int i = 0; i < expected.length; i++) {
                assertTrue("sequential read of entry " + i, tailer.readBytes(result.clear()));
                assertEquals(expected[i], result.toString());
                assertEquals(firstIndex + i + 1, tailer.index());
            }
            assertFalse(tailer.readBytes(result.clear()));
        }
        for (int i = 0; i < expected.length; i++) {
            try (ExcerptTailer tailer = queue.createTailer()) {
                assertTrue("moveToIndex " + i, tailer.moveToIndex(firstIndex + i));
                assertTrue(tailer.readBytes(result.clear()));
                assertEquals(expected[i], result.toString());
            }
        }
    }

    @Test
    public void sameCycleGapDoesNotPublishReadyCrashRecord() throws IOException {
        final File directory = queueDirectory.newFolder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("published"));
            final int cycle = appender.cycle();
            commitRecordWithoutPublishing(queue, cycle, Bytes.from("ready but unpublished"));
            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", cycle);

            final long writePositionBefore = appender.store.writePosition();
            final long nextIndexedSequenceBefore = appender.store.indexing.nextEntryToBeIndexed();
            final long cursorBefore = cursor.getVolatileValue();
            final long headerNumberBefore = appender.wire().headerNumber();
            final long lastIndexBefore = appender.lastIndexAppended();
            final long listingModCountBefore = queue.tableStoreGet("listing.modCount");
            final String[] cycleFileNamesBefore = cycleFileNames(directory);
            final long gapIndex = queue.rollCycle().toIndex(cycle, 3);

            assertThrows(IllegalIndexException.class,
                    () -> ((InternalAppender) appender).writeBytes(gapIndex, Bytes.from("gap")));

            assertEquals(writePositionBefore, appender.store.writePosition());
            assertEquals(nextIndexedSequenceBefore, appender.store.indexing.nextEntryToBeIndexed());
            assertEquals(cursorBefore, cursor.getVolatileValue());
            assertEquals(cycle, appender.cycle());
            assertEquals(headerNumberBefore, appender.wire().headerNumber());
            assertEquals(lastIndexBefore, appender.lastIndexAppended());
            assertEquals(listingModCountBefore, queue.tableStoreGet("listing.modCount"));
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
        }
    }

    @Test
    public void publishedDuplicateDoesNotAdoptLaterReadyCrashRecord() throws IOException {
        final File directory = queueDirectory.newFolder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("published"));
            final long publishedIndex = appender.lastIndexAppended();
            commitRecordWithoutPublishing(queue, appender.cycle(), Bytes.from("later ready record"));
            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", appender.cycle());

            final long writePositionBefore = appender.store.writePosition();
            final long nextIndexedSequenceBefore = appender.store.indexing.nextEntryToBeIndexed();
            final long cursorBefore = cursor.getVolatileValue();
            final long headerNumberBefore = appender.wire().headerNumber();

            expectException("Exact-index duplicate differs from published content and was ignored");
            ((InternalAppender) appender).writeBytes(publishedIndex, Bytes.from("ignored duplicate"));

            assertEquals(writePositionBefore, appender.store.writePosition());
            assertEquals(nextIndexedSequenceBefore, appender.store.indexing.nextEntryToBeIndexed());
            assertEquals(cursorBefore, cursor.getVolatileValue());
            assertEquals(headerNumberBefore, appender.wire().headerNumber());
            assertEquals(publishedIndex, appender.lastIndexAppended());
        }
    }

    @Test
    public void exactWriteDoesNotRecreateDeletedPublishedMaximum() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong stalledTime = new AtomicLong();
        final AtomicLong publishingTime = new AtomicLong(TEST_DAILY.lengthInMillis());
        try (SingleChronicleQueue stalledQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(stalledTime::get)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = stalledQueue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("cycle-zero"));

            final File publishedFile;
            try (SingleChronicleQueue publishingQueue = SingleChronicleQueueBuilder.binary(directory)
                    .timeProvider(publishingTime::get)
                    .rollCycle(TEST_DAILY)
                    .testBlockSize()
                    .build();
                 SingleChronicleQueueStore publishedStore = publishingQueue.storeForCycle(
                         1, publishingQueue.epoch(), true, null)) {
                assertNotNull(publishedStore);
                publishedFile = publishedStore.file();
            }
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(1, stalledQueue.lastPublishedCycle());
            assertTrue("test precondition: published maximum must be deleted", publishedFile.delete());

            final LongValue cursor = stalledQueue.tableStoreAcquire("normalisedEOFsTo", 0);
            final long sourceWritePositionBefore = appender.store.writePosition();
            final int sourceNextWordBefore = readWordAfterLastEntry(appender.store);
            final long cursorBefore = cursor.getVolatileValue();
            final long headerNumberBefore = appender.wire().headerNumber();
            final long lastIndexBefore = appender.lastIndexAppended();
            final long listingModCountBefore = stalledQueue.tableStoreGet("listing.modCount");
            final String[] cycleFileNamesBefore = cycleFileNames(directory);
            final long missingIndex = stalledQueue.rollCycle().toIndex(1, 0);

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(missingIndex, Bytes.from("must-fail")));

            assertTrue(failure.getMessage().contains("Highest/current roll 1 disappeared"));
            assertFalse("the exact request must not recreate the authoritative roll", publishedFile.exists());
            assertEquals(sourceWritePositionBefore, appender.store.writePosition());
            assertEquals(sourceNextWordBefore, readWordAfterLastEntry(appender.store));
            assertEquals(cursorBefore, cursor.getVolatileValue());
            assertEquals(0, appender.cycle());
            assertEquals(headerNumberBefore, appender.wire().headerNumber());
            assertEquals(lastIndexBefore, appender.lastIndexAppended());
            assertEquals(1, stalledQueue.lastPublishedCycle());
            assertEquals(listingModCountBefore, stalledQueue.tableStoreGet("listing.modCount"));
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
        }
    }

    @Test
    public void exactWriteRejectsAbsentCycleWithinPublishedRange() throws IOException {
        final File directory = queueDirectory.newFolder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("cycle-zero"));
            try (SingleChronicleQueue publisher = SingleChronicleQueueBuilder.binary(directory)
                    .timeProvider(() -> 2L * TEST_DAILY.lengthInMillis())
                    .rollCycle(TEST_DAILY)
                    .testBlockSize()
                    .build();
                 SingleChronicleQueueStore publishedStore = publisher.storeForCycle(
                         2, publisher.epoch(), true, null)) {
                assertNotNull(publishedStore);
            }
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(2, queue.lastPublishedCycle());
            assertFalse("test precondition: the interior cycle must be absent", queue.cycleFileExists(1));

            final LongValue cursor = queue.tableStoreAcquire("normalisedEOFsTo", 0);
            final long sourceWritePositionBefore = appender.store.writePosition();
            final int sourceNextWordBefore = readWordAfterLastEntry(appender.store);
            final long cursorBefore = cursor.getVolatileValue();
            final long headerNumberBefore = appender.wire().headerNumber();
            final long lastIndexBefore = appender.lastIndexAppended();
            final long listingModCountBefore = queue.tableStoreGet("listing.modCount");
            final String[] cycleFileNamesBefore = cycleFileNames(directory);
            final long missingInteriorIndex = queue.rollCycle().toIndex(1, 0);

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(
                            missingInteriorIndex, Bytes.from("ambiguous-interior")));

            assertTrue(failure.getMessage().contains("within retained published range 0..2"));
            assertFalse(queue.cycleFileExists(1));
            assertEquals(sourceWritePositionBefore, appender.store.writePosition());
            assertEquals(sourceNextWordBefore, readWordAfterLastEntry(appender.store));
            assertEquals(cursorBefore, cursor.getVolatileValue());
            assertEquals(0, appender.cycle());
            assertEquals(headerNumberBefore, appender.wire().headerNumber());
            assertEquals(lastIndexBefore, appender.lastIndexAppended());
            assertEquals(listingModCountBefore, queue.tableStoreGet("listing.modCount"));
            assertArrayEquals(cycleFileNamesBefore, cycleFileNames(directory));
        }
    }

    @Test
    public void rejectedExactGapIntoExistingLaterRollDoesNotSealTheCurrentRoll() throws IOException {
        final AtomicLong clock = new AtomicLong();
        final File directory = queueDirectory.newFolder();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(clock::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeText("cycle-0 entry");
            final int currentCycle = appender.cycle();
            // The next roll exists (e.g. created by another node's backfill) but holds no entry yet.
            try (SingleChronicleQueueStore next = queue.storeForCycle(currentCycle + 1, queue.epoch(), true, null)) {
                assertNotNull(next);
            }
            final long gapIndex = queue.rollCycle().toIndex(currentCycle + 1, 5);
            assertTrue("precondition: the current roll is open before the exact write", rollIsOpen(queue, currentCycle));

            assertThrows(IllegalIndexException.class, () -> appender.writeBytes(gapIndex, Bytes.from("gap")));

            // Nothing may have changed: the appender must not have moved and the current roll must not
            // have been sealed.
            assertEquals("a rejected exact write must not move the appender", currentCycle, appender.cycle());
            assertTrue("a rejected exact write must not seal the current roll", rollIsOpen(queue, currentCycle));
        }
    }

    @Test(timeout = 60_000)
    public void independentQueueExactWritersReplayAcrossCyclesAndRestart() throws Exception {
        ignoreException("Exact-index duplicate differs from published content and was ignored");
        final File directory = queueDirectory.newFolder();
        final SetTimeProvider time = new SetTimeProvider();
        final int entriesPerCycle = 4;

        replayRangeFromIndependentQueues(directory, time, 0, entriesPerCycle);
        // Closing both Queue object graphs and constructing two more supplies the restart/mapping boundary.
        replayRangeFromIndependentQueues(directory, time, 2, entriesPerCycle);

        time.currentTimeMillis(3L * TEST_DAILY.lengthInMillis());
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender completion = queue.createAppender()) {
            completion.normaliseEOFs();
            assertEquals(2, queue.lastCycle());
            assertEquals(entriesPerCycle * 2L, queue.entryCount());
            assertTrue(hasEOF(queue, 0));
            assertTrue(hasEOF(queue, 2));
            assertEquals("sparse exact replay must create only its two named rolls",
                    2, cycleFileNames(directory).length);

            try (ExcerptTailer tailer = queue.createTailer()) {
                final Bytes<?> read = Bytes.allocateElasticOnHeap();
                for (int cycle : new int[]{0, 2}) {
                    for (int sequence = 0; sequence < entriesPerCycle; sequence++) {
                        final long index = queue.rollCycle().toIndex(cycle, sequence);
                        assertTrue("missing index 0x" + Long.toHexString(index), tailer.moveToIndex(index));
                        assertTrue(tailer.readBytes(read.clear()));
                        assertTrue(read.toString(), read.toString().startsWith(
                                "cycle-" + cycle + "-entry-" + sequence + "-writer-"));
                    }
                }
            }
        }
    }

    private void replayRangeFromIndependentQueues(File directory,
                                                   SetTimeProvider time,
                                                   int cycle,
                                                   int entries) throws Exception {
        final int writers = 2;
        final java.util.concurrent.ExecutorService executor =
                java.util.concurrent.Executors.newFixedThreadPool(writers);
        final java.util.concurrent.CyclicBarrier replayStep = new java.util.concurrent.CyclicBarrier(writers);
        try (SingleChronicleQueue firstQueue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build();
             SingleChronicleQueue secondQueue = SingleChronicleQueueBuilder.binary(directory)
                     .timeProvider(time)
                     .rollCycle(TEST_DAILY)
                     .testBlockSize()
                     .build()) {
            final SingleChronicleQueue[] queues = {firstQueue, secondQueue};
            final java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int writer = 0; writer < writers; writer++) {
                final int writerId = writer;
                futures.add(executor.submit(() -> {
                    try (ExcerptAppender appender = queues[writerId].createAppender()) {
                        for (int sequence = 0; sequence < entries; sequence++) {
                            replayStep.await(10, TimeUnit.SECONDS);
                            final long index = TEST_DAILY.toIndex(cycle, sequence);
                            ((InternalAppender) appender).writeBytes(index, Bytes.from(
                                    "cycle-" + cycle + "-entry-" + sequence + "-writer-" + writerId));
                        }
                    }
                    return null;
                }));
            }
            for (java.util.concurrent.Future<?> future : futures)
                future.get();
        } finally {
            executor.shutdownNow();
            assertTrue("exact replay workers did not stop",
                    executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /** True when the roll has no end-of-data marker yet; checks without writing one (a probe read of the header scan). */
    private static boolean rollIsOpen(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, queue.epoch(), false, null);
             MappedBytes bytes = store.bytes()) {
            final Wire wire = queue.wireType().apply(bytes);
            wire.usePadding(store.dataVersion() > 0);
            long position = store.writePosition();
            position += net.openhft.chronicle.wire.Wires.lengthOf(bytes.readVolatileInt(position)) + 4;
            for (; ; ) {
                position += net.openhft.chronicle.bytes.BytesUtil.padOffset(position);
                final int header = bytes.readVolatileInt(position);
                if (header == net.openhft.chronicle.wire.Wires.NOT_INITIALIZED)
                    return true;
                if (header == net.openhft.chronicle.wire.Wires.END_OF_DATA)
                    return false;
                position += 4 + net.openhft.chronicle.wire.Wires.lengthOf(header);
            }
        }
    }

    @Test
    public void constructionSealsEveryRollBelowTheFirstSealedRoll() throws IOException {
        final AtomicLong clock = new AtomicLong();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDirectory.newFolder())
                .timeProvider(clock::get)
                .rollCycle(TEST_DAILY)
                .build()) {
            try (ExcerptAppender first = queue.createAppender()) {
                first.writeText("roll 0 is left open");
            }
            sealCycle(queue, 1);
            assertFalse("precondition: roll 0 is open while roll 1 exists and is sealed", hasEOF(queue, 0));

            clock.set(2L * TEST_DAILY.lengthInMillis());
            queue.createAppender().close();
            assertTrue("construction must seal roll 0, which lies below the sealed roll 1", hasEOF(queue, 0));
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

    private static void forceWritePosition(SingleChronicleQueueStore store, long newWritePosition) {
        try {
            final Field field = store.getClass().getDeclaredField("writePosition");
            field.setAccessible(true);
            ((LongValue) field.get(store)).setValue(newWritePosition);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to simulate interrupted write-position publication", e);
        }
    }

    private static void clearEncodedSequence(SingleChronicleQueueStore store) {
        writePositionAndSequence(store).setValue2(0);
    }

    private static TwoLongValue writePositionAndSequence(SingleChronicleQueueStore store) {
        try {
            final Field field = store.getClass().getDeclaredField("writePosition");
            field.setAccessible(true);
            return (TwoLongValue) field.get(store);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to access the paired write-position publication", e);
        }
    }

    private static void removeEndOfData(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, 0, false, null);
             MappedBytes bytes = store.bytes()) {
            long position = store.writePosition();
            position += lengthOf(bytes.readVolatileInt(position)) + SPB_HEADER_SIZE;
            for (; ; ) {
                if (store.dataVersion() > 0)
                    position += BytesUtil.padOffset(position);
                final int header = bytes.readVolatileInt(position);
                if (header == END_OF_DATA) {
                    assertTrue(bytes.compareAndSwapInt(position, END_OF_DATA, NOT_INITIALIZED));
                    return;
                }
                assertTrue("expected metadata before EOF at position " + position,
                        isReadyMetaData(header));
                position += SPB_HEADER_SIZE + lengthOf(header);
            }
        }
    }

    private static void leaveIncompleteRecordAtEof(SingleChronicleQueue queue,
                                                    int cycle,
                                                    Bytes<?> partialPayload) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, 0, false, null);
             MappedBytes bytes = store.bytes()) {
            long position = store.writePosition();
            position += lengthOf(bytes.readVolatileInt(position)) + SPB_HEADER_SIZE;
            for (; ; ) {
                position += BytesUtil.padOffset(position);
                final int header = bytes.readVolatileInt(position);
                if (header == END_OF_DATA) {
                    StoreAppender.replaceEndOfDataMarkerForRecovery(bytes, position);
                    final int length = Math.toIntExact(partialPayload.readRemaining());
                    bytes.write(position + SPB_HEADER_SIZE, partialPayload,
                            partialPayload.readPosition(), length);
                    bytes.writeVolatileInt(position, NOT_COMPLETE | length);
                    return;
                }
                assertTrue("expected metadata before EOF at position " + position,
                        isReadyMetaData(header));
                position += SPB_HEADER_SIZE + lengthOf(header);
            }
        }
    }

    private static boolean hasEOF(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, 0, false, null)) {
            final String dump = store.dump(WireType.BINARY_LIGHT);
            return dump.contains(" EOF") && dump.contains("--- !!not-ready-meta-data");
        }
    }

    private static int countQueueFiles(File directory) {
        final File[] queueFiles = directory.listFiles(
                (ignored, name) -> name.endsWith(SingleChronicleQueue.SUFFIX));
        return queueFiles == null ? 0 : queueFiles.length;
    }

    private static boolean containsCause(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (type.isInstance(current))
                return true;
            for (Throwable suppressed : current.getSuppressed()) {
                if (containsCause(suppressed, type))
                    return true;
            }
        }
        return false;
    }

    private static boolean containsMessage(Throwable failure, String fragment) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains(fragment))
                return true;
            for (Throwable suppressed : current.getSuppressed()) {
                if (containsMessage(suppressed, fragment))
                    return true;
            }
        }
        return false;
    }

    private static final class PaddingRecordingStore extends SingleChronicleQueueStore {
        private boolean replacementUsesPadding;

        PaddingRecordingStore(MappedBytes bytes) {
            super(TEST4_DAILY, WireType.BINARY, bytes, 8, 1);
        }

        @Override
        boolean writeEOFAndShrink(Wire wire, long timeoutMS) {
            replacementUsesPadding = wire.usePadding();
            return true;
        }
    }

    private static final class FailingEofStore extends SingleChronicleQueueStore {
        FailingEofStore(MappedBytes bytes) {
            super(TEST4_DAILY, WireType.BINARY, bytes, 8, 1);
        }

        @Override
        public boolean writeEOF(Wire wire, long timeoutMS) {
            return false;
        }

        @Override
        public long writePosition() {
            return 0;
        }
    }

    private static final class ConfigurableRollCycle implements RollCycle {
        private final String format;
        private final int lengthInMillis;
        private final RollCycleArithmetic arithmetic;

        private ConfigurableRollCycle(String format,
                                      int lengthInMillis,
                                      int indexCount,
                                      int indexSpacing) {
            this.format = format;
            this.lengthInMillis = lengthInMillis;
            this.arithmetic = RollCycleArithmetic.of(indexCount, indexSpacing);
        }

        @Override
        public String format() {
            return format;
        }

        @Override
        public int lengthInMillis() {
            return lengthInMillis;
        }

        @Override
        public int defaultIndexCount() {
            return arithmetic.indexCount();
        }

        @Override
        public int defaultIndexSpacing() {
            return arithmetic.indexSpacing();
        }

        @Override
        public long toIndex(int cycle, long sequenceNumber) {
            return arithmetic.toIndex(cycle, sequenceNumber);
        }

        @Override
        public long toSequenceNumber(long index) {
            return arithmetic.toSequenceNumber(index);
        }

        @Override
        public int toCycle(long index) {
            return arithmetic.toCycle(index);
        }

        @Override
        public long maxMessagesPerCycle() {
            return arithmetic.maxMessagesPerCycle();
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
