/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueSystemProperties;
import net.openhft.chronicle.queue.QueueTestCommon;
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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_HOURLY;
import static net.openhft.chronicle.wire.Wires.*;

public class StoreAppenderTest extends QueueTestCommon {

    private static final String TEST_TEXT = "Some text some text some text";
    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);

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
    public void stalledAppenderRollsPastDeletedPublishedCycle() throws IOException {
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

            assertEquals(1, stalledQueue.lastPublishedCycle());
            assertTrue("test precondition: published cycle file must be removed",
                    publishedFile.delete());

            stalledClock.set(2 * TEST_DAILY.lengthInMillis());
            stalledWriter.writeBytes(Bytes.from("cycle-2"));
            assertEquals(2, stalledWriter.cycle());
        }
    }

    @Test
    public void deletingHighestRollDoesNotMoveActiveQueueBackwards() throws IOException {
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

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("must-not-move-back"));
            assertEquals("the persistent write floor must survive partial deletion and reopen",
                    1, appender.cycle());
        }
    }

    @Test
    public void reopeningAfterAllRollsAreDeletedRetainsHighWater() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong time = new AtomicLong(3L * TEST_DAILY.lengthInMillis());
        final File onlyRoll;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("cycle-3"));
            onlyRoll = appender.currentFile();
            assertEquals(3, appender.cycle());
        }

        assertTrue("test precondition: every roll file must be removed", onlyRoll.delete());
        time.set(0);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(time::get)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("retain-cycle-3"));
            assertEquals("an empty reopened queue must retain the roll high-water", 3, appender.cycle());
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
            queue.tableStoreAcquire("listing.highestCycleWriteFloor", sealedCycle)
                    .setVolatileValue(sealedCycle);

            assertThrows(WriteAfterEOFException.class,
                    () -> appender.writeText("must fail at second EOF"));
            assertEquals(sealedCycle + 1, appender.cycle());
            assertEquals(1, queue.entryCount());
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

            org.junit.Assert.assertThrows(IllegalArgumentException.class,
                    () -> ((InternalAppender) appender).writeBytes(
                            unsupportedIndex, Bytes.from("unsupported")));

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
    public void eosOnlyRestartPreservesReadySequenceZeroBeforeOrdinaryAppend() throws IOException {
        QueueSystemProperties.CHECK_INDEX = false;
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
            final File[] rollFiles = directory.listFiles(
                    ignored -> ignored.getName().endsWith(SingleChronicleQueue.SUFFIX));
            assertEquals("normalisation must not create files for sparse gaps", 2,
                    rollFiles == null ? 0 : rollFiles.length);
        }
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

    private static void commitRecordWithoutPublishing(SingleChronicleQueue queue,
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
                assertTrue("expected metadata before the free slot at " + position,
                        isReadyMetaData(header));
                position += SPB_HEADER_SIZE + lengthOf(header);
            }
            final long length = payload.readRemaining();
            bytes.write(position + SPB_HEADER_SIZE, payload, payload.readPosition(), length);
            bytes.writeOrderedInt(position, (int) length);
            assertTrue(isReadyData(bytes.readVolatileInt(position)));
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
