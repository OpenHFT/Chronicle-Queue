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
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    @Test
    public void recoveryClassifiesTheRequestedHeaderWithoutAdvancingItsIndex() {
        assertEquals(StoreAppender.RecoveryAction.WRITE_AND_RESEAL,
                StoreAppender.recoveryActionForHeader(Wires.END_OF_DATA));
        assertEquals(StoreAppender.RecoveryAction.WARN_AND_WRITE,
                StoreAppender.recoveryActionForHeader(Wires.NOT_COMPLETE));
        assertEquals(StoreAppender.RecoveryAction.WRITE,
                StoreAppender.recoveryActionForHeader(Wires.NOT_INITIALIZED));
        assertEquals(StoreAppender.RecoveryAction.ALREADY_PRESENT,
                StoreAppender.recoveryActionForHeader(8));
        assertEquals(StoreAppender.RecoveryAction.SKIP_METADATA,
                StoreAppender.recoveryActionForHeader(Wires.META_DATA | 8));
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

    @Test
    public void exactWriteAdoptsReadyFirstRecordAfterInterruptedPublication() throws IOException {
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
            appender.store.writePositionForTesting(0);
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            ((InternalAppender) appender).writeBytes(firstIndex, Bytes.from("first"));
            assertEquals("the ready record must be adopted as the last committed index",
                    firstIndex, appender.lastIndexAppended());

            appender.writeBytes(Bytes.from("second"));
            assertEquals(firstIndex + 1, appender.lastIndexAppended());
            assertEquals(2, queue.entryCount());

            final Bytes<?> result = Bytes.elasticHeapByteBuffer();
            assertNextBytes(tailer, result, "first");
            assertNextBytes(tailer, result, "second");
            assertFalse(tailer.readBytes(result.clear()));
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

    @Test
    public void restartAdoptsReadyRecoveryBeyondAStaleWritePosition() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong clock = new AtomicLong();
        final long recoveredIndex;
        final int recoveredCycle;
        final long writePositionBeforeRecovery;
        final long recoveredWritePosition;
        final long eofPosition;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(clock::get)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("first-cycle"));
            recoveredIndex = appender.lastIndexAppended() + 1;
            recoveredCycle = queue.rollCycle().toCycle(recoveredIndex);
            writePositionBeforeRecovery = appender.store.writePosition();
            try (DocumentContext metadata = appender.writingDocument(true)) {
                metadata.wire().write("recovery-context").text("metadata-before-eof");
            }

            clock.addAndGet(TimeUnit.HOURS.toMillis(25));
            appender.writeBytes(Bytes.from("later-cycle"));
            expectException("queue=" + queue.fileAbsolutePath() + ", cycle=" + recoveredCycle
                    + ", index=0x" + Long.toHexString(recoveredIndex));
            ((InternalAppender) appender).writeBytes(recoveredIndex, Bytes.from("recovered"));
            recoveredWritePosition = appender.store.writePosition();

            queue.tableStorePut("recoveryInProgress", recoveredIndex);
            queue.tableStorePut("recoveryIndexed", Long.MIN_VALUE);
            eofPosition = endOfDataPosition(queue, recoveredCycle);
            replaceEndOfData(queue, recoveredCycle, eofPosition);
            try (SingleChronicleQueueStore store = queue.storeForCycle(recoveredCycle, 0, false, null)) {
                store.writePositionForTesting(writePositionBeforeRecovery);
            }
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(clock::get)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.normaliseEOFs();

            assertEquals("restart must restore EOF after adopting the ready record",
                    Wires.END_OF_DATA, readHeader(firstQueueFile(directory), eofPosition));
            try (SingleChronicleQueueStore store = queue.storeForCycle(recoveredCycle, 0, false, null)) {
                assertEquals("restart must publish the adopted record as the store write position",
                        recoveredWritePosition, store.writePosition());
            }
            assertTrue(tailer.moveToIndex(recoveredIndex));
            assertNextBytes(tailer, Bytes.elasticHeapByteBuffer(), "recovered");
            assertEquals(Long.MIN_VALUE, queue.tableStoreGet("recoveryInProgress"));
            assertEquals(Long.MIN_VALUE, queue.tableStoreGet("recoveryIndexed"));
        }
    }

    @Test
    public void recoveryInspectionFailureKeepsTheDurableIntent() throws IOException {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder()).build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("existing"));
            final long recoveryIndex = appender.lastIndexAppended() + 1;
            queue.tableStorePut("recoveryInProgress", recoveryIndex);
            queue.tableStorePut("recoveryIndexed", Long.MIN_VALUE);
            final SingleChronicleQueueStore realStore = appender.store;

            try (MappedBytes fakeBytes = MappedBytes.mappedBytes(queueDirectory.newFile(), 64 << 10);
                 SingleChronicleQueueStore failingStore = new FailingSequenceStore(fakeBytes)) {
                appender.store = failingStore;
                final IllegalStateException failure = org.junit.Assert.assertThrows(
                        IllegalStateException.class, appender::normaliseEOFs);
                assertTrue(failure.getMessage().contains("Unable to reconcile persisted recovery"));
                assertTrue(failure.getCause() instanceof StreamCorruptedException);
                assertEquals("failed inspection must retain recovery intent",
                        recoveryIndex, queue.tableStoreGet("recoveryInProgress"));
            } finally {
                appender.store = realStore;
                queue.tableStorePut("recoveryInProgress", Long.MIN_VALUE);
                queue.tableStorePut("recoveryIndexed", Long.MIN_VALUE);
            }
        }
    }

    @Test
    public void normalisationDoesNotAdoptDataForANonNextPendingIndex() throws IOException {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(queueDirectory.newFolder()).build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            final StoreAppender appender = (StoreAppender) excerptAppender;
            appender.writeBytes(Bytes.from("existing"));
            final long nonNextRecoveryIndex = appender.lastIndexAppended() + 2;
            queue.tableStorePut("recoveryInProgress", nonNextRecoveryIndex);
            queue.tableStorePut("recoveryIndexed", Long.MIN_VALUE);

            final IllegalStateException failure = org.junit.Assert.assertThrows(
                    IllegalStateException.class, appender::normaliseEOFs);

            assertTrue(failure.getMessage().contains("recovery remains incomplete"));
            assertEquals(nonNextRecoveryIndex, queue.tableStoreGet("recoveryInProgress"));
            queue.tableStorePut("recoveryInProgress", Long.MIN_VALUE);
            queue.tableStorePut("recoveryIndexed", Long.MIN_VALUE);
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

    private static void replaceEndOfData(SingleChronicleQueue queue, int cycle, long position) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, 0, false, null);
             MappedBytes bytes = store.bytes()) {
            assertTrue(bytes.compareAndSwapInt(position, Wires.END_OF_DATA, Wires.NOT_INITIALIZED));
        }
    }

    private static long endOfDataPosition(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, 0, false, null);
             MappedBytes bytes = store.bytes()) {
            long position = store.writePosition();
            position += Wires.lengthOf(bytes.readVolatileInt(position)) + Wires.SPB_HEADER_SIZE;
            for (; ; ) {
                position += net.openhft.chronicle.bytes.BytesUtil.padOffset(position);
                final int header = bytes.readVolatileInt(position);
                if (header == Wires.END_OF_DATA)
                    return position;
                if (Wires.isNotComplete(header))
                    throw new AssertionError("Reached an incomplete header before EOF");
                position += Wires.SPB_HEADER_SIZE + Wires.lengthOf(header);
            }
        }
    }

    private static int readHeader(File queueFile, long position) throws IOException {
        try (MappedBytes bytes = MappedBytes.mappedBytes(queueFile, 64 << 10)) {
            return bytes.readVolatileInt(position);
        }
    }

    private static File firstQueueFile(File directory) {
        final File[] queueFiles = directory.listFiles(
                (ignored, name) -> name.endsWith(SingleChronicleQueue.SUFFIX));
        if (queueFiles == null || queueFiles.length == 0)
            throw new AssertionError("No queue files in " + directory);
        java.util.Arrays.sort(queueFiles);
        return queueFiles[0];
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

    private static final class FailingSequenceStore extends SingleChronicleQueueStore {
        FailingSequenceStore(MappedBytes bytes) {
            super(TEST4_DAILY, WireType.BINARY, bytes, 8, 1);
        }

        @Override
        public long sequenceForPosition(ExcerptContext ec, long position, boolean inclusive)
                throws StreamCorruptedException {
            throw new StreamCorruptedException("simulated corrupt recovery position");
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
