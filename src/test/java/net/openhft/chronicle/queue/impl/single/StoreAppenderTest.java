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
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    public void reopeningAfterAllRollsAreDeletedUsesWallClockCycle() throws IOException {
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
            appender.writeBytes(Bytes.from("reset-cycle-0"));
            assertEquals("an empty reopened queue must reset the roll high-water", 0, appender.cycle());
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
    public void recoveryClassifiesTheRequestedHeaderWithoutAdvancingItsIndex() {
        assertEquals(StoreAppender.RecoveryAction.WRITE_AND_RESEAL,
                StoreAppender.recoveryActionForHeader(Wires.END_OF_DATA));
        assertEquals(StoreAppender.RecoveryAction.WRITE,
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
    public void exactBackfillRejectsLegacyUnpaddedRollBeforeMutation() throws IOException {
        final File directory = queueDirectory.newFolder();
        final AtomicLong clock = new AtomicLong();
        final long recoveredIndex;
        final int recoveredCycle;
        final long eofPosition;

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(clock::get)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeBytes(Bytes.from("first-cycle"));
            recoveredIndex = appender.lastIndexAppended() + 1;
            recoveredCycle = queue.rollCycle().toCycle(recoveredIndex);

            clock.addAndGet(TimeUnit.HOURS.toMillis(25));
            appender.writeBytes(Bytes.from("later-cycle"));
            eofPosition = endOfDataPosition(queue, recoveredCycle);
        }

        final File recoveredFile = firstQueueFile(directory);
        setDataFormatToLegacyUnpadded(recoveredFile);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(clock::get)
                .rollCycle(TEST4_DAILY)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            assertLegacyDataVersion(queue, recoveredCycle);
            assertEquals(Wires.END_OF_DATA, readHeader(recoveredFile, eofPosition));
            final IllegalStateException unsupported = org.junit.Assert.assertThrows(
                    IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(
                            recoveredIndex, Bytes.from("must-not-be-written")));
            assertEquals("Exact-index EOF recovery is only supported for padded stores",
                    unsupported.getMessage());
            assertEquals("failed recovery must leave the existing seal unchanged",
                    Wires.END_OF_DATA, readHeader(recoveredFile, eofPosition));

            try (ExcerptTailer tailer = queue.createTailer()) {
                assertFalse("failed legacy recovery must not make its index visible",
                        tailer.moveToIndex(recoveredIndex));
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

    private static void assertLegacyDataVersion(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, 0, false, null)) {
            assertEquals("legacy store precondition", 0, store.dataVersion());
        }
    }

    private static long endOfDataPosition(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, 0, false, null);
             MappedBytes bytes = store.bytes()) {
            long position = store.writePosition();
            position += Wires.lengthOf(bytes.readVolatileInt(position)) + Wires.SPB_HEADER_SIZE;
            for (; ; ) {
                if (store.dataVersion() > 0)
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
        final File[] queueFiles = directory.listFiles((ignored, name) -> name.endsWith(SingleChronicleQueue.SUFFIX));
        if (queueFiles == null || queueFiles.length == 0)
            throw new AssertionError("No queue files in " + directory);
        java.util.Arrays.sort(queueFiles);
        return queueFiles[0];
    }

    private static void setDataFormatToLegacyUnpadded(File queueFile) throws IOException {
        final byte[] fieldName = "dataFormat".getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
        try (MappedBytes bytes = MappedBytes.mappedBytes(queueFile, 64 << 10)) {
            for (long position = 0; position < 1024; position++) {
                int i = 0;
                while (i < fieldName.length && bytes.readUnsignedByte(position + i) == (fieldName[i] & 0xff))
                    i++;
                if (i != fieldName.length)
                    continue;

                final long valuePosition = position + fieldName.length + 1;
                assertEquals("dataFormat must initially be version 1", 1, bytes.readUnsignedByte(valuePosition));
                bytes.writeByte(valuePosition, (byte) 0);
                return;
            }
        }
        throw new AssertionError("Unable to locate dataFormat in " + queueFile);
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
