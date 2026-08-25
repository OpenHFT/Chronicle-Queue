/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_HOURLY;
import static net.openhft.chronicle.wire.Wires.*;
import static org.junit.Assert.assertEquals;

public class InternalAppenderWriteBytesTest extends QueueTestCommon {

    @Before
    public void before() {
        if (OS.isMacOSX())
            ignoreException(exceptionKey -> exceptionKey.clazz == DirectoryUtils.class, "Ignore DirectoryUtils");
        ignoreException(e -> e.level == LogLevel.PERF, "ignore all PERF");
    }

    @Test
    public void exactWriteInitializesUnusedRequestedEntryWithoutWarning() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world again");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        RollCycle rollCycle = RollCycles.DEFAULT;
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(() -> 0)
                .rollCycle(rollCycle)
                .build();
             ExcerptAppender appender = q.createAppender();
             ExcerptTailer tailer = q.createTailer()) {

            // write at cycle 0, sequence 0
            appender.writeBytes(test);
            // append at cycle 0, sequence 1
            ((InternalAppender) appender).writeBytes(rollCycle.toIndex(0, 1), test2);

            tailer.readBytes(result);
            assertEquals(test, result);
            result.clear();

            tailer.readBytes(result);
            assertEquals(test2, result);
            result.clear();
        }
    }

    @Test
    public void exactWriteReplacesAnIncompleteRequestedEntryWithAWarning() {
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(Bytes.from("first"));
            final long requestedIndex = appender.lastIndexAppended() + 1;
            putNextHeader(q, appender.cycle(), NOT_COMPLETE);

            expectException("Incomplete header found at pos");
            ((InternalAppender) appender).writeBytes(requestedIndex, Bytes.from("recovered"));

            assertBytesAtIndex(q, requestedIndex, Bytes.from("recovered"));
            assertNoDataAfter(q, requestedIndex);
        }
    }

    @Test
    public void ordinaryAppenderContinuesInCurrentCycleAfterIndexedWriteToUnsealedRoll() {
        @NotNull Bytes<byte[]> first = Bytes.from("first ordinary entry");
        @NotNull Bytes<byte[]> indexed = Bytes.from("indexed entry");
        @NotNull Bytes<byte[]> following = Bytes.from("following ordinary entry");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender appender = q.createAppender();
             ExcerptTailer tailer = q.createTailer()) {
            appender.writeBytes(first);
            final long indexedWriteIndex = appender.lastIndexAppended() + 1;
            final int currentCycle = appender.cycle();

            ((InternalAppender) appender).writeBytes(indexedWriteIndex, indexed);
            Assert.assertEquals(currentCycle, appender.cycle());
            Assert.assertFalse("an indexed write to an unsealed current roll must not add EOF",
                    hasEOF(q, currentCycle));

            appender.writeBytes(following);
            Assert.assertEquals(currentCycle, appender.cycle());
            Assert.assertEquals(indexedWriteIndex + 1, appender.lastIndexAppended());

            assertNextBytes(tailer, result, first);
            assertNextBytes(tailer, result, indexed);
            assertNextBytes(tailer, result, following);
            Assert.assertFalse(tailer.readBytes(result.clear()));
        }
    }

    @Test
    public void canWriteAtBeginningOfNextRollCycle() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world again");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        RollCycle rollCycle = RollCycles.DEFAULT;
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(() -> 0)
                .rollCycle(rollCycle)
                .build();
             ExcerptAppender appender = q.createAppender();
             ExcerptTailer tailer = q.createTailer()) {

            // write at cycle 0, sequence 0
            appender.writeBytes(test);
            // append at cycle 1, sequence 0
            ((InternalAppender) appender).writeBytes(rollCycle.toIndex(1, 0), test2);

            tailer.readBytes(result);
            assertEquals(test, result);
            result.clear();

            tailer.readBytes(result);
            assertEquals(test2, result);
            result.clear();
        }
    }

    @Test
    public void cannotOverwriteExistingEntries() {
        @NotNull Bytes<byte[]> originalBytes = Bytes.from("hello world");
        final Bytes<byte[]> overwriteBytes = Bytes.from("HELLO WORLD");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(originalBytes);

            // try to overwrite - will not overwrite
            ((InternalAppender) appender).writeBytes(0, overwriteBytes);

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            assertEquals(originalBytes, result);
            assertEquals(1, tailer.index());
        }
    }

    @Test
    public void cannotOverwriteExistingEntries_DifferentQueueInstance() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world2");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        long index;
        final File tmpDir = getTmpDir();
        final String expected = "" +
                "--- !!meta-data #binary\n" +
                "header: !STStore {\n" +
                "  wireType: !WireType BINARY_LIGHT,\n" +
                "  metadata: !SCQMeta {\n" +
                "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T4', epoch: 0 },\n" +
                "    sourceId: 0\n" +
                "  }\n" +
                "}\n" +
                "# position: 176, header: 0\n" +
                "--- !!data #binary\n" +
                "listing.highestCycle: 0\n" +
                "# position: 216, header: 1\n" +
                "--- !!data #binary\n" +
                "listing.lowestCycle: 0\n" +
                "# position: 256, header: 2\n" +
                "--- !!data #binary\n" +
                "listing.modCount: 1\n" +
                "# position: 288, header: 3\n" +
                "--- !!data #binary\n" +
                "chronicle.write.lock: -9223372036854775808\n" +
                "# position: 328, header: 4\n" +
                "--- !!data #binary\n" +
                "chronicle.append.lock: -9223372036854775808\n" +
                "# position: 368, header: 5\n" +
                "--- !!data #binary\n" +
                "chronicle.lastIndexReplicated: -1\n" +
                "# position: 416, header: 6\n" +
                "--- !!data #binary\n" +
                "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                "...\n" +
                "# 130596 bytes remaining\n" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    792,\n" +
                "    3401614098433\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 196,\n" +
                "    lastIndex: 4\n" +
                "  },\n" +
                "  dataFormat: 1\n" +
                "}\n" +
                "# position: 196, header: -1\n" +
                "--- !!meta-data #binary\n" +
                "index2index: [\n" +
                "  # length: 32, used: 1\n" +
                "  488,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "# position: 488, header: -1\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 32, used: 1\n" +
                "  776,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "# position: 776, header: 0\n" +
                "--- !!data\n" +
                "hello world\n" +
                "# position: 792, header: 1\n" +
                "--- !!data\n" +
                "hello world2\n" +
                "...\n" +
                "# 130260 bytes remaining\n";
        try (SingleChronicleQueue q = createQueue(tmpDir);
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(test);
            appender.writeBytes(test2);
            index = appender.lastIndexAppended();
        }
        assertEquals(1, index);

        // has to be the same tmpDir
        try (SingleChronicleQueue q = createQueue(tmpDir);
             InternalAppender appender = (InternalAppender) q.createAppender()) {
            appender.writeBytes(0, Bytes.from("HELLO WORLD"));
            appender.writeBytes(1, Bytes.from("HELLO WORLD"));

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            assertEquals(test, result);
            assertEquals(1, tailer.index());
        }
    }

    @NotNull
    private SingleChronicleQueue createQueue(File tmpDir) {
        return SingleChronicleQueueBuilder.binary(tmpDir).timeProvider(() -> 0).testBlockSize().rollCycle(TEST4_DAILY).build();
    }

    @Test(expected = IllegalIndexException.class)
    public void cannotAppendToExistingCycleIfNotNextIndex() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build();
             ExcerptAppender appender = q.createAppender()) {
            // append to cycle 0, sequence 0
            appender.writeBytes(test);

            // this will throw because it is not in sequence (cycle 0, sequence 2)
            ((InternalAppender) appender).writeBytes(2, test);
        }
    }

    @Test(expected = IllegalIndexException.class)
    public void cannotWriteToNonZeroIndexOfNewRollCycle() {
        final RollCycle rollCycle = RollCycles.DEFAULT;
        try (SingleChronicleQueue q = binary(tempDir("q"))
                .rollCycle(rollCycle)
                .timeProvider(() -> 0).build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeText("hello");    // cycle 0, sequence 0

            // attempt to write to cycle 1, sequence 1
            ((InternalAppender) appender).writeBytes(rollCycle.toIndex(1, 1), Bytes.from("text"));
        }
    }

    @Test
    public void canBackfillPreviousCycleAfterEOF() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test1 = Bytes.from("hello world again cycle1");
        @NotNull Bytes<byte[]> test1b = Bytes.from("second recovered entry cycle1");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world cycle2");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        SetTimeProvider timeProvider = new SetTimeProvider();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(timeProvider).rollCycle(TEST_HOURLY).build();
             ExcerptTailer tailer = q.createTailer()) {
            final int firstCycle;
            try (ExcerptAppender appender = q.createAppender()) {
                appender.writeBytes(test);
                long nextIndexInFirstCycle = appender.lastIndexAppended() + 1;
                firstCycle = q.rollCycle().toCycle(nextIndexInFirstCycle);

                timeProvider.advanceMillis(TimeUnit.SECONDS.toMillis(65 * 60));
                appender.writeBytes(test2);

                assertNextBytes(tailer, result, test);
                assertNextBytes(tailer, result, test2);
                Assert.assertFalse(tailer.readBytes(result.clear()));

                Assert.assertTrue(hasEOF(q, firstCycle));
                expectException("queue=" + q.fileAbsolutePath() + ", cycle=" + firstCycle
                        + ", index=0x" + Long.toHexString(nextIndexInFirstCycle));
                ((InternalAppender) appender).writeBytes(nextIndexInFirstCycle, test1);
                Assert.assertTrue("indexed recovery must immediately restore EOF", hasEOF(q, firstCycle));

                final long secondRecoveryIndex = nextIndexInFirstCycle + 1;
                expectException("queue=" + q.fileAbsolutePath() + ", cycle=" + firstCycle
                        + ", index=0x" + Long.toHexString(secondRecoveryIndex));
                ((InternalAppender) appender).writeBytes(secondRecoveryIndex, test1b);
                Assert.assertTrue("each indexed recovery must leave the roll sealed", hasEOF(q, firstCycle));
            }
            Assert.assertTrue(hasEOF(q, firstCycle));
            Assert.assertFalse("a live tailer that crossed the roll must not rewind implicitly",
                    tailer.readBytes(result.clear()));

            try (ExcerptTailer restartedTailer = q.createTailer()) {
                assertNextBytes(restartedTailer, result, test);
                assertNextBytes(restartedTailer, result, test1);
                assertNextBytes(restartedTailer, result, test1b);
                assertNextBytes(restartedTailer, result, test2);
                Assert.assertFalse(restartedTailer.readBytes(result.clear()));
            }
        }
    }

    @Test
    public void exactBackfillCanAddASecondaryIndexBeforeResealing() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        final int entriesInFirstSecondaryIndex = TEST4_DAILY.defaultIndexCount()
                * TEST4_DAILY.defaultIndexSpacing();

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(timeProvider)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            for (int i = 0; i < entriesInFirstSecondaryIndex; i++)
                appender.writeText("entry-" + i);

            final long recoveredIndex = appender.lastIndexAppended() + 1;
            final int recoveredCycle = q.rollCycle().toCycle(recoveredIndex);
            Assert.assertTrue(cycleDump(q, recoveredCycle)
                    .contains("index2index: [\n  # length: 32, used: 1\n"));

            timeProvider.advanceMillis(TimeUnit.HOURS.toMillis(25));
            appender.writeText("next-cycle");
            Assert.assertTrue(hasEOF(q, recoveredCycle));

            expectException("queue=" + q.fileAbsolutePath() + ", cycle=" + recoveredCycle
                    + ", index=0x" + Long.toHexString(recoveredIndex));
            ((InternalAppender) appender).writeBytes(recoveredIndex, Bytes.from("recovered"));

            final String recoveredCycleDump = cycleDump(q, recoveredCycle);
            Assert.assertTrue("backfill must add the second secondary index before resealing",
                    recoveredCycleDump.contains("index2index: [\n  # length: 32, used: 2\n"));
            Assert.assertTrue("backfill must leave the recovered cycle sealed",
                    recoveredCycleDump.contains(" EOF")
                            && recoveredCycleDump.contains("--- !!not-ready-meta-data"));
        }
    }

    @Test
    public void exactBackfillFindsEofAfterSecondaryIndexMetadata() {
        final int entriesBeforeRecovery = TEST4_DAILY.defaultIndexCount()
                * TEST4_DAILY.defaultIndexSpacing() + 1;

        assertExactBackfillDoesNotRollForward(entriesBeforeRecovery, false);
    }

    @Test
    public void exactBackfillFindsEofAfterTrailingUserMetadata() {
        assertExactBackfillDoesNotRollForward(1, true);
    }

    @Test
    public void exactBackfillFindsEofInEmptySealedCycle() {
        assertExactBackfillDoesNotRollForward(0, true);
    }

    private void assertExactBackfillDoesNotRollForward(int initialEntries, boolean trailingMetadata) {
        final SetTimeProvider timeProvider = new SetTimeProvider();
        final Bytes<?> recovered = Bytes.from("recovered");
        final Bytes<?> later = Bytes.from("later-cycle");

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(timeProvider)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            for (int i = 0; i < initialEntries; i++)
                appender.writeBytes(Bytes.from("entry-" + i));

            if (trailingMetadata) {
                try (DocumentContext document = appender.writingDocument(true)) {
                    document.wire().write("meta").text("trailing metadata");
                }
            }

            final long recoveredIndex = initialEntries == 0
                    ? q.rollCycle().toIndex(0, 0)
                    : appender.lastIndexAppended() + 1;
            final int recoveredCycle = q.rollCycle().toCycle(recoveredIndex);

            timeProvider.advanceMillis(TimeUnit.HOURS.toMillis(25));
            appender.writeBytes(later);
            final long laterIndex = appender.lastIndexAppended();
            final int filesBeforeRecovery = queueFiles(q).length;
            Assert.assertTrue(hasEOF(q, recoveredCycle));

            Throwable failure = null;
            try {
                // Other focused tests assert the warning. Here the structural assertions must remain
                // discriminating even when recovery fails before it reaches the warning site.
                ignoreException("Reopened end-of-data for exact-index recovery");
                ((InternalAppender) appender).writeBytes(recoveredIndex, recovered);
            } catch (Throwable t) {
                failure = t;
            }

            Assert.assertEquals("exact recovery must not create another roll",
                    filesBeforeRecovery, queueFiles(q).length);
            assertBytesAtIndex(q, laterIndex, later);
            assertNoDataAfter(q, laterIndex);
            if (failure != null)
                throw new AssertionError("exact recovery should succeed in its requested cycle", failure);
            assertBytesAtIndex(q, recoveredIndex, recovered);
            Assert.assertTrue("the recovered cycle must be resealed", hasEOF(q, recoveredCycle));
        }
    }

    @Test
    public void indexCapacityFailureDoesNotCommitOrReopenRecoveredCycle() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(timeProvider)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            for (long i = 0; i < q.rollCycle().maxMessagesPerCycle(); i++)
                appender.writeText("entry-" + i);

            final long recoveredIndex = appender.lastIndexAppended() + 1;
            final int recoveredCycle = q.rollCycle().toCycle(recoveredIndex);
            timeProvider.advanceMillis(TimeUnit.HOURS.toMillis(25));
            appender.writeText("next-cycle");
            final long lastIndexBeforeRecovery = appender.lastIndexAppended();
            final long writePositionBeforeRecovery = writePosition(q, recoveredCycle);
            Assert.assertTrue(hasEOF(q, recoveredCycle));

            IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(recoveredIndex, Bytes.from("recovered")));

            Assert.assertTrue(failure.getMessage().contains("number of entries exceeds max number"));
            Assert.assertEquals("failed recovery must not advance the recovered store write position",
                    writePositionBeforeRecovery, writePosition(q, recoveredCycle));
            Assert.assertEquals("failed recovery must not change the appender's last committed index",
                    lastIndexBeforeRecovery, appender.lastIndexAppended());
            try (ExcerptTailer tailer = q.createTailer()) {
                if (tailer.moveToIndex(recoveredIndex)) {
                    final Bytes<?> result = Bytes.elasticHeapByteBuffer();
                    Assert.assertTrue(tailer.readBytes(result));
                    Assert.assertNotEquals("failed recovery payload must not become readable",
                            Bytes.from("recovered"), result);
                }
            }
            Assert.assertTrue("prevalidation failure must leave the existing EOF unchanged",
                    hasEOF(q, recoveredCycle));
        }
    }

    private static void assertBytesAtIndex(SingleChronicleQueue q, long index, Bytes<?> expected) {
        try (ExcerptTailer tailer = q.createTailer()) {
            Assert.assertTrue("unable to move to index 0x" + Long.toHexString(index), tailer.moveToIndex(index));
            assertNextBytes(tailer, Bytes.elasticHeapByteBuffer(), expected);
        }
    }

    private static void assertNoDataAfter(SingleChronicleQueue q, long index) {
        try (ExcerptTailer tailer = q.createTailer()) {
            Assert.assertTrue("unable to move to index 0x" + Long.toHexString(index), tailer.moveToIndex(index));
            Assert.assertTrue(tailer.readBytes(Bytes.elasticHeapByteBuffer()));
            Assert.assertFalse("exact recovery must not append data after the later-cycle entry",
                    tailer.readBytes(Bytes.elasticHeapByteBuffer()));
        }
    }

    private static File[] queueFiles(SingleChronicleQueue q) {
        final File[] files = q.file().listFiles((directory, name) -> name.endsWith(SingleChronicleQueue.SUFFIX));
        Assert.assertNotNull(files);
        return files;
    }

    private static long writePosition(SingleChronicleQueue q, int cycle) {
        try (SingleChronicleQueueStore store = q.storeForCycle(cycle, 0, false, null)) {
            return store.writePosition();
        }
    }

    private static void putNextHeader(SingleChronicleQueue q, int cycle, int header) {
        try (SingleChronicleQueueStore store = q.storeForCycle(cycle, 0, false, null);
             MappedBytes bytes = store.bytes()) {
            long position = store.writePosition();
            position += lengthOf(bytes.readVolatileInt(position)) + SPB_HEADER_SIZE;
            position += BytesUtil.padOffset(position);
            Assert.assertEquals(NOT_INITIALIZED, bytes.readVolatileInt(position));
            bytes.writeVolatileInt(position, header);
        }
    }

    private static void assertNextBytes(ExcerptTailer tailer, Bytes<?> result, Bytes<?> expected) {
        result.clear();
        Assert.assertTrue(tailer.readBytes(result));
        assertEquals(expected, result);
    }

    private boolean hasEOF(SingleChronicleQueue q, int cycle) {
        String dump = cycleDump(q, cycle);
        return dump.contains(" EOF") && dump.contains("--- !!not-ready-meta-data");
    }

    private String cycleDump(SingleChronicleQueue q, int cycle) {
        try (SingleChronicleQueueStore store = q.storeForCycle(cycle, 0, false, null)) {
            return store.dump(WireType.BINARY_LIGHT);
        }
    }
}
