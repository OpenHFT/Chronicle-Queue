/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_HOURLY;
import static net.openhft.chronicle.wire.Wires.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

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
    public void exactWritePreservesIncompleteHeader() {
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(Bytes.from("first"));
            final long requestedIndex = appender.lastIndexAppended() + 1;
            putNextHeader(q, appender.cycle(), NOT_COMPLETE);

            final String before = cycleDump(q, q.rollCycle().toCycle(requestedIndex));
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(requestedIndex, Bytes.from("retry")));
            Assert.assertTrue(failure.getMessage().contains("incomplete header"));
            assertEquals(before, cycleDump(q, q.rollCycle().toCycle(requestedIndex)));
            assertEquals(requestedIndex - 1, q.lastIndex());
        }
    }

    @Test
    public void exactWritePreservesIncompleteHeaderAfterQueueRestart() {
        final File directory = getTmpDir();
        final long requestedIndex;

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(Bytes.from("first"));
            requestedIndex = appender.lastIndexAppended() + 1;
            putNextHeader(q, appender.cycle(), NOT_COMPLETE);
        }

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(() -> 0)
                .rollCycle(TEST4_DAILY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            Assert.assertEquals("CQE must replay the incomplete index after restart",
                    requestedIndex, q.lastIndex() + 1);
            final String before = cycleDump(q, q.rollCycle().toCycle(requestedIndex));
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(requestedIndex, Bytes.from("retry")));
            Assert.assertTrue(failure.getMessage().contains("incomplete header"));
            assertEquals(before, cycleDump(q, q.rollCycle().toCycle(requestedIndex)));
            assertEquals(requestedIndex - 1, q.lastIndex());
        }
    }

    @Test
    public void restartedHistoricalBackfillLowersEofNormalisationBound() {
        final File directory = getTmpDir();
        final SetTimeProvider timeProvider = new SetTimeProvider();
        final long requestedIndex;
        final int recoveredCycle;

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(timeProvider)
                .rollCycle(TEST_HOURLY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(Bytes.from("cycle-0"));
            requestedIndex = appender.lastIndexAppended() + 1;
            recoveredCycle = q.rollCycle().toCycle(requestedIndex);

            for (int i = 1; i <= 3; i++) {
                timeProvider.advanceMillis(TimeUnit.MINUTES.toMillis(65));
                appender.writeText("cycle-" + i);
                appender.normaliseEOFs();
            }

            Assert.assertTrue("test requires the EOF cursor to have advanced beyond the recovery cycle",
                    q.tableStoreGet("normalisedEOFsTo") > recoveredCycle);
            removeEndOfData(q, recoveredCycle);
            Assert.assertFalse(hasEOF(q, recoveredCycle));
        }

        // Simulate a process restart while the recovered roll remains historical by timestamp.
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(directory)
                .timeProvider(timeProvider)
                .rollCycle(TEST_HOURLY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            ((InternalAppender) appender).writeBytes(requestedIndex, Bytes.from("recovered"));

            Assert.assertTrue("every backfilled cycle must lower the shared EOF normalisation bound",
                    q.tableStoreGet("normalisedEOFsTo") <= recoveredCycle);
            Assert.assertFalse("historical backfill remains open until completion normalisation",
                    hasEOF(q, recoveredCycle));

            appender.normaliseEOFs();

            Assert.assertTrue("completion normalisation must restore the historical seal",
                    hasEOF(q, recoveredCycle));
            assertBytesAtIndex(q, requestedIndex, Bytes.from("recovered"));
        }
    }

    @Test
    public void currentTimestampBackfillIsNormalisedOnlyAfterItBecomesHistorical() {
        final SetTimeProvider timeProvider = new SetTimeProvider();

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(timeProvider)
                .rollCycle(TEST_HOURLY)
                .build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(Bytes.from("first"));
            final long backfillIndex = appender.lastIndexAppended() + 1;
            final int backfillCycle = q.rollCycle().toCycle(backfillIndex);

            ((InternalAppender) appender).writeBytes(backfillIndex, Bytes.from("backfilled"));
            Assert.assertTrue("every backfill must retain its normalization lower bound",
                    q.tableStoreGet("normalisedEOFsTo") <= backfillCycle);

            appender.normaliseEOFs();
            Assert.assertFalse("normalisation must leave the wall-clock roll cycle open",
                    hasEOF(q, backfillCycle));

            timeProvider.advanceMillis(TimeUnit.MINUTES.toMillis(65));
            appender.normaliseEOFs();
            Assert.assertTrue("the retained lower bound must seal the cycle once it is historical",
                    hasEOF(q, backfillCycle));
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
    public void publishedDuplicatePayloadsAreComparedWithoutMutation() {
        @NotNull Bytes<byte[]> originalBytes = Bytes.from("hello world");
        final Bytes<byte[]> matchingBytes = Bytes.from("hello world");
        final Bytes<byte[]> overwriteBytes = Bytes.from("HELLO WORLD");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        final List<String> debugMessages = new ArrayList<>();
        final List<String> warningMessages = new ArrayList<>();
        Jvm.setThreadLocalExceptionHandlers(null,
                (clazz, message, thrown) -> warningMessages.add(message),
                (clazz, message, thrown) -> debugMessages.add(message));
        try {
            try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                    .timeProvider(() -> 0)
                    .build();
                 ExcerptAppender appender = q.createAppender()) {
                appender.writeBytes(originalBytes);
                final String queueBeforeDuplicates = q.dump();
                final long matchingReadPosition = matchingBytes.readPosition();
                final long overwriteReadPosition = overwriteBytes.readPosition();

                ((InternalAppender) appender).writeBytes(0, matchingBytes);
                ((InternalAppender) appender).writeBytes(0, overwriteBytes);

                assertEquals(matchingReadPosition, matchingBytes.readPosition());
                assertEquals(overwriteReadPosition, overwriteBytes.readPosition());
                assertEquals(queueBeforeDuplicates, q.dump());
                ExcerptTailer tailer = q.createTailer();
                tailer.readBytes(result);
                assertEquals(originalBytes, result);
                assertEquals(1, tailer.index());
            }
        } finally {
            Jvm.setThreadLocalExceptionHandlers(null, null, null);
        }

        final List<String> duplicateDebugMessages = debugMessages.stream()
                .filter(message -> message.contains("duplicate matches published content and was ignored"))
                .collect(Collectors.toList());
        assertEquals(1, duplicateDebugMessages.size());
        final List<String> duplicateWarningMessages = warningMessages.stream()
                .filter(message -> message.contains("duplicate differs from published content and was ignored"))
                .collect(Collectors.toList());
        assertEquals(1, duplicateWarningMessages.size());
        final String warning = duplicateWarningMessages.get(0);
        Assert.assertTrue(warning.contains("duplicate differs from published content and was ignored"));
        Assert.assertTrue(warning.contains("\nexisting:\n"));
        Assert.assertTrue(warning.contains("68 65 6c 6c  6f 20 77 6f"));
        Assert.assertTrue(warning.contains("\nsupplied:\n"));
        Assert.assertTrue(warning.contains("48 45 4c 4c 4f 20 57 4f"));
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

    @Test
    public void cannotWriteToNonZeroIndexOfNewRollCycleWithoutMutation() {
        final RollCycle rollCycle = RollCycles.DEFAULT;
        try (SingleChronicleQueue q = binary(tempDir("q"))
                .rollCycle(rollCycle)
                .timeProvider(() -> 0).build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeText("hello");    // cycle 0, sequence 0

            final int cycleBefore = appender.cycle();
            final long publishedMaximumBefore = q.tableStoreGet("listing.highestCycle");
            final long normalisedBefore = q.tableStoreGet("normalisedEOFsTo");
            final long rollCountBefore = Arrays.stream(q.file().listFiles())
                    .filter(file -> file.getName().endsWith(SingleChronicleQueue.SUFFIX))
                    .count();
            Assert.assertFalse(hasEOF(q, cycleBefore));

            // attempt to write to cycle 1, sequence 1
            assertThrows(IllegalIndexException.class,
                    () -> ((InternalAppender) appender).writeBytes(
                            rollCycle.toIndex(cycleBefore + 1, 1), Bytes.from("text")));

            Assert.assertEquals(cycleBefore, appender.cycle());
            Assert.assertEquals(publishedMaximumBefore, q.tableStoreGet("listing.highestCycle"));
            Assert.assertEquals(normalisedBefore, q.tableStoreGet("normalisedEOFsTo"));
            Assert.assertEquals(rollCountBefore, Arrays.stream(q.file().listFiles())
                    .filter(file -> file.getName().endsWith(SingleChronicleQueue.SUFFIX))
                    .count());
            Assert.assertFalse(hasEOF(q, cycleBefore));
        }
    }

    @Test
    public void exactWritePreservesHistoricalSeal() {
        final SetTimeProvider time = new SetTimeProvider(0);
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(time).rollCycle(TEST_HOURLY).build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(Bytes.from("first"));
            final long published = appender.lastIndexAppended();
            final int firstCycle = appender.cycle();
            time.advanceMillis(TimeUnit.MINUTES.toMillis(65));
            appender.writeText("second");
            final int current = appender.cycle();
            final String sealed = cycleDump(q, firstCycle);
            Assert.assertTrue(hasEOF(q, firstCycle));
            final long cursor = q.tableStoreGet("normalisedEOFsTo");

            ((InternalAppender) appender).writeBytes(published, Bytes.from("first"));
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(published + 1, Bytes.from("late")));
            Assert.assertTrue(failure.getMessage().contains("sealed roll"));
            assertEquals(sealed, cycleDump(q, firstCycle));
            assertEquals(current, appender.cycle());
            assertEquals(cursor, q.tableStoreGet("normalisedEOFsTo"));
            appender.writeText("third");
            try (ExcerptTailer tailer = q.createTailer()) {
                assertNextBytes(tailer, Bytes.elasticHeapByteBuffer(), Bytes.from("first"));
                assertEquals("second", tailer.readText());
                assertEquals("third", tailer.readText());
                Assert.assertNull(tailer.readText());
            }
        }
    }

    @Test
    public void exactWritePreservesSecondaryIndexAtSeal() {
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

            final String before = cycleDump(q, recoveredCycle);
            assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(recoveredIndex, Bytes.from("late")));
            assertEquals(before, cycleDump(q, recoveredCycle));
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

            final String before = cycleDump(q, recoveredCycle);
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ((InternalAppender) appender).writeBytes(recoveredIndex, recovered));
            Assert.assertTrue(failure.getMessage().contains("sealed roll"));
            assertEquals(before, cycleDump(q, recoveredCycle));
            assertEquals(filesBeforeRecovery, queueFiles(q).length);
            assertBytesAtIndex(q, laterIndex, later);
            assertNoDataAfter(q, laterIndex);
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

    private static void removeEndOfData(SingleChronicleQueue q, int cycle) {
        try (SingleChronicleQueueStore store = q.storeForCycle(cycle, 0, false, null);
             MappedBytes bytes = store.bytes()) {
            long position = store.writePosition();
            position += lengthOf(bytes.readVolatileInt(position)) + SPB_HEADER_SIZE;
            for (; ; ) {
                if (store.dataVersion() > 0)
                    position += BytesUtil.padOffset(position);
                final int header = bytes.readVolatileInt(position);
                if (header == END_OF_DATA) {
                    Assert.assertTrue(bytes.compareAndSwapInt(position, END_OF_DATA, NOT_INITIALIZED));
                    return;
                }
                Assert.assertTrue("expected metadata before EOF at position " + position,
                        isReadyMetaData(header));
                position += SPB_HEADER_SIZE + lengthOf(header);
            }
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
