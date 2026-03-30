/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.StreamCorruptedException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.*;

class PartialUpdateTest extends QueueTestCommon {

    private static final long LAST_INDEX = RollCycles.FAST_DAILY.toIndex(2, 2);
    private Path queuePath;
    private SetTimeProvider setTimeProvider;
    private static boolean originalCheckIndexValue;

    enum PartialQueueCreator {
        CONSISTENT_BUT_STALE_WP_SEQ {
            @Override
            void createQueue(SetTimeProvider timeProvider, Path path) {
                createQueueWithConsistentButStaleWritePositionAndSequence(timeProvider, path);
            }
        },
        INCONSISTENT_WP_SEQ {
            @Override
            void createQueue(SetTimeProvider timeProvider, Path path) {
                createQueueWithInconsistentWritePositionAndSequence(timeProvider, path);
            }
        },
        FULLY_CONSISTENT {
            @Override
            void createQueue(SetTimeProvider timeProvider, Path path) {
                createAFullyConsistentQueue(timeProvider, path);
            }
        };

        abstract void createQueue(SetTimeProvider timeProvider, Path path);
    }

    public static Iterable<Object[]> params() {
        return Arrays.stream(PartialQueueCreator.values()).map(qc -> new Object[]{qc}).collect(Collectors.toList());
    }

    private void setUp(PartialQueueCreator queueCreator) {
        queuePath = IOTools.createTempDirectory("partialUpdate");
        setTimeProvider = new SetTimeProvider();
        queueCreator.createQueue(setTimeProvider, queuePath);
    }

    @BeforeAll
    static void disableCheckIndexAssertions() {
        // This turns off assertions, so we see what would happen in the real world
        originalCheckIndexValue = QueueSystemProperties.CHECK_INDEX;
        QueueSystemProperties.CHECK_INDEX = false;
    }

    @AfterAll
    static void restoreCheckIndexAssertions() {
        QueueSystemProperties.CHECK_INDEX = originalCheckIndexValue;
    }

    @ParameterizedTest
    @MethodSource("params")
    void testBackwardsToEndArrivesAtCorrectPosition(PartialQueueCreator queueCreator) {
        setUp(queueCreator);
        try {
            try (SingleChronicleQueue queue = createQueue(setTimeProvider, queuePath);
                 ExcerptTailer tailer = queue.createTailer()) {
                tailer.direction(TailerDirection.BACKWARD).toEnd();
                // toEnd in the backward direction positions the cursor at the last excerpt (ready to read it)
                assertEquals("Six", tailer.readText());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath.toFile());
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void testBackwardsToEndReportsCorrectIndex(PartialQueueCreator queueCreator) {
        setUp(queueCreator);
        try {
            try (SingleChronicleQueue queue = createQueue(setTimeProvider, queuePath);
                 ExcerptTailer tailer = queue.createTailer()) {
                tailer.direction(TailerDirection.BACKWARD).toEnd();
                // toEnd in the backward direction positions the cursor at the last excerpt (ready to read it)
                assertEquals(LAST_INDEX, tailer.index());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath.toFile());
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void testForwardsToEndArrivesAtCorrectPosition(PartialQueueCreator queueCreator) {
        setUp(queueCreator);
        try {
            try (SingleChronicleQueue queue = createQueue(setTimeProvider, queuePath);
                 ExcerptTailer tailer = queue.createTailer()) {
                tailer.toEnd();
                // toEnd in the forward direction positions the cursor AFTER the last excerpt
                assertNull(tailer.readText());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath.toFile());
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void testForwardsToEndReportsCorrectIndex(PartialQueueCreator queueCreator) {
        setUp(queueCreator);
        try {
            try (SingleChronicleQueue queue = createQueue(setTimeProvider, queuePath);
                 ExcerptTailer tailer = queue.createTailer()) {
                tailer.toEnd();
                // toEnd in the forward direction positions the cursor AFTER the last excerpt
                assertEquals(LAST_INDEX + 1, tailer.index());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath.toFile());
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void testLastIndexIsCorrect(PartialQueueCreator queueCreator) {
        setUp(queueCreator);
        try {
            try (SingleChronicleQueue queue = createQueue(setTimeProvider, queuePath)) {
                // Should report last index correctly
                assertEquals(LAST_INDEX, queue.lastIndex());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath.toFile());
        }
    }

    @ParameterizedTest
    @MethodSource("params")
    void testAppendReturnsCorrectLastAppendedIndex(PartialQueueCreator queueCreator) {
        setUp(queueCreator);
        try {
            try (SingleChronicleQueue queue = createQueue(setTimeProvider, queuePath);
                 ExcerptAppender appender = queue.createAppender()) {
                // Should report last index correctly
                appender.writeText("Seven");
                assertEquals(LAST_INDEX + 1, appender.lastIndexAppended());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath.toFile());
        }
    }

    /**
     * Create a queue where the excerpt was written, but the writePosition/sequence are still from the previous
     * entry. This can happen in an abnormal termination.
     */
    private static void createQueueWithConsistentButStaleWritePositionAndSequence(SetTimeProvider setTimeProvider, Path path) {
        try (SingleChronicleQueue queue = createQueue(setTimeProvider, path);
             ExcerptAppender appender = queue.createAppender();
             StoreTailer tailer = (StoreTailer) queue.createTailer()) {
            appender.writeText("One");
            appender.writeText("Two");
            appender.writeText("Three");
            setTimeProvider.advanceMillis(TimeUnit.HOURS.toMillis(2));
            appender.writeText("Four");
            appender.writeText("Five");
            int currentCycle = RollCycles.FAST_DAILY.toCycle(appender.lastIndexAppended());
            try (SingleChronicleQueueStore secondRollCycle = queue.storeForCycle(currentCycle, 0, false, null)) {
                assertNotNull(secondRollCycle);
                long previousWritePosition = secondRollCycle.writePosition();
                long previousSequence = secondRollCycle.lastSequenceNumber(tailer);
                printLastWritePositionAndSequence("before append last excerpt", tailer, secondRollCycle);
                assertEquals(1, previousSequence);
                appender.writeText("Six");
                printLastWritePositionAndSequence("after append last excerpt", tailer, secondRollCycle);

                // simulate the last write being partial
                forceUpdateWritePositionAndSequence(tailer, secondRollCycle, previousWritePosition, previousSequence);
                printLastWritePositionAndSequence("after over-writing write position & seq", tailer, secondRollCycle);
            } catch (StreamCorruptedException e) {
                throw new RuntimeException("Error reading last sequence number", e);
            }
        }
    }

    /**
     * Create a queue where the writePosition was updated, but the sequence was from the second-last entry.
     * This can happen in an abnormal termination.
     */
    private static void createQueueWithInconsistentWritePositionAndSequence(SetTimeProvider setTimeProvider, Path path) {
        try (SingleChronicleQueue queue = createQueue(setTimeProvider, path);
             ExcerptAppender appender = queue.createAppender();
             StoreTailer tailer = (StoreTailer) queue.createTailer()) {
            appender.writeText("One");
            appender.writeText("Two");
            appender.writeText("Three");
            setTimeProvider.advanceMillis(TimeUnit.HOURS.toMillis(2));
            appender.writeText("Four");
            appender.writeText("Five");
            int currentCycle = RollCycles.FAST_DAILY.toCycle(appender.lastIndexAppended());
            try (SingleChronicleQueueStore secondRollCycle = queue.storeForCycle(currentCycle, 0, false, null)) {
                assertNotNull(secondRollCycle);
                long previousWritePosition = secondRollCycle.writePosition();
                long previousSequence = secondRollCycle.lastSequenceNumber(tailer);
                printLastWritePositionAndSequence("before append last excerpt", tailer, secondRollCycle);
                assertEquals(1, previousSequence);
                appender.writeText("Six");
                printLastWritePositionAndSequence("after append last excerpt", tailer, secondRollCycle);

                // simulate the last write being partial
                forceUpdateWritePosition(secondRollCycle, previousWritePosition);
                printLastWritePositionAndSequence("after over-writing write position", tailer, secondRollCycle);
            } catch (StreamCorruptedException e) {
                throw new RuntimeException("Error reading last sequence number", e);
            }
        }
    }

    private static void printLastWritePositionAndSequence(String description, StoreTailer context, SingleChronicleQueueStore store) {
        try {
            context.toStart();
            long writePosition = store.writePosition();
            long lastSequenceNumber = store.lastSequenceNumber(context);
            Jvm.startup().on(PartialUpdateTest.class, format("Last wp/seq = %x/%d (%s)", writePosition, lastSequenceNumber, description));
        } catch (StreamCorruptedException e) {
            throw new RuntimeException("Error reading last sequence number", e);
        }
    }

    /**
     * Create a fully consistent queue, this is the "control"
     */
    private static void createAFullyConsistentQueue(SetTimeProvider setTimeProvider, Path path) {
        try (SingleChronicleQueue queue = createQueue(setTimeProvider, path);
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("One");
            appender.writeText("Two");
            appender.writeText("Three");
            setTimeProvider.advanceMillis(TimeUnit.HOURS.toMillis(2));
            appender.writeText("Four");
            appender.writeText("Five");
            appender.writeText("Six");
        }
    }

    private static void forceUpdateWritePositionAndSequence(StoreTailer storeTailer, SingleChronicleQueueStore store, long newWritePosition, long newSequence) {
        try {
            forceUpdateWritePosition(store, newWritePosition);
            store.setPositionForSequenceNumber(storeTailer, newSequence, newWritePosition);
        } catch (StreamCorruptedException e) {
            throw new RuntimeException("Error setting position for sequence");
        }
    }

    /**
     * This is horrible code required to bypass the checks that prevent writePosition from going backwards
     * <p>
     * We need to do this to simulate writePosition not being updated
     */
    private static void forceUpdateWritePosition(SingleChronicleQueueStore store, long newWritePosition) {
        try {
            Field wpField = store.getClass().getDeclaredField("writePosition");
            wpField.setAccessible(true);
            LongValue writePosition = (LongValue) wpField.get(store);
            writePosition.setValue(newWritePosition);
        } catch (Exception e) {
            throw new RuntimeException("Could not set write position/sequence", e);
        }
    }

    @NotNull
    private static SingleChronicleQueue createQueue(SetTimeProvider setTimeProvider, Path queuePath) {
        return SingleChronicleQueueBuilder
                .binary(queuePath)
                .timeProvider(setTimeProvider)
                .testBlockSize()
                .rollCycle(RollCycles.FAST_HOURLY)
                .build();
    }
}
