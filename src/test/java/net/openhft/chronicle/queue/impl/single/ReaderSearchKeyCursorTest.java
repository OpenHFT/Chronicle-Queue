/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;

import static org.junit.Assert.*;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.queue.reader.comparator.BinarySearchComparator;
import net.openhft.chronicle.wire.*;

import org.junit.Test;

import java.lang.reflect.*;

public class ReaderSearchKeyCursorTest extends QueueTestCommon {

    private static class Comparing implements BinarySearchComparator {
        final Wire key;

        Comparing(Wire key) {
            this.key = key;
        }

        public void accept(net.openhft.chronicle.queue.reader.Reader reader) {
            // This comparator already owns its search key and needs no reader configuration.
        }

        public Wire wireKey() {
            return key;
        }

        public int compare(Wire entry, Wire key) {
            int value = entry.read("key").int32();
            return Integer.compare(value, key.read("key").int32());
        }
    }

    private void duplicates(boolean backwards, boolean checkKeyPosition) throws Exception {
        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        Wire key = new BinaryWire(bytes);
        key.write("key").int32(4);
        try (SingleChronicleQueue queue =
                        SingleChronicleQueueBuilder.binary(getTmpDir())
                                .testBlockSize()
                                .rollCycle(TEST_SECONDLY)
                                .build();
                ExcerptAppender appender = queue.createAppender();
                ExcerptTailer tailer = queue.createTailer()) {
            long[] indexes = new long[3];
            for (int i = 0; i < 3; i++) {
                appender.writeDocument(w -> w.write("key").int32(4));
                indexes[i] = appender.lastIndexAppended();
            }
            ChronicleReader reader = new ChronicleReader();
            Field comparator = ChronicleReader.class.getDeclaredField("binarySearch");
            comparator.setAccessible(true);
            comparator.set(reader, new Comparing(key));
            Field direction = ChronicleReader.class.getDeclaredField("tailerDirection");
            direction.setAccessible(true);
            direction.set(reader, backwards ? TailerDirection.BACKWARD : TailerDirection.FORWARD);
            Method scan =
                    ChronicleReader.class.getDeclaredMethod(
                            "scanToFirstMatchingEntry",
                            ExcerptTailer.class,
                            Wire.class,
                            long.class);
            scan.setAccessible(true);
            long position = bytes.readPosition();
            scan.invoke(reader, tailer, key, indexes[1]);
            if (checkKeyPosition) assertEquals(position, bytes.readPosition());
            else assertEquals(indexes[backwards ? 2 : 0], tailer.index());
        } finally {
            bytes.releaseLast();
        }
    }

    @Test
    public void duplicateSearchFindsFirstForwardMatch() throws Exception {
        duplicates(false, false);
    }

    @Test
    public void duplicateSearchFindsFirstBackwardMatch() throws Exception {
        duplicates(true, false);
    }

    @Test
    public void duplicateSearchPreservesKeyCursor() throws Exception {
        duplicates(false, true);
    }

    @Test
    public void crossCycleSearchRestoresConsumingComparatorKey() throws Exception {
        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        Wire key = new BinaryWire(bytes);
        key.write("key").int32(2);
        SetTimeProvider time = new SetTimeProvider();
        time.currentTimeMillis(100000);
        try (SingleChronicleQueue queue =
                        SingleChronicleQueueBuilder.binary(getTmpDir())
                                .testBlockSize()
                                .rollCycle(TEST_SECONDLY)
                                .timeProvider(time)
                                .build();
                ExcerptAppender appender = queue.createAppender();
                ExcerptTailer tailer = queue.createTailer()) {
            appender.writeDocument(w -> w.write("key").int32(1));
            time.advanceMillis(2000);
            appender.writeDocument(w -> w.write("key").int32(2));
            long expected = appender.lastIndexAppended();
            long position = bytes.readPosition();
            assertEquals(expected, BinarySearch.search(tailer, key, new Comparing(key)));
            assertEquals(position, bytes.readPosition());
        } finally {
            bytes.releaseLast();
        }
    }

    @Test
    public void followingEntryScanRestoresKeyForForwardComparisons() throws Exception {
        following(false, false);
    }

    @Test
    public void followingEntryScanRestoresKeyForBackwardComparisons() throws Exception {
        following(true, false);
    }

    @Test
    public void failedComparisonRestoresKey() throws Exception {
        following(false, true);
    }

    private void following(boolean backwards, boolean fail) throws Exception {
        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        Wire key = new BinaryWire(bytes);
        key.write("key").int32(4);
        IllegalStateException failure = new IllegalStateException("comparison failed");
        Comparing comparing = new Comparing(key) {
            @Override
            public int compare(Wire entry, Wire searchKey) {
                int result = super.compare(entry, searchKey);
                if (fail)
                    throw failure;
                return result;
            }
        };
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .testBlockSize().rollCycle(TEST_SECONDLY).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            long[] indexes = new long[3];
            for (int i = 0; i < 3; i++) {
                int value = 1 + i * 2;
                appender.writeDocument(w -> w.write("key").int32(value));
                indexes[i] = appender.lastIndexAppended();
            }
            ChronicleReader reader = new ChronicleReader();
            Field comparator = ChronicleReader.class.getDeclaredField("binarySearch");
            comparator.setAccessible(true);
            comparator.set(reader, comparing);
            Field direction = ChronicleReader.class.getDeclaredField("tailerDirection");
            direction.setAccessible(true);
            direction.set(reader, backwards ? TailerDirection.BACKWARD : TailerDirection.FORWARD);
            Method scan = ChronicleReader.class.getDeclaredMethod("scanToFirstEntryFollowingMatch",
                    ExcerptTailer.class, Wire.class, long.class);
            scan.setAccessible(true);
            long position = bytes.readPosition();
            long start = indexes[backwards ? 2 : 0];
            if (fail) {
                InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
                        () -> scan.invoke(reader, tailer, key, start));
                assertSame(failure, thrown.getCause());
            } else {
                scan.invoke(reader, tailer, key, start);
                assertEquals(indexes[backwards ? 1 : 2], tailer.index());
            }
            assertEquals(position, bytes.readPosition());
        } finally {
            bytes.releaseLast();
        }
    }
}
