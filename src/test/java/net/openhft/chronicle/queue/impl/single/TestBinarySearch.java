/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestBinarySearch extends QueueTestCommon {

    private final Map<Integer, Long> keyToIndex = new HashMap<>();
    private final int numberOfMessages;
    private final int numberOfMessagesToVerify;
    private final RetrievalStrategy retrievalStrategy;
    private final EmptyCyclesStrategy emptyCyclesStrategy;

    public TestBinarySearch(int numberOfMessages, int numberOfMessagesToVerify, EmptyCyclesStrategy emptyCyclesStrategy) {
        this.numberOfMessages = numberOfMessages;
        this.numberOfMessagesToVerify = numberOfMessagesToVerify;
        this.retrievalStrategy = numberOfMessages == numberOfMessagesToVerify ? RetrievalStrategy.LINEAR : RetrievalStrategy.RANDOM;
        this.emptyCyclesStrategy = emptyCyclesStrategy;
    }

    @Parameterized.Parameters(name = "items: {0} verify: {1} emptyCycles: {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(runForEveryEmptyCycleStrategy(new Object[][]{
                {0, 0},
                {1, 1},
                {2, 2},
                {10, 10},
                {100, 50},
                {100, 100},
        }));
    }

    private static Object[][] runForEveryEmptyCycleStrategy(Object[][] parameters) {
        EmptyCyclesStrategy[] emptyCyclesStrategies = EmptyCyclesStrategy.values();
        int noStrategies = emptyCyclesStrategies.length;
        Object[][] result = new Object[parameters.length * noStrategies][];
        for (int i = 0; i < parameters.length; i++) {
            Object[] parameter = parameters[i];
            for (int j = 0; j < noStrategies; j++) {
                Object[] newParameter = Arrays.copyOf(parameter, parameter.length + 1);
                newParameter[parameter.length] = emptyCyclesStrategies[j];
                result[i*noStrategies + j] = newParameter;
            }
        }
        return result;
    }

    @Test
    public void testBinarySearch() {
        final SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(0);

        boolean writtenEmptyCycles = false;

        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(TestRollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            if (emptyCyclesStrategy.atStart()) {
                writeEmptyCycles(appender);
                writtenEmptyCycles = true;
            }

            for (int i = 0; i < numberOfMessages; i++) {
                try (final DocumentContext dc = appender.writingDocument()) {
                    final MyData myData = new MyData();
                    myData.key = i;
                    myData.value = "some value where the key=" + i;
                    myData.writeMarshallable(dc.wire());
                    Jvm.startup().on(getClass(), "written key: " + myData.key + " at index: " + dc.index() + " cycle: " + appender.cycle());
                    stp.advanceMillis(300);
                    keyToIndex.put(myData.key, dc.index());
                }

                if (i > 0 && numberOfMessages > 10 && i % (numberOfMessages / 10) == 0) {
                    Jvm.startup().on(getClass(), "Written " + i + " messages");
                }

                if (!writtenEmptyCycles && emptyCyclesStrategy.inMiddle() && appender.cycle() != queue.firstCycle()) {
                    writeEmptyCycles(appender);
                    writtenEmptyCycles = true;
                }

            }
            Jvm.startup().on(getClass(), "Written " + numberOfMessages + " messages");

            if (emptyCyclesStrategy.atEnd()) {
                writeEmptyCycles(appender);
            }

            MyData reusableComparatorData = new MyData();
            final Comparator<Wire> comparator = (o1, o2) -> {
                reusableComparatorData.readMarshallable(o1);
                int o1Key = reusableComparatorData.key;
                reusableComparatorData.readMarshallable(o2);
                int o2Key = reusableComparatorData.key;
                return Integer.compare(o1Key, o2Key);
            };

            try (final ExcerptTailer binarySearchTailer = queue.createTailer()) {
                for (int j = 0; j < numberOfMessagesToVerify; j++) {
                    int indexToVerify = (int) retrievalStrategy.retrieveIndex(j, numberOfMessages);
                    Wire key = toWire(indexToVerify);
                    long index = BinarySearch.search(binarySearchTailer, key, comparator);
                    long expectedIndex = keyToIndex.get(indexToVerify);
                    assertEquals("Failed looking for item at index: " + expectedIndex, expectedIndex, index);
                    key.bytes().releaseLast();

                    if (j > 0 && numberOfMessagesToVerify > 10 && j % (numberOfMessagesToVerify / 10) == 0) {
                        Jvm.startup().on(getClass(), "Verified " + j + " messages");
                    }
                }
                Jvm.startup().on(getClass(), "Verified " + numberOfMessagesToVerify + " messages");

                Wire key = toWire(numberOfMessages);
                long result = BinarySearch.search(binarySearchTailer, key, comparator);
                Assert.assertTrue("Should not find non-existent", result < 0);
            }
        }
    }

    private void writeEmptyCycles(ExcerptAppender appender) {
        int numberOfEmptyCycles = emptyCyclesStrategy.single() ? 1 : 2;
        for (int i = 0; i < numberOfEmptyCycles; i++) {
            try (final DocumentContext dc = appender.writingDocument()) {
                // easiest way to write an empty cycle is to start writing a document and then rollback
                dc.rollbackOnClose();
            }
            ((SetTimeProvider)appender.queue().time()).advanceMillis(appender.queue().rollCycle().lengthInMillis() + 1);
        }
    }

    @NotNull
    private Wire toWire(int key) {
        final MyData myData = new MyData();
        myData.key = key;
        myData.value = Integer.toString(key);
        Wire wire = WireType.BINARY.apply(Bytes.allocateElasticOnHeap());
        wire.usePadding(true);
        myData.writeMarshallable(wire);

        return wire;
    }

    public static class MyData extends SelfDescribingMarshallable {
        private int key;
        private String value;

        @NotNull
        @Override
        public String toString() {
            return "MyData{" +
                    "key=" + key +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    public enum RetrievalStrategy {
        LINEAR {
            @Override
            public long retrieveIndex(long currentIndex, long totalNumberOfMessages) {
                return currentIndex;
            }
        },
        RANDOM {
            final Random random = new Random(234563434L);
            @Override
            public long retrieveIndex(long currentIndex, long totalNumberOfMessages) {
                return random.nextInt((int) totalNumberOfMessages);
            }
        };

        public abstract long retrieveIndex(long currentIndex, long totalNumberOfMessages);

    }

    public enum EmptyCyclesStrategy {
        NO_EMPTY_CYCLES,
        SINGLE_EMPTY_AT_START,
        SINGLE_EMPTY_AT_END,
        MULTIPLE_EMPTY_AT_START,
        MULTIPLE_EMPTY_AT_END,
        SINGLE_EMPTY_IN_MIDDLE,
        MULTIPLE_CONCURRENTLY_EMPTY_IN_MIDDLE
        ;

        public boolean atStart() {
            return this == SINGLE_EMPTY_AT_START || this == MULTIPLE_EMPTY_AT_START;
        }

        public boolean atEnd() {
            return this == SINGLE_EMPTY_AT_END || this == MULTIPLE_EMPTY_AT_END;
        }

        public boolean inMiddle() {
            return this == SINGLE_EMPTY_IN_MIDDLE || this == MULTIPLE_CONCURRENTLY_EMPTY_IN_MIDDLE;
        }

        public boolean single() {
            return this == SINGLE_EMPTY_AT_START || this == SINGLE_EMPTY_AT_END || this == SINGLE_EMPTY_IN_MIDDLE;
        }
    }
}
