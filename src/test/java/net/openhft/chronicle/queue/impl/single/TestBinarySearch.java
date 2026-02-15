/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.text.ParseException;
import java.util.*;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.*;

@RunWith(Parameterized.class)
public class TestBinarySearch extends QueueTestCommon {

    private final Map<Integer, Long> keyToIndex = new HashMap<>();
    private final int numberOfMessages;
    private final int numberOfMessagesToVerify;
    private final RetrievalStrategy retrievalStrategy;

    public TestBinarySearch(int numberOfMessages, int numberOfMessagesToVerify) {
        this.numberOfMessages = numberOfMessages;
        this.numberOfMessagesToVerify = numberOfMessagesToVerify;
        this.retrievalStrategy = numberOfMessages == numberOfMessagesToVerify ? RetrievalStrategy.LINEAR : RetrievalStrategy.RANDOM;
    }

    @Parameterized.Parameters(name = "items in queue: {0} items to verify: {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {0, 0},

                {1, 1},

                {2, 2},

                {100, 100},

                {1000, 100},

                {100000, 500},
        });
    }


    @Test
    public void testBinarySearch() throws ParseException {
        final SetTimeProvider stp = new SetTimeProvider();
        long time = 0;
        stp.currentTimeMillis(time);

        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(TEST_SECONDLY)
                .timeProvider(stp)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            for (int i = 0; i < numberOfMessages; i++) {
                try (final DocumentContext dc = appender.writingDocument()) {
                    final MyData myData = new MyData();
                    myData.key = i;
                    myData.value = "some value where the key=" + i;
                    myData.writeMarshallable(dc.wire());
                    time += 300;
                    stp.currentTimeMillis(time);
                    keyToIndex.put(myData.key, dc.index());
                }

                if (i > 0 && numberOfMessages > 10 && i % (numberOfMessages / 10) == 0) {
                    System.out.println("Written " + i + " messages");
                }

            }
            System.out.println("Written " + numberOfMessages + " messages");

            MyData reusableComparatorData = new MyData();
            final Comparator<Wire> comparator = (o1, o2) -> {
                reusableComparatorData.readMarshallable(o1);
                int o1Key = reusableComparatorData.key;
                reusableComparatorData.readMarshallable(o2);
                int o2Key = reusableComparatorData.key;
                return Integer.compare(o1Key, o2Key);
            };

            try (final ExcerptTailer tailer = queue.createTailer();
                 final ExcerptTailer binarySearchTailer = queue.createTailer()) {
                for (int j = 0; j < numberOfMessagesToVerify; j++) {
                    int indexToVerify = (int) retrievalStrategy.retrieveIndex(j, numberOfMessages);
                    Wire key = toWire(indexToVerify);
                    long index = BinarySearch.search(binarySearchTailer, key, comparator);
                    long expectedIndex = keyToIndex.get(indexToVerify);
                    Assert.assertEquals("Failed looking for item at index: " + expectedIndex, expectedIndex, index);
                    key.bytes().releaseLast();

                    if (j > 0 && numberOfMessagesToVerify > 10 && j % (numberOfMessagesToVerify / 10) == 0) {
                        System.out.println("Verified " + j + " messages");
                    }
                }
                System.out.println("Verified " + numberOfMessagesToVerify + " messages");

                Wire key = toWire(numberOfMessages);
                long result = BinarySearch.search(tailer, key, comparator);
                Assert.assertTrue("Should not find non-existent", result < 0);
            }
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
            final Random random = new Random();
            @Override
            public long retrieveIndex(long currentIndex, long totalNumberOfMessages) {
                return random.nextInt((int) totalNumberOfMessages);
            }
        };

        public abstract long retrieveIndex(long currentIndex, long totalNumberOfMessages);
    }
}
