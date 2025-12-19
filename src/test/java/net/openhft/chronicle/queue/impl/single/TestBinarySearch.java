/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Comparator;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBinarySearch extends QueueTestCommon {

    @ParameterizedTest(name = "items in queue: {0}")
    @ValueSource(ints = {0, 1, 2, 100})
    public void testBinarySearch(int numberOfMessages) {
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
                    dc.wire().getValueOut().typedMarshallable(myData);
                    time += 300;
                    stp.currentTimeMillis(time);
                }
            }

            final Comparator<Wire> comparator = (o1, o2) -> {
                final long readPositionO1 = o1.bytes().readPosition();
                final long readPositionO2 = o2.bytes().readPosition();
                try {
                    final MyData myDataO1;
                    try (final DocumentContext dc = o1.readingDocument()) {
                        myDataO1 = dc.wire().getValueIn().typedMarshallable();
                    }

                    final MyData myDataO2;
                    try (final DocumentContext dc = o2.readingDocument()) {
                        myDataO2 = dc.wire().getValueIn().typedMarshallable();
                    }

                    return Integer.compare(myDataO1.key, myDataO2.key);
                } finally {
                    o1.bytes().readPosition(readPositionO1);
                    o2.bytes().readPosition(readPositionO2);
                }
            };

            try (final ExcerptTailer tailer = queue.createTailer();
                 final ExcerptTailer binarySearchTailer = queue.createTailer()) {
                for (int j = 0; j < numberOfMessages; j++) {
                    try (DocumentContext ignored = tailer.readingDocument()) {
                        assertNotNull(ignored, "tailer reading document context");
                        Wire key = toWire(j);
                        long index = BinarySearch.search(binarySearchTailer, key, comparator);
                        assertEquals(tailer.index(), index, "binary search: index at j=" + j);
                        key.bytes().releaseLast();
                    }
                }

                Wire key = toWire(numberOfMessages);
                assertTrue(BinarySearch.search(tailer, key, comparator) < 0, "binary search: should not find non-existent");
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

        try (final DocumentContext dc = wire.writingDocument()) {
            dc.wire().getValueOut().typedMarshallable(myData);
        }

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
}
