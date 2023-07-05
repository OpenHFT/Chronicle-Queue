/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;

@RunWith(Parameterized.class)
public class TestBinarySearch extends QueueTestCommon {

    private final int numberOfMessages;

    public TestBinarySearch(int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
    }

    @Parameterized.Parameters(name = "items in queue: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {0},
                {1},
                {2},
                {100}
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

                    final int compare = Integer.compare(myDataO1.key, myDataO2.key);
                    return compare;
                } finally {
                    o1.bytes().readPosition(readPositionO1);
                    o2.bytes().readPosition(readPositionO2);
                }
            };

            try (final ExcerptTailer tailer = queue.createTailer();
                 final ExcerptTailer binarySearchTailer = queue.createTailer()) {
                for (int j = 0; j < numberOfMessages; j++) {
                    try (DocumentContext ignored = tailer.readingDocument()) {
                        Wire key = toWire(j);
                        long index = BinarySearch.search(binarySearchTailer, key, comparator);
                        Assert.assertEquals(tailer.index(), index);
                        key.bytes().releaseLast();
                    }
                }

                Wire key = toWire(numberOfMessages);
                Assert.assertTrue("Should not find non-existent", BinarySearch.search(tailer, key, comparator) < 0);
            }
        }
    }

    @NotNull
    private Wire toWire(int key) {
        final MyData myData = new MyData();
        myData.key = key;
        myData.value = Integer.toString(key);
        Wire wire = WireType.BINARY.apply(Bytes.elasticByteBuffer());
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

