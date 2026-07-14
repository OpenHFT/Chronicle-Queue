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
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BinarySearchEmptyRollCycleRegressionTest extends QueueTestCommon {

    @Test
    public void shouldFindMessageWrittenAfterConsecutiveEmptyRollCycles() {
        final int keyAfterEmptyCycles = 12;
        final Map<Integer, Long> keyToIndex = new HashMap<>();
        final SetTimeProvider time = new SetTimeProvider();
        time.currentTimeMillis(0);

        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(TestRollCycles.TEST_SECONDLY)
                .timeProvider(time)
                .build();
             ExcerptAppender appender = queue.createAppender()) {

            for (int key = 0; key < 10; key++) {
                keyToIndex.put(key, write(appender, time, key));
            }

            writeEmptyCycle(appender, time);
            writeEmptyCycle(appender, time);

            for (int key = 10; key < 30; key++) {
                keyToIndex.put(key, write(appender, time, key));
            }

            final Comparator<Wire> comparator = new MyDataComparator();

            try (ExcerptTailer tailer = queue.createTailer()) {
                final Wire keyWire = toWire(keyAfterEmptyCycles);
                try {
                    final long foundIndex = BinarySearch.search(tailer, keyWire, comparator);
                    assertEquals(keyToIndex.get(keyAfterEmptyCycles).longValue(), foundIndex);
                } finally {
                    keyWire.bytes().releaseLast();
                }
            }
        }
    }

    private static long write(ExcerptAppender appender, SetTimeProvider time, int key) {
        try (DocumentContext dc = appender.writingDocument()) {
            final MyData data = new MyData();
            data.key = key;
            data.value = "key-" + key;
            data.writeMarshallable(dc.wire());
            final long index = dc.index();
            time.advanceMillis(300);
            return index;
        }
    }

    private static void writeEmptyCycle(ExcerptAppender appender, SetTimeProvider time) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.rollbackOnClose();
        }
        time.advanceMillis(appender.queue().rollCycle().lengthInMillis() + 1);
    }

    private static Wire toWire(int key) {
        final MyData data = new MyData();
        data.key = key;
        data.value = "key-" + key;
        final Wire wire = WireType.BINARY.apply(Bytes.allocateElasticOnHeap());
        wire.usePadding(true);
        data.writeMarshallable(wire);
        return wire;
    }

    private static final class MyDataComparator implements Comparator<Wire> {
        private final MyData data = new MyData();

        @Override
        public int compare(Wire candidate, Wire key) {
            data.readMarshallable(candidate);
            final int candidateKey = data.key;
            data.readMarshallable(key);
            return Integer.compare(candidateKey, data.key);
        }
    }

    public static class MyData extends SelfDescribingMarshallable {
        private int key;
        private String value;
    }
}
