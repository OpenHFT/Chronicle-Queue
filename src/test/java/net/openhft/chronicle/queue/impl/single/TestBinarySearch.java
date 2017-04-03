package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.text.ParseException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * @author Rob Austin.
 */
public class TestBinarySearch extends ChronicleQueueTestBase {

    private static final int MAX_NUMBER_OF_TESTED_MESSAGES = 50;

    @Test
    public void testBinarySearch() throws ParseException {

        final SetTimeProvider stp = new SetTimeProvider();
        long time = 0;
        stp.currentTimeMillis(time);

        final File tmpDir = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            for (int i = 0; i < MAX_NUMBER_OF_TESTED_MESSAGES; i++) {

                try (final DocumentContext dc = appender.writingDocument()) {

                    final MyData myData = new MyData();
                    myData.key = i;
                    myData.value = "some value where the key=" + String.valueOf(i);
                    dc.wire().getValueOut().typedMarshallable(myData);
                    time += 300;
                    stp.currentTimeMillis(time);
                }

            }
            //     System.out.println(queue.dump());

            for (int j = 0; j < MAX_NUMBER_OF_TESTED_MESSAGES; j++) {

                Wire key = toWire(j);

                final Comparator<Wire> comparator = (o1, o2) -> {

                    final long readPositionO1 = o1.bytes().readPosition();
                    final long readPositionO2 = o2.bytes().readPosition();
                    try {
                        MyData myDataO1 = null;
                        MyData myDataO2 = null;

                        try (final DocumentContext dc = o1.readingDocument()) {
                            myDataO1 = dc.wire().getValueIn().typedMarshallable();
                            assert myDataO1.value != null;
                        }


                        try (final DocumentContext dc = o2.readingDocument()) {
                            myDataO2 = dc.wire().getValueIn().typedMarshallable();
                            assert myDataO2.value != null;
                        }


                        final int compare = Integer.compare(myDataO1.key, myDataO2.key);

                        return compare;

                    } finally {
                        o1.bytes().readPosition(readPositionO1);
                        o2.bytes().readPosition(readPositionO2);
                    }
                };


                long index = BinarySearch.search(queue, key, comparator);
                //   assert index != -1 : "i=" + j;

                final ExcerptTailer tailer = queue.createTailer();
                tailer.moveToIndex(index);
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    Assert.assertTrue(documentContext.toString().contains("some value where the key=" + j));
                }
            }
        } finally {
            deleteDir(tmpDir);
        }

    }

    @NotNull
    private Wire toWire(int key) {
        final MyData myData = new MyData();
        myData.key = key;
        myData.value = Integer.toString(key);
        Wire result = WireType.BINARY.apply(Bytes.elasticByteBuffer());

        try (final DocumentContext dc = result.writingDocument()) {
            dc.wire().getValueOut().typedMarshallable(myData);
        }

        return result;
    }

    /**
     * @author Rob Austin.
     */
    enum BinarySearch {
        INSTANCE;

        /**
         * returns the index or -1 if not found
         */
        public static long search(@NotNull SingleChronicleQueue q,
                                  @NotNull Wire key,
                                  @NotNull Comparator<Wire> c) throws ParseException {
            final long readPosition = key.bytes().readPosition();
            try {
                final ExcerptTailer tailer = q.createTailer();
                final long start = tailer.toStart().index();
                final long end = tailer.toEnd().index();

                final RollCycle rollCycle = q.rollCycle();
                final int startCycle = rollCycle.toCycle(start);
                final int endCycle = rollCycle.toCycle(end);

                if (startCycle == endCycle)
                    return findWithinCycle(key, c, startCycle, tailer, q, rollCycle);

                final NavigableSet<Long> cycles = q.listCyclesBetween(startCycle, endCycle);
                final int cycle = (int) findCycleLinearSearch(cycles, key, c, tailer, q);

                if (cycle == -1)
                    return -1;
                return findWithinCycle(key, c, cycle, tailer, q, rollCycle);
            } finally {
                key.bytes().readPosition(readPosition);
            }

        }

        private static long findCycleLinearSearch(NavigableSet<Long> cycles, Wire key,
                                                  Comparator<Wire> c,
                                                  ExcerptTailer tailer,
                                                  final SingleChronicleQueue queue) {

            final Iterator<Long> iterator = cycles.iterator();
            if (!iterator.hasNext())
                return -1;
            final RollCycle rollCycle = queue.rollCycle();
            long prevIndex = iterator.next();

            while (iterator.hasNext()) {

                final Long current = iterator.next();

                final boolean b = tailer.moveToIndex(rollCycle.toIndex((int) (long) current, 0));
                if (!b)
                    return prevIndex;

                try (final DocumentContext dc = tailer.readingDocument()) {
                    final int compare = c.compare(dc.wire(), key);
                    if (compare == 0)
                        return current;
                    else if (compare == 1)
                        return prevIndex;
                    prevIndex = current;
                }
            }

            return prevIndex;
        }


        private static long findWithinCycle(Wire key,
                                            Comparator<Wire> c,
                                            int cycle,
                                            ExcerptTailer tailer,
                                            SingleChronicleQueue q,
                                            final RollCycle rollCycle) {
            final long readPosition = key.bytes().readPosition();
            try {
                long lowSeqNum = 0;

                long highSeqNum = q.exceptsPerCycle(cycle) - 1;
                if (highSeqNum == 0)
                    return rollCycle.toIndex(cycle, 0);

                while (lowSeqNum <= highSeqNum) {
                    long midSeqNumber = (lowSeqNum + highSeqNum) >>> 1L;

                    final long midIndex = rollCycle.toIndex(cycle, midSeqNumber);
                    try (DocumentContext dc = moveTo(midIndex, tailer)) {
                        if (!dc.isPresent())
                            return -1;
                        key.bytes().readPosition(readPosition);
                        int cmp = c.compare(dc.wire(), key);

                        if (cmp < 0)
                            lowSeqNum = midSeqNumber + 1;
                        else if (cmp > 0)
                            highSeqNum = midSeqNumber - 1;
                        else
                            return midIndex; // key found
                    }

                }
                return -1;  // key not found
            } finally {
                key.bytes().readPosition(readPosition);
            }
        }

        /**
         * Gets the ith element from the given list by repositioning the specified
         * list listIterator.
         */
        private static DocumentContext moveTo(long index, final ExcerptTailer tailer) {
            final boolean b = tailer.moveToIndex(index);
            assert b;
            return tailer.readingDocument();
        }

    }

    public static class MyData extends AbstractMarshallable {
        private int key;
        private String value;

        @Override
        public String toString() {
            return "MyData{" +
                    "key=" + key +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}

