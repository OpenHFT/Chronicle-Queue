package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.annotation.RequiredForClient;
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

import static org.junit.Assert.assertTrue;

@RequiredForClient
@RunWith(Parameterized.class)
public class SparseBinarySearchTest extends ChronicleQueueTestBase {

    private static final GapTolerantComparator GAP_TOLERANT_COMPARATOR = new GapTolerantComparator();

    private final int numberOfMessages;
    private final float percentageWithValues;

    public SparseBinarySearchTest(int numberOfMessages, float percentageWithValues) {
        this.numberOfMessages = numberOfMessages;
        this.percentageWithValues = percentageWithValues;
    }

    @Parameterized.Parameters(name = "items in queue: {0}, percentage with values: {1}")
    public static Collection<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        List<Integer> numbersOfMessages = Arrays.asList(0, 1, 2, 100);
        List<Float> percentagesWithValues = Arrays.asList(0.0f, 0.1f, 0.9f);

        for (int nom : numbersOfMessages) {
            for (float pwv : percentagesWithValues) {
                parameters.add(new Object[]{nom, pwv});
            }
        }
        return parameters;
    }

    @Test
    public void testBinarySearchWithManyGapsAndManyRollCycles() throws ParseException {
        runWithTimeParameters(RollCycles.TEST_SECONDLY, 300);
    }

    @Test
    public void testBinarySearchWithManyGaps() throws ParseException {
        runWithTimeParameters(RollCycles.DAILY, 1);
    }

    public void runWithTimeParameters(RollCycle rollCycle, long incrementInMillis) throws ParseException {
        final SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(0);

        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(rollCycle)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            Set<Integer> entriesWithValues = new HashSet<>();
            Random random = new Random();
            for (int i = 0; i < numberOfMessages; i++) {
                try (final DocumentContext dc = appender.writingDocument()) {
                    final MyData myData = new MyData();
                    final boolean putValueAtIndex = random.nextFloat() < percentageWithValues;
                    myData.key = putValueAtIndex ? i : -1;
                    myData.value = "some value where the key=" + i;
                    dc.wire().getValueOut().typedMarshallable(myData);
                    stp.currentTimeMillis(stp.currentTimeMillis() + incrementInMillis);
                    if (putValueAtIndex) {
                        entriesWithValues.add(i);
                    }
                }
            }

            try (final ExcerptTailer tailer = queue.createTailer()) {
                for (int j = 0; j < numberOfMessages; j++) {
                    try (DocumentContext ignored = tailer.readingDocument()) {
                        Wire key = toWire(j);
                        long index = BinarySearch.search(queue, key, GAP_TOLERANT_COMPARATOR);
                        if (entriesWithValues.contains(j)) {
                            Assert.assertEquals(tailer.index(), index);
                        } else {
                            assertTrue(index < 0);
                        }
                        key.bytes().releaseLast();
                    }
                }
            }

            Wire key = toWire(numberOfMessages);
            assertTrue("Should not find non-existent", BinarySearch.search(queue, key, GAP_TOLERANT_COMPARATOR) < 0);
        }
    }

    static class GapTolerantComparator implements Comparator<Wire> {

        @Override
        public int compare(Wire o1, Wire o2) {
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

                if (myDataO1.key >= 0 && myDataO2.key >= 0) {
                    final int compare = Integer.compare(myDataO1.key, myDataO2.key);
                    return compare;
                } else {
                    throw NotComparableException.INSTANCE;
                }
            } finally {
                o1.bytes().readPosition(readPositionO1);
                o2.bytes().readPosition(readPositionO2);
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
