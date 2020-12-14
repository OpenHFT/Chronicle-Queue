package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.text.ParseException;
import java.util.Comparator;

public class TestBinarySearch extends ChronicleQueueTestBase {

    @Test
    public void testBinarySearch() throws ParseException {
        test(50);
    }

    @Test
    public void testBinarySearchOne() throws ParseException {
        test(1);
    }

    private void test(int numberOfMessages) throws ParseException {
        final SetTimeProvider stp = new SetTimeProvider();
        long time = 0;
        stp.currentTimeMillis(time);

        final File tmpDir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            for (int i = 0; i < numberOfMessages; i++) {

                try (final DocumentContext dc = appender.writingDocument()) {

                    final MyData myData = new MyData();
                    myData.key = i;
                    myData.value = "some value where the key=" + String.valueOf(i);
                    dc.wire().getValueOut().typedMarshallable(myData);
                    time += 300;
                    stp.currentTimeMillis(time);
                }
            }
                // System.out.println(queue.dump());

            final Comparator<Wire> comparator = (o1, o2) -> {

                final long readPositionO1 = o1.bytes().readPosition();
                final long readPositionO2 = o2.bytes().readPosition();
                try {
                    MyData myDataO1;
                    MyData myDataO2;

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

            try (final ExcerptTailer tailer = queue.createTailer()) {
                for (int j = 0; j < numberOfMessages; j++) {

                    Wire key = toWire(j);

                    long index = BinarySearch.search(queue, key, comparator);
                      // assert index != -1 : "i=" + j;

                    tailer.moveToIndex(index);
                    try (final DocumentContext documentContext = tailer.readingDocument()) {
                        Assert.assertTrue(documentContext.toString().contains("some value where the key=" + j));
                    }
                    key.bytes().releaseLast();
                }
            }

            Wire key = toWire(numberOfMessages);
            Assert.assertTrue("Should not find non-existent", BinarySearch.search(queue, key, comparator) < 0);
        } finally {
            System.gc();
            IOTools.deleteDirWithFiles(tmpDir);
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

