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

    private static final int MAX_NUMBER_OF_TESTED_MESSAGES = 50;

    @Test
    public void testBinarySearch() throws ParseException {

        final SetTimeProvider stp = new SetTimeProvider();
        long time = 0;
        stp.currentTimeMillis(time);

        final File tmpDir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
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

                try (final ExcerptTailer tailer = queue.createTailer()) {
                    tailer.moveToIndex(index);
                    try (final DocumentContext documentContext = tailer.readingDocument()) {
                        Assert.assertTrue(documentContext.toString().contains("some value where the key=" + j));
                    }
                }
                key.bytes().releaseLast();
            }
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

