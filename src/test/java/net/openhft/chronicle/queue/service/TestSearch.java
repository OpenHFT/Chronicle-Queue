package net.openhft.chronicle.queue.service;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.text.ParseException;
import java.util.Comparator;

/**
 * @author Rob Austin.
 */
public class TestSearch extends ChronicleQueueTestBase {


    public static final int MAX_SIZE = 6;


    @Test
    public void test() throws ParseException {

        final SetTimeProvider stp = new SetTimeProvider();
        long time = 0;
        stp.currentTimeMillis(time);

        final File tmpDir = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build()) {


            final ExcerptAppender appender = queue.acquireAppender();


            for (int i = 0; i < MAX_SIZE; i++) {

                try (final DocumentContext dc = appender.writingDocument()) {

                    final MyData myData = new MyData();
                    myData.key = i;
                    myData.value = "some value where the key=" + String.valueOf(i);
                    dc.wire().getValueOut().typedMarshallable(myData);
                    time += 300;
                    stp.currentTimeMillis(time);
                }

            }
            System.out.println(queue.dump());

            for (int j = 0; j < MAX_SIZE; j++) {
                System.out.println("j=" + j);
                int i = j;
                Wire key = toWire(i);

                final Comparator<Wire> comparator = (o1, o2) -> {

                    MyData myDataO1 = new MyData();
                    MyData myDataO2 = new MyData();

                    try (final DocumentContext dc = o1.readingDocument()) {
                        myDataO1 = dc.wire().getValueIn().typedMarshallable();
                        assert myDataO1.value != null;
                        System.out.println("Comparator - low=" + myDataO1);
                    }


                    try (final DocumentContext dc = o2.readingDocument()) {
                        myDataO2 = dc.wire().getValueIn().typedMarshallable();
                        if (myDataO2.value == null)
                            System.out.println("");
                        assert myDataO2.value != null;
                        System.out.println("Comparator - high=" + myDataO2);
                    }


                    final int compare = Integer.compare(myDataO1.key, myDataO2.key);
                    return compare;
                };

                if (i == 3)
                    System.out.println("");
                long index = BinarySearch.INSTANCE.search(queue, key, comparator);
                if (index == -1) {
                    System.out.println("fault=" + i);
                    index = BinarySearch.INSTANCE.search(queue, key, comparator);
                }
                assert index != -1 : "i=" + i;

                final ExcerptTailer tailer = queue.createTailer();
                tailer.moveToIndex(index);
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    Assert.assertTrue(documentContext.toString().contains("some value where the key=" + i));
                }
            }
        } finally

        {
            deleteDir(tmpDir);
        }

    }

    @NotNull
    private Wire toWire(int key) {
        final MyData myData = new MyData();
        myData.key = key;
        myData.value = "unknown";

        Wire result = WireType.BINARY.apply(Bytes.elasticByteBuffer());

        try (final DocumentContext dc = result.writingDocument()) {
            dc.wire().getValueOut().typedMarshallable(myData);
        }

        return result;
    }

    public static class MyData extends AbstractMarshallable {
        int key;
        String value;

        @Override
        public String toString() {
            return "MyData{" +
                    "key=" + key +
                    ", value='" + value + '\'' +
                    '}';
        }

        public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
            key = wire.read("key").int32();
            value = wire.read("value").text();
        }

        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write("key").int32(key);
            wire.write("value").text(value);
        }
    }
}
