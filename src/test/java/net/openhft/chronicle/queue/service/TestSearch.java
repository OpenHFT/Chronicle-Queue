package net.openhft.chronicle.queue.service;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.BinarySearch;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.Comparator;

/**
 * @author Rob Austin.
 */
public class TestSearch extends ChronicleQueueTestBase {


    final MyData myDataO1 = new MyData();
    final MyData myDataO2 = new MyData();

    @Test
    public void test() throws ParseException {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder
                .fieldlessBinary(getTmpDir())
                .blockSize(128 << 20)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            final MyData myData = new MyData();
            for (int i = 0; i < 100; i++) {
                myData.key = i;
                myData.value = "some value where the key=" + String.valueOf(i);
                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().getValueOut().marshallable(myData);
                }

            }


            System.out.println(queue.dump());

            Wire key = toWire(10);

            final Comparator<Wire> comparator = (o1, o2) -> {
                try (final DocumentContext dc = o1.readingDocument()) {
                    dc.wire().getValueIn().marshallable(myDataO1);
                }


                try (final DocumentContext dc = o2.readingDocument()) {
                    dc.wire().getValueIn().marshallable(myDataO2);
                }

                final int compare = Integer.compare(myDataO1.key, myDataO2.key);
                return compare;
            };


            final long index = BinarySearch.INSTANCE.search(queue, key, comparator);
            assert index != -1;

            final ExcerptTailer tailer = queue.createTailer();
            tailer.moveToIndex(index);
            try (final DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertTrue(documentContext.toString().contains("some value where the key=10"));
            }


        }
    }

    @NotNull
    private Wire toWire(int key) {
        final MyData myData = new MyData();
        myData.key = key;
        myData.value = null;

        Wire result = WireType.BINARY.apply(Bytes.elasticByteBuffer());

        try (final DocumentContext dc = result.writingDocument()) {
            dc.wire().getValueOut().marshallable(myData);
        }

        return result;
    }

    class MyData extends AbstractMarshallable implements KeyedMarshallable {
        int key;
        String value;
    }
}
