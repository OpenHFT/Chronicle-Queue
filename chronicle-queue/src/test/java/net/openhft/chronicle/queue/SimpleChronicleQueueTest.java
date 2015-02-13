package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.DirectChronicleQueue;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.ObjectSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class SimpleChronicleQueueTest {

    enum Field implements WireKey {
        TEXT
    }

    @Test
    public void testSimpleWire() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getName()).build();

            final ExcerptAppender appender = chronicle.createAppender();
            appender.writeDocument(wire -> wire.write(() -> "FirstName").text("Steve"));
            appender.writeDocument(wire -> wire.write(() -> "Surname").text("Jobs"));

            StringBuilder first = new StringBuilder();
            StringBuilder surname = new StringBuilder();

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.readDocument(wire -> wire.read(() -> "FirstName").text(first));
            tailer.readDocument(wire -> wire.read(() -> "Surname").text(surname));

            Assert.assertEquals("Steve Jobs", first + " " + surname);
        } finally {
            file.delete();
        }

    }

    @Test
    public void testSimpleDirect() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();


        DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getName()).build();

        final ExcerptAppender appender = chronicle.createAppender();

        // create 100 documents
        for (int i = 0; i < 100; i++) {
            final int j = i;
            appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
        }

        StringBuilder first = new StringBuilder();
        StringBuilder surname = new StringBuilder();

        final ExcerptTailer tailer = chronicle.createTailer();
        final Bytes toRead = DirectStore.allocate(512).bytes();
        ObjectSerializer objectSerializer = toRead.objectSerializer();
        System.out.println(objectSerializer);


        Bytes bytes = chronicle.bytes();

        for (int i = 0; i < chronicle.lastIndex(); i++) {


            //  System.out.println(AbstractBytes.toHex(toRead.flip()));
            tailer.readDocument(new Function<WireIn, Object>() {
                @Override
                public Object apply(WireIn wireIn) {
                    Bytes bytes = wireIn.bytes();
                    long remaining = bytes.remaining();
                    System.out.println(bytes.position());
                    bytes.skip(remaining);
                    System.out.println(bytes.position());
                    return null;
                }
            });
        }


    }

    @Test
    public void testReadAtIndex() throws Exception {

        File file = File.createTempFile("chronicle.", "q");
        file.deleteOnExit();
        try {

            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(file.getName()).build();

            final ExcerptAppender appender = chronicle.createAppender();

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write(() -> "key").text("value=" + j));
            }

            final ExcerptTailer tailer = chronicle.createTailer();
            tailer.index(5);

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read(() -> "key").text(sb));

            Assert.assertEquals("value=5", sb.toString());

        } finally {
            file.delete();
        }

    }

}