package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.DirectChronicleQueue;
import net.openhft.chronicle.wire.WireKey;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * @author Rob Austin.
 */
public class SimpleChronicleQueueTest {

    enum Field implements WireKey {
        TEXT
    }

    @Test
    public void testName() throws Exception {

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


}
