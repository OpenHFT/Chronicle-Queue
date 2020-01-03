
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.InternalAppender;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ChronicleQueueIndexTest {

    @Test
    public void indexQueueTest() {

        ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path("test-chronicle")
                .rollCycle(RollCycles.DAILY)
                .build();
        InternalAppender appender = (InternalAppender) queue.acquireAppender();

        Bytes<byte[]> hello_world = Bytes.fromString("Hello World 1");
        appender.writeBytes(RollCycles.DAILY.toIndex(18264, 0L), hello_world);
        hello_world = Bytes.fromString("Hello World 2");
        appender.writeBytes(RollCycles.DAILY.toIndex(18264, 1L), hello_world);

        // Simulate the end of the day i.e the queue closes the day rolls
        // (note the change of index from 18264 to 18265)
        queue.close();
        queue = SingleChronicleQueueBuilder.builder()
                .path("test-chronicle")
                .rollCycle(RollCycles.DAILY)
                .build();
        appender = (InternalAppender) queue.acquireAppender();

        // add a message for the new day
        hello_world = Bytes.fromString("Hello World 3");
        appender.writeBytes(RollCycles.DAILY.toIndex(18265, 0L), hello_world);

        final ExcerptTailer tailer = queue.createTailer();

        final Bytes forRead = Bytes.elasticByteBuffer();
        final List<String> results = new ArrayList<>();
        while (tailer.readBytes(forRead)) {
            results.add(forRead.toString());
            forRead.clear();
        }
        Assert.assertTrue(results.contains("Hello World 1"));
        Assert.assertTrue(results.contains("Hello World 2"));
        // The reader fails to read the third message. The reason for this is
        // that there was no EOF marker placed at end of the 18264 indexed file
        // so when the reader started reading through the queues it got stuck on
        // that file and never progressed to the latest queue file.
        Assert.assertTrue(results.contains("Hello World 3"));
    }
}