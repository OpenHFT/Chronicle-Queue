package net.openhft.chronicle.queue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by peter on 15/03/16.
 */
public class ReadmeTest {
    @Test
    public void createAQueue() {
        try (ChronicleQueue queue = ChronicleQueueBuilder.single("queue-dir").build()) {
            // Obtain an ExcerptAppender
            ExcerptAppender appender = queue.createAppender();

            // write - {msg: TestMessage}
            appender.writeDocument(w -> w.write(() -> "msg").text("TestMessage"));

            // write - TestMessage
            appender.writeText("TestMessage");

            ExcerptTailer tailer = queue.createTailer();

            tailer.readDocument(w -> System.out.println("msg: " + w.read(() -> "msg").text()));

            assertEquals("TestMessage", tailer.readText());
        }
    }
}
