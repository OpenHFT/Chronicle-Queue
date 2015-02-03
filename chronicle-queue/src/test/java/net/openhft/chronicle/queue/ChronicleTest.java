package net.openhft.chronicle.queue;

import org.junit.Test;

import java.io.File;

public class ChronicleTest {

    @Test
    public void testCreateAppender() throws Exception {
        String name = "single.q";
        new File(name).deleteOnExit();
        Chronicle chronicle = new ChronicleQueueBuilder(name).build();
        ExcerptAppender appender = chronicle.createAppender();
        appender.writeDocument(wire -> wire.write().text("Hello World"));
    }
}