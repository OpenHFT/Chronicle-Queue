package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class LatinCharTest {

    private static class Message extends SelfDescribingMarshallable {
        String s;
        long l;

        Message(final String s, final long l) {
            this.s = s;
            this.l = l;
        }

        Message() {
        }
    }

    @Ignore("see https://github.com/OpenHFT/Chronicle-Queue/issues/744")
    @Test
    public void testFailsOnJava11() {

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(DirectoryUtils.tempDir("temp"))
                .rollCycle(RollCycles.MINUTELY)
                .build();
             ExcerptAppender appender = queue.acquireAppender();
             ExcerptTailer tailer = queue.createTailer("test-tailer")) {

            // the é character in the line below is causing it to fail under java 11
            Message expected = new Message("awésome-message-1", 1L);
            appender.writeDocument(expected);

            Message actual = new Message();
            tailer.readDocument(actual);

            Assert.assertEquals(expected, actual);
        }

    }

}
