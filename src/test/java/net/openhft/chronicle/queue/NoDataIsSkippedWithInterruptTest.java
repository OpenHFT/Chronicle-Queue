package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

public class NoDataIsSkippedWithInterruptTest {

    private static final String EXPECTED = "Hello World";

    @Test
    public void test() {

        Thread.currentThread().interrupt();

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.single(DirectoryUtils.tempDir(".")).build();
             final ExcerptAppender excerptAppender = q.acquireAppender();
             final ExcerptTailer tailer = q.createTailer()) {

            excerptAppender.writeText(EXPECTED);
            Assert.assertEquals(EXPECTED, tailer.readText());
        }
    }

}


