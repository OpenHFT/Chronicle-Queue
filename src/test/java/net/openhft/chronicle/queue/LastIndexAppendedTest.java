package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import static java.lang.System.getProperty;

/**
 * @author Rob Austin.
 */
public class LastIndexAppendedTest {

    @Test
    public void testLastIndexAppendedAcrossRestarts() throws Exception {

        long expectedIndex;
        String path = getProperty("java.io.tmpdir");
        {
            SingleChronicleQueueBuilder builder = ChronicleQueueBuilder.single(path);
            SingleChronicleQueue queue = builder.build();

            ExcerptAppender appender = queue.createAppender();

            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write().text("hello world");
                expectedIndex = documentContext.index();
            }
        }

        {
            SingleChronicleQueueBuilder builder = ChronicleQueueBuilder.single(path);
            SingleChronicleQueue queue = builder.build();

            ExcerptAppender appender = queue.createAppender();
            Assert.assertEquals(expectedIndex, appender.lastIndexAppended());

        }

    }

}
