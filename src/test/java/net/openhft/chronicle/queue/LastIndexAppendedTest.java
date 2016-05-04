package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import static net.openhft.chronicle.queue.ChronicleQueueTestBase.getTmpDir;

/**
 * @author Rob Austin.
 */
public class LastIndexAppendedTest {

    @Test
    public void testLastIndexAppendedAcrossRestarts() throws Exception {

        long expectedIndex;

        String path = getTmpDir() + "/" + System.nanoTime() + "/deleteme.q";

        {
            SingleChronicleQueueBuilder builder = ChronicleQueueBuilder.single(path);
            SingleChronicleQueue queue = builder.build();

            ExcerptAppender appender = queue.createAppender();

            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write().text("hello world");
//                expectedIndex = documentContext.index();
            }
            expectedIndex = appender.lastIndexAppended();

        }


        {
            SingleChronicleQueueBuilder builder = ChronicleQueueBuilder.single(path);
            SingleChronicleQueue queue = builder.build();

            Assert.assertEquals(expectedIndex, queue.lastIndex());

        }

    }

}
