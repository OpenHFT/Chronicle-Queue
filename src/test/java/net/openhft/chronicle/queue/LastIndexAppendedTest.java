package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Rob Austin.
 */
public class LastIndexAppendedTest {

    @Test
    public void testLastIndexAppendedAcrossRestarts() throws Exception {
        String path = OS.TARGET + "/deleteme.q-" + System.nanoTime();

        long expectedIndex = 0;
        for (int i = 0; i < 5; i++) {
            try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(path).build()) {
                ExcerptAppender appender = queue.createAppender();

                try (DocumentContext documentContext = appender.writingDocument()) {
                    long expectedIndex2 = documentContext.index();
                    assertTrue("expectedIndex: " + expectedIndex2, expectedIndex2 > 0);
                    if (expectedIndex == 0)
                        expectedIndex = expectedIndex2;
                    else
                        assertEquals(++expectedIndex, expectedIndex2);

                    documentContext.wire().write().text("hello world");
                }
                assertEquals(expectedIndex, queue.lastIndex());

                assertEquals(expectedIndex, appender.lastIndexAppended());
            }
        }
        try {
            IOTools.deleteDirWithFiles(path, 2);
        } catch (Exception index) {
        }
    }
}