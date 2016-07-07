package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class LastIndexAppendedTest {

    @Test
    public void testLastIndexAppendedAcrossRestarts() throws Exception {
        String path = OS.TARGET + "/deleteme.q-" + System.nanoTime();

        for (int i = 0; i < 5; i++) {
            try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(path)
                    .rollCycle(TEST_DAILY)
                    .build()) {
                ExcerptAppender appender = queue.acquireAppender();

                try (DocumentContext documentContext = appender.writingDocument()) {
                    int index = (int) documentContext.index();
                    assertEquals(i, index);

                    documentContext.wire().write().text("hello world");
                }

                assertEquals(i, (int) appender.lastIndexAppended());
            }
        }
        try {
            IOTools.deleteDirWithFiles(path, 2);
        } catch (Exception index) {
        }
    }
}