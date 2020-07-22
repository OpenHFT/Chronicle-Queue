package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MarshallableTest extends ChronicleQueueTestBase {
    @Test
    public void testWriteText() {
        File dir = getTmpDir();
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            ExcerptTailer tailer = queue.createTailer();
            ExcerptTailer tailer2 = queue.createTailer();

            int runs = 1000;
            for (int i = 0; i < runs; i++)
                appender.writeText("" + i);
            for (int i = 0; i < runs; i++)
                assertEquals("" + i, tailer.readText());
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < runs; i++) {
                assertTrue(tailer2.readText(sb));
                assertEquals("" + i, sb.toString());
            }
        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }

}
