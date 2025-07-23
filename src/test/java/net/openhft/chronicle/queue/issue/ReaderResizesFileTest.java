package net.openhft.chronicle.queue.issue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

import static org.junit.jupiter.api.Assertions.*;

public class ReaderResizesFileTest {
    public static final String QUEUE_NAME = OS.getTarget() + "/ReaderResizesFileTest-" + System.nanoTime();

    @After
    public void cleanup() {
        IOTools.deleteDirWithFiles(new File(QUEUE_NAME));
    }

    @Test
    public void testReaderResizesFile() throws IOException {
        int blockSize = 64 << 10;
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(QUEUE_NAME).rollCycle(TestRollCycles.TEST4_DAILY).blockSize(blockSize).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("Hello World");
            // get the first file ending in .cq4 in the queue directory
            File firstFile = new File(QUEUE_NAME).listFiles(f -> f.getName().endsWith(".cq4"))[0];
            assertEquals(blockSize * 2, firstFile.length());

            // Simulate a resize by writing more data
            try (DocumentContext dc = appender.writingDocument()) {
                Bytes<?> bytes = dc.wire().bytes();
                bytes.append("More data to increase file size");
                for (int i = 0; i < 8192; i++)
                    bytes.writeLong(i);
            }
            assertEquals(blockSize * 2, firstFile.length());

            try (RandomAccessFile raf = new RandomAccessFile(firstFile, "rw");
                 FileLock lockFile = raf.getChannel().lock()) {
                assertNotNull(lockFile);
                for (int i = 1; i <= 2; i++) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        assertTrue(dc.isPresent(), "Document should be present");
                    }
                    assertEquals(blockSize * 2, firstFile.length());
                }

                try (DocumentContext dc = tailer.readingDocument()) {
                    assertFalse(dc.isPresent(), "Document should not be present");
                }
            }
        }
    }
}
