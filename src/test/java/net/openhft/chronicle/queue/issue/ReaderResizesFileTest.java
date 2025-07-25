package net.openhft.chronicle.queue.issue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
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
    private static final File QUEUE_DIR = new File(OS.getTarget(), "ReaderResizesFileTest-" + System.nanoTime());

    @After
    public void cleanup() {
        IOTools.deleteDirWithFiles(QUEUE_DIR);
    }

    @Test
    public void testReaderResizesFile() throws IOException {
        // go for the smallest possible block size to ensure we can test resizing
        int blockSize = 1 << 10;
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(QUEUE_DIR).rollCycle(TestRollCycles.TEST4_DAILY).blockSize(blockSize).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("Hello World");
            // retrieve the actual block size used by the queue
            long actualChunkSize = appender.wire().bytes().bytesStore().capacity();

            File[] files = QUEUE_DIR.listFiles((d, n) -> n.endsWith(".cq4"));
            assertNotNull(files, "Queue directory must exist");
            assertEquals(1, files.length, "Expected exactly one cycle file");
            File firstFile = files[0];

            assertEquals(actualChunkSize, firstFile.length());

            // Trigger a potential resize by writing more data
            try (DocumentContext dc = appender.writingDocument()) {
                Bytes<?> bytes = dc.wire().bytes();
                bytes.append("More data to increase file size");
                for (int i = 0; i < blockSize; i += 8)
                    bytes.writeLong(i);
            }
            assertEquals(actualChunkSize, firstFile.length());

            try (RandomAccessFile raf = new RandomAccessFile(firstFile, "rw");
                 FileLock lockFile = raf.getChannel().lock()) {
                assertNotNull(lockFile);
                for (int i = 1; i <= 2; i++) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        assertTrue(dc.isPresent(), "Document should be present");
                    }
                    assertEquals(actualChunkSize, firstFile.length());
                }

                try (DocumentContext dc = tailer.readingDocument()) {
                    assertFalse(dc.isPresent(), "Document should not be present");
                }
            }
        }
    }
}
