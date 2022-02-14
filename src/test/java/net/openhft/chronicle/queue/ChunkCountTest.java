package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChunkCountTest extends QueueTestCommon {
    @Test
    public void chunks() {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(IOTools.createTempFile("chunks")).blockSize(64 << 10).rollCycle(RollCycles.DAILY);
        try (SingleChronicleQueue queue = builder.build();
             ExcerptAppender appender = queue.acquireAppender()) {
            assertEquals(0, queue.chunkCount());
            appender.writeText("Hello");
            assertEquals(1, queue.chunkCount());

            for (int i = 0; i < 100; i++) {
                long pos;
                try (DocumentContext dc = appender.writingDocument()) {
                    pos = dc.wire().bytes().writePosition();
                    dc.wire().bytes().writeSkip(16000);
                }
                final long expected = builder.useSparseFiles() ? 1 : 1 + (pos >> 18);

                assertEquals("i: " + i, expected, queue.chunkCount());
            }
        }
    }
}
