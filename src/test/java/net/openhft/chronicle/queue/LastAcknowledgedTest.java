package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RequiredForClient
public class LastAcknowledgedTest {
    @Test
    public void testLastAcknowledge() {
        String name = OS.getTarget() + "/testLastAcknowledge-" + System.nanoTime();
        long lastIndexAppended;
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(name).testBlockSize().build()) {
            ExcerptAppender excerptAppender = q.acquireAppender();
            excerptAppender.writeText("Hello World");
            lastIndexAppended = excerptAppender.lastIndexAppended();

            ExcerptTailer tailer = q.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isData());
                assertEquals(lastIndexAppended, tailer.index());
            }

            ExcerptTailer tailer2 = q.createTailer();
            tailer2.readAfterReplicaAcknowledged(true);
            try (DocumentContext dc = tailer2.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(name).testBlockSize().build()) {
            assertEquals(-1, q.lastAcknowledgedIndexReplicated());

            q.lastAcknowledgedIndexReplicated(lastIndexAppended - 1);

            ExcerptTailer tailer2 = q.createTailer();
            tailer2.readAfterReplicaAcknowledged(true);
            try (DocumentContext dc = tailer2.readingDocument()) {
                assertFalse(dc.isPresent());
            }

            q.lastAcknowledgedIndexReplicated(lastIndexAppended);

            try (DocumentContext dc = tailer2.readingDocument()) {
                assertTrue(dc.isData());
                assertEquals(lastIndexAppended, tailer2.index());
            }
        }
    }
}
