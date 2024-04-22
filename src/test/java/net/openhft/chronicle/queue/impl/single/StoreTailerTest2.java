package net.openhft.chronicle.queue.impl.single;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.queue.impl.single.StoreTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class StoreTailerTest2 {

    private ChronicleQueue queue;
    private ExcerptTailer tailer;

    @Before
    public void setUp() {
        // Set up a Chronicle Queue and a StoreTailer for testing
        queue = ChronicleQueue.single("test-queue");
        tailer =queue.createTailer();
    }

    @Test
    public void testMoveToIndex() {
        // Move to a specific index and assert it was successful
        boolean moved = tailer.moveToIndex(1);
        assertTrue("Failed to move to index 1", moved);
    }

    @Test
    public void testReadingDocument() {
        // Test reading a document after writing to the queue
        ExcerptAppender appender = queue.acquireAppender();
        appender.writeText("Test Message");

        DocumentContext context = tailer.readingDocument();
        assertTrue("No document was found", context.isPresent());
        assertEquals("Test Message", context.wire().readText());
    }

    @Test
    public void testAcknowledgeCheck() {
        // Test the acknowledgment check logic
        boolean repCheck = tailer.readAfterReplicaAcknowledged();
        assertFalse("Default acknowledgment check should be false", repCheck);

        tailer.readAfterReplicaAcknowledged(true);
        assertTrue("Acknowledgment check should be true after setting", tailer.readAfterReplicaAcknowledged());
    }

    @Test
    public void testCycleHandling() {
        // Test the handling of cycles in the queue
        boolean moved = tailer.moveToCycle(0);
        assertTrue("Failed to move to cycle 0", moved);
        assertEquals("Expected cycle 0", 0, tailer.cycle());
    }

    @Test
    public void testDirectionChange() {
        // Test changing the direction of the tailer
        tailer.direction(TailerDirection.BACKWARD);
        assertEquals("Expected backward direction", TailerDirection.BACKWARD, tailer.direction());
    }
}
