package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LastAcknowledgedTest {
    @Test
    public void testLastAcknowledge() {
        String name = OS.getTarget() + "/testLastAcknowledge-" + System.nanoTime();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(name).testBlockSize().build()) {
            q.acquireAppender().writeText("Hello World");
        }
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(name).testBlockSize().build()) {
            assertEquals(-1, q.lastAcknowledgedIndexReplicated());
        }
    }
}
