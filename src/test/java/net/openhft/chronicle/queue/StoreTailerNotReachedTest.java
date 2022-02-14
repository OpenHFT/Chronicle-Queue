package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StoreTailerNotReachedTest extends QueueTestCommon {
    @Test
    public void afterNotReached() {
        String path = OS.getTarget() + "/afterNotReached-" + Time.uniqueId();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .build()) {
            q.acquireAppender().writeText("Hello");
            ExcerptTailer tailer = q.createTailer();
            assertEquals("Hello", tailer.readText());
            assertNull(tailer.readText());
            q.acquireAppender().writeText("World");
            assertEquals("World", tailer.readText());
            assertNull(tailer.readText());
        }
    }
}
