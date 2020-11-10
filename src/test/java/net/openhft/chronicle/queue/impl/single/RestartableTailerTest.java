package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RestartableTailerTest extends QueueTestCommon {
    @Test
    public void restartable() {
        String tmp = OS.getTarget() + "/restartable-" + Time.uniqueId();
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build()) {
            for (int i = 0; i < 7; i++)
                cq.acquireAppender().writeText("test " + i);
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 0", atailer.readText());
            assertEquals("test 1", atailer.readText());
            assertEquals("test 2", atailer.readText());

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 0", btailer.readText());
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 3", atailer.readText());
            assertEquals("test 4", atailer.readText());
            assertEquals("test 5", atailer.readText());

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 1", btailer.readText());
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 6", atailer.readText());
            assertNull(atailer.readText());

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 2", btailer.readText());
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertNull(atailer.readText());

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 3", btailer.readText());
            }
        }
    }
}
