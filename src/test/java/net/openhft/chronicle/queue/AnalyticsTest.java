package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;

public class AnalyticsTest {

    @Test
    public void test() {
        final String fileName = "q";

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(fileName).build()) {

        } finally {
            new File(fileName).delete();
        }
    }

}
