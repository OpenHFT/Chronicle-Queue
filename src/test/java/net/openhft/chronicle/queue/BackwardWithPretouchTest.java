package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by Rob Austin
 */
public class BackwardWithPretouchTest {

    @Test
    public void testAppenderBackwardWithPretoucher() throws Exception {

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(UUID.randomUUID().toString()).rollCycle(RollCycles.TEST_SECONDLY).build();
        ExcerptAppender excerptAppender = queue.acquireAppender();
        excerptAppender.writeText("hello");

        // sleep til next cycle
        Thread.sleep(1000);

        // pretouch to create next cycle file  ----- IF YOU UNCOMMENT THIS LINE THE TEST PASSES
        excerptAppender.pretouch();

        ExcerptTailer tailer = queue.createTailer().direction(TailerDirection.BACKWARD);
        Assert.assertEquals("hello", tailer.toEnd().readText());
    }
}
