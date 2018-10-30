package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class BackwardWithPretouchTest extends ChronicleQueueTestBase {

    @Test
    public void testAppenderBackwardWithPretoucher() {

        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(System.currentTimeMillis());
        File tmpDir = getTmpDir();
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir).timeProvider(timeProvider).rollCycle(RollCycles.TEST_SECONDLY).build();
        ExcerptAppender excerptAppender = queue.acquireAppender();
        excerptAppender.writeText("hello");

        timeProvider.advanceMillis(1000);

        // pretouch to create next cycle file  ----- IF YOU COMMENT THIS LINE THE TEST PASSES
        excerptAppender.pretouch();

        ExcerptTailer tailer = queue.createTailer().direction(TailerDirection.BACKWARD);
        Assert.assertEquals("hello", tailer.toEnd().readText());
    }
}
