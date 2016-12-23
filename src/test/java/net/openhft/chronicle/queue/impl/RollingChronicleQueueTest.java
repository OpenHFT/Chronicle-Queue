package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.ChronicleQueueTestBase.getTmpDir;
import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

/**
 * @author Rob Austin.
 */
public class RollingChronicleQueueTest {

    @Test
    public void testCountExcerptsWhenTheCycleIsRolled() throws Exception {

        final AtomicLong time = new AtomicLong();

        final SingleChronicleQueue q = binary(getTmpDir())
                .timeProvider(time::get)
                .rollCycle(TEST_SECONDLY)
                .build();

        final ExcerptAppender appender = q.acquireAppender();
        time.set(0);

        appender.writeText("1. some  text");
        long start = appender.lastIndexAppended();
        appender.writeText("2. some more text");
        time.set(1000);
        appender.writeText("3. some text - first cycle");
        time.set(2000);
        time.set(3000); // large gap to miss a cycle file
        time.set(4000);
        appender.writeText("4. some text - second cycle");
        appender.writeText("some more text");
        long end = appender.lastIndexAppended();

        Assert.assertEquals(4, q.countExcerpts(start, end));
    }

}