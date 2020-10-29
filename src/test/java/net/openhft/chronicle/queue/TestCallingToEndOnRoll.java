package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

public class TestCallingToEndOnRoll extends ChronicleQueueTestBase implements TimeProvider {

    private long currentTime = 0;
    private ExcerptAppender appender;
    private ExcerptTailer tailer;

    @Ignore("long running soak test to check https://github.com/OpenHFT/Chronicle-Queue/issues/702")
    @Test
    public void test() {
        SingleChronicleQueue build = binary(getTmpDir()).rollCycle(RollCycles.TEST_SECONDLY).timeProvider(this).build();
        appender = build.acquireAppender();

        tailer = build.createTailer();
        Executors.newSingleThreadExecutor().submit(this::append);

        Executors.newSingleThreadExecutor().submit(this::toEnd);
        LockSupport.park();
    }

    private void append() {
        for (; ; ) {
            toEnd0();
            appender.writeText("hello world");
            toEnd0();
        }
    }

    private void toEnd() {
        for (; ; ) {
            toEnd0();
        }
    }

    private void toEnd0() {
        try {
            long index = tailer.toEnd().index();
            System.out.println("index = " + index);
        } catch (IllegalStateException e) {
            e.printStackTrace();
            Assert.fail();
            System.exit(-1);
        }
    }

    @Override
    public long currentTimeMillis() {
        return (currentTime = currentTime + 1000);
    }
}
