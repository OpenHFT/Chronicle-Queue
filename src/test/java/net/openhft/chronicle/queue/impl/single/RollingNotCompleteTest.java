package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Jerry Shea on 30/10/17.
 */
public class RollingNotCompleteTest {

    // @Ignore("Intermittent failures:" +
    ////         "java.lang.AssertionError: Nothing should have been written until timeout \n" +
    ///       "Expected :0\n" +
    //       "Actual   :xxxx\n" +
    //     "\tat org.junit.Assert.assertEquals(Assert.java:645)\n" +
    //     "\tat net.openhft.chronicle.queue.impl.single.RollingNotCompleteTest.concurrentLockItUp(RollingNotCompleteTest.java:60)")
    @Test
    public void concurrentLockItUp() throws InterruptedException {
        final AtomicBoolean started = new AtomicBoolean();
        final AtomicInteger written = new AtomicInteger();
        int sourceId = 1;
        int timeoutMS = 5_000;
        boolean readOnly = false;
        RollCycle rollCycle = RollCycles.TEST_SECONDLY;
        File path = DirectoryUtils.tempDir(this.getClass().getSimpleName());
        SingleChronicleQueueBuilder builder = ChronicleQueueBuilder.single(path).
                sourceId(sourceId).rollCycle(rollCycle).readOnly(readOnly).timeoutMS(timeoutMS);

        builder.build().acquireAppender().writingDocument().wire().write("abcd");
        // don't close context. We should not be able to write to this queue until timeout

        long start0 = System.currentTimeMillis();
        Thread writerThread = new Thread(() -> {
            ExcerptAppender appender = builder.build().acquireAppender();
            started.set(true);
            while (!Thread.currentThread().isInterrupted()) {
                try (@NotNull DocumentContext context = appender.writingDocument()) {
                    // only get in here after unlocked
                    written.incrementAndGet();
                    Wire wire = context.wire();
                    wire.write("hello");
                    wire.padToCacheAlign();
                }
            }
        });
        writerThread.start();
        while (!started.get())
            ;

        Thread.sleep(timeoutMS - 50);
        long duration = start0 - System.currentTimeMillis();

        if (duration < timeoutMS) {
            assertEquals("Nothing should have been written until timeout", 0, written.get());

            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() < start + timeoutMS) {
                if (written.get() > 0)
                    break;
            }
            assertTrue("Nothing was written after header was repaired", written.get() > 0);
        }
    }

    @After
    public void checkMappedFiles() {
        MappedFile.checkMappedFiles();
    }
}
