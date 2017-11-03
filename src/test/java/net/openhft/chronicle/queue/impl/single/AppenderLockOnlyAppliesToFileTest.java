package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class AppenderLockOnlyAppliesToFileTest {

    private static final RollCycle ROLL_CYCLE = RollCycles.TEST_SECONDLY;
    private static final int TIMEOUT_MS = 1_500;
    private static final int WAIT_FOR_ROLL_MS = 1_100;

    /*
    Failed tests:
  AppenderLockOnlyAppliesToFileTest.concurrentLockItUp:59 Writer thread completed before timeout

     */
    @Ignore("fails too often")
    @Test
    public void concurrentLockItUp() throws InterruptedException {
        final AtomicReference<String> writerQueueFile = new AtomicReference<>();
        final File path = DirectoryUtils.tempDir(this.getClass().getSimpleName());
        final SingleChronicleQueueBuilder builder = ChronicleQueueBuilder.single(path).
                sourceId(1).rollCycle(ROLL_CYCLE).timeoutMS(TIMEOUT_MS);
        final String initialFile;
        final DocumentContext initialContext = builder.build().acquireAppender().writingDocument();
        initialContext.wire().writeText("abcd");
        initialFile = getFilename(initialContext);
        // don't close context

        final long afterInitialWrite = System.currentTimeMillis();
        final CountDownLatch writerStarted = new CountDownLatch(1);
        final CountDownLatch writerFinished = new CountDownLatch(1);

        Thread writerThread = new Thread(() -> {
            ExcerptAppender appender = builder.build().acquireAppender();
            writerStarted.countDown();
            // wait for less than timeout and more than roll
            Jvm.pause(WAIT_FOR_ROLL_MS);
            try (@NotNull DocumentContext context = appender.writingDocument()) {
                // should not have been held up by locking
                writerQueueFile.set(getFilename(context));
                Wire wire = context.wire();
                wire.writeText("hello");
                writerFinished.countDown();
            }
        });
        writerThread.start();
        assertTrue("Writer thread not started",
                writerStarted.await(1, TimeUnit.SECONDS));

        assertTrue("Writer thread completed before timeout",
                writerFinished.await(WAIT_FOR_ROLL_MS + 50, TimeUnit.MILLISECONDS));

        assertFalse("Threads wrote to different queue cycles, so no locking occurred",
                initialFile.equals(writerQueueFile.get()));

        assertTrue("We are within timeout", System.currentTimeMillis() < afterInitialWrite + TIMEOUT_MS);

        ExcerptTailer tailer = builder.build().createTailer();
        // this call to readingDocument waits for timeout and logs out ".... resetting header after timeout ...."
        try (DocumentContext rd = tailer.readingDocument()) {
            assertFalse("We are outside timeout", System.currentTimeMillis() < afterInitialWrite + TIMEOUT_MS);
            assertTrue("Something was written", rd.isPresent());
            String value = rd.wire().readText();
            assertEquals("the first (locked) write is lost", "hello", value);
        }
        try (DocumentContext rd = tailer.readingDocument()) {
            assertFalse("Should be only one message in the queue", rd.isPresent());
        }

        try {
            initialContext.close();
            fail("close should have thrown");
        } catch (IllegalStateException e) {
            // expected for close to throw
        }
    }

    @NotNull
    private String getFilename(final DocumentContext context) {
        return ((MappedBytes) context.wire().bytes()).mappedFile().file().getName();
    }

    @After
    public void checkMappedFiles() {
        MappedFile.checkMappedFiles();
    }
}
