package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class RollingNotCompleteTest {

    private static final RollCycle ROLL_CYCLE = RollCycles.TEST_SECONDLY;
    private static final int TIMEOUT_MS = 1_000;

    @Test
    public void concurrentLockItUp() throws InterruptedException {
        final AtomicInteger written = new AtomicInteger();
        final AtomicReference<String> writerQueueFile = new AtomicReference<>();
        final File path = DirectoryUtils.tempDir(this.getClass().getSimpleName());
        final SingleChronicleQueueBuilder builder = ChronicleQueueBuilder.single(path).
                sourceId(1).rollCycle(ROLL_CYCLE).timeoutMS(TIMEOUT_MS);
        final String initialFile;
        final DocumentContext initialContext = builder.build().acquireAppender().writingDocument();
        initialContext.wire().write("abcd");
        initialFile = getFilename(initialContext);
        // don't close context. We should not be able to write to this queue until timeout

        final long afterInitialWrite = System.currentTimeMillis();
        final CountDownLatch writerStarted = new CountDownLatch(1);

        Thread writerThread = new Thread(() -> {
            ExcerptAppender appender = builder.build().acquireAppender();
            writerStarted.countDown();
            try (@NotNull DocumentContext context = appender.writingDocument()) {
                // only get in here after unlocked
                written.incrementAndGet();
                writerQueueFile.set(getFilename(context));
                Wire wire = context.wire();
                wire.write("hello");
                wire.padToCacheAlign();
            }
        });
        writerThread.start();
        assertTrue("Writer thread not started",
                writerStarted.await(1, TimeUnit.SECONDS));

        while (System.currentTimeMillis() < afterInitialWrite + (TIMEOUT_MS - 50)) {
            Thread.sleep(10);
        }

        final long elapsedMillis = System.currentTimeMillis() - afterInitialWrite;

        assertTrue("Test is duff", elapsedMillis < TIMEOUT_MS);

        assumeTrue("Threads wrote to different queue cycles, so no locking occurred",
                writerQueueFile.get() == null ||
                initialFile.equals(writerQueueFile.get()));

        assertEquals("Nothing should have been written until timeout", 0, written.get());

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + TIMEOUT_MS) {
            if (written.get() > 0)
                break;
        }
        assertTrue("Nothing was written after header was repaired", written.get() > 0);
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
