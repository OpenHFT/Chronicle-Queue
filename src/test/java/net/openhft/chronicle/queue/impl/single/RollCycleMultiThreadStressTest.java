package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;

public class RollCycleMultiThreadStressTest {
    private static final Logger LOG = LoggerFactory.getLogger(RollCycleMultiThreadStressTest.class);
    private static final long SLEEP_PER_WRITE_NANOS = 50_000;
    private static final int RUN_TIME_MILLIS = 120_400;

    @Ignore("long running")
    @Test
    public void stress() throws Exception {
        final File path = DirectoryUtils.tempDir("rollCycleStress");
        LOG.warn("using path {}", path);
        final int numWriters = 1;
        final int numThreads = Runtime.getRuntime().availableProcessors() * 2;
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        final AtomicInteger wrote = new AtomicInteger();
        final int[] read = new int[numThreads - numWriters];
        final AtomicBoolean[] finished = new AtomicBoolean[numThreads];
        final String[] errors = new String[numThreads];

        final long endTime = System.currentTimeMillis() + RUN_TIME_MILLIS;
        for (int threadId = 0; threadId < numThreads; threadId++) {
            final int finalThreadId = threadId;
            finished[finalThreadId] = new AtomicBoolean();
            executorService.submit(() -> {
                // create a queue per thread
                try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                        .testBlockSize()
                        .rollCycle(RollCycles.TEST_SECONDLY)
                        .build()) {
                    if (finalThreadId < numWriters) {
                        final ExcerptAppender appender = queue.acquireAppender();
                        while (System.currentTimeMillis() < endTime) {
                            try (DocumentContext writingDocument = appender.writingDocument()) {
                                ValueOut valueOut = writingDocument.wire().getValueOut();
                                valueOut.int32(wrote.getAndIncrement());
                                LockSupport.parkNanos(SLEEP_PER_WRITE_NANOS);
                            }
                        }
                    } else {
                        final ExcerptTailer tailer = queue.createTailer();
                        while (System.currentTimeMillis() < endTime + 500) {
                            try (DocumentContext rd = tailer.readingDocument()) {
                                if (rd.isPresent()) {
                                    int v = rd.wire().getValueIn().int32();
                                    assertEquals(read[finalThreadId - numWriters], v);
                                    read[finalThreadId - numWriters]++;
                                }
                                //LockSupport.parkNanos(SLEEP_PER_WRITE_NANOS / 2);
                            }
                        }
                    }
                } catch (Throwable t) {
                    LOG.error("Error " + t.getMessage());
                    StringWriter sw = new StringWriter();
                    t.printStackTrace(new PrintWriter(sw));
                    errors[finalThreadId] = Thread.currentThread().getName() + ": " + sw.toString();
                }
                finished[finalThreadId].set(true);
            });
        }

        executorService.shutdown();
        boolean ok = executorService.awaitTermination(RUN_TIME_MILLIS + 1000, TimeUnit.MILLISECONDS);
        if (!ok) {
            LOG.error("shutdown now");
            executorService.shutdownNow();
        }
        Thread.sleep(100);

        System.out.println("read:  "+ Arrays.toString(read));
        if (Arrays.stream(read).anyMatch(readValue -> readValue < wrote.get()))
            Assert.fail("Not all readers read all values " + wrote.get() + " vs " + Arrays.toString(read));

        if (Arrays.stream(finished).anyMatch(atomicBoolean -> ! atomicBoolean.get()))
            Assert.fail("Thread did not finish " + Arrays.toString(finished));

        Arrays.stream(errors).filter(Objects::nonNull).forEach(System.out::println);
        assertEquals(0, Arrays.stream(errors).filter(Objects::nonNull).count());
    }
}