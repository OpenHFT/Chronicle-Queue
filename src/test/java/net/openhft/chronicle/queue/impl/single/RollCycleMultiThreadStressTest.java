package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RollCycleMultiThreadStressTest {
    private static final Logger LOG = LoggerFactory.getLogger(RollCycleMultiThreadStressTest.class);
    private static final long SLEEP_PER_WRITE_NANOS = Long.getLong("writeLatency", 10_000);

    @Ignore("long running")
    @Test
    public void stress() throws Exception {
        final File path = DirectoryUtils.tempDir("rollCycleStress");
        LOG.warn("using path {}", path);
        final int numThreads = Runtime.getRuntime().availableProcessors();
        final int numWriters = numThreads / 4 + 1;
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        final AtomicInteger wrote = new AtomicInteger();
        final int expectedNumberOfMessages = (int) (90 * 1e9 / SLEEP_PER_WRITE_NANOS);

        System.out.printf("Running test with %d writers and %d readers%n",
                numWriters, numThreads - numWriters);
        System.out.printf("Writing %d messages with %dns interval%n", expectedNumberOfMessages,
                SLEEP_PER_WRITE_NANOS);
        System.out.printf("Should take ~%dsec%n",
                TimeUnit.NANOSECONDS.toSeconds(expectedNumberOfMessages * SLEEP_PER_WRITE_NANOS) / numWriters);

        final List<Future<Throwable>> results = new ArrayList<>();
        final List<Reader> readers = new ArrayList<>();

        for (int i = 0; i < numWriters; i++) {
            results.add(executorService.submit(new Writer(path, wrote, expectedNumberOfMessages)));
        }
        for (int i = 0; i < numThreads - numWriters; i++) {
            final Reader reader = new Reader(path, expectedNumberOfMessages);
            readers.add(reader);
            results.add(executorService.submit(reader));
        }


        final long maxWritingTime = TimeUnit.MINUTES.toMillis(3);
        final long giveUpWritingAt = System.currentTimeMillis() + maxWritingTime;
        int i = 0;
        while (System.currentTimeMillis() < giveUpWritingAt) {
            if (wrote.get() < expectedNumberOfMessages) {
                System.out.printf("Writer has written %d of %d messages after %ds. Waiting...%n",
                        wrote.get() + 1, expectedNumberOfMessages,
                        i * 10);
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10L));
            } else {
                break;
            }
            i++;
        }

        assertTrue("Did not write " + expectedNumberOfMessages + " within timeout",
                wrote.get() >= expectedNumberOfMessages);

        final long giveUpReadingAt = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < giveUpReadingAt) {
            boolean allReadersComplete = areAllReadersComplete(expectedNumberOfMessages, readers);

            if (allReadersComplete) {
                break;
            }

            System.out.printf("Not all readers are complete. Waiting...%n");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10L));
        }
        assertTrue("Readers did not catch up",
                areAllReadersComplete(expectedNumberOfMessages, readers));

        executorService.shutdownNow();

        results.forEach(f -> {
            try {

                final Throwable exception = f.get();
                if (exception != null) {
                    exception.printStackTrace();
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        System.out.println("Test complete");
    }

    private boolean areAllReadersComplete(final int expectedNumberOfMessages, final List<Reader> readers) {
        boolean allReadersComplete = true;

        for (Reader reader : readers) {
            if (reader.lastRead < expectedNumberOfMessages - 1) {
                allReadersComplete = false;
                System.out.printf("Reader last read: %d%n", reader.lastRead);
            }
        }
        return allReadersComplete;
    }

    private static final class Reader implements Callable<Throwable> {
        private final File path;
        private final int expectedNumberOfMessages;
        private volatile int lastRead = -1;

        Reader(final File path, final int expectedNumberOfMessages) {
            this.path = path;
            this.expectedNumberOfMessages = expectedNumberOfMessages;
        }

        @Override
        public Throwable call() {

            try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                    .testBlockSize()
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .build()) {

                final ExcerptTailer tailer = queue.createTailer();

                while (lastRead != expectedNumberOfMessages - 1) {
                    try (DocumentContext rd = tailer.readingDocument()) {
                        if (rd.isPresent()) {
                            int v = rd.wire().getValueIn().int32();

                            assertEquals(lastRead + 1, v);
                            lastRead = v;
                        }
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
                return e;
            }

            return null;
        }
    }

    private static final class Writer implements Callable<Throwable> {

        private final File path;
        private final AtomicInteger wrote;
        private final int expectedNumberOfMessages;

        private Writer(final File path, final AtomicInteger wrote,
                       final int expectedNumberOfMessages) {
            this.path = path;
            this.wrote = wrote;
            this.expectedNumberOfMessages = expectedNumberOfMessages;
        }

        @Override
        public Throwable call() {
            try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                    .testBlockSize()
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .build()) {
                final ExcerptAppender appender = queue.acquireAppender();
                long nextTime = System.nanoTime();
                while (true) {
                    final int value;

                    try (DocumentContext writingDocument = appender.writingDocument()) {
                        value = wrote.getAndIncrement();
                        ValueOut valueOut = writingDocument.wire().getValueOut();
                        valueOut.int32(value);
                        writingDocument.wire().addPadding(60); // make the message longer
                        long delay = nextTime - System.nanoTime();
                        if (delay > 0) {
                            LockSupport.parkNanos(delay);
                        }
                        nextTime += SLEEP_PER_WRITE_NANOS * 0.99;
                    }
                    if (value >= expectedNumberOfMessages) {
                        return null;
                    }
                }
            } catch (Exception e) {
                return e;
            }
        }
    }
}