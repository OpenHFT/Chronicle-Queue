package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class RollCycleMultiThreadStressTest {
    private static final Logger LOG = LoggerFactory.getLogger(RollCycleMultiThreadStressTest.class);
    private static final long SLEEP_PER_WRITE_NANOS = Long.getLong("writeLatency", 10_000);
    private static final int TEST_TIME = Integer.getInteger("testTime", 90);
    static final int NUMBER_OF_INTS = 18;//1060 / 4;

    @Ignore("long running")
    @Test
    public void stress() throws Exception {
        final File path = Optional.ofNullable(System.getProperty("stress.test.dir")).
                map(s -> new File(s, UUID.randomUUID().toString())).
                orElse(DirectoryUtils.tempDir("rollCycleStress"));

        System.out.printf("Queue dir: %s at %s%n", path.getAbsolutePath(), Instant.now());
        final int numThreads = Runtime.getRuntime().availableProcessors();
        final int numWriters = numThreads / 4 + 1;
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        final AtomicInteger wrote = new AtomicInteger();
        final int expectedNumberOfMessages = (int) (TEST_TIME * 1e9 / SLEEP_PER_WRITE_NANOS);

        System.out.printf("Running test with %d writers and %d readers, sleep %dns%n",
                numWriters, numThreads - numWriters, SLEEP_PER_WRITE_NANOS);
        System.out.printf("Writing %d messages with %dns interval%n", expectedNumberOfMessages,
                SLEEP_PER_WRITE_NANOS);
        System.out.printf("Should take ~%dsec%n",
                TimeUnit.NANOSECONDS.toSeconds(expectedNumberOfMessages * SLEEP_PER_WRITE_NANOS) / numWriters);

        final List<Future<Throwable>> results = new ArrayList<>();
        final List<Reader> readers = new ArrayList<>();
        final List<Writer> writers = new ArrayList<>();

        for (int i = 0; i < numWriters; i++) {
            final Writer writer = new Writer(path, wrote, expectedNumberOfMessages);
            writers.add(writer);
            results.add(executorService.submit(writer));
        }
        for (int i = 0; i < numThreads - numWriters; i++) {
            final Reader reader = new Reader(path, expectedNumberOfMessages);
            readers.add(reader);
            results.add(executorService.submit(reader));
        }


        final long maxWritingTime = TimeUnit.SECONDS.toMillis(TEST_TIME);
        final long giveUpWritingAt = System.currentTimeMillis() + maxWritingTime;
        int i = 0;
        while (System.currentTimeMillis() < giveUpWritingAt) {
            if (wrote.get() < expectedNumberOfMessages) {
                String readersLastRead = readers.stream().map(reader -> Integer.toString(reader.lastRead)).collect(Collectors.joining(","));
                System.out.printf("Writer has written %d of %d messages after %ds. Readers at %s. Waiting...%n",
                        wrote.get() + 1, expectedNumberOfMessages,
                        i * 10, readersLastRead);
                readers.stream().filter(r -> !r.isMakingProgress()).findAny().ifPresent(reader -> {
                    if (reader.exception != null) {
                        throw new AssertionError("Reader encountered exception, so stopped reading messages",
                                reader.exception);
                    }
                    throw new AssertionError("Reader is stuck");

                });
                final long waitUntil = System.currentTimeMillis() + 10_000L;
                while (wrote.get() < expectedNumberOfMessages && System.currentTimeMillis() < waitUntil) {
                    LockSupport.parkNanos(1L);
                }
            } else {
                break;
            }
            i++;
        }

        final StringBuilder writerExceptions = new StringBuilder();
        writers.stream().filter(w -> w.exception != null).forEach(w -> {
            writerExceptions.append("Writer failed due to: ").append(w.exception.getMessage()).append("\n");
        });

        assertTrue("Did not write " + expectedNumberOfMessages + " within timeout. " + writerExceptions,
                wrote.get() >= expectedNumberOfMessages);

        readers.stream().filter(r -> r.exception != null).findAny().ifPresent(reader -> {
            throw new AssertionError("Reader encountered exception, so stopped reading messages",
                    reader.exception);
        });

        final long giveUpReadingAt = System.currentTimeMillis() + 60_000L;
        final long dumpThreadsAt = giveUpReadingAt - 15_000L;
        while (System.currentTimeMillis() < giveUpReadingAt) {
            boolean allReadersComplete = areAllReadersComplete(expectedNumberOfMessages, readers);

            if (allReadersComplete) {
                break;
            }

            if (dumpThreadsAt < System.currentTimeMillis()) {
                Thread.getAllStackTraces().forEach((n, st) -> {
                    System.out.println("\n\n" + n + "\n\n");
                    Arrays.stream(st).forEach(System.out::println);
                });
            }

            System.out.printf("Not all readers are complete. Waiting...%n");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1L));
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
        DirectoryUtils.deleteDir(path);
    }

    private boolean areAllReadersComplete(final int expectedNumberOfMessages, final List<Reader> readers) {
        boolean allReadersComplete = true;

        int count = 0;
        for (Reader reader : readers) {
            ++count;
            if (reader.lastRead < expectedNumberOfMessages - 1) {
                allReadersComplete = false;
                System.out.printf("Reader #%d last read: %d%n", count, reader.lastRead);
            }
        }
        return allReadersComplete;
    }

    private static final class Reader implements Callable<Throwable> {
        private final File path;
        private final int expectedNumberOfMessages;
        private volatile int lastRead = -1;
        private volatile Throwable exception;
        private int readSequenceAtLastProgressCheck = -1;

        Reader(final File path, final int expectedNumberOfMessages) {
            this.path = path;
            this.expectedNumberOfMessages = expectedNumberOfMessages;
        }

        boolean isMakingProgress() {
            if (readSequenceAtLastProgressCheck == -1) {
                return true;
            }

            final boolean makingProgress = lastRead > readSequenceAtLastProgressCheck;
            readSequenceAtLastProgressCheck = lastRead;

            return makingProgress;
        }

        @Override
        public Throwable call() {

            try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                    .testBlockSize()
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .build()) {

                final ExcerptTailer tailer = queue.createTailer();
                int lastTailerCycle = -1;
                int lastQueueCycle = -1;
                while (lastRead != expectedNumberOfMessages - 1) {
                    try (DocumentContext rd = tailer.readingDocument()) {
                        if (rd.isPresent()) {
                            int v = -1;
                            for (int i = 0; i < NUMBER_OF_INTS; i++) {
                                v = rd.wire().getValueIn().int32();
                                if (lastRead + 1 != v) {
                                    String failureMessage = "Expected: " + (lastRead + 1) +
                                            ", actual: " + v + ", pos: " + i + ", index: " + rd.index() +
                                            ", cycle: " + tailer.cycle();
                                    if (lastTailerCycle != -1) {
                                        failureMessage += ". Tailer cycle at last read: " + lastTailerCycle +
                                                " (current: " + (tailer.cycle()) +
                                                "), queue cycle at last read: " + lastQueueCycle +
                                                " (current: " + queue.cycle() + ")";
                                    }
                                    throw new AssertionError(failureMessage);
                                }
                            }
                            lastRead = v;
                            lastTailerCycle = tailer.cycle();
                            lastQueueCycle = queue.cycle();
                        }
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
                exception = e;
                return e;
            }

            return null;
        }
    }

    private static final class Writer implements Callable<Throwable> {

        private final File path;
        private final AtomicInteger wrote;
        private final int expectedNumberOfMessages;
        private volatile Throwable exception;

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
                final long startTime = System.nanoTime();
                int loopIteration = 0;
                while (true) {
                    final int value;

                    try (DocumentContext writingDocument = appender.writingDocument()) {
                        value = wrote.getAndIncrement();
                        ValueOut valueOut = writingDocument.wire().getValueOut();
                        // make the message longer
                        for (int i = 0; i < NUMBER_OF_INTS; i++) {
                            valueOut.int32(value);
                        }
                        while (System.nanoTime() < (startTime + (loopIteration * SLEEP_PER_WRITE_NANOS))) {
                            // spin
                        }
                    }
                    loopIteration++;

                    if (value >= expectedNumberOfMessages) {
                        return null;
                    }
                }
            } catch (Exception e) {
                exception = e;
                return e;
            }
        }
    }
}