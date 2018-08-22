package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class RollCycleMultiThreadStressTest {
    final Logger LOG = LoggerFactory.getLogger(getClass());
    static final long SLEEP_PER_WRITE_NANOS;
    static final int TEST_TIME;
    static final int ROLL_EVERY_MS;
    static final int DELAY_READER_RANDOM_MS;
    static final int DELAY_WRITER_RANDOM_MS;
    static final int WRITE_ONE_THEN_WAIT_MS;
    static final int CORES;
    static final Random random;
    static final int NUMBER_OF_INTS;
    static final boolean PRETOUCH;
    static final boolean READERS_READ_ONLY;
    static final boolean DUMP_QUEUE;

    static {
        SLEEP_PER_WRITE_NANOS = Long.getLong("writeLatency", 40_000);
        TEST_TIME = Integer.getInteger("testTime", 2);
        ROLL_EVERY_MS = Integer.getInteger("rollEvery", 100);
        DELAY_READER_RANDOM_MS = Integer.getInteger("delayReader", 1);
        DELAY_WRITER_RANDOM_MS = Integer.getInteger("delayWriter", 1);
        WRITE_ONE_THEN_WAIT_MS = Integer.getInteger("writeOneThenWait", 0);
        CORES = Integer.getInteger("cores", Runtime.getRuntime().availableProcessors());
        random = new Random(99);
        NUMBER_OF_INTS = Integer.getInteger("numberInts", 18);//1060 / 4;
        PRETOUCH = Boolean.getBoolean("pretouch");
        READERS_READ_ONLY = Boolean.getBoolean("read_only");
        DUMP_QUEUE = Boolean.getBoolean("dump_queue");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");

        System.setProperty("disableFastForwardHeaderNumber", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");
    }

    final SetTimeProvider timeProvider = new SetTimeProvider();

    /*@Ignore("run manually")
    @Test
    public void repeatStress() throws InterruptedException {
        //Jvm.setExceptionHandlers(null, null, null);
        for (int i = 0; i < 100; i++) {
            stress();
        }
    }*/

    @Test
    public void stress() throws InterruptedException {
        final File path = Optional.ofNullable(System.getProperty("stress.test.dir")).
                map(s -> new File(s, UUID.randomUUID().toString())).
                orElse(DirectoryUtils.tempDir("rollCycleStress"));

        System.out.printf("Queue dir: %s at %s%n", path.getAbsolutePath(), Instant.now());
        final int numThreads = CORES;
        final int numWriters = numThreads / 4 + 1;
        final ExecutorService executorServicePretouch = Executors.newSingleThreadExecutor(new NamedThreadFactory("pretouch"));
        final ExecutorService executorServiceWrite = Executors.newFixedThreadPool(numWriters, new NamedThreadFactory("writer"));
        final ExecutorService executorServiceRead = Executors.newFixedThreadPool(numThreads - numWriters, new NamedThreadFactory("reader"));

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

        PretoucherThread pretoucherThread = null;
        if (PRETOUCH) {
            pretoucherThread = new PretoucherThread(path);
            executorServicePretouch.submit(pretoucherThread);
        }

        if (WRITE_ONE_THEN_WAIT_MS > 0) {
            final Writer tempWriter = new Writer(path, wrote, expectedNumberOfMessages);
            try (ChronicleQueue queue = tempWriter.queue()) {
                tempWriter.write(queue.acquireAppender());
            }
        }
        for (int i = 0; i < numThreads - numWriters; i++) {
            final Reader reader = new Reader(path, expectedNumberOfMessages);
            readers.add(reader);
            results.add(executorServiceRead.submit(reader));
        }
        if (WRITE_ONE_THEN_WAIT_MS > 0) {
            LOG.warn("Wrote one now waiting for {}ms", WRITE_ONE_THEN_WAIT_MS);
            Jvm.pause(WRITE_ONE_THEN_WAIT_MS);
        }
        for (int i = 0; i < numWriters; i++) {
            final Writer writer = new Writer(path, wrote, expectedNumberOfMessages);
            writers.add(writer);
            results.add(executorServiceWrite.submit(writer));
        }

        final long maxWritingTime = TimeUnit.SECONDS.toMillis(TEST_TIME + 5) + queueBuilder(path).timeoutMS();
        long startTime = System.currentTimeMillis();
        final long giveUpWritingAt = startTime + maxWritingTime;
        long nextRollTime = System.currentTimeMillis() + ROLL_EVERY_MS, nextCheckTime = System.currentTimeMillis() + 5_000;
        int i = 0;
        long now;
        while ((now = System.currentTimeMillis()) < giveUpWritingAt) {
            if (wrote.get() >= expectedNumberOfMessages)
                break;
            if (now > nextRollTime) {
                timeProvider.advanceMillis(1000);
                nextRollTime += ROLL_EVERY_MS;
            }
            if (now > nextCheckTime) {
                String readersLastRead = readers.stream().map(reader -> Integer.toString(reader.lastRead)).collect(Collectors.joining(","));
                System.out.printf("Writer has written %d of %d messages after %dms. Readers at %s. Waiting...%n",
                        wrote.get() + 1, expectedNumberOfMessages,
                        i * 10, readersLastRead);
                readers.stream().filter(r -> !r.isMakingProgress()).findAny().ifPresent(reader -> {
                    if (reader.exception != null) {
                        throw new AssertionError("Reader encountered exception, so stopped reading messages",
                                reader.exception);
                    }
                    throw new AssertionError("Reader is stuck");

                });
                if (pretoucherThread != null && pretoucherThread.exception != null)
                    throw new AssertionError("Preloader encountered exception", pretoucherThread.exception);
                nextCheckTime = System.currentTimeMillis() + 10_000L;
            }
            i++;
            Jvm.pause(5);
        }
        double timeToWriteSecs = (System.currentTimeMillis() - startTime) / 1000d;

        final StringBuilder writerExceptions = new StringBuilder();
        writers.stream().filter(w -> w.exception != null).forEach(w -> {
            writerExceptions.append("Writer failed due to: ").append(w.exception.getMessage()).append("\n");
        });

        assertTrue("Wrote " + wrote.get() + " which is less than " + expectedNumberOfMessages + " within timeout. " + writerExceptions,
                wrote.get() >= expectedNumberOfMessages);

        readers.stream().filter(r -> r.exception != null).findAny().ifPresent(reader -> {
            throw new AssertionError("Reader encountered exception, so stopped reading messages",
                    reader.exception);
        });

        System.out.println(String.format("All messages written in %,.0fsecs at rate of %,.0f/sec %,.0f/sec per writer (actual writeLatency %,.0fns)",
                timeToWriteSecs, expectedNumberOfMessages / timeToWriteSecs, (expectedNumberOfMessages / timeToWriteSecs) / numWriters,
                1_000_000_000 / ((expectedNumberOfMessages / timeToWriteSecs) / numWriters)));

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

        executorServiceRead.shutdown();
        executorServiceWrite.shutdown();
        executorServicePretouch.shutdown();

        if (!executorServiceRead.awaitTermination(1, TimeUnit.SECONDS))
            executorServiceRead.shutdownNow();

        if (!executorServiceWrite.awaitTermination(1, TimeUnit.SECONDS))
            executorServiceWrite.shutdownNow();

        if (!executorServicePretouch.awaitTermination(1, TimeUnit.SECONDS))
            executorServicePretouch.shutdownNow();

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

    @NotNull
    SingleChronicleQueueBuilder queueBuilder(File path) {
        return SingleChronicleQueueBuilder.binary(path)
                    .testBlockSize()
                    .timeProvider(timeProvider)
                    .rollCycle(RollCycles.TEST_SECONDLY);
    }

    static boolean areAllReadersComplete(final int expectedNumberOfMessages, final List<Reader> readers) {
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

    final class Reader implements Callable<Throwable> {
        final File path;
        final int expectedNumberOfMessages;
        volatile int lastRead = -1;
        volatile Throwable exception;
        int readSequenceAtLastProgressCheck = -1;

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

            SingleChronicleQueueBuilder builder = queueBuilder(path);
            if (READERS_READ_ONLY)
                builder.readOnly(true);
            try (final RollingChronicleQueue queue = builder.build()) {

                final ExcerptTailer tailer = queue.createTailer();
                int lastTailerCycle = -1;
                int lastQueueCycle = -1;
                Jvm.pause(random.nextInt(DELAY_READER_RANDOM_MS));
                while (lastRead != expectedNumberOfMessages - 1) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (dc.isPresent()) {
                            int v = -1;

                            final ValueIn valueIn = dc.wire().getValueIn();
                            final long documentAcquireTimestamp = valueIn.int64();
                            if (documentAcquireTimestamp == 0L) {
                                throw new AssertionError("No timestamp");
                            }
                            for (int i = 0; i < NUMBER_OF_INTS; i++) {
                                v = valueIn.int32();
                                if (lastRead + 1 != v) {
                                    System.out.println(dc.wire());
                                    String failureMessage = "Expected: " + (lastRead + 1) +
                                            ", actual: " + v + ", pos: " + i + ", index: " + Long
                                            .toHexString(dc.index()) +
                                            ", cycle: " + tailer.cycle();
                                    if (lastTailerCycle != -1) {
                                        failureMessage += ". Tailer cycle at last read: " + lastTailerCycle +
                                                " (current: " + (tailer.cycle()) +
                                                "), queue cycle at last read: " + lastQueueCycle +
                                                " (current: " + queue.cycle() + ")";
                                    }
                                    if (DUMP_QUEUE)
                                        DumpQueueMain.dump(queue.file(), System.out, Long.MAX_VALUE);
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

    final class Writer implements Callable<Throwable> {

        final File path;
        final AtomicInteger wrote;
        final int expectedNumberOfMessages;
        volatile Throwable exception;

        Writer(final File path, final AtomicInteger wrote,
                       final int expectedNumberOfMessages) {
            this.path = path;
            this.wrote = wrote;
            this.expectedNumberOfMessages = expectedNumberOfMessages;
        }

        @Override
        public Throwable call() {
            try (final ChronicleQueue queue = queue()) {
                final ExcerptAppender appender = queue.acquireAppender();
                Jvm.pause(random.nextInt(DELAY_WRITER_RANDOM_MS));
                final long startTime = System.nanoTime();
                int loopIteration = 0;
                while (true) {
                    final int value = write(appender);

                    while (System.nanoTime() < (startTime + (loopIteration * SLEEP_PER_WRITE_NANOS))) {
                        // spin
                    }
                    loopIteration++;

                    if (value >= expectedNumberOfMessages) {
                        return null;
                    }
                }
            } catch (Throwable e) {
                exception = e;
                return e;
            }
        }

        private ChronicleQueue queue() {
            return queueBuilder(path).build();
        }

        private int write(ExcerptAppender appender) {
            int value;
            try (DocumentContext writingDocument = appender.writingDocument()) {
                final long documentAcquireTimestamp = System.nanoTime();
                value = wrote.getAndIncrement();
                ValueOut valueOut = writingDocument.wire().getValueOut();
                // make the message longer
                valueOut.int64(documentAcquireTimestamp);
                for (int i = 0; i < NUMBER_OF_INTS; i++) {
                    valueOut.int32(value);
                }
                writingDocument.wire().padToCacheAlign();
            }
            return value;
        }
    }

    class PretoucherThread implements Callable<Throwable> {

        final File path;
        volatile Throwable exception;

        PretoucherThread(File path) {
            this.path = path;
        }

        @Override
        public Throwable call() throws Exception {
            ChronicleQueue queue0 = null;
            try (ChronicleQueue queue = queueBuilder(path).build()) {
                queue0 = queue;
                ExcerptAppender appender = queue.acquireAppender();
                System.out.println("Starting pretoucher");
                while (!Thread.currentThread().isInterrupted() && !queue.isClosed()) {
                    Jvm.pause(50);
                    appender.pretouch();
                }
            } catch (Throwable e) {
                if (queue0 != null && queue0.isClosed())
                    return null;
                exception = e;
                return e;
            }
            return null;
        }
    }
}