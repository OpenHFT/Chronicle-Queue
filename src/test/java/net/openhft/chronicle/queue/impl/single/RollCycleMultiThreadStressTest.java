package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.*;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Thread.currentThread;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RollCycleMultiThreadStressTest {
    static {
        Jvm.disableDebugHandler();
    }

    final long SLEEP_PER_WRITE_NANOS;
    final int TEST_TIME;
    final int ROLL_EVERY_MS;
    final int DELAY_READER_RANDOM_MS;
    final int DELAY_WRITER_RANDOM_MS;
    final int WRITE_ONE_THEN_WAIT_MS;
    final int CORES;
    final Random random;
    final int NUMBER_OF_INTS;
    final boolean PRETOUCH;
    final boolean READERS_READ_ONLY;
    final boolean DUMP_QUEUE;
    final boolean SHARED_WRITE_QUEUE;
    final boolean DOUBLE_BUFFER;
    private ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptionKeyIntegerMap;
    final SetTimeProvider timeProvider = new SetTimeProvider();
    private ChronicleQueue sharedWriterQueue;

    public RollCycleMultiThreadStressTest() {
        this(StressTestType.VANILLA);
    }

    protected RollCycleMultiThreadStressTest(StressTestType type) {
        SLEEP_PER_WRITE_NANOS = Long.getLong("writeLatency", 30_000);
        TEST_TIME = Integer.getInteger("testTime", 2);
        ROLL_EVERY_MS = Integer.getInteger("rollEvery", 300);
        DELAY_READER_RANDOM_MS = Integer.getInteger("delayReader", 1);
        DELAY_WRITER_RANDOM_MS = Integer.getInteger("delayWriter", 1);
        WRITE_ONE_THEN_WAIT_MS = Integer.getInteger("writeOneThenWait", 0);
        CORES = Integer.getInteger("cores", Runtime.getRuntime().availableProcessors());
        random = new Random(99);
        NUMBER_OF_INTS = Integer.getInteger("numberInts", 18);//1060 / 4;
        PRETOUCH = type == StressTestType.PRETOUCH;
        READERS_READ_ONLY = type == StressTestType.READONLY;
        DUMP_QUEUE = false;
        SHARED_WRITE_QUEUE = type == StressTestType.SHAREDWRITEQ;
        DOUBLE_BUFFER = type == StressTestType.DOUBLEBUFFER;

        if (TEST_TIME > 2) {
            AbstractReferenceCounted.disableReferenceTracing();
            if (Jvm.isResourceTracing()) {
                throw new IllegalStateException("This test will run out of memory - change your system properties");
            }
        }
    }

    static boolean areAllReadersComplete(final int expectedNumberOfMessages, final List<Reader> readers) {
        boolean allReadersComplete = true;

        int count = 0;
        for (Reader reader : readers) {
            ++count;
            if (reader.lastRead < expectedNumberOfMessages - 1) {
                allReadersComplete = false;
                // System.out.printf("Reader #%d last read: %d%n", count, reader.lastRead);
            }
        }
        return allReadersComplete;
    }

    PretoucherThread pretoucherThread = null;

    @Test
    public void stress() throws Exception {
        assert warnIfAssertsAreOn();

        File file = DirectoryUtils.tempDir("stress");
        // System.out.printf("Queue dir: %s at %s%n", file.getAbsolutePath(), Instant.now());
        final int numThreads = CORES;
        final int numWriters = numThreads / 4 + 1;
        final ExecutorService executorServicePretouch = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("pretouch"));
        final ExecutorService executorServiceWrite = Executors.newFixedThreadPool(numWriters,
                new NamedThreadFactory("writer"));
        final ExecutorService executorServiceRead = Executors.newFixedThreadPool(numThreads - numWriters,
                new NamedThreadFactory("reader"));

        final AtomicInteger wrote = new AtomicInteger();
        final double expectedPerSecond = Jvm.isAzulZing() ? 3e8 : 1e9;
        final int expectedNumberOfMessages = (int) (TEST_TIME * expectedPerSecond / SLEEP_PER_WRITE_NANOS) * Math.max(1, numWriters / 2);
        Jvm.debug().on(getClass(), "Expecting " + expectedNumberOfMessages + " messages");

        // System.out.printf("Running test with %d writers and %d readers, sleep %dns%n",
        // numWriters, numThreads - numWriters, SLEEP_PER_WRITE_NANOS);
        // System.out.printf("Writing %d messages with %dns interval%n", expectedNumberOfMessages,
        // SLEEP_PER_WRITE_NANOS);
        // System.out.printf("Should take ~%dms%n",
        // TimeUnit.NANOSECONDS.toMillis(expectedNumberOfMessages * SLEEP_PER_WRITE_NANOS) / (numWriters / 2));

        final List<Future<Throwable>> results = new ArrayList<>();
        final List<Reader> readers = new ArrayList<>();
        final List<Writer> writers = new ArrayList<>();

        if (READERS_READ_ONLY)
            try (ChronicleQueue roq = createQueue(file)) {

            }

        if (SHARED_WRITE_QUEUE)
            sharedWriterQueue = createQueue(file);

        if (PRETOUCH) {
            pretoucherThread = new PretoucherThread(file);
            executorServicePretouch.submit(pretoucherThread);
        }

        if (WRITE_ONE_THEN_WAIT_MS > 0) {
            final Writer tempWriter = new Writer(file, wrote, expectedNumberOfMessages);
            try (ChronicleQueue queue = writerQueue(file)) {
                tempWriter.write(queue.acquireAppender());
            }
        }
        for (int i = 0; i < numThreads - numWriters; i++) {
            final Reader reader = new Reader(file, expectedNumberOfMessages, getReaderCheckingStrategy());
            readers.add(reader);
            results.add(executorServiceRead.submit(reader));
        }
        if (WRITE_ONE_THEN_WAIT_MS > 0) {
            Jvm.warn().on(getClass(), "Wrote one now waiting for " + WRITE_ONE_THEN_WAIT_MS + "ms");
            Jvm.pause(WRITE_ONE_THEN_WAIT_MS);
        }
        for (int i = 0; i < numWriters; i++) {
            final Writer writer = new Writer(file, wrote, expectedNumberOfMessages);
            writers.add(writer);
            results.add(executorServiceWrite.submit(writer));
        }

        final long maxWritingTime = TimeUnit.SECONDS.toMillis(TEST_TIME + 5) + queueBuilder(file).timeoutMS();
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
                // System.out.printf("Writer has written %d of %d messages after %dms. Readers at %s. Waiting...%n",
                // wrote.get() + 1, expectedNumberOfMessages,
                // i * 10, readersLastRead);
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

/*
       // System.out.println(String.format("All messages written in %,.0fsecs at rate of %,.0f/sec %,.0f/sec per writer (actual writeLatency %,.0fns)",
                timeToWriteSecs, expectedNumberOfMessages / timeToWriteSecs, (expectedNumberOfMessages / timeToWriteSecs) / numWriters,
                1_000_000_000 / ((expectedNumberOfMessages / timeToWriteSecs) / numWriters)));
*/

        final long giveUpReadingAt = System.currentTimeMillis() + 20_000L;
        final long dumpThreadsAt = giveUpReadingAt - 5_000L;

        try {
            while (System.currentTimeMillis() < giveUpReadingAt) {
                results.forEach(f -> {
                    try {
                        if (f.isDone()) {
                            final Throwable exception = f.get();
                            if (exception != null) {
                                throw Jvm.rethrow(exception);
                            }
                        }
                    } catch (InterruptedException e) {
                        // ignored
                    } catch (ExecutionException e) {
                        throw Jvm.rethrow(e);
                    }
                });

                boolean allReadersComplete = areAllReadersComplete(expectedNumberOfMessages, readers);

                if (allReadersComplete) {
                    break;
                }

                // System.out.printf("Not all readers are complete. Waiting...%n");
                Jvm.pause(2000);
            }
            assertTrue("Readers did not catch up",
                    areAllReadersComplete(expectedNumberOfMessages, readers));

        } finally {

            Jvm.resetExceptionHandlers();

            executorServiceRead.shutdown();
            executorServiceWrite.shutdown();
            executorServicePretouch.shutdown();

            if (!executorServiceRead.awaitTermination(10, TimeUnit.SECONDS))
                executorServiceRead.shutdownNow();

            if (!executorServiceWrite.awaitTermination(10, TimeUnit.SECONDS))
                executorServiceWrite.shutdownNow();

            Closeable.closeQuietly(pretoucherThread);

            if (!executorServicePretouch.awaitTermination(10, TimeUnit.SECONDS))
                executorServicePretouch.shutdownNow();

            closeQuietly(sharedWriterQueue);
            results.forEach(f -> {
                try {
                    final Throwable exception = f.get(100, TimeUnit.MILLISECONDS);
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                } catch (InterruptedException | TimeoutException e) {
                    // ignored
                } catch (ExecutionException e) {
                    throw Jvm.rethrow(e);
                }
            });
        }

        IOTools.deleteDirWithFiles("stress");
        // System.out.println("Test complete");
    }

    protected ReaderCheckingStrategy getReaderCheckingStrategy() {
        return new DefaultReaderCheckingStrategy();
    }

    private boolean warnIfAssertsAreOn() {
        Jvm.warn().on(getClass(), "Reminder: asserts are on");
        return true;
    }

    @NotNull
    SingleChronicleQueueBuilder queueBuilder(File path) {
        return SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .timeProvider(timeProvider)
                .doubleBuffer(DOUBLE_BUFFER)
                .rollCycle(RollCycles.TEST_SECONDLY);
    }

    @NotNull
    private ChronicleQueue createQueue(File path) {
        return queueBuilder(path).build();
    }

    @NotNull
    private ChronicleQueue writerQueue(File path) {
        return sharedWriterQueue != null ? sharedWriterQueue : createQueue(path);
    }

    @Before
    public void multiCPU() {
        Assume.assumeTrue(Runtime.getRuntime().availableProcessors() > 1);
    }

    @Before
    public void before() {
        threadDump = new ThreadDump();
        exceptionKeyIntegerMap = Jvm.recordExceptions();
    }

    @After
    public void after() {
        threadDump.assertNoNewThreads();
        // warnings are often expected
        exceptionKeyIntegerMap.entrySet().removeIf(entry -> entry.getKey().level.equals(LogLevel.WARN));
        if (Jvm.hasException(exceptionKeyIntegerMap)) {
            Jvm.dumpException(exceptionKeyIntegerMap);
            fail();
        }
        Jvm.resetExceptionHandlers();
        AbstractReferenceCounted.assertReferencesReleased();
    }

    interface ReaderCheckingStrategy {

        /**
         * Executed for each queue entry, validates the contents of the entry,
         * assuming timestamp has already been read and validated
         */
        void checkDocument(DocumentContext dc, ExcerptTailer tailer, RollingChronicleQueue queue,
                           int lastTailerCycle, int lastQueueCycle, int expected, ValueIn valueIn);

        /**
         * Executed after all documents have been read
         */
        void postReadCheck(RollingChronicleQueue queue);
    }

    /**
     * This is the existing check the reader was doing, it ensures
     * all the values in the document are the same as the expected
     * value
     */
    class DefaultReaderCheckingStrategy implements ReaderCheckingStrategy {

        @Override
        public void checkDocument(DocumentContext dc, ExcerptTailer tailer, RollingChronicleQueue queue,
                                  int lastTailerCycle, int lastQueueCycle, int expected, ValueIn valueIn) {
            for (int i = 0; i < NUMBER_OF_INTS; i++) {
                int v = valueIn.int32();
                if (v != expected) {
                    // System.out.println(dc.wire());
                    String failureMessage = "Expected: " + expected +
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
        }

        @Override
        public void postReadCheck(RollingChronicleQueue queue) {
            // Do nothing
        }
    }

    final class Reader implements Callable<Throwable> {
        final File path;
        final int expectedNumberOfMessages;
        final ReaderCheckingStrategy readerCheckingStrategy;
        volatile int lastRead = -1;
        volatile Throwable exception;
        int readSequenceAtLastProgressCheck = -1;

        Reader(final File path, final int expectedNumberOfMessages, ReaderCheckingStrategy readerCheckingStrategy) {
            this.path = path;
            this.expectedNumberOfMessages = expectedNumberOfMessages;
            this.readerCheckingStrategy = readerCheckingStrategy;
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
            long last = System.currentTimeMillis();
            try (RollingChronicleQueue queue = builder.build();
                 ExcerptTailer tailer = queue.createTailer()) {

                int lastTailerCycle = -1;
                int lastQueueCycle = -1;
                Jvm.pause(random.nextInt(DELAY_READER_RANDOM_MS));
                while (lastRead != expectedNumberOfMessages - 1) {
                    if (Thread.currentThread().isInterrupted())
                        return null;
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent()) {
                            long now = System.currentTimeMillis();
                            if (now > last + 2000) {
                                if (lastRead < 0)
                                    throw new AssertionError("read nothing after 2 seconds");
                                // System.out.println(Thread.currentThread() + " - Last read: " + lastRead);
                                last = now;
                            }
                            continue;
                        }

                        final ValueIn valueIn = dc.wire().getValueIn();
                        final long documentAcquireTimestamp = valueIn.int64();
                        if (documentAcquireTimestamp == 0L) {
                            throw new AssertionError("No timestamp");
                        }
                        int expected = lastRead + 1;
                        readerCheckingStrategy.checkDocument(
                                dc, tailer, queue, lastTailerCycle, lastQueueCycle, expected, valueIn);
                        lastRead = expected;
                        lastTailerCycle = tailer.cycle();
                        lastQueueCycle = queue.cycle();
                    }
                }
                readerCheckingStrategy.postReadCheck(queue);
            } catch (Throwable e) {
                exception = e;
                Jvm.debug().on(getClass(), "Finished reader", e);
                return e;
            }

            Jvm.debug().on(getClass(), "Finished reader OK");
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
            ChronicleQueue queue = writerQueue(path);
            try (final ExcerptAppender appender = queue.acquireAppender()) {
                Jvm.pause(random.nextInt(DELAY_WRITER_RANDOM_MS));
                final long startTime = System.nanoTime();
                int loopIteration = 0;
                while (true) {
                    final int value = write(appender);
                    if (currentThread().isInterrupted())
                        return null;
                    while (System.nanoTime() < (startTime + (loopIteration * SLEEP_PER_WRITE_NANOS))) {
                        if (currentThread().isInterrupted())
                            return null;
                        // spin
                    }
                    loopIteration++;

                    if (value >= expectedNumberOfMessages) {
                        Jvm.debug().on(getClass(), "Finished writer");
                        return null;
                    }
                }
            } catch (Throwable e) {
                Jvm.debug().on(getClass(), "Finished writer", e);
                exception = e;
                return e;
            } finally {
                if (queue != sharedWriterQueue)
                    queue.close();
            }
        }

        private int write(ExcerptAppender appender) {
            int value;
            try (DocumentContext writingDocument = appender.writingDocument()) {
                final long documentAcquireTimestamp = System.nanoTime();
                value = wrote.getAndIncrement();
                if (value >= expectedNumberOfMessages) {
                    /*
                        Mutual exclusion was previously relied on to ensure
                        we didn't write more than expectedNumberOfMessages
                        however when double buffering is turned on multiple
                        threads can get in here and end up writing more.
                        Exit early and rollback if that's the case.
                     */
                    writingDocument.rollbackOnClose();
                    return value;
                }
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

    class PretoucherThread extends AbstractCloseable implements Callable<Throwable> {

        private final SingleChronicleQueue queue;
        volatile Throwable exception;

        PretoucherThread(File path) {
            this.queue = queueBuilder(path).build();
        }

        @SuppressWarnings("resource")
        @Override
        public Throwable call() {
            try {
                ExcerptAppender appender = queue.acquireAppender();
                // System.out.println("Starting pretoucher");
                while (!Thread.currentThread().isInterrupted() && !queue.isClosed()) {
                    appender.pretouch();
                    Jvm.pause(50);
                }
            } catch (Throwable e) {
                if (e instanceof ClosedIllegalStateException || queue.isClosed())
                    return null;
                exception = e;
                return e;
            } finally {
                Closeable.closeQuietly(queue);
            }
            return null;
        }

        @Override
        protected void performClose() {
            Closeable.closeQuietly(queue);
        }
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressTest().stress();
    }

    enum StressTestType {
        VANILLA, READONLY, PRETOUCH, DOUBLEBUFFER, SHAREDWRITEQ;
    }
}