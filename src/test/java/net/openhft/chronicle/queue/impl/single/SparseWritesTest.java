package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SparseWritesTest {

    private static final int NUM_ENTRIES = 15_000;
    private static final long SEED = 23408932178093412L;
    private static final int LOG_INTERVAL = 1_000;
    private static final long ONE_DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1L);
    private static final String COUNTER_KEY = "counter";
    private String pathToQueue;

    @Before
    public void setUp() {
        pathToQueue = IOTools.createTempDirectory("SparseWritesTest").toAbsolutePath().toString();
    }

    @After
    public void tearDown() {
        IOTools.deleteDirWithFilesOrThrow(pathToQueue);
    }

    @Test
    public void tailerWillSeeAllMessagesWhenWritesAreSparse() throws InterruptedException {
        String numberOfEntries = String.valueOf(NUM_ENTRIES);
        String pathToQueue = IOTools.createTempDirectory("SparseWritesTest").toAbsolutePath().toString();
        final Process writerProcess = JavaProcessBuilder.create(SparseWriter.class)
                .withProgramArguments(pathToQueue, numberOfEntries)
                .inheritingIO()
                .start();
        final Process readerProcess = JavaProcessBuilder.create(SparseReader.class)
                .withProgramArguments(pathToQueue, numberOfEntries)
                .inheritingIO()
                .start();
        try {
            assertTrue("Writer process didn't terminate", writerProcess.waitFor(30, TimeUnit.SECONDS));
            assertTrue("Reader process didn't terminate", readerProcess.waitFor(30, TimeUnit.SECONDS));
            assertEquals(0, writerProcess.exitValue());
            assertEquals(0, readerProcess.exitValue());
        } finally {
            writerProcess.destroyForcibly();
            readerProcess.destroyForcibly();
        }
    }

    static class SparseReader {

        public static void main(String[] args) {
            String pathToQueue = args[0];
            int expectedMessages = Integer.parseInt(args[1]);
            Jvm.startup().on(SparseReader.class, "Starting with queue at " + pathToQueue + " expecting " + expectedMessages);
            int nextIndexExpected = 1;

            try (final SingleChronicleQueue queue = createQueue(pathToQueue, SystemTimeProvider.INSTANCE);
                 final ExcerptTailer tailer = queue.createTailer()) {
                while (nextIndexExpected <= expectedMessages) {
                    try (final DocumentContext documentContext = tailer.readingDocument()) {
                        if (documentContext.isPresent()) {
                            final int counter = documentContext.wire().read(COUNTER_KEY).int32();
                            if (nextIndexExpected != counter) {
                                throw new IllegalStateException("Expected " + nextIndexExpected + " got " + counter);
                            }
                            if (nextIndexExpected % LOG_INTERVAL == 0) {
                                Jvm.startup().on(SparseReader.class, "Read " + nextIndexExpected);
                            }
                            nextIndexExpected++;
                        } else {
                            Jvm.pause(1);
                        }
                    }
                }
            }
        }
    }

    static class SparseWriter {

        public static void main(String[] args) {
            String pathToQueue = args[0];
            int expectedMessages = Integer.parseInt(args[1]);
            Jvm.startup().on(SparseWriter.class, "Starting with queue at " + pathToQueue + " writing " + expectedMessages);
            int counter = 1;
            SetTimeProvider timeProvider = new SetTimeProvider();
            Random random = new Random(SEED);

            try (final SingleChronicleQueue queue = createQueue(pathToQueue, timeProvider);
                 final ExcerptAppender appender = queue.acquireAppender()) {
                while (counter <= expectedMessages) {
                    try (final DocumentContext documentContext = appender.writingDocument()) {
                        documentContext.wire().write(COUNTER_KEY).int32(counter);
                        timeProvider.set((long) (random.nextDouble() * 3 * ONE_DAY_IN_MILLIS) + timeProvider.get());
                    }
                    if (counter % LOG_INTERVAL == 0) {
                        Jvm.startup().on(SparseReader.class, "Wrote " + counter + " current date " + Instant.ofEpochMilli(timeProvider.get()));
                    }
                    counter++;
                    Jvm.pause(1);
                }
            }
        }
    }

    private static SingleChronicleQueue createQueue(String path, TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.binary(path)
                .rollCycle(RollCycles.FAST_DAILY)
                .timeProvider(timeProvider)
                .build();
    }
}
