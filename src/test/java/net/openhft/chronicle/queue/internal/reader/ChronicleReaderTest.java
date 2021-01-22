package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.openhft.chronicle.queue.impl.single.GcControls.waitForGcCycle;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class ChronicleReaderTest extends ChronicleQueueTestBase {
    private static final byte[] ONE_KILOBYTE = new byte[1024];
    private static final String LAST_MESSAGE = "LAST_MESSAGE";

    static {
        Arrays.fill(ONE_KILOBYTE, (byte) 7);
    }

    private final Queue<String> capturedOutput = new ConcurrentLinkedQueue<>();
    private Path dataDir;

    private static long getCurrentQueueFileLength(final Path dataDir) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(
                Files.list(dataDir).filter(p -> p.toString().endsWith("cq4")).findFirst().
                        orElseThrow(AssertionError::new).toFile(), "r")) {
            return file.length();
        }
    }

    @Before
    public void before() {
        dataDir = getTmpDir().toPath();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .sourceId(1)
                .testBlockSize().build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder =
                    excerptAppender.methodWriterBuilder(Say.class);
            methodWriterBuilder.recordHistory(true);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < 24; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }
    }

    @Test(timeout = 10_000L)
    public void shouldReadQueueWithNonDefaultRollCycle() {
        if (OS.isWindows())
            return;

        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(RollCycles.MINUTELY).
                testBlockSize().sourceId(1).build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder = excerptAppender.methodWriterBuilder(Say.class);
            methodWriterBuilder.recordHistory(true);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < 24; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }

        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertFalse(capturedOutput.isEmpty());
    }

    @Test(timeout = 10_000L)
    public void shouldReadQueueWithNonDefaultRollCycleWhenMetadataDeleted() throws IOException {
        if (OS.isWindows())
            return;
        expectException("Failback to readonly tablestore");
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(RollCycles.MINUTELY).
                testBlockSize().sourceId(1).build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder = excerptAppender.methodWriterBuilder(Say.class);
            methodWriterBuilder.recordHistory(true);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < 24; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }
        Files.list(path).filter(f -> f.getFileName().toString().endsWith(SingleTableStore.SUFFIX)).findFirst().ifPresent(p -> p.toFile().delete());
        waitForGcCycle();

        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertFalse(capturedOutput.isEmpty());
    }

/*
    @Test(timeout = 30_000L)
    public void shouldReadQueueWithDifferentRollCycleWhenCreatedAfterReader() throws InterruptedException {
        ReferenceCounter.TRACING_ENABLED = false;
        // TODO FIX
//        AbstractCloseable.disableCloseableTracing();

        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicLong recordsProcessed = new AtomicLong(0);
        final ChronicleReader reader = new ChronicleReader().withBasePath(path).withMessageSink(m -> {
            latch.countDown();
            recordsProcessed.incrementAndGet();
        });

        final AtomicReference<Throwable> readerException = new AtomicReference<>();
        final CountDownLatch executeLatch = new CountDownLatch(1);

try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(RollCycles.MINUTELY).
                build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final VanillaMethodWriterBuilder<StringEvents> methodWriterBuilder = excerptAppender.methodWriterBuilder(StringEvents.class);
            methodWriterBuilder.recordHistory(true);
            final StringEvents events = methodWriterBuilder.build();

            for (int i = 0; i < 24; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }

        final Thread readerThread = new Thread(() -> {
            long end = System.currentTimeMillis() + 5000;
            do {
                try {
                    reader.execute();
                    executeLatch.countDown();
                } catch (Throwable t) {
                    readerException.set(t);
                    throw t;
                }
            } while(System.currentTimeMillis() < end);
        });
        readerThread.start();

        assertTrue(executeLatch.await(5, TimeUnit.SECONDS));
        readerThread.interrupt();
        assertTrue(capturedOutput.isEmpty());

        assertTrue(latch.await(15, TimeUnit.SECONDS));
        while (recordsProcessed.get() < 10) {
            LockSupport.parkNanos(1L);
        }

        readerThread.join();

        assertNull(readerException.get());
    }
*/

    @Test
    public void shouldNotFailOnEmptyQueue() {
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        expectException("Failback to readonly tablestore");
        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertTrue(capturedOutput.isEmpty());
    }

    @Test
    public void shouldNotFailWhenNoMetadata() throws IOException {
        expectException("Failback to readonly tablestore");
        Files.list(dataDir).filter(f -> f.getFileName().toString().endsWith(SingleTableStore.SUFFIX)).findFirst().ifPresent(path -> path.toFile().delete());
        basicReader().execute();
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")));
    }

    @Test
    public void shouldIncludeMessageHistoryByDefault() {
        basicReader().execute();

        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")));
    }

    @Test
    public void shouldApplyIncludeRegexToHistoryMessagesAndBusinessMessages() {
        basicReader().
                // matches goodbye, but not hello or history
                        withInclusionRegex("goodbye").
                asMethodReader(null).
                execute();
        assertFalse(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")));
    }

    @Test(timeout = 5000)
    public void readOnlyQueueTailerShouldObserveChangesAfterInitiallyObservedReadLimit() throws IOException, InterruptedException, TimeoutException, ExecutionException {
        DirectoryUtils.deleteDir(dataDir.toFile());
        dataDir.toFile().mkdirs();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).testBlockSize().build()) {

            final Say events = queue.acquireAppender().methodWriterBuilder(Say.class).build();
            events.say("hello");

            final long readerCapacity = getCurrentQueueFileLength(dataDir);

            final RecordCounter recordCounter = new RecordCounter();
            final ChronicleReader chronicleReader = basicReader().withMessageSink(recordCounter);

            final ExecutorService executorService = Executors.newSingleThreadExecutor(
                    new NamedThreadFactory("executor"));
            Future<?> submit = executorService.submit(chronicleReader::execute);

            final long expectedReadingDocumentCount = (readerCapacity / ONE_KILOBYTE.length) + 1;
            int i;
            for (i = 0; i < expectedReadingDocumentCount; i++) {
                events.say(new String(ONE_KILOBYTE));
            }

            recordCounter.latch.countDown();
            executorService.shutdown();
            executorService.awaitTermination(Jvm.isDebug() ? 50 : 5, TimeUnit.SECONDS);
            submit.get(1, TimeUnit.SECONDS);

            // #460 read only not supported on windows.
            if (!OS.isWindows())
                assertEquals(expectedReadingDocumentCount, recordCounter.recordCount.get() - 1);
        }
    }

    @Test
    public void shouldBeAbleToReadFromReadOnlyFile() throws IOException {
        if (OS.isWindows()) {
            System.err.println("#460 read-only not supported on Windows");
            return;
        }
        final Path queueFile = Files.list(dataDir).
                filter(f -> f.getFileName().toString().endsWith(SingleChronicleQueue.SUFFIX)).findFirst().
                orElseThrow(() ->
                        new AssertionError("Could not find queue file in directory " + dataDir));

        assertTrue(queueFile.toFile().setWritable(false));

        basicReader().execute();
    }

    @Test
    public void shouldConvertEntriesToText() {
        basicReader().execute();

        assertEquals(48, capturedOutput.size());
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("hello")));
    }

    @Test
    public void shouldFilterByInclusionRegex() {
        basicReader().withInclusionRegex(".*good.*").execute();

        assertEquals(24, capturedOutput.size());
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, containsString("goodbye")));
    }

    @Test
    public void shouldFilterByMultipleInclusionRegex() {
        basicReader().withInclusionRegex(".*bye$").withInclusionRegex(".*o.*").execute();

        assertEquals(24, capturedOutput.size());
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, containsString("goodbye")));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, not(containsString("hello"))));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfInputDirectoryDoesNotExist() {
        basicReader().withBasePath(Paths.get("/does/not/exist")).execute();
    }

    @Test
    public void shouldFilterByExclusionRegex() {
        basicReader().withExclusionRegex(".*good.*").execute();

        assertEquals(24, capturedOutput.size());
        capturedOutput.forEach(msg -> assertThat(msg, not(containsString("goodbye"))));
    }

    @Test
    public void shouldFilterByMultipleExclusionRegex() {
        basicReader().withExclusionRegex(".*bye$").withExclusionRegex(".*ell.*").execute();

        assertEquals(0L, capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).count());
    }

    @Test
    public void shouldReturnNoMoreThanTheSpecifiedNumberOfMaxRecords() {
        basicReader().historyRecords(5).execute();

        assertThat(capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), is(5L));
    }

    @Test
    public void shouldForwardToSpecifiedIndex() {
        final long knownIndex = Long.decode(findAnExistingIndex());
        basicReader().withStartIndex(knownIndex).execute();

        assertEquals(24, capturedOutput.size());
        assertTrue(capturedOutput.poll().contains(Long.toHexString(knownIndex)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailIfSpecifiedIndexIsBeforeFirstIndex() {
        basicReader().withStartIndex(1L).execute();
    }

    @Test
    public void shouldNotRewindPastStartOfQueueWhenDisplayingHistory() {
        basicReader().historyRecords(Long.MAX_VALUE).execute();

        assertThat(capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), is(24L));
    }

    @Test
    public void shouldContinueToPollQueueWhenTailModeIsEnabled() {
        final int expectedPollCountWhenDocumentIsEmpty = 3;
        final FiniteDocumentPollMethod pollMethod = new FiniteDocumentPollMethod(expectedPollCountWhenDocumentIsEmpty);
        try {
            basicReader().withDocumentPollMethod(pollMethod).tail().execute();
        } catch (ArithmeticException e) {
            // expected
        }

        assertEquals(expectedPollCountWhenDocumentIsEmpty, pollMethod.invocationCount);
    }

    private String findAnExistingIndex() {
        basicReader().execute();
        final List<String> indicies = capturedOutput.stream()
                .filter(s -> s.startsWith("0x"))
                .collect(Collectors.toList());
        capturedOutput.clear();
        return indicies.get(indicies.size() / 2)
                .trim()
                .replaceAll(":", "");
    }

    private ChronicleReader basicReader() {
        return new ChronicleReader()
                .withBasePath(dataDir)
                .withMessageSink(capturedOutput::add);
    }

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }

    @After
    public void checkRegisteredBytes() {
        try {
            AbstractCloseable.assertCloseablesClosed();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // TODO FIX
        try {
            AbstractReferenceCounted.assertReferencesReleased();
        } catch (IllegalStateException todoFix) {
            todoFix.printStackTrace();
        }
    }

    private static final class RecordCounter implements Consumer<String> {
        private final AtomicLong recordCount = new AtomicLong();
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void accept(final String msg) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                // ignore
            }

            if (!msg.startsWith("0x")) {
                recordCount.incrementAndGet();
            }
        }
    }

    private static final class FiniteDocumentPollMethod implements Function<ExcerptTailer, DocumentContext> {

        private final int maxPollsReturningEmptyDocument;
        private int invocationCount;

        private FiniteDocumentPollMethod(final int maxPollsReturningEmptyDocument) {
            this.maxPollsReturningEmptyDocument = maxPollsReturningEmptyDocument;
        }

        @Override
        public DocumentContext apply(final ExcerptTailer excerptTailer) {
            final DocumentContext documentContext = excerptTailer.readingDocument();

            if (!documentContext.isPresent()) {
                invocationCount++;
                if (invocationCount >= maxPollsReturningEmptyDocument) {
                    throw new ArithmeticException("For testing purposes");
                }
            }

            return documentContext;
        }
    }
}