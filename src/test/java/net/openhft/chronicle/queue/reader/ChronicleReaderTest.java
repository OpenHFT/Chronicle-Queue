package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.bytes.MethodWriterBuilder;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Ignore;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class ChronicleReaderTest {
    private static final byte[] ONE_KILOBYTE = new byte[1024];
    private static final String LAST_MESSAGE = "LAST_MESSAGE";

    static {
        Arrays.fill(ONE_KILOBYTE, (byte) 7);
    }

    private final Queue<String> capturedOutput = new ConcurrentLinkedQueue<>();
    private Path dataDir;

    @Before
    public void before() throws Exception {
        dataDir = DirectoryUtils.tempDir(ChronicleReaderTest.class.getSimpleName()).toPath();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).testBlockSize().build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final MethodWriterBuilder<StringEvents> methodWriterBuilder = excerptAppender.methodWriterBuilder(StringEvents.class);
            methodWriterBuilder.recordHistory(true);
            final StringEvents events = methodWriterBuilder.build();

            for (int i = 0; i < 24; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }
    }

    @Test
    public void shouldNotFailOnEmptyQueue() {
        Path path = DirectoryUtils.tempDir("shouldNotFailOnEmptyQueue").toPath();
        path.toFile().mkdirs();
        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertTrue(capturedOutput.isEmpty());
    }

    @Test
    public void shouldFailWhenNoDirectoryListing() throws IOException {
        Files.list(dataDir).filter(f -> f.getFileName().toString().endsWith(SingleTableBuilder.SUFFIX)).findFirst().ifPresent(path -> path.toFile().delete());
        basicReader().execute();
        assertThat(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), is(true));
    }

    @Test
    public void shouldIncludeMessageHistoryByDefault() throws Exception {
        basicReader().execute();

        assertThat(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), is(true));
    }

    @Test
    public void shouldApplyIncludeRegexToHistoryMessagesAndBusinessMessages() throws Exception {
        basicReader().
                // matches goodbye, but not hello or history
                withInclusionRegex("goodbye").
                asMethodReader().
                execute();
        assertThat(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), is(false));
    }

    @Test(timeout = 5000)
    public void readOnlyQueueTailerShouldObserveChangesAfterInitiallyObservedReadLimit() throws Exception {
        DirectoryUtils.deleteDir(dataDir.toFile());
        dataDir.toFile().mkdirs();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).testBlockSize().build()) {

            final StringEvents events = queue.acquireAppender().methodWriterBuilder(StringEvents.class).build();
            events.say("hello");

            final long readerCapacity = getCurrentQueueFileLength(dataDir);

            final RecordCounter recordCounter = new RecordCounter();
            final ChronicleReader chronicleReader = basicReader().withMessageSink(recordCounter);

            final ExecutorService executorService = Executors.newSingleThreadExecutor();
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

            assertEquals(expectedReadingDocumentCount, recordCounter.recordCount.get() - 1);
        }
    }

    @Test
    @Ignore("TODO FIX")
    public void readOnlyQueueTailerInFollowModeShouldObserveChangesAfterInitiallyObservedReadLimit() throws Exception {
        DirectoryUtils.deleteDir(dataDir.toFile());
        dataDir.toFile().mkdirs();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).testBlockSize().build()) {

            final StringEvents events = queue.acquireAppender().methodWriterBuilder(StringEvents.class).build();
            events.say("hello");

            final long readerCapacity = getCurrentQueueFileLength(dataDir);

            final AtomicReference<String> messageReceiver = new AtomicReference<>();
            final ChronicleReader chronicleReader = basicReader().tail().
                    withMessageSink(messageReceiver::set);

            final ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<?> submit = executorService.submit(chronicleReader::execute);

            final long expectedReadingDocumentCount = (readerCapacity / ONE_KILOBYTE.length) + 1;
            int i;
            for (i = 0; i < expectedReadingDocumentCount; i++) {
                events.say(new String(ONE_KILOBYTE));
            }
            events.say(LAST_MESSAGE);

            while (!(messageReceiver.get() != null && messageReceiver.get().contains(LAST_MESSAGE))) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000L));
            }
            executorService.shutdownNow();
            executorService.awaitTermination(5L, TimeUnit.SECONDS);
            submit.get();
        }
    }

    @Test
    public void shouldBeAbleToReadFromReadOnlyFile() throws Exception {
        final Path queueFile = Files.list(dataDir).
                filter(f -> f.getFileName().toString().endsWith(SingleChronicleQueue.SUFFIX)).findFirst().
                orElseThrow(() ->
                new AssertionError("Could not find queue file in directory " + dataDir));

        assertThat(queueFile.toFile().setWritable(false), is(true));

        basicReader().execute();
    }

    @Test
    public void shouldConvertEntriesToText() throws Exception {
        basicReader().execute();

        assertThat(capturedOutput.size(), is(48));
        assertThat(capturedOutput.stream().anyMatch(msg -> msg.contains("hello")), is(true));
    }

    @Test
    public void shouldFilterByInclusionRegex() throws Exception {
        basicReader().withInclusionRegex(".*good.*").execute();

        assertThat(capturedOutput.size(), is(24));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, containsString("goodbye")));
    }

    @Test
    public void shouldFilterByMultipleInclusionRegex() throws Exception {
        basicReader().withInclusionRegex(".*bye$").withInclusionRegex(".*o.*").execute();

        assertThat(capturedOutput.size(), is(24));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, containsString("goodbye")));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, not(containsString("hello"))));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfInputDirectoryDoesNotExist() throws Exception {
        basicReader().withBasePath(Paths.get("/does/not/exist")).execute();
    }

    @Test
    public void shouldFilterByExclusionRegex() throws Exception {
        basicReader().withExclusionRegex(".*good.*").execute();

        assertThat(capturedOutput.size(), is(24));
        capturedOutput.forEach(msg -> assertThat(msg, not(containsString("goodbye"))));
    }

    @Test
    public void shouldFilterByMultipleExclusionRegex() throws Exception {
        basicReader().withExclusionRegex(".*bye$").withExclusionRegex(".*ell.*").execute();

        assertThat(capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).count(), is(0L));
    }

    @Test
    public void shouldReturnNoMoreThanTheSpecifiedNumberOfMaxRecords() throws Exception {
        basicReader().historyRecords(5).execute();

        assertThat(capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), is(5L));
    }

    @Test
    public void shouldForwardToSpecifiedIndex() throws Exception {
        final long knownIndex = Long.decode(findAnExistingIndex());
        basicReader().withStartIndex(knownIndex).execute();

        assertThat(capturedOutput.size(), is(25));
        // discard first message
        capturedOutput.poll();
        assertThat(capturedOutput.poll().contains(Long.toHexString(knownIndex)), is(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailIfSpecifiedIndexIsBeforeFirstIndex() {
        basicReader().withStartIndex(1L).execute();
    }

    @Test
    public void shouldNotRewindPastStartOfQueueWhenDisplayingHistory() throws Exception {
        basicReader().historyRecords(Long.MAX_VALUE).execute();

        assertThat(capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), is(24L));
    }

    @Test
    public void shouldContinueToPollQueueWhenTailModeIsEnabled() throws Exception {
        final int expectedPollCountWhenDocumentIsEmpty = 3;
        final FiniteDocumentPollMethod pollMethod = new FiniteDocumentPollMethod(expectedPollCountWhenDocumentIsEmpty);
        try {
            basicReader().withDocumentPollMethod(pollMethod).tail().execute();
        } catch (ArithmeticException e) {
            // expected
        }

        assertThat(pollMethod.invocationCount, is(expectedPollCountWhenDocumentIsEmpty));
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

    private static long getCurrentQueueFileLength(final Path dataDir) throws IOException {
        return new RandomAccessFile(
                Files.list(dataDir).filter(p -> p.toString().endsWith("cq4")).findFirst().
                        orElseThrow(AssertionError::new).toFile(), "r").length();
    }

    private String findAnExistingIndex() {
        basicReader().execute();
        final List<String> indicies = capturedOutput.stream().
                filter(s -> s.startsWith("0x")).
                collect(Collectors.toList());
        capturedOutput.clear();
        return indicies.get(indicies.size() / 2).trim().replaceAll(":", "");
    }

    private ChronicleReader basicReader() {
        return new ChronicleReader().
                withBasePath(dataDir).withMessageSink(capturedOutput::add);
    }

    @FunctionalInterface
    private interface StringEvents {
        void say(final String msg);
    }
}