package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class ChronicleReaderTest extends ChronicleQueueTestBase {
    private static final byte[] ONE_KILOBYTE = new byte[1024];
    private static final long TOTAL_EXCERPTS_IN_QUEUE = 24;

    static {
        Arrays.fill(ONE_KILOBYTE, (byte) 7);
    }

    private final Queue<String> capturedOutput = new ConcurrentLinkedQueue<>();
    private Path dataDir;
    private long lastIndex = Long.MIN_VALUE;
    private long firstIndex = Long.MAX_VALUE;

    private static long getCurrentQueueFileLength(final Path dataDir) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(
                Files.list(dataDir).filter(p -> p.toString().endsWith("cq4")).findFirst().
                        orElseThrow(AssertionError::new).toFile(), "r")) {
            return file.length();
        }
    }

    @Before
    public void before() {
        // Reader opens queues in read-only mode
        if (OS.isWindows())
            if (!(testName.getMethodName().equals("shouldThrowExceptionIfInputDirectoryDoesNotExist") || testName.getMethodName().equals("shouldBeAbleToReadFromReadOnlyFile")))
                expectException("Read-only mode is not supported on Windows");

        dataDir = getTmpDir().toPath();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .sourceId(1)
                .testBlockSize().build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder =
                    excerptAppender.methodWriterBuilder(Say.class);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < TOTAL_EXCERPTS_IN_QUEUE; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
            lastIndex = queue.lastIndex();
            firstIndex = queue.firstIndex();
        }
        ignoreException("Overriding sourceId from existing metadata, was 0, overriding to 1");
    }

    @Test(timeout = 10_000L)
    public void shouldReadQueueInReverse() {
        addCountToEndOfQueue();

        new ChronicleReader().withBasePath(dataDir)
                .withMessageSink(capturedOutput::add)
                .inReverseOrder()
                .suppressDisplayIndex()
                .execute();
        final List<String> firstFourElements = capturedOutput.stream().limit(4).collect(Collectors.toList());
        assertEquals(Arrays.asList("\"4\"\n", "\"3\"\n", "\"2\"\n", "\"1\"\n"), firstFourElements);
    }

    @Test
    public void reverseOrderShouldIgnoreOptionsThatDontMakeSense() {
        addCountToEndOfQueue();

        new ChronicleReader().withBasePath(dataDir)
                .withMessageSink(capturedOutput::add)
                .inReverseOrder()
                .suppressDisplayIndex()
                .tail()               // Ignored
                .historyRecords(10)   // Ignored
                .execute();
        final List<String> firstFourElements = capturedOutput.stream().limit(4).collect(Collectors.toList());
        assertEquals(Arrays.asList("\"4\"\n", "\"3\"\n", "\"2\"\n", "\"1\"\n"), firstFourElements);
    }

    @Test
    public void reverseOrderWorksWithStartPosition() {
        List<Long> indices = addCountToEndOfQueue();

        new ChronicleReader().withBasePath(dataDir)
                .withMessageSink(capturedOutput::add)
                .inReverseOrder()
                .suppressDisplayIndex()
                .withStartIndex(indices.get(1))
                .execute();
        final List<String> firstFourElements = capturedOutput.stream().limit(2).collect(Collectors.toList());
        assertEquals(Arrays.asList("\"2\"\n", "\"1\"\n"), firstFourElements);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reverseOrderThrowsWhenStartPositionIsAfterEndOfQueue() {
        new ChronicleReader().withBasePath(dataDir)
                .withMessageSink(capturedOutput::add)
                .inReverseOrder()
                .suppressDisplayIndex()
                .withStartIndex(lastIndex + 1)
                .execute();
    }

    @Test(expected = IllegalArgumentException.class)
    public void reverseOrderThrowsWhenStartPositionIsBeforeStartOfQueue() {
        new ChronicleReader().withBasePath(dataDir)
                .withMessageSink(capturedOutput::add)
                .inReverseOrder()
                .suppressDisplayIndex()
                .withStartIndex(firstIndex - 1)
                .execute();
    }

    private List<Long> addCountToEndOfQueue() {
        List<Long> indices = new ArrayList<>();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .sourceId(1)
                .testBlockSize().build()) {
            try (final ExcerptAppender appender = queue.acquireAppender()) {
                for (int i = 1; i < 5; i++) {
                    appender.writeText(String.valueOf(i));
                    indices.add(appender.lastIndexAppended());
                }
            }
        }
        return indices;
    }

    @Test(timeout = 10_000L)
    public void shouldReadQueueWithNonDefaultRollCycle() {
        expectException("Overriding roll length from existing metadata");
//        expectException("Overriding roll cycle from");
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(RollCycles.MINUTELY).
                testBlockSize().sourceId(1).build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder = excerptAppender.methodWriterBuilder(Say.class);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < TOTAL_EXCERPTS_IN_QUEUE; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }

        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertFalse(capturedOutput.isEmpty());
    }

    @Test(timeout = 10_000L)
    public void shouldReadQueueWithNonDefaultRollCycleWhenMetadataDeleted() throws IOException {
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(RollCycles.MINUTELY).
                testBlockSize().sourceId(1).build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder = excerptAppender.methodWriterBuilder(Say.class);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < TOTAL_EXCERPTS_IN_QUEUE; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }
        Files.list(path).filter(f -> f.getFileName().toString().endsWith(SingleTableStore.SUFFIX)).findFirst().ifPresent(p -> p.toFile().delete());
        waitForGcCycle();

        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertFalse(capturedOutput.isEmpty());
    }

    @Test
    public void shouldNotFailOnEmptyQueue() {
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertTrue(capturedOutput.isEmpty());
    }

    @Test
    public void shouldNotFailWhenNoMetadata() throws IOException {
        if (!OS.isWindows())
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
    public void shouldApplyIncludeRegexToHistoryMessagesAndBusinessMessagesMethodReaderDummy() {
        basicReader()
                // matches goodbye, but not hello or history
                .withInclusionRegex("goodbye")
                .asMethodReader(null)
                .execute();
        assertFalse(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")));
    }

    @Test
    public void shouldNotIncludeMessageHistoryByDefaultMethodReader() {
        basicReader().
                asMethodReader(Say.class.getName()).
                execute();

        assertFalse(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")));
    }

    @Test
    public void shouldIncludeMessageHistoryMethodReaderShowHistory() {
        basicReader().
                asMethodReader(Say.class.getName()).
                showMessageHistory(true).
                execute();

        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("MessageHistory")));
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
        assumeFalse("#460 read-only not supported on Windows", OS.isWindows());

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

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE, capturedOutput.size());
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, containsString("goodbye")));
    }

    @Test
    public void shouldFilterByMultipleInclusionRegex() {
        basicReader().withInclusionRegex(".*bye$").withInclusionRegex(".*o.*").execute();

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE, capturedOutput.size());
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

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE, capturedOutput.size());
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

        assertEquals(5L, capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count());
    }

    @Test
    public void shouldCombineIncludeFilterAndMaxRecords() {
        basicReader().historyRecords(5).withInclusionRegex("hello").execute();

        assertEquals(2L, capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count());
    }

    @Test
    public void shouldForwardToSpecifiedIndex() {
        final long knownIndex = Long.decode(findAnExistingIndex());
        basicReader().withStartIndex(knownIndex).execute();

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE, capturedOutput.size());
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
                filter(msg -> !msg.startsWith("0x")).count(), is(TOTAL_EXCERPTS_IN_QUEUE));
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

    @Test
    public void shouldPrintTimestampsToLocalTime() {
        final File queueDir = getTmpDir();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build();
             final ExcerptAppender excerptAppender = queue.acquireAppender()) {
            final VanillaMethodWriterBuilder<SayWhen> methodWriterBuilder =
                    excerptAppender.methodWriterBuilder(SayWhen.class);
            final SayWhen events = methodWriterBuilder.build();

            long microTimestamp = System.currentTimeMillis() * 1000;
            List<Long> timestamps = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                events.sayWhen(microTimestamp, "Hello!");
                timestamps.add(microTimestamp);
                microTimestamp += 1000 * i;
            }

            // UTC by default
            System.clearProperty(AbstractTimestampLongConverter.TIMESTAMP_LONG_CONVERTERS_ZONE_ID_SYSTEM_PROPERTY);
            assertTimesAreInZone(queueDir, ZoneId.of("UTC"), timestamps);

            // Local timezone
            System.setProperty(AbstractTimestampLongConverter.TIMESTAMP_LONG_CONVERTERS_ZONE_ID_SYSTEM_PROPERTY, ZoneId.systemDefault().toString());
            assertTimesAreInZone(queueDir, ZoneId.systemDefault(), timestamps);
        } finally {
            System.clearProperty(AbstractTimestampLongConverter.TIMESTAMP_LONG_CONVERTERS_ZONE_ID_SYSTEM_PROPERTY);
        }
    }

    @Test
    public void shouldOnlyOutputUpToMatchLimitAfterFiltering() {
        basicReader().withInclusionRegex("goodbye").withMatchLimit(3).execute();

        final List<String> matchedMessages = capturedOutput.stream()
                .filter(msg -> !msg.startsWith("0x"))
                .collect(Collectors.toList());
        assertEquals(3, matchedMessages.size());
        assertTrue(matchedMessages.stream().allMatch(s -> s.contains("goodbye")));
    }

    private void assertTimesAreInZone(File queueDir, ZoneId zoneId, List<Long> timestamps) {
        ChronicleReader reader = new ChronicleReader()
                .asMethodReader(SayWhen.class.getName())
                .withBasePath(queueDir.toPath())
                .withMessageSink(capturedOutput::add);
        reader.execute();

        MicroTimestampLongConverter mtlc = new MicroTimestampLongConverter(zoneId.toString());
        int i = 0;
        while (!capturedOutput.isEmpty()) {
            final String actualValue = capturedOutput.poll();
            if (actualValue.contains("sayWhen")) {
                final String expectedTimestamp = mtlc.asString(timestamps.get(i++));
                assertTrue(String.format("%s contains %s", actualValue, expectedTimestamp), actualValue.contains(expectedTimestamp));
            }
        }
        assertEquals("Didn't check all the timestamps", timestamps.size(), i);
    }

    @Test
    public void findByBinarySearch() {
        final File queueDir = getTmpDir();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build();
             final ExcerptAppender excerptAppender = queue.acquireAppender()) {

            TimeUnit timeUnit = ServicesTimestampLongConverter.timeUnit();
            long start = timeUnit.convert(1610000000000L, TimeUnit.MILLISECONDS);
            int max = 10;
            for (long i = 0; i < max; i++) {
                long ts = start + i * timeUnit.convert(1, TimeUnit.SECONDS);
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write(TimestampComparator.TS).int64(ts);
                }
            }

            int eventToStartAt = 3;
            long tsToLookFor = start + timeUnit.convert(eventToStartAt, TimeUnit.SECONDS);
            ChronicleReader reader = new ChronicleReader()
                    .withArg(ServicesTimestampLongConverter.INSTANCE.asString(tsToLookFor))
                    .withBinarySearch(TimestampComparator.class.getCanonicalName())
                    .withBasePath(queueDir.toPath())
                    .withMessageSink(capturedOutput::add);
            reader.execute();
            assertEquals(max - eventToStartAt, capturedOutput.size() / 2);
        }
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