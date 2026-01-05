/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.queue.reader.ContentBasedLimiter;
import net.openhft.chronicle.queue.reader.Reader;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static net.openhft.chronicle.testframework.GcControls.waitForGcCycle;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@SuppressWarnings({"deprecation", "removal"})
public class ChronicleReaderTest extends QueueTestCommon {
    private static final byte[] ONE_KILOBYTE = new byte[1024];
    private static final long TOTAL_EXCERPTS_IN_QUEUE = 24;

    static {
        Arrays.fill(ONE_KILOBYTE, (byte) 7);
    }

    private final Queue<String> capturedOutput = new ConcurrentLinkedQueue<>();
    private Path dataDir;
    private long lastIndex = Long.MIN_VALUE;
    private long firstIndex = Long.MAX_VALUE;
    private String testMethodName = "";

    private static long getCurrentQueueFileLength(final Path dataDir) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(
                Files.list(dataDir).filter(p -> p.toString().endsWith("cq4")).findFirst().
                        orElseThrow(AssertionError::new).toFile(), "r")) {
            return file.length();
        }
    }

    @BeforeEach
    public void before(TestInfo testInfo) {
        testMethodName = testInfo.getTestMethod().map(method -> method.getName()).orElse("");
        assumeFalse(Jvm.isArm());

        // Reader opens queues in read-only mode
        if (OS.isWindows())
            if (!(testMethodName.equals("shouldThrowExceptionIfInputDirectoryDoesNotExist") ||
                    testMethodName.equals("shouldNotShowIndexForHistoryMessages") ||
                    testMethodName.equals("shouldBeAbleToReadFromReadOnlyFile") ||
                    testMethodName.equals("shouldPrintTimestampsToLocalTime") ||
                    testMethodName.equals("namedTailerRequiresReadWrite") ||
                    testMethodName.equals("matchLimitThenNamedTailer")))
                expectException("Read-only mode is not supported on Windows");

        dataDir = getTmpDir().toPath();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .sourceId(1)
                .testBlockSize().build()) {
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder =
                    queue.methodWriterBuilder(Say.class);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < TOTAL_EXCERPTS_IN_QUEUE; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
            lastIndex = queue.lastIndex();
            firstIndex = queue.firstIndex();
        }
        ignoreException("Overriding sourceId from existing metadata, was 0, overriding to 1");
    }

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
    public void shouldReadQueueInReverse() {
        addCountToEndOfQueue();

        new ChronicleReader().withBasePath(dataDir)
                .withMessageSink(capturedOutput::add)
                .inReverseOrder()
                .suppressDisplayIndex()
                .execute();
        final List<String> firstFourElements = capturedOutput.stream().limit(4).collect(Collectors.toList());
        assertEquals(Arrays.asList("\"4\"\n", "\"3\"\n", "\"2\"\n", "\"1\"\n"), firstFourElements, "reading queue in reverse order should return messages from newest to oldest");
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
        assertEquals(Arrays.asList("\"4\"\n", "\"3\"\n", "\"2\"\n", "\"1\"\n"), firstFourElements, "reverse order reader should ignore incompatible tail and history options");
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
        assertEquals(Arrays.asList("\"2\"\n", "\"1\"\n"), firstFourElements, "reverse order reader with start index should read backwards from specified position");
    }

    @Test
    public void reverseOrderThrowsWhenStartPositionIsAfterEndOfQueue() {
        assertThrows(IllegalArgumentException.class, () -> new ChronicleReader().withBasePath(dataDir)
                        .withMessageSink(capturedOutput::add)
                        .inReverseOrder()
                        .suppressDisplayIndex()
                        .withStartIndex(lastIndex + 1)
                        .execute(),
                "reverse order reader should throw exception when start index is beyond queue end");
    }

    @Test
    public void reverseOrderThrowsWhenStartPositionIsBeforeStartOfQueue() {
        assertThrows(IllegalArgumentException.class, () -> new ChronicleReader().withBasePath(dataDir)
                        .withMessageSink(capturedOutput::add)
                        .inReverseOrder()
                        .suppressDisplayIndex()
                        .withStartIndex(firstIndex - 1)
                        .execute(),
                "reverse order reader should throw exception when start index is before queue beginning");
    }

    private List<Long> addCountToEndOfQueue() {
        List<Long> indices = new ArrayList<>();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .sourceId(1)
                .testBlockSize().build();
             final ExcerptAppender appender = queue.createAppender()) {
            for (int i = 1; i < 5; i++) {
                appender.writeText(String.valueOf(i));
                indices.add(appender.lastIndexAppended());
            }
        }
        return indices;
    }

    // CPD-OFF - duplicated setup for metadata deletion variants
    @Test
    @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
    public void shouldReadQueueWithNonDefaultRollCycle() {
        expectException("Overriding roll length from existing metadata");
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(MINUTELY).
                testBlockSize().sourceId(1).build()) {
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder = queue.methodWriterBuilder(Say.class);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < TOTAL_EXCERPTS_IN_QUEUE; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }

        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertFalse(capturedOutput.isEmpty(), "reader should successfully read queue created with non-default roll cycle");
    }

    @Test
    @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
    public void shouldReadQueueWithNonDefaultRollCycleWhenMetadataDeleted() throws IOException {
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).rollCycle(MINUTELY).
                testBlockSize().sourceId(1).build()) {
            final VanillaMethodWriterBuilder<Say> methodWriterBuilder = queue.methodWriterBuilder(Say.class);
            final Say events = methodWriterBuilder.build();

            for (int i = 0; i < TOTAL_EXCERPTS_IN_QUEUE; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }
        Files.list(path).filter(f -> f.getFileName().toString().endsWith(SingleTableStore.SUFFIX)).findFirst().ifPresent(p -> p.toFile().delete());
        waitForGcCycle();

        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertFalse(capturedOutput.isEmpty(), "reader should successfully read queue with non-default roll cycle after metadata deletion");
    }
    // CPD-ON

    @Test
    public void shouldNotFailOnEmptyQueue() {
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add).execute();
        assertTrue(capturedOutput.isEmpty(), "reading empty queue should produce no output");
    }

    @Test
    public void shouldNotFailWhenNoMetadata() throws IOException {
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        Files.list(dataDir).filter(f -> f.getFileName().toString().endsWith(SingleTableStore.SUFFIX)).findFirst().ifPresent(path -> path.toFile().delete());
        basicReader().execute();
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), "reader should include message history when metadata is missing");
    }

    @Test
    public void shouldIncludeMessageHistoryByDefault() {
        basicReader().execute();

        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), "reader output should include message history by default");
    }

    @Test
    public void shouldApplyIncludeRegexToHistoryMessagesAndBusinessMessagesMethodReaderDummy() {
        basicReader()
                // matches goodbye, but not hello or history
                .withInclusionRegex("goodbye")
                .asMethodReader("")
                .execute();
        assertFalse(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), "inclusion regex should filter out history messages when not matching pattern");
    }

    @Test
    public void shouldNotShowIndexForFilteredMessages() {
        basicReader()
                .asMethodReader(SayWhen.class.getName())
                .execute();

        assertTrue(capturedOutput.isEmpty(), "method reader filtering non-matching messages should produce no output");
    }

    @Test
    public void shouldNotShowIndexForHistoryMessages() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).testBlockSize().build();
             final ExcerptAppender appender = queue.createAppender()) {
            try (DocumentContext dc = appender.writingDocument()) {
                MessageHistory.writeHistory(dc);
            }
        }
        basicReader()
                .asMethodReader(SayWhen.class.getName())
                .execute();
        assertTrue(capturedOutput.isEmpty(), "method reader should not display indices for history-only messages");
    }

    @Test
    public void canReadPastEmptyMessageInReverseOrder() {
        dataDir = getTmpDir().toPath();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .sourceId(1)
                .testBlockSize()
                .build()) {
            final VanillaMethodWriterBuilder<ChronicleMethodReaderTest.All> methodWriterBuilder = queue.methodWriterBuilder(ChronicleMethodReaderTest.All.class);
            final ChronicleMethodReaderTest.All events = methodWriterBuilder.build();
            final ExcerptAppender appender = queue.createAppender();

            for (int i = 0; i < 3; ) {
                ChronicleMethodReaderTest.Method1Type m1 = new ChronicleMethodReaderTest.Method1Type();
                m1.text = "hello";
                m1.value = i;
                m1.number = i;
                events.method1(m1);

                try (DocumentContext dc = appender.writingDocument()) {
                    MessageHistory.writeHistory(dc);
                }

                i++;
                ChronicleMethodReaderTest.Method2Type m2 = new ChronicleMethodReaderTest.Method2Type();
                m2.text = "goodbye";
                m2.value = i;
                m2.number = i;
                events.method2(m2);
                i++;
            }
        }

        ChronicleReader methodReaderForQueue = new ChronicleReader()
                .withBasePath(dataDir)
                .asMethodReader(ChronicleMethodReaderTest.All.class.getName())
                .inReverseOrder()
                .withMessageSink(capturedOutput::add);

        methodReaderForQueue.execute();

        assertEquals(8, capturedOutput.size(), "reverse order method reader should produce expected number of output lines");
        capturedOutput.poll();
        String message = capturedOutput.poll();
        assertNotNull(message, "first content message polled from reverse order output should not be null");
        assertTrue(message.contains("goodbye"), "first content message in reverse order should be goodbye");
        capturedOutput.poll();
        message = capturedOutput.poll();
        assertNotNull(message, "second content message polled from reverse order output should not be null");
        assertTrue(message.contains("hello"), "second content message in reverse order should be hello");
        capturedOutput.poll();
        message = capturedOutput.poll();
        assertNotNull(message, "third content message polled from reverse order output should not be null");
        assertTrue(message.contains("goodbye"), "third content message in reverse order should be goodbye");
        capturedOutput.poll();
        message = capturedOutput.poll();
        assertNotNull(message, "fourth content message polled from reverse order output should not be null");
        assertTrue(message.contains("hello"), "fourth content message in reverse order should be hello");
        capturedOutput.poll();
    }

    @Test
    public void shouldNotIncludeMessageHistoryByDefaultMethodReader() {
        basicReader().
                asMethodReader(Say.class.getName()).
                execute();

        assertFalse(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), "method reader output should not include message history by default");
    }

    @Test
    public void shouldIncludeMessageHistoryMethodReaderShowHistory() {
        basicReader().
                asMethodReader(Say.class.getName()).
                showMessageHistory(true).
                execute();

        String first = capturedOutput.poll();
        assertTrue(first.startsWith("0x"), "method reader with history enabled should output index as first line");
        String second = capturedOutput.poll();
        assertNotNull(second, "method reader with history enabled should output message content as second line");
        assertTrue(second.matches("VanillaMessageHistory *. *sources: ..,? timings: .[0-9]+.,? addSourceDetails=false ?}" +
                        System.lineSeparator() +
                        "say: hello\n" +
                        "...\n"),
                second);
    }

    @Test
    @Timeout(value = 5000L, unit = TimeUnit.MILLISECONDS)
    public void readOnlyQueueTailerShouldObserveChangesAfterInitiallyObservedReadLimit() throws IOException, InterruptedException, TimeoutException, ExecutionException {
        IOTools.deleteDirWithFiles(dataDir.toFile());
        dataDir.toFile().mkdirs();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).testBlockSize().build()) {

            final Say events = queue.methodWriterBuilder(Say.class).build();
            events.say("hello");

            final long readerCapacity = getCurrentQueueFileLength(dataDir);

            final RecordCounter recordCounter = new RecordCounter();
            final ChronicleReader chronicleReader = basicReader().withMessageSink(recordCounter);

            final ExecutorService executorService = Executors.newSingleThreadExecutor(
                    new NamedThreadFactory("executor"));
            final Future<?> submit = executorService.submit(chronicleReader::execute);

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
                assertEquals(expectedReadingDocumentCount, recordCounter.recordCount.get() - 1, "read-only queue tailer should observe changes after initial read limit");
        }
    }

    @Test
    public void shouldBeAbleToReadFromReadOnlyFile() throws IOException {
        assumeFalse(OS.isWindows(), "#460 read-only not supported on Windows");

        final Path queueFile = Files.list(dataDir).
                filter(f -> f.getFileName().toString().endsWith(SingleChronicleQueue.SUFFIX)).findFirst().
                orElseThrow(() ->
                        new AssertionError("Could not find queue file in directory " + dataDir));

        assertTrue(queueFile.toFile().setWritable(false), "setting file to read-only should succeed");

        basicReader().execute();
    }

    @Test
    public void shouldConvertEntriesToText() {
        basicReader().execute();

        assertEquals(48, capturedOutput.size(), "reader should convert all queue entries to text output");
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("hello")), "text output should contain expected message content");
    }

    @Test
    public void shouldFilterByInclusionRegex() {
        basicReader().withInclusionRegex(".*good.*").execute();

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE, capturedOutput.size(), "inclusion regex should match expected number of messages");
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertTrue(msg.contains("goodbye"), "filtered output should only contain messages matching inclusion regex"));
    }

    @Test
    public void shouldFilterByMultipleInclusionRegex() {
        basicReader().withInclusionRegex(".*bye$").withInclusionRegex(".*o.*").execute();

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE, capturedOutput.size(), "multiple inclusion regexes should match expected number of messages");
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertTrue(msg.contains("goodbye"), "filtered output should contain messages matching all inclusion patterns"));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertFalse(msg.contains("hello"), "filtered output should exclude messages not matching all inclusion patterns"));
    }

    @Test
    public void shouldThrowExceptionIfInputDirectoryDoesNotExist() {
        assertThrows(IllegalArgumentException.class, () -> basicReader().withBasePath(Paths.get("/does/not/exist")).execute(),
                "reader should throw exception when input directory does not exist");
    }

    @Test
    public void shouldFilterByExclusionRegex() {
        basicReader().withExclusionRegex(".*good.*").execute();

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE, capturedOutput.size(), "exclusion regex should produce expected number of output lines");
        capturedOutput.forEach(msg -> assertFalse(msg.contains("goodbye"), "filtered output should not contain messages matching exclusion regex"));
    }

    @Test
    public void shouldFilterByMultipleExclusionRegex() {
        basicReader().withExclusionRegex(".*bye$").withExclusionRegex(".*ell.*").execute();

        assertEquals(0L, capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).count(), "multiple exclusion regexes matching all messages should produce no content output");
    }

    @Test
    public void shouldReturnNoMoreThanTheSpecifiedNumberOfMaxRecords() {
        basicReader().historyRecords(5).execute();

        assertEquals(5L, capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), "history records limit should restrict output to specified number of messages");
    }

    @Test
    public void shouldCombineIncludeFilterAndMaxRecords() {
        basicReader().historyRecords(5).withInclusionRegex("hello").execute();

        assertEquals(2L, capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), "history records limit combined with inclusion regex should produce filtered result count");
    }

    @Test
    public void shouldForwardToSpecifiedIndex() {
        final long knownIndex = Long.decode(findAnExistingIndex());
        basicReader().withStartIndex(knownIndex).execute();

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE, capturedOutput.size(), "reader starting from specified index should produce expected output size");
        assertTrue(capturedOutput.poll().contains(Long.toHexString(knownIndex)), "first output line should contain the specified start index");
    }

    @Test
    public void shouldFailIfSpecifiedIndexIsBeforeFirstIndex() {
        assertThrows(IllegalArgumentException.class, () -> basicReader().withStartIndex(1L).execute(),
                "reader should throw exception when start index is before first available index");
    }

    @Test
    public void shouldNotRewindPastStartOfQueueWhenDisplayingHistory() {
        basicReader().historyRecords(Long.MAX_VALUE).execute();

        assertEquals(TOTAL_EXCERPTS_IN_QUEUE,
                capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).count(),
                "history records with large limit should not rewind past queue start");
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

        assertEquals(expectedPollCountWhenDocumentIsEmpty, pollMethod.invocationCount, "tail mode should continue polling empty documents until limit reached");
    }

    @RequiredForClient
    @Test
    @Timeout(value = 20_000L, unit = TimeUnit.MILLISECONDS)
    public void shouldPrintTimestampsToLocalTime() throws IOException {
        finishedNormally = false;
        final File queueDir = getTmpDir();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            final VanillaMethodWriterBuilder<SayWhen> methodWriterBuilder =
                    queue.methodWriterBuilder(SayWhen.class);
            final SayWhen events = methodWriterBuilder.build();

            long microTimestamp = System.currentTimeMillis() * 1000;
            List<Long> timestamps = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                events.sayWhen(microTimestamp, "Hello!");
                timestamps.add(microTimestamp);
                microTimestamp += 1000 * i;
            }

            // UTC by default
            assertTimesAreInZone(queueDir, ZoneId.of("UTC"), timestamps);

            // Local timezone
            assertTimesAreInZone(queueDir, ZoneId.systemDefault(), timestamps);
        }
        IOTools.deleteDirWithFiles(queueDir);
        finishedNormally = true;
    }

    @Test
    public void shouldOnlyOutputUpToMatchLimitAfterFiltering() {
        basicReader().withInclusionRegex("goodbye").withMatchLimit(3).execute();

        final List<String> matchedMessages = capturedOutput.stream()
                .filter(msg -> !msg.startsWith("0x"))
                .collect(Collectors.toList());
        assertEquals(3, matchedMessages.size(), "match limit should restrict output to specified number of matched messages");
        assertTrue(matchedMessages.stream().allMatch(s -> s.contains("goodbye")), "all matched messages should satisfy the inclusion filter");
    }

    @Test
    public void matchLimitThenNamedTailer() {
        final long maxRecords = 5;
        final String tailerId = "myTailer";
        basicReader().withMatchLimit(maxRecords).withReadOnly(false).withTailerId(tailerId).execute();

        assertEquals(maxRecords, capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), "first read with match limit should return specified number of messages");

        capturedOutput.clear();
        basicReader().withReadOnly(false).withTailerId(tailerId).execute();
        assertEquals(TOTAL_EXCERPTS_IN_QUEUE - maxRecords, capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), "named tailer should resume from previous position and read remaining messages");
    }

    @Test
    public void namedTailerRequiresReadWrite() {
        assumeFalse(OS.isWindows());
        assertThrows(IllegalArgumentException.class, () -> basicReader().withTailerId("tailerId").withReadOnly(true).execute(),
                "named tailer should throw exception when used with read-only mode");
    }

    @Test
    public void shouldStopReadingWhenContentBasedLimitHasBeenReached() {
        AtomicInteger helloCount = new AtomicInteger();
        AtomicInteger goodbyeCount = new AtomicInteger();
        final Say say = msg -> {
            if ("hello".equals(msg)) {
                helloCount.incrementAndGet();
            }
            if ("goodbye".equals(msg)) {
                goodbyeCount.incrementAndGet();
            }
        };
        final ContentBasedLimiter cbl = new ContentBasedLimiter() {

            private int limit = -1;

            @Override
            public boolean shouldHaltReading(DocumentContext dc) {
                dc.wire().bytes().readSkip(-4); // skip back to the start of the document context (this feels a tad horrid)
                final MethodReader methodReader = dc.wire().methodReader(say);
                methodReader.readOne();
                return helloCount.get() > limit;
            }

            @Override
            public void configure(Reader reader) {
                limit = Integer.parseInt(reader.limiterArg());
            }
        };
        basicReader().withContentBasedLimiter(cbl).withLimiterArg("4").execute();
        assertEquals(4, capturedOutput.stream().filter(msg -> msg.contains("hello")).count(), "content based limiter should stop reading after configured message threshold");
    }

    private void assertTimesAreInZone(File queueDir, ZoneId zoneId, List<Long> timestamps) throws IOException {
        final Process readerProcess = JavaProcessBuilder.create(ChronicleReaderRunner.class)
                .withProgramArguments(queueDir.toString())
                .withJvmArguments("-D" + AbstractTimestampLongConverter.TIMESTAMP_LONG_CONVERTERS_ZONE_ID_SYSTEM_PROPERTY + "=" + zoneId.toString())
                .start();
        while (readerProcess.isAlive()) {
            Jvm.pause(10);
        }
        String output = new String(IOTools.readAsBytes(readerProcess.getInputStream()));
        MicroTimestampLongConverter mtlc = new MicroTimestampLongConverter(zoneId.toString());
        for (Long timestamp : timestamps) {
            final String expectedTimestamp = mtlc.asString(timestamp);
            int timestampIndex = output.indexOf(expectedTimestamp);
            assertTrue(timestampIndex > 0, String.format("reader output should contain expected timestamp formatted for zone: %s should be found in %s", expectedTimestamp, output));
            output = output.substring(timestampIndex + expectedTimestamp.length());
        }
    }

    @Test
    public void findByBinarySearch() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            int max = 10, reps = 5;
            populateQueueWithTimestamps(queue, max, reps);

            for (int i = 0; i < max; i++) {
                long tsToLookFor = getTimestampAtIndex(i);
                assertEquals(reps * (max - i), executeBinarySearch(queueDir, tsToLookFor, false), "binary search forward from timestamp should return messages from position to end at index " + i);
            }
        }
    }

    @Test
    public void findByBinarySearchReverse() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            int max = 10, reps = 5;
            populateQueueWithTimestamps(queue, max, reps);

            for (int i = 0; i < max; i++) {
                long tsToLookFor = getTimestampAtIndex(i);
                assertEquals(reps * (i + 1), executeBinarySearch(queueDir, tsToLookFor, true), "binary search reverse from timestamp should return messages from start to position at index " + i);
            }
        }
    }

    @Test
    public void findByBinarySearchSparseRepeated() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            writeSparseRepeatedData(queue);

            long tsToLookFor = getTimestampAtIndex(2);
            assertEquals(7, executeBinarySearch(queueDir, tsToLookFor, false), "binary search forward with repeated timestamps should find first occurrence and count to end");
        }
    }

    @Test
    public void findByBinarySearchSparseRepeatedReverse() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            writeSparseRepeatedData(queue);

            long tsToLookFor = getTimestampAtIndex(2);
            assertEquals(7, executeBinarySearch(queueDir, tsToLookFor, true), "binary search reverse with repeated timestamps should find last occurrence and count from start");
        }
    }

    @Test
    public void findByBinarySearchSparseApprox() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            writeSparseApproxData(queue);

            long tsToLookFor = getTimestampAtIndex(3);
            assertEquals(3, executeBinarySearch(queueDir, tsToLookFor, false), "binary search forward with approximate timestamp should find nearest match and count to end");
        }
    }

    @Test
    public void findByBinarySearchSparseApproxReverse() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            writeSparseApproxData(queue);

            long tsToLookFor = getTimestampAtIndex(3);
            assertEquals(3, executeBinarySearch(queueDir, tsToLookFor, true), "binary search reverse with approximate timestamp should find nearest match and count from start");
        }
    }

    @Test
    public void findByBinarySearchApprox() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            final int reps = 5;
            final int max = 10;
            populateQueueWithTimestamps(queue, max, reps);

            for (int i = 0; i < max; i++) {
                long tsToLookFor = getTimestampAtIndex(i) - 1;
                assertEquals(reps * (max - i), executeBinarySearch(queueDir, tsToLookFor, false), "binary search forward with timestamp before exact match should find next closest entry at index " + i);
            }
        }
    }

    @Test
    public void findByBinarySearchApproxReverse() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            final int reps = 5;
            final int max = 10;
            populateQueueWithTimestamps(queue, max, reps);

            for (int i = 0; i < max; i++) {
                long tsToLookFor = getTimestampAtIndex(i) + 1;
                assertEquals(reps * (i + 1), executeBinarySearch(queueDir, tsToLookFor, true), "binary search reverse with timestamp after exact match should find previous closest entry at index " + i);
            }
        }
    }

    @Test
    public void findByBinarySearchAfterEnd() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            int max = 10, reps = 5;
            populateQueueWithTimestamps(queue, max, reps);

            // this should be after the end
            long tsToLookFor = getTimestampAtIndex(11);
            assertEquals(0, executeBinarySearch(queueDir, tsToLookFor, false), "binary search forward with timestamp after queue end should return no messages");
        }
    }

    @Test
    public void findByBinarySearchAfterEndReverse() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            int max = 10, reps = 5;
            populateQueueWithTimestamps(queue, max, reps);

            // this should be after the end
            long tsToLookFor = getTimestampAtIndex(11);
            assertEquals(max * reps, executeBinarySearch(queueDir, tsToLookFor, true), "binary search reverse with timestamp after queue end should return all messages");
        }
    }

    @Test
    public void findByBinarySearchBeforeStart() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            int max = 10, reps = 5;
            populateQueueWithTimestamps(queue, max, reps);

            // this should be before the start
            long tsToLookFor = getTimestampAtIndex(-1);
            assertEquals(max * reps, executeBinarySearch(queueDir, tsToLookFor, false), "binary search forward with timestamp before queue start should return all messages");
        }
    }

    @Test
    public void findByBinarySearchBeforeStartReverse() {
        final File queueDir = getTmpDir();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {

            int max = 10, reps = 5;
            populateQueueWithTimestamps(queue, max, reps);

            // this should be before the start
            long tsToLookFor = getTimestampAtIndex(-1);
            assertEquals(0, executeBinarySearch(queueDir, tsToLookFor, true), "binary search reverse with timestamp before queue start should return no messages");
        }
    }

    @Test
    public void findByBinarySearchWithDeletedRollCyles() {
        final File queueDir = getTmpDir();
        final SetTimeProvider timeProvider = new SetTimeProvider();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir)
                .timeProvider(timeProvider)
                .rollCycle(TEST_SECONDLY)
                .build()) {

            for (int i = 0; i < 5; i++) {
                int entries = 10, reps = 5;
                populateQueueWithTimestamps(queue, entries, reps, i);
                timeProvider.advanceMillis(3_000);
            }
        }
        // Just make sure Windows has closed all the files before we try to delete
        BackgroundResourceReleaser.releasePendingResources();

        // delete the 4th roll cycle
        assertTrue(queueDir.toPath().resolve("19700101-000009T.cq4").toFile().delete(), "test setup requires successful deletion of roll cycle file");

        // this should be before the start
        long tsToLookFor = getTimestampAtIndex(22); // third index in 3rd roll cycle, should be ({reps=5} * 8) + ({remaining_cycles=1} * ({reps=5} * {entries=10})) = 90 in output
        System.out.println(tsToLookFor);
        ChronicleReader reader = new ChronicleReader()
                .withArg(ServicesTimestampLongConverter.INSTANCE.asString(tsToLookFor))
                .withBinarySearch(TimestampComparator.class.getCanonicalName())
                .withBasePath(queueDir.toPath())
                .withMessageSink(capturedOutput::add);
        reader.execute();
        assertEquals(90 * 2, capturedOutput.size(), "binary search with deleted roll cycles should skip missing files and return remaining messages");
    }

    @Test
    public void shouldRespectWireType() {
        basicReader().
                asMethodReader(Say.class.getName()).
                withWireType(WireType.JSON).
                execute();

        capturedOutput.poll();
        assertEquals("{\"say\":\"hello\"}", capturedOutput.poll().trim(), "method reader with JSON wire type should output messages in JSON format");
    }

    @Test
    public void shouldRespectWireType2() {
        basicReader()
                .asMethodReader(Say.class.getName())
                .withWireType(WireType.JSON_ONLY)
                .execute();

        capturedOutput.poll();
        assertEquals("{\"say\":\"hello\"}", capturedOutput.poll().trim(), "method reader with JSON_ONLY wire type should output messages in JSON format");
    }

    private int executeBinarySearch(File queueDir, long tsToLookFor, boolean reverseOrder) {
        capturedOutput.clear();
        ChronicleReader reader = new ChronicleReader()
                .withArg(ServicesTimestampLongConverter.INSTANCE.asString(tsToLookFor))
                .withBinarySearch(TimestampComparator.class.getCanonicalName())
                .withBasePath(queueDir.toPath())
                .withMessageSink(capturedOutput::add);
        if (reverseOrder) {
            reader.inReverseOrder();
        }
        reader.execute();
        return capturedOutput.size() / 2;
    }

    private void writeSparseRepeatedData(SingleChronicleQueue queue) {
        try (ExcerptAppender appender = queue.createAppender()) {
            writeTimestamp(appender, getTimestampAtIndex(1));
            writeTimestamp(appender, getTimestampAtIndex(2));
            writeTimestamp(appender, getTimestampAtIndex(2));
            appender.writeText("aaaa");
            writeTimestamp(appender, getTimestampAtIndex(2));
            writeTimestamp(appender, getTimestampAtIndex(2));
            writeTimestamp(appender, getTimestampAtIndex(2));
            writeTimestamp(appender, getTimestampAtIndex(3));
        }
    }

    private void writeSparseApproxData(SingleChronicleQueue queue) {
        try (ExcerptAppender appender = queue.createAppender()) {
            writeTimestamp(appender, getTimestampAtIndex(1));
            writeTimestamp(appender, getTimestampAtIndex(2));
            writeTimestamp(appender, getTimestampAtIndex(2));
            appender.writeText("aaaa");
            writeTimestamp(appender, getTimestampAtIndex(4));
            writeTimestamp(appender, getTimestampAtIndex(4));
            writeTimestamp(appender, getTimestampAtIndex(4));
        }
    }

    private void populateQueueWithTimestamps(SingleChronicleQueue queue, int entries, int repeatsPerEntry) {
        populateQueueWithTimestamps(queue, entries, repeatsPerEntry, 0);
    }

    private void populateQueueWithTimestamps(SingleChronicleQueue queue, int entries, int repeatsPerEntry, int batch) {
        try (ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < entries; i++) {
                int effectiveIndex = i + (entries * batch);
                // write multiple so we can confirm that binary search finds the 1st
                for (int j = 0; j < repeatsPerEntry; j++) {
                    final long timestampAtIndex = getTimestampAtIndex(effectiveIndex);
                    writeTimestamp(appender, timestampAtIndex);
                    System.out.printf("%s:%s -- %s%n", (effectiveIndex * repeatsPerEntry) + j, Long.toHexString(appender.lastIndexAppended()), timestampAtIndex);
                }
            }
        }
    }

    private void writeTimestamp(ExcerptAppender appender, long timestamp) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().write(TimestampComparator.TS).int64(timestamp);
        }
    }

    private long getTimestampAtIndex(int index) {
        TimeUnit timeUnit = ServicesTimestampLongConverter.timeUnit();
        long start = timeUnit.convert(1610000000000L, TimeUnit.MILLISECONDS);
        return start + index * timeUnit.convert(1, TimeUnit.SECONDS);
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

    @AfterEach
    public void clearInterrupt() {
        Thread.interrupted();
    }

    private static class ChronicleReaderRunner {
        public static void main(String[] args) {
            ChronicleReader reader = new ChronicleReader()
                    .asMethodReader(SayWhen.class.getName())
                    .withBasePath(Paths.get(args[0]))
                    .withMessageSink(System.out::println);
            reader.execute();
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
