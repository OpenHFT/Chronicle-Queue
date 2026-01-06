/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import net.openhft.chronicle.testframework.GcControls;
import net.openhft.chronicle.testframework.mappedfiles.MappedFileUtil;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.YieldingPauser;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.RollCycles.DEFAULT;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.QUEUE_METADATA_FILE;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.SUFFIX;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.*;
import static net.openhft.chronicle.queue.rollcycles.SparseRollCycles.SMALL_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(SingleChronicleQueueTest.SingleChronicleQueueTemplateProvider.class)
@SuppressWarnings({"try", "serial", "deprecation", "removal"})
public class SingleChronicleQueueTest extends QueueTestCommon {

    private static final long TIMES = (4L << 20L);
    @NotNull
    protected final WireType wireType;
    protected final boolean named;
    private final Bytes<?> appenderListenerDump = Bytes.allocateElasticOnHeap(256);

    public SingleChronicleQueueTest(@NotNull WireType wireType, boolean named) {
        this.wireType = wireType;
        this.named = named;
    }

    private static Stream<SingleChronicleQueueCase> cases() {
        return Stream.of(
                new SingleChronicleQueueCase(WireType.BINARY_LIGHT, true),
                new SingleChronicleQueueCase(WireType.BINARY, false),
                new SingleChronicleQueueCase(WireType.BINARY_LIGHT, false)
        );
    }

    private static final class SingleChronicleQueueCase {
        private final WireType wireType;
        private final boolean named;

        private SingleChronicleQueueCase(WireType wireType, boolean named) {
            this.wireType = wireType;
            this.named = named;
        }
    }

    static final class SingleChronicleQueueTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(SingleChronicleQueueInvocationContext::new);
        }
    }

    private static final class SingleChronicleQueueInvocationContext implements TestTemplateInvocationContext {
        private final SingleChronicleQueueCase testCase;

        private SingleChronicleQueueInvocationContext(SingleChronicleQueueCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return "wireType=" + testCase.wireType + ", named=" + testCase.named;
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return java.util.Collections.singletonList(new SingleChronicleQueueParameterResolver(testCase));
        }
    }

    private static final class SingleChronicleQueueParameterResolver implements ParameterResolver {
        private final SingleChronicleQueueCase testCase;

        private SingleChronicleQueueParameterResolver(SingleChronicleQueueCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            return type == WireType.class || type == boolean.class || type == Boolean.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            if (type == WireType.class) {
                return testCase.wireType;
            }
            return testCase.named;
        }
    }

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    private static List<String> getMappedQueueFiles() {
        return MappedFileUtil.getAllMappedFiles().stream()
                .filter(filename -> filename.contains(SUFFIX))
                .collect(Collectors.toList());
    }

    private static long countEntries(final ChronicleQueue queue, boolean named) {
        final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
        tailer.toStart().direction(TailerDirection.FORWARD);
        long entryCount = 0L;
        while (true) {
            try (final DocumentContext ctx = tailer.readingDocument()) {
                if (!ctx.isPresent()) {
                    break;
                }

                entryCount++;
            }
        }

        return entryCount;
    }

    private static void waitFor(final Supplier<Boolean> condition, final String message) {
        final long timeoutAt = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < timeoutAt) {
            if (condition.get()) {
                return;
            }
        }

        fail(message);
    }

    @TestTemplate
    @DisplayName("Append writes update the sequence number")
    public void testAppend() {
        try (final ChronicleQueue queue =
                     builderWithAppendListener(getTmpDir(), wireType)
                             .build();
             final ExcerptAppender appender = queue.createAppender()) {

            for (int i = 0; i < 10; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(n, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()),
                        "Sequence number should match append count during append/read at index " + i);
            }

            assertEquals(10L, countEntries(queue, named),
                    "Entry count should be 10 after append loop");
        }
        assertEquals(expectedForTestAppend(),
                appenderListenerDump.toString(), "Appender listener dump should match append output");
    }

    @TestTemplate
    @DisplayName("Create appender returns new instance each time")
    public void createAppenderWillReturnANewAppenderEachTime() {
        try (final ChronicleQueue queue = builder(getTmpDir(), wireType).build();
             final ExcerptAppender appender1 = queue.createAppender();
             final ExcerptAppender appender2 = queue.createAppender()) {
            assertNotSame(appender1, appender2,
                    "Second appender instance should be distinct from first appender");
        }
    }

    /**
     * readOnly=true is not supported on Windows so this test does not run on Windows targets, please see
     * {@link SingleChronicleQueueBuilder#readOnly(boolean)}.
     */
    @TestTemplate
    @DisplayName("Read-only queue rejects createAppender calls")
    public void createAppenderWillThrowWhenQueueIsReadOnly() {
        assumeFalse(OS.isWindows(), "Read-only createAppender test does not run on Windows");
        final File queueDir = getTmpDir();
        try (final ChronicleQueue queue = builder(queueDir, wireType).build();
             final ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("hello world");
            try (final ChronicleQueue readOnlyQueue = builder(queueDir, wireType).readOnly(true).build()) {
                assertThrows(IllegalStateException.class, readOnlyQueue::createAppender,
                        "createAppender should fail for read-only queue");
            }
        }
    }

    @NotNull
    private String expectedForTestAppend() {
        return "idx: 4a0400000000\n" +
                "# position: 784, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 0\n" +
                "\n" +
                "idx: 4a0400000001\n" +
                "# position: 796, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 1\n" +
                "\n" +
                "idx: 4a0400000002\n" +
                "# position: 808, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 2\n" +
                "\n" +
                "idx: 4a0400000003\n" +
                "# position: 820, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 3\n" +
                "\n" +
                "idx: 4a0400000004\n" +
                "# position: 832, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 4\n" +
                "\n" +
                "idx: 4a0400000005\n" +
                "# position: 844, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 5\n" +
                "\n" +
                "idx: 4a0400000006\n" +
                "# position: 856, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 6\n" +
                "\n" +
                "idx: 4a0400000007\n" +
                "# position: 868, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 7\n" +
                "\n" +
                "idx: 4a0400000008\n" +
                "# position: 880, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 8\n" +
                "\n" +
                "idx: 4a0400000009\n" +
                "# position: 892, header: 0\n" +
                "--- !!data #binary\n" +
                "test: 9\n" +
                "\n";
    }

    @TestTemplate
    @DisplayName("Text write and read round trip")
    public void testTextReadWrite() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue =
                     builderWithAppendListener(tmpDir, wireType)
                             .build();
             final ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("hello world");
            assertEquals("hello world", queue.createTailer(named ? "named" : null).readText(),
                    "Tailer should read written text");
        }
        assertEquals(expectedForTestTextReadWrite(), appenderListenerDump.toString(),
                "Appender listener dump should match text write output");
    }

    @NotNull
    private String expectedForTestTextReadWrite() {
        return "idx: 4a0400000000\n" +
                "# position: 784, header: 0\n" +
                "--- !!data #binary\n" +
                "hello world\n" +
                "\n";
    }

    @TestTemplate
    @DisplayName("Cleanup removes queue directory after use")
    public void testCleanupDir() throws Throwable {
        AtomicBoolean executed = new AtomicBoolean();
        if (OS.isWindows())
            FlakyTestRunner.builder(() -> {
                testCleanupDir0();
                executed.set(true);
            }).build().run();
        else {
            testCleanupDir0();
            executed.set(true);
        }
        assertTrue(executed.get(), "Cleanup directory test should execute");
    }

    private void testCleanupDir0() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue =
                     builder(tmpDir, wireType)
                             .build();
             final ExcerptAppender appender = queue.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("world");
            }
        }
        // explicitly call so as to clean and release everything
        afterChecks();
        recordExceptions();
        IOTools.deleteDirWithFilesOrThrow(tmpDir);
    }

    @TestTemplate
    @DisplayName("Rollback on append keeps earlier writes")
    public void testRollbackOnAppend() {
        try (final ChronicleQueue queue =
                     builder(getTmpDir(), wireType)
                             .build();
             final ExcerptAppender appender = queue.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("world");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("world2");
            }

            ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            try (DocumentContext dc = tailer.readingDocument()) {
                dc.wire().read("hello");
                dc.rollbackOnClose();
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("world", dc.wire().read("hello").text(),
                        "Tailer should read first message after rollback");
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("world2", dc.wire().read("hello").text(),
                        "Tailer should read second message after rollback");
            }
        }
    }

    @TestTemplate
    @DisplayName("Write with document read bytes different threads")
    public void testWriteWithDocumentReadBytesDifferentThreads() throws InterruptedException, TimeoutException, ExecutionException {
        try (final ChronicleQueue queue = builder(getTmpDir(), wireType)
                .build()) {

            final String expected = "some long message";

            ExecutorService service1 = Executors.newSingleThreadExecutor(
                    new NamedThreadFactory("service1"));
            ScheduledExecutorService service2 = null;
            try {
                Future<?> f = service1.submit(() -> {
                    try (final ExcerptAppender appender = queue.createAppender()) {

                        try (final DocumentContext dc = appender.writingDocument()) {
                            dc.wire().writeEventName("key").text(expected);
                        }
                    }
                });

                BlockingQueue<Bytes<?>> result = new ArrayBlockingQueue<>(10);

                service2 = Executors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("service2"));
                service2.scheduleAtFixedRate(() -> {
                    Bytes<?> b = Bytes.allocateElasticOnHeap(128);
                    final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
                    tailer.readBytes(b);
                    if (b.readRemaining() == 0)
                        return;
                    b.readPosition(0);
                    b.singleThreadedCheckReset();
                    result.add(b);
                    throw new RejectedExecutionException("Stop scheduled task after first read");
                }, 1, 1, TimeUnit.MICROSECONDS);

                final Bytes<?> bytes = result.poll(5, TimeUnit.SECONDS);
                if (bytes == null) {
                    f.get(1, TimeUnit.SECONDS);
                    throw new NullPointerException("nothing in result");
                }
                try {
                    final String actual = this.wireType.apply(bytes).read("key").text();
                    assertEquals(expected, actual, "read text should match written text across threads");
                    f.get(1, TimeUnit.SECONDS);
                } finally {
                    bytes.releaseLast();
                }
            } finally {
                service1.shutdownNow();
                if (service2 != null)
                    service2.shutdownNow();
            }
        }
    }

    @TestTemplate
    @DisplayName("Should blow up if trying to create queue with unparseable roll cycle")
    public void shouldBlowUpIfTryingToCreateQueueWithUnparseableRollCycle() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(new RollCycleDefaultingTest.MyRollcycle()).build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {
            try (DocumentContext documentContext = excerptAppender.writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }

        assertThrows(IllegalStateException.class, () -> {
            try (final ChronicleQueue ignored = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {
                assertNotNull(ignored, "unparseable roll cycle: opened");
            }
        }, "unparseable roll cycle: reopen with HOURLY");
    }

    @TestTemplate
    @DisplayName("Can append metadata if append lock is set")
    public void testCanAppendMetadataIfAppendLockIsSet() {
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).build()) {
            final SingleChronicleQueue scq = (SingleChronicleQueue) queue;
            scq.appendLock().lock();
            try (final ExcerptAppender appender = queue.createAppender()) {
                assumeTrue(appender instanceof StoreAppender, "StoreAppender is required for CQE append lock test");
                try (DocumentContext dc = appender.writingDocument(true)) {
                    dc.wire().write("Hello World");
                }
            }
            try (ExcerptTailer tailer = queue.createTailer()) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isPresent(), "append lock: metadata present");
                    assertTrue(dc.isMetaData(), "append lock: metadata entry");
                }
            }
        }
    }

    @TestTemplate
    @DisplayName("Append lock prevents additional appends queue behaviour scenario")
    public void testCantAppendIfAppendLockIsSet() {
        File tmpDir = getTmpDir();
        assertThrows(IllegalStateException.class, () -> {
            try (final ChronicleQueue queue = builder(tmpDir, wireType).build()) {
                ((SingleChronicleQueue) queue).appendLock().lock();
                try (final ExcerptAppender appender = queue.createAppender()) {
                    appender.writeText("Hello World");
                }
            }
        }, "append lock: cannot append");
    }

    @TestTemplate
    @DisplayName("Cant append if append lock is set in different queue")
    public void testCantAppendIfAppendLockIsSetInDifferentQueue() {
        expectException("Overriding roll length from existing metadata");
        expectException("Overriding roll cycle from");

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).build()) {
            ((SingleChronicleQueue) queue).appendLock().lock();
        }

        assertThrows(IllegalStateException.class, () -> {
            try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(new RollCycleDefaultingTest.MyRollcycle()).build();
                 final ExcerptAppender excerptAppender = queue.createAppender()) {
                excerptAppender.writeText("hello");
            }
        }, "append lock: cannot append from different queue");
    }

    @TestTemplate
    @DisplayName("Can append write bytes internal if append lock is set")
    public void testCanAppendWriteBytesInternalIfAppendLockIsSet() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builderWithAppendListener(tmpDir, wireType).build()) {
            ((SingleChronicleQueue) queue).appendLock().lock();
            try (final ExcerptAppender appender = queue.createAppender()) {
                assumeTrue(appender instanceof StoreAppender, "StoreAppender is required for writeBytesInternal");
                StoreAppender storeAppender = (StoreAppender) appender;
                ((SingleChronicleQueue) queue).writeLock().lock();
                storeAppender.writeBytesInternal(0, test);
            }
        }
        assertEquals(expectedForTestCanAppendWriteBytesInternalIfAppendLockIsSet(), appenderListenerDump.toString(),
                "Appender listener dump should match writeBytesInternal output");
    }

    @NotNull
    private String expectedForTestCanAppendWriteBytesInternalIfAppendLockIsSet() {
        return "idx: 0\n" +
                "# position: 784, header: 0\n" +
                "--- !!data\n" +
                "hello world\n" +
                "\n";
    }

    @TestTemplate
    @DisplayName("Should not blow up if trying to create queue with incorrect roll cycle")
    public void shouldNotBlowUpIfTryingToCreateQueueWithIncorrectRollCycle() {
        expectException("Overriding roll length from existing metadata");
        expectException("Overriding roll cycle from");
        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(DEFAULT).build();
             final ExcerptAppender appender = queue.createAppender()) {
            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }

        // we don't store which RollCycles enum was used and we try and match by format string, we
        // match the first RollCycles with the same format string, which may not
        // be the RollCycles it was written with
        try (final ChronicleQueue reopen = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {
            assertEquals(DEFAULT, reopen.rollCycle(), "Reopened queue should default to RollCycles.DEFAULT");
        }
    }

    @TestTemplate
    @DisplayName("Epoch override takes effect queue behaviour scenario")
    public void shouldOverrideDifferentEpoch() {
        expectException("Overriding roll epoch from existing metadata, was 10, overriding to 100");
        File tmpDir = getTmpDir();
        final int shouldBeEpoch = 100;
        try (final ChronicleQueue queue = builder(tmpDir, wireType).rollCycle(TEST_SECONDLY).epoch(shouldBeEpoch).build();
             final ExcerptAppender appender = queue.createAppender()) {
            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }

        try (final ChronicleQueue ignored = builder(tmpDir, wireType).rollCycle(TEST_SECONDLY).epoch(10).build()) {
            assertEquals(shouldBeEpoch, ((SingleChronicleQueue) ignored).epoch(),
                    "Queue epoch should match configured value");
        }
    }

    @TestTemplate
    @DisplayName("Read and write with hourly roll cycle")
    public void testReadWriteHourly() {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue qAppender = builder(tmpDir, wireType).rollCycle(HOURLY).build();
             final ExcerptAppender appender = qAppender.createAppender()) {
            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("somekey").text("somevalue");
            }
        }

        try (final ChronicleQueue qTailer = builder(tmpDir, wireType).rollCycle(HOURLY).build()) {

            try (DocumentContext documentContext2 = qTailer.createTailer(named ? "named" : null).readingDocument()) {
                String str = documentContext2.wire().read("somekey").text();
                assertEquals("somevalue", str, "read value should match written value in hourly queue");
            }
        }
    }

    private long toSeq(final ChronicleQueue q, final long index) {
        return q.rollCycle().toSequenceNumber(index);
    }

    private void assertMetaEntry(ChronicleQueue queue,
                                 ExcerptTailer tailer,
                                 boolean includeMetaData,
                                 int expectedSeq,
                                 boolean expectedMeta,
                                 String expectedText,
                                 String context) {
        try (DocumentContext documentContext = tailer.readingDocument(includeMetaData)) {
            assertEquals(expectedSeq, toSeq(queue, documentContext.index()),
                    "Sequence number should be " + expectedSeq + " for " + context);
            assertEquals(expectedMeta, documentContext.isMetaData(),
                    "Metadata flag should be " + expectedMeta + " for " + context);
            assertEquals(expectedText, documentContext.wire().getValueIn().text(),
                    "Text should be " + expectedText + " for " + context);
        }
    }

    @TestTemplate
    @DisplayName("Should allow directory to be deleted when queue is closed")
    public void shouldAllowDirectoryToBeDeletedWhenQueueIsClosed() throws IOException {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test deleting after close on windows");
            return;
        }

        final File dir = getTmpDir();
        try (final ChronicleQueue queue =
                     builder(dir, wireType).
                             testBlockSize().build();
             final ExcerptAppender appender = queue.createAppender()) {
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().write().text("foo");
            }
            try (final DocumentContext dc = queue.createTailer(named ? "named" : null).readingDocument()) {
                assertEquals("foo", dc.wire().read().text(), "Tailer should read written foo text");
            }
        }

        try (Stream<Path> paths = Files.walk(dir.toPath())) {
            final List<Path> unDeletable = paths
                    .filter(p -> !Files.isDirectory(p))
                    .filter(p -> !p.toFile().delete())
                    .collect(Collectors.toList());
            assertTrue(unDeletable.isEmpty(), "Unable to delete " + unDeletable);
        }
        assertTrue(dir.delete(), "Directory should delete after queue close");
    }

    @TestTemplate
    @DisplayName("Reading fewer bytes than written queue behaviour scenario")
    public void testReadingLessBytesThanWritten() {
        try (final ChronicleQueue queue = builder(getTmpDir(), wireType)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            final Bytes<byte[]> expected = Bytes.wrapForRead("some long message".getBytes(ISO_8859_1));
            for (int i = 0; i < 10; i++) {

                appender.writeBytes(expected);
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            // Sequential read
            for (int i = 0; i < 10; i++) {

                Bytes<?> b = Bytes.allocateDirect(8);

                tailer.readBytes(b);

                assertEquals(expected.readInt(0), b.readInt(0),
                        "Read prefix should match expected bytes at iteration " + i);

                b.releaseLast();
            }
        }
    }

    @TestTemplate
    @DisplayName("Append entries and read them back")
    public void testAppendAndRead() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            final int cycle = appender.cycle();
            for (int i = 0; i < 10; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(n, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()),
                        "Sequence number should match append count at index " + i);
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            // Sequential read
            for (int i = 0; i < 10; i++) {
                final int n = i;
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent(), "sequential readDocument should succeed at index " + i);
                    assertEquals(n, dc.wire().read(TestKey.test).int32(),
                            "Sequential TestKey.test should equal " + n + " at index " + i);
                }
                assertEquals(n + 1, queue.rollCycle().toSequenceNumber(tailer.index()),
                        "Tailer sequence number should advance after sequential read at index " + i);
            }

            // Random read
            for (int i = 0; i < 10; i++) {
                final int n = i;
                assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, n)),
                        "Tailer should move to index " + i + " for random read");
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent(), "Tailer should read document for random index " + i);
                    assertEquals(n, dc.wire().read(TestKey.test).int32(),
                            "Random TestKey.test should equal " + n + " at index " + i);
                }
                assertEquals(n + 1, queue.rollCycle().toSequenceNumber(tailer.index()),
                        "Tailer sequence number should advance after random read at index " + i);
            }
        }
    }

    @TestTemplate
    @DisplayName("Read and append concurrently queue behaviour scenario")
    public void testReadAndAppend() throws InterruptedException {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType).build();
             final ExcerptAppender appender = queue.createAppender()) {

            final CountDownLatch started = new CountDownLatch(1);
            int[] results = new int[2];

            Thread t = new Thread(() -> {
                try {
                    started.countDown();
                    final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
                    for (int i = 0; i < 2; ) {
                        boolean read = tailer.readDocument(r -> {
                            int result = r.read(TestKey.test).int32();
                            results[result] = result;
                        });

                        if (read) {
                            i++;
                        } else {
                            // Pause for a little
                            Jvm.pause(10);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Exception while reading document in background thread");
                }
            });
            t.setDaemon(true);
            t.start();

            assertTrue(started.await(1, TimeUnit.SECONDS),
                    "Reader thread should start within 1 second");

            for (int i = 0; i < 2; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
            }

            t.join(1_000);

            assertArrayEquals(new int[]{0, 1}, results, "appender thread should read both written documents in order");
        }
    }

    @TestTemplate
    @DisplayName("Check index with writingDocument writes queue behaviour scenario")
    public void testCheckIndexWithWritingDocument() {
        int documents = doTestCheckIndex(
                (appender, n) -> {
                    try (final DocumentContext dc = appender.writingDocument()) {
                        dc.wire().writeEventName("").object("" + n);
                    }
                });
        assertEquals(6, documents, "Document count should be 6 for writingDocument");
    }

    @TestTemplate
    @DisplayName("Check index with writingDocument bytes queue behaviour scenario")
    public void testCheckIndexWithWritingDocument2() {
        int documents = doTestCheckIndex(
                (appender, n) -> {
                    try (final DocumentContext dc = appender.writingDocument()) {
                        dc.wire().bytes().writeUtf8("Hello")
                                .writeStopBit(12345)
                                .writeStopBit(1.2) // float also supported.
                                .writeInt(1);
                    }
                });
        assertEquals(6, documents, "Document count should be 6 for writingDocument2");
    }

    @TestTemplate
    @DisplayName("Check index while writing bytes queue behaviour scenario")
    public void testCheckIndexWithWriteBytes() {
        int documents = doTestCheckIndex(
                (appender, n) -> appender.writeBytes(Bytes.from("Message-" + n)));
        assertEquals(6, documents, "Document count should be 6 for writeBytes");
    }

    @TestTemplate
    @DisplayName("Check index with bytes lambda writes")
    public void testCheckIndexWithWriteBytes2() {
        int documents = doTestCheckIndex(
                (appender, n) -> appender.writeBytes(b -> b.append8bit("Message-").append(n)));
        assertEquals(6, documents, "Document count should be 6 for writeBytes2");
    }

    @TestTemplate
    @DisplayName("Check index with bytes marshalling writes")
    public void testCheckIndexWithWriteBytes3() {
        int documents = doTestCheckIndex(
                (appender, n) -> appender.writeBytes(b ->
                        b.writeUtf8("Hello")
                                .writeStopBit(12345)
                                .writeStopBit(1.2) // float also supported.
                                .writeInt(1)));
        assertEquals(6, documents, "Document count should be 6 for writeBytes3");
    }

    @TestTemplate
    @DisplayName("Check index with map writes queue behaviour scenario")
    public void testCheckIndexWithWriteMap() {
        int documents = doTestCheckIndex(
                (appender, n) -> appender.writeMap(new HashMap<String, String>() {{
                    put("key", "Message-" + n);
                }}));
        assertEquals(6, documents, "Document count should be 6 for writeMap");
    }

    @TestTemplate
    @DisplayName("Check index with text writes queue behaviour scenario")
    public void testCheckIndexWithWriteText() {
        int documents = doTestCheckIndex(
                (appender, n) -> appender.writeText("Message-" + n)
        );
        assertEquals(6, documents, "Document count should be 6 for writeText");
    }

    private int doTestCheckIndex(@NotNull BiConsumer<ExcerptAppender, Integer> writeTo) {
        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(System.currentTimeMillis() - 3 * 86400_000L);
        int documents = 0;
        try (final ChronicleQueue queue = builder(getTmpDir(), wireType)
                .timeProvider(stp)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            int cycle = appender.cycle();
            for (int i = 0; i <= 5; i++) {

                writeTo.accept(appender, i);
                assertEquals(cycle + i, appender.cycle(),
                        "Appender should advance to next cycle at iteration " + i);

                try (DocumentContext dc = tailer.readingDocument()) {
                    long index = tailer.index();
                    assertEquals(appender.cycle(), tailer.cycle(),
                            "Tailer should be on same cycle as appender at iteration " + i);
                    assertEquals(cycle + i, DEFAULT.toCycle(index),
                            "Document index should reflect current cycle at iteration " + i);
                }
                stp.currentTimeMillis(stp.currentTimeMillis() + 86400_000L);
                documents++;

            }
        }
        return documents;
    }

    @TestTemplate
    @DisplayName("Append and read across rolling cycles")
    public void testAppendAndReadWithRollingB() {
        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(System.currentTimeMillis() - 3 * 86400_000L);

        try (final ChronicleQueue queue =
                     builder(getTmpDir(), this.wireType)
                             .rollCycle(TEST_DAILY)
                             .timeProvider(stp)
                             .build();
             final ExcerptAppender appender = queue.createAppender()) {

            appender.writeDocument(w -> w.write(TestKey.test).int32(0));
            appender.writeDocument(w -> w.write(TestKey.test2).int32(1000));
            int cycle = appender.cycle();
            for (int i = 1; i <= 5; i++) {
                stp.currentTimeMillis(stp.currentTimeMillis() + 86400_000L);
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(cycle + i, appender.cycle(),
                        "Appender should be on current cycle after first write at iteration " + i);
                appender.writeDocument(w -> w.write(TestKey.test2).int32(n + 1000));
                assertEquals(cycle + i, appender.cycle(),
                        "Appender should remain on same cycle after second write at iteration " + i);
            }

            /* Note this means the file has rolled
            --- !!not-ready-meta-data! #binary
            ...
             */
            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null).toStart();
            for (int i = 0; i < 6; i++) {
                final int n = i;
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent(), "Tailer should read first document at iteration " + i);
                    assertEquals(n, dc.wire().read(TestKey.test).int32(),
                            "first document value should match loop index " + i);
                }
                assertEquals(cycle + i, tailer.cycle(),
                        "Tailer should be on current cycle after first read at iteration " + i);

                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent(), "Tailer should read second document at iteration " + i);
                    assertEquals(n + 1000, dc.wire().read(TestKey.test2).int32(),
                            "second document value should be offset by 1000 at loop index " + i);
                }
                assertEquals(cycle + i, tailer.cycle(),
                        "Tailer should remain on same cycle after second read at iteration " + i);
            }
        }
    }

    @TestTemplate
    @DisplayName("Append entries and read back by index")
    public void testAppendAndReadAtIndex() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            appender.cycle();
            for (int i = 0; i < 5; i++) {
                final int n = i;
                appender.writeDocument(w -> w.write(TestKey.test).int32(n));
                assertEquals(i, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()),
                        "Sequence number should match append count before index read at index " + i);
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            for (int i = 0; i < 5; i++) {
                final long index = queue.rollCycle().toIndex(appender.cycle(), i);
                assertTrue(tailer.moveToIndex(index), "Tailer should move to index " + index + " at iteration " + i);

                final int n = i;
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent(), "readDocument after moveToIndex: i=" + i);
                    assertEquals(n, queue.rollCycle().toSequenceNumber(dc.wire().read(TestKey.test).int32()),
                            "Sequence number should match read value at index " + n + " for loop index " + i);
                }
                long index2 = tailer.index();
                long sequenceNumber = queue.rollCycle().toSequenceNumber(index2);
                assertEquals(n + 1, sequenceNumber,
                        "Sequence number should increment after moveToIndex at iteration " + i);
            }
        }
    }

    @TestTemplate
    @DisplayName("Simple wire write and read queue behaviour scenario")
    public void testSimpleWire() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            appender.writeDocument(wire -> wire.write("FirstName").text("Steve"));
            appender.writeDocument(wire -> wire.write("Surname").text("Jobs"));

            StringBuilder first = new StringBuilder();
            StringBuilder surname = new StringBuilder();

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            tailer.readDocument(wire -> wire.read("FirstName").text(first));
            tailer.readDocument(wire -> wire.read("Surname").text(surname));
            assertEquals("Steve Jobs", first + " " + surname,
                    "First name and surname should concatenate to Steve Jobs");
        }
    }

    @TestTemplate
    @DisplayName("Index writing updates last index queue behaviour scenario")
    public void testIndexWritingDocument() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            long index;
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("FirstName").text("Quartilla");
                index = dc.index();
            }

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Quartilla");
            }

            assertEquals(index, appender.lastIndexAppended(),
                    "Last index appended should match first document index");
        }
    }

    @TestTemplate
    @DisplayName("Marshallable document round trip queue behaviour scenario")
    public void testReadingWritingMarshallableDocument() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            MyMarshable myMarshable = new MyMarshable();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("myMarshable").typedMarshallable(myMarshable);
            }

            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            try (DocumentContext dc = tailer.readingDocument()) {

                assertEquals(myMarshable, dc.wire().read("myMarshable").typedMarshallable(),
                        "Typed marshallable should round trip from queue");
            }
        }
    }

    // CPD-OFF - metadata scenarios mirror each other
    @TestTemplate
    @DisplayName("Metadata entries are reported correctly queue behaviour scenario")
    public void testMetaData() {
        assumeFalse(named, "Metadata test requires unnamed tailer mode");
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("FirstName").text("Rob");
            }

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Steve");
            }

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            StringBuilder event = new StringBuilder();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData(), "Document context should be metadata for Quartilla entry in dump");
                    ValueIn in = dc.wire().read(event);
                    // first we will pick up header, index etc.
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Quartilla", org.junit.jupiter.api.Assertions::assertEquals);
                    break;
                }
            }

            long robIndex;
            try (DocumentContext dc = tailer.readingDocument(true)) {
                assertTrue(dc.isData(), "Document context should be data for Rob entry");
                robIndex = dc.index();
                dc.wire().read("FirstName").text("Rob", org.junit.jupiter.api.Assertions::assertEquals);
            }

            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData(), "Document context should be metadata for Steve entry in dump");
                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Steve", org.junit.jupiter.api.Assertions::assertEquals);
                    break;
                }
            }

            assertTrue(tailer.moveToIndex(robIndex), "Tailer should move to Rob entry index");
            try (DocumentContext dc = tailer.readingDocument(false)) {
                assertTrue(dc.isData(), "Document context should be data when revisiting Rob entry");
                dc.wire().read("FirstName").text("Rob", org.junit.jupiter.api.Assertions::assertEquals);
            }
        }
    }

    @TestTemplate
    @DisplayName("Second document should be absent after single write")
    public void testReadingSecondDocumentNotExist() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().write("FirstName").text("Quartilla");
            }

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            try (DocumentContext dc = tailer.readingDocument()) {
                String text = dc.wire().read("FirstName").text();
                assertEquals("Quartilla", text, "first name should match written value");
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent(), "Second document should not be present after reading first entry");
            }
        }
    }

    @TestTemplate
    @DisplayName("Document index increments as expected queue behaviour scenario")
    public void testDocumentIndexTest() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                long index = dc.index();
                assertEquals(0, chronicle.rollCycle().toSequenceNumber(index),
                        "Sequence number should be 0 for first write");
                dc.wire().write("FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                assertEquals(1, chronicle.rollCycle().toSequenceNumber(dc.index()),
                        "Sequence number should be 1 for second write");
                dc.wire().write("FirstName").text("Rob");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                assertEquals(2, chronicle.rollCycle().toSequenceNumber(dc.index()),
                        "Sequence number should be 2 for third write");
                dc.wire().write("FirstName").text("Rob");
            }

            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            try (DocumentContext dc = tailer.readingDocument()) {
                long index = dc.index();
                assertEquals(0, chronicle.rollCycle().toSequenceNumber(index),
                        "Sequence number should be 0 for first read");

            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals(1, chronicle.rollCycle().toSequenceNumber(dc.index()),
                        "Sequence number should be 1 for second read");

            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals(2, chronicle.rollCycle().toSequenceNumber(dc.index()),
                        "Sequence number should be 2 for third read");

            }
        }
    }

    @TestTemplate
    @DisplayName("Reading second document not exist including meta")
    public void testReadingSecondDocumentNotExistIncludingMeta() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().write("FirstName").text("Quartilla");
            }

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);
            StringBuilder event = new StringBuilder();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {

                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Quartilla", org.junit.jupiter.api.Assertions::assertEquals);
                    break;
                }
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent(), "Second document should not be present after metadata read");
            }
        }
    }

    @TestTemplate
    @DisplayName("Simple byte write and read queue behaviour scenario")
    public void testSimpleByteTest() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST2_DAILY)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            Bytes<?> steve = Bytes.allocateDirect("Steve".getBytes());
            appender.writeBytes(steve);
            Bytes<?> jobs = Bytes.allocateDirect("Jobs".getBytes());
            appender.writeBytes(jobs);

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);
            Bytes<?> bytes = Bytes.elasticByteBuffer();
            try {
                tailer.readBytes(bytes);
                assertEquals("Steve", bytes.toString(), "First payload should be Steve");
                bytes.clear();
                tailer.readBytes(bytes);
                assertEquals("Jobs", bytes.toString(), "Second payload should be Jobs");
            } finally {
                steve.releaseLast();
                jobs.releaseLast();
                bytes.releaseLast();
            }
        }
    }

    @TestTemplate
    @DisplayName("Read values by index in sparse positions")
    public void testReadAtIndex() {
        try (final RollingChronicleQueue queue = builder(getTmpDir(), wireType)
                .indexCount(8)
                .indexSpacing(8)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            // create 100 documents
            for (int i = 0; i < 100; i++) {
                try (final DocumentContext context = appender.writingDocument()) {
                    context.wire().write("key").text("value=" + i);
                }
            }
            long lastIndex = appender.lastIndexAppended();

            final int cycle = queue.rollCycle().toCycle(lastIndex);
            assertEquals(queue.firstCycle(), cycle, "first cycle should match current cycle");
            assertEquals(queue.lastCycle(), cycle, "last cycle should match current cycle");
            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            StringBuilder sb = new StringBuilder();

            for (int i : new int[]{0, 8, 7, 9, 64, 65, 66}) {
                final long index = queue.rollCycle().toIndex(cycle, i);
                assertTrue(tailer.moveToIndex(
                        index), "Tailer should move to index " + i + " when reading sparse positions");
                try (final DocumentContext context = tailer.readingDocument()) {
                    assertEquals(index, context.index(), "document index should match target index at position " + i);
                    context.wire().read("key").text(sb);
                    assertEquals("value=" + i, sb.toString(),
                            "Document value should match expected sequence at index " + i);
                }
            }
        }
    }

    @Disabled("Long running test disabled for regular runs")
    @TestTemplate
    @DisplayName("Read value at index for 4MB payloads queue behaviour scenario")
    public void testReadAtIndex4MB() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(getTmpDir(), this.wireType).rollCycle(SMALL_DAILY)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            for (long i = 0; i < TIMES; i++) {
                final long j = i;
                appender.writeDocument(wire -> wire.write("key").text("value=" + j));
            }
            long lastIndex = appender.lastIndexAppended();

            final int cycle = queue.rollCycle().toCycle(lastIndex);

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            StringBuilder sb = new StringBuilder();

            for (long i = 0; i < (4L << 20L); i++) {
                assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, i)),
                        "Tailer should move to index " + i + " in cycle " + cycle);
                tailer.readDocument(wire -> wire.read("key").text(sb));
                assertEquals("value=" + i, sb.toString(),
                        "Read value should match for index " + i);
            }
        }
    }

    @TestTemplate
    @DisplayName("Metadata index entries are correct queue behaviour scenario")
    public void testMetaIndexTest() {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue q = builderWithAppendListener(tmpDir, wireType).rollCycle(HOURLY).build();
             ExcerptAppender appender = q.createAppender()) {
            {
                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().getValueOut().text("one");
                }
                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().getValueOut().text("two");
                }
                try (DocumentContext documentContext = appender.writingDocument(true)) {
                    documentContext.wire().getValueOut().text("meta1");
                }

                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().getValueOut().text("three");
                }

                try (DocumentContext documentContext = appender.writingDocument(true)) {
                    documentContext.wire().getValueOut().text("meta2");
                }
                try (DocumentContext documentContext = appender.writingDocument(true)) {
                    documentContext.wire().getValueOut().text("meta3");
                }
                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().getValueOut().text("four");
                }
            }
            {

                ExcerptTailer tailer = q.createTailer(named ? "named" : null);

                assertMetaEntry(q, tailer, false, 0, false, "one", "first data entry in sequence 0");
                assertMetaEntry(q, tailer, true, 1, false, "two", "second data entry in sequence 1");
                assertMetaEntry(q, tailer, true, 2, true, "meta1", "first metadata entry in sequence 2");
                assertMetaEntry(q, tailer, true, 2, false, "three", "third data entry in sequence 2");
                assertMetaEntry(q, tailer, true, 3, true, "meta2", "second metadata entry in sequence 3");
                assertMetaEntry(q, tailer, true, 3, true, "meta3", "third metadata entry in sequence 3");
                assertMetaEntry(q, tailer, true, 3, false, "four", "fourth data entry in sequence 3");
            }

            {
                ExcerptTailer tailer = q.createTailer(named ? "named2" : null);

                assertMetaEntry(q, tailer, false, 0, false, "one", "named tailer first data entry");
                assertMetaEntry(q, tailer, false, 1, false, "two", "named tailer second data entry");
                assertMetaEntry(q, tailer, false, 2, false, "three", "named tailer third data entry");
            }
        }
        assertEquals("idx: 6f06c00000000\n" +
                        "# position: 65808, header: 0\n" +
                        "--- !!data #binary\n" +
                        "one\n" +
                        "\n" +
                        "idx: 6f06c00000001\n" +
                        "# position: 65816, header: 0\n" +
                        "--- !!data #binary\n" +
                        "two\n" +
                        "\n" +
                        "idx: 6f06c00000002\n" +
                        "# position: 65836, header: 0\n" +
                        "--- !!data #binary\n" +
                        "three\n" +
                        "\n" +
                        "idx: 6f06c00000003\n" +
                        "# position: 65872, header: 0\n" +
                        "--- !!data #binary\n" +
                        "four\n" +
                        "\n",
                appenderListenerDump.toString(), "Appender listener dump should match meta index output");
    }

    @TestTemplate
    @DisplayName("Last written index is per appender")
    public void testLastWrittenIndexPerAppender() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            appender.writeDocument(wire -> wire.write("key").text("test"));
            assertEquals(0, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()),
                    "Sequence number should be 0 after first write");
        }
    }

    @TestTemplate
    @DisplayName("Last written index per appender no data")
    public void testLastWrittenIndexPerAppenderNoData() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {
            assertThrows(IllegalStateException.class, appender::lastIndexAppended,
                    "lastIndexAppended should throw when no data is written");
        }
    }

    @TestTemplate
    @DisplayName("Last index throws when no messages written")
    public void testNoMessagesWritten() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            assertThrows(IllegalStateException.class, appender::lastIndexAppended,
                    "lastIndexAppended should throw when no messages are written");
        }
    }

    @TestTemplate
    @DisplayName("Header index is read at index queue behaviour scenario")
    public void testHeaderIndexReadAtIndex() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            final int cycle = appender.cycle();
            // create 100 documents
            for (int i = 0; i < 100; i++) {
                final int j = i;
                appender.writeDocument(wire -> wire.write("key").text("value=" + j));
            }

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 0)),
                    "Tailer should move to index 0 for header read");

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read("key").text(sb));

            assertEquals("value=0", sb.toString(), "First value should be value=0 at index 0");
        }
    }

    /**
     * test that if we make EPOC the current time, then the cycle is == 0
     */
    @TestTemplate
    @DisplayName("Epoch value is applied correctly queue behaviour scenario")
    public void testEPOC() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .epoch(System.currentTimeMillis())
                .rollCycle(HOURLY)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            appender.writeDocument(wire -> wire.write("key").text("value=v"));
            assertEquals(0, appender.cycle(), "cycle should be 0 when epoch is current time");
        }
    }

    @TestTemplate
    @DisplayName("Should be able to read from queue with non zero epoch")
    public void shouldBeAbleToReadFromQueueWithNonZeroEpoch() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .epoch(System.currentTimeMillis())
                .rollCycle(DEFAULT)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            appender.writeDocument(wire -> wire.write("key").text("value=v"));
            assertEquals(0, appender.cycle(), "cycle should be 0 when epoch matches write time");

            final ExcerptTailer excerptTailer = chronicle.createTailer(named ? "named" : null).toStart();
            assertTrue(excerptTailer.readingDocument().isPresent(),
                    "Tailer should read first document when epoch matches write time");
        }
    }

    @TestTemplate
    @DisplayName("Large epoch values are supported queue behaviour scenario")
    public void shouldHandleLargeEpoch() {
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .epoch(System.currentTimeMillis())
                .epoch(1284739200000L)
                .rollCycle(DEFAULT)
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            appender.writeDocument(wire -> wire.write("key").text("value=v"));

            final ExcerptTailer excerptTailer = chronicle.createTailer(named ? "named" : null).toStart();
            assertTrue(excerptTailer.readingDocument().isPresent(),
                    "Tailer should read first document with large epoch");
        }
    }

    @TestTemplate
    @DisplayName("Negative epoch values are supported queue behaviour scenario")
    public void testNegativeEPOC() {
        for (int h = -14; h <= 14; h++) {
            try (final ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                    .epoch(TimeUnit.HOURS.toMillis(h))
                    .build();
                 final ExcerptAppender appender = chronicle.createAppender()) {

                appender.writeDocument(wire -> wire.write("key").text("value=v"));
                final StringBuilder actual = new StringBuilder();
                final boolean read = chronicle.createTailer(named ? "named" : null)
                        .readDocument(wire -> wire.read("key").text(actual));
                assertTrue(read, "negative epoch: read h=" + h);
                assertEquals("value=v", actual.toString(), "negative epoch: value h=" + h);
            }
        }
    }

    @TestTemplate
    @DisplayName("Index lookup reads expected values queue behaviour scenario")
    public void testIndex() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(HOURLY)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            int cycle = writeSequentialValueDocuments(queue, appender);

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2)),
                    "Tailer should move to index 2 for first move read");

            StringBuilder sb = new StringBuilder();
            tailer.readDocument(wire -> wire.read("key").text(sb));
            assertEquals(valueString(2), sb.toString(), "Tailer should read value=2 after moveToIndex");

            tailer.readDocument(wire -> wire.read("key").text(sb));
            assertEquals(valueString(3), sb.toString(), "Tailer should read value=3 on next document");

            tailer.readDocument(wire -> wire.read("key").text(sb));
            assertEquals(valueString(4), sb.toString(), "Tailer should read final value after moveToIndex");
        }
    }

    @TestTemplate
    @DisplayName("Reading document returns expected values queue behaviour scenario")
    public void testReadingDocument() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(HOURLY)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            long cycle = writeSequentialValueDocuments(queue, appender);

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            verifyTailerReadsValues(tailer, valueString(0), valueString(1), valueString(2), valueString(3), valueString(4));
        }
    }

    @TestTemplate
    @DisplayName("Reading document after moveToIndex queue behaviour scenario")
    public void testReadingDocumentWithFirstAMove() {

        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(HOURLY)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            int cycle = writeSequentialValueDocuments(queue, appender);

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2)),
                    "Tailer should move to index 2 before reading");
            verifyTailerReadsValues(tailer, valueString(2), valueString(3), valueString(4));
        }
    }

    @TestTemplate
    @DisplayName("Reading document with first amove with epoch")
    public void testReadingDocumentWithFirstAMoveWithEpoch() {
        Instant hourly = Instant.parse("2018-02-12T00:59:59.999Z");

        Date epochHourlyFirstCycle = Date.from(hourly);
        Date epochHourlySecondCycle = Date.from(hourly.plusMillis(1));

        assertTrue(doTestEpochMove(epochHourlyFirstCycle.getTime(), MINUTELY), "epoch move: hourly first minutely");
        assertTrue(doTestEpochMove(epochHourlySecondCycle.getTime(), MINUTELY), "epoch move: hourly second minutely");
        assertTrue(doTestEpochMove(epochHourlyFirstCycle.getTime(), HOURLY), "epoch move: hourly first hourly");
        assertTrue(doTestEpochMove(epochHourlySecondCycle.getTime(), HOURLY), "epoch move: hourly second hourly");
        assertTrue(doTestEpochMove(epochHourlyFirstCycle.getTime(), DAILY), "epoch move: hourly first daily");
        assertTrue(doTestEpochMove(epochHourlySecondCycle.getTime(), DAILY), "epoch move: hourly second daily");

        Instant minutely = Instant.parse("2018-02-12T00:00:59.999Z");
        Date epochMinutelyFirstCycle = Date.from(minutely);
        Date epochMinutelySecondCycle = Date.from(minutely.plusMillis(1));

        assertTrue(doTestEpochMove(epochMinutelyFirstCycle.getTime(), MINUTELY), "epoch move: minutely first minutely");
        assertTrue(doTestEpochMove(epochMinutelySecondCycle.getTime(), MINUTELY), "epoch move: minutely second minutely");
        assertTrue(doTestEpochMove(epochMinutelyFirstCycle.getTime(), HOURLY), "epoch move: minutely first hourly");
        assertTrue(doTestEpochMove(epochMinutelySecondCycle.getTime(), HOURLY), "epoch move: minutely second hourly");
        assertTrue(doTestEpochMove(epochMinutelyFirstCycle.getTime(), DAILY), "epoch move: minutely first daily");
        assertTrue(doTestEpochMove(epochMinutelySecondCycle.getTime(), DAILY), "epoch move: minutely second daily");
    }

    private boolean doTestEpochMove(long epoch, RollCycle rollCycle) {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(rollCycle)
                .epoch(epoch)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            int cycle = writeSequentialValueDocuments(queue, appender);
            long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
            if (cycle + 1 != cycle1) {
                assertEquals(cycle, cycle1, "Epoch move should keep cycle aligned with appender");
            }

            try (final ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
                final boolean moved = tailer.moveToIndex(queue.rollCycle().toIndex(cycle, 2));
                if (!moved) {
                    return false;
                }
                verifyTailerReadsValues(tailer, valueString(2), valueString(3), valueString(4));
            }
        }
        return true;
    }

    private String valueString(int value) {
        return "value=" + value;
    }

    private void verifyTailerReadsValues(ExcerptTailer tailer, String... expected) {
        StringBuilder sb = new StringBuilder();
        for (String value : expected) {
            try (final DocumentContext dc = tailer.readingDocument()) {
                assert dc.isPresent();
                assert dc.isData();
                dc.wire().read("key").text(sb);
                assertEquals(value, sb.toString(), "Tailer should read expected value " + value);
            }
        }
        try (final DocumentContext dc = tailer.readingDocument()) {
            assert !dc.isPresent();
            assert !dc.isData();
            assert !dc.isMetaData();
        }
    }

    private int writeSequentialValueDocuments(ChronicleQueue queue, ExcerptAppender appender) {
        int cycle = appender.cycle();
        for (int i = 0; i < 5; i++) {
            final int j = i;
            appender.writeDocument(wire -> wire.write("key").text("value=" + j));
            if (i == 2) {
                final long cycle1 = queue.rollCycle().toCycle(appender.lastIndexAppended());
                assertEquals(cycle1, cycle, "document cycle should remain consistent during writes at iteration " + i);
            }
        }
        return cycle;
    }

    private void writeTwoMessages(File dir, RollCycle rollCycle, SetTimeProvider timeProvider, long advanceMillis) {
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider).build();
             final ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("first message");
        }

        timeProvider.advanceMillis(advanceMillis);

        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider).build();
             final ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("second message");
        }
    }

    private void verifyTwoMessagesRead(File dir, RollCycle rollCycle, SetTimeProvider timeProvider, boolean backwards) {
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle).timeProvider(timeProvider).build();
             ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
            if (backwards) {
                ExcerptTailer excerptTailer = tailer.direction(TailerDirection.BACKWARD).toEnd();
                assertEquals("second message", excerptTailer.readText(),
                        "Backward tailer should read second message first");
                assertEquals("first message", excerptTailer.readText(),
                        "Backward tailer should read first message next");
            } else {
                assertEquals("first message", tailer.readText(),
                        "Forward tailer should read first message");
                assertEquals("second message", tailer.readText(),
                        "Forward tailer should read second message");
            }
        }
    }

    @TestTemplate
    @DisplayName("Append before toEnd keeps reads consistent")
    public void testAppendedBeforeToEnd() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, this.wireType)
                .rollCycle(TEST_SECONDLY)
                .build();
             ChronicleQueue chronicle2 = builder(dir, this.wireType)
                     .rollCycle(TEST_SECONDLY)
                     .build();
             ExcerptAppender append = chronicle2.createAppender()) {
            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            append.writeDocument(w -> w.write("test").text("text"));

            while (tailer.state() == TailerState.UNINITIALISED)
                tailer.toEnd();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent(), tailer.index() + " " + tailer.state());
            }

            append.writeDocument(w -> w.write("test").text("text2"));
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent(), "Document context should be present after second append");

                assertEquals("text2", dc.wire().read("test").text(),
                        "Tailer should read second appended text");
            }
        }
    }

    @TestTemplate
    @DisplayName("Reentrant appender and tailer usage queue behaviour scenario")
    public void testReentrant() {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = binary(tmpDir)
                .testBlockSize()
                .rollCycle(TEST_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build();
             ExcerptAppender appender = queue.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");

                try (DocumentContext dc2 = appender.writingDocument()) {
                    dc2.wire().write("some2").text("other");
                }
                assertTrue(dc.isOpen(), "Outer document context should remain open");
            }
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 3\n" +
                    queueLockForTestReentrant() +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexMSynced: -1\n" +
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    400,\n" +
                    "    1717986918400\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  400,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!data #binary\n" +
                    "some: data\n" +
                    "some2: other\n" +
                    "...\n", tidyDump(queue), "Queue dump should match expected simple data output");
        }
    }

    private String queueLockForTestReentrant() {
        return "";
    }

    @TestTemplate
    @DisplayName("Tailer toEnd moves to last index")
    public void testToEnd() throws InterruptedException {
        File dir = getTmpDir();
        try (ChronicleQueue queue = builder(dir, wireType)
                .rollCycle(HOURLY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer(named ? "named" : null);

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            try (ChronicleQueue chronicle2 = builder(dir, wireType)
                    .rollCycle(HOURLY)
                    .build();
                 ExcerptAppender append = chronicle2.createAppender()) {

                append.writeDocument(w -> w.write("test").text("text"));

            }
            // this is needed to avoid caching of first and last cycle, see SingleChronicleQueue#setFirstAndLastCycle
            Thread.sleep(1);

            try (DocumentContext dc = tailer.readingDocument()) {
                try (final SingleChronicleQueue build = builder(dir, wireType).rollCycle(HOURLY).build()) {
                    String message = "dump: " + build.dump();
                    assertTrue(dc.isPresent(), message);
                    assertEquals("text", dc.wire().read("test").text(), message);
                }
            }
        }
    }

    @TestTemplate
    @DisplayName("toEnd handles roll cycle end queue behaviour scenario")
    public void testToEnd2() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, wireType)
                .build();
             ChronicleQueue chronicle2 = builder(dir, wireType)
                     .build();
             ExcerptAppender append = chronicle2.createAppender()) {

            append.writeDocument(w -> w.write("test").text("before text"));

            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            append.writeDocument(w -> w.write("test").text("text"));

            assertTrue(tailer.readDocument(w -> w.read("test").text("text", org.junit.jupiter.api.Assertions::assertEquals)),
                    "Tailer should read 'text' after toEnd and append");
        }
    }

    @TestTemplate
    @DisplayName("To end on deleted queue files")
    public void testToEndOnDeletedQueueFiles() throws IOException {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test delete after close on windows");
            return;
        }

        File dir = getTmpDir();
        try (ChronicleQueue q = builder(dir, wireType).build();
             ExcerptAppender append = q.createAppender()) {
            append.writeDocument(w -> w.write("test").text("before text"));

            ExcerptTailer tailer = q.createTailer(named ? "named" : null);

            // move to the end even though it doesn't exist yet.
            tailer.toEnd();

            append.writeDocument(w -> w.write("test").text("text"));

            assertTrue(tailer.readDocument(w -> w.read("test").text("text", org.junit.jupiter.api.Assertions::assertEquals)),
                    "Tailer should read 'text' after delete and append");

            try (Stream<Path> cq4Files = Files.find(dir.toPath(), 1, (p, basicFileAttributes) -> p.toString().endsWith("cq4"), FileVisitOption.FOLLOW_LINKS)) {
                final List<Path> unDeletable = cq4Files.filter(path -> !path.toFile().delete())
                        .collect(Collectors.toList());
                assertTrue(unDeletable.isEmpty(), "Unable to delete" + unDeletable);
            }

            try (ChronicleQueue q2 = builder(dir, wireType).build();
                 final ExcerptAppender q2Appender = q2.createAppender()) {
                tailer = q2.createTailer(named ? "named" : null);
                tailer.toEnd();
                assertEquals(TailerState.UNINITIALISED, tailer.state(), "tailer should be uninitialized after toEnd on empty queue");
                q2Appender.writeDocument(w -> w.write("test").text("before text"));

                assertTrue(tailer.readDocument(w -> w.read("test").text("before text", org.junit.jupiter.api.Assertions::assertEquals)), "tailer.readDocument(w -> w.read(\"test\").text(\"before text\", org.junit.jupiter.api.Assertions::assertEquals))");
            }
        }
    }

    @TestTemplate
    @DisplayName("Read and write round trip queue behaviour scenario")
    public void testReadWrite() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, wireType)
                .rollCycle(HOURLY)
                .testBlockSize()
                .build();
             ChronicleQueue chronicle2 = builder(dir, wireType)
                     .rollCycle(HOURLY)
                     .testBlockSize()
                     .build();
             ExcerptAppender append = chronicle2.createAppender()) {
            int runs = 50_000;
            for (int i = 0; i < runs; i++) {
                append.writeDocument(w -> w
                        .write("test - message")
                        .text("text"));
            }

            try (ExcerptTailer tailer = chronicle.createTailer(named ? "named1" : null);
                 ExcerptTailer tailer2 = chronicle.createTailer(named ? "named2" : null);
                 ExcerptTailer tailer3 = chronicle.createTailer(named ? "named3" : null);
                 ExcerptTailer tailer4 = chronicle.createTailer(named ? "named4" : null)) {
                for (int i = 0; i < runs; i++) {
                    if (i % 10000 == 0)
                        System.gc();
                    if (i % 2 == 0)
                        assertTrue(tailer2.readDocument(w -> w.read("test - message").text("text", org.junit.jupiter.api.Assertions::assertEquals)),
                                "tailer2 should read expected message at iteration " + i);
                    if (i % 3 == 0)
                        assertTrue(tailer3.readDocument(w -> w.read("test - message").text("text", org.junit.jupiter.api.Assertions::assertEquals)),
                                "tailer3 should read expected message at iteration " + i);
                    if (i % 4 == 0)
                        assertTrue(tailer4.readDocument(w -> w.read("test - message").text("text", org.junit.jupiter.api.Assertions::assertEquals)),
                                "tailer4 should read expected message at iteration " + i);
                    assertTrue(tailer.readDocument(w -> w.read("test - message").text("text", org.junit.jupiter.api.Assertions::assertEquals)),
                            "tailer should read expected message at iteration " + i);
                }
            }
        }
    }

    @TestTemplate
    @DisplayName("Reading document on empty queue returns false queue behaviour scenario")
    public void testReadingDocumentForEmptyQueue() {
        File dir = getTmpDir();
        try (ChronicleQueue chronicle = builder(dir, this.wireType)
                .rollCycle(HOURLY)
                .build()) {
            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);
            // DocumentContext is empty as we have no queue and don't know what the wire type will be.
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent(), "Document context should be empty for empty queue");
            }

            try (ChronicleQueue chronicle2 = builder(dir, this.wireType)
                    .rollCycle(HOURLY)
                    .build();
                 ExcerptAppender appender = chronicle2.createAppender()) {
                appender.writeDocument(w -> w.write("test - message").text("text"));

                while (tailer.state() == TailerState.UNINITIALISED)
                    tailer.toStart();

                // DocumentContext should not be empty as we know what the wire type will be.
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent(), "Document context should be present after writing a document");
                    dc.wire().read("test - message").text("text", org.junit.jupiter.api.Assertions::assertEquals);
                }
            }
        }
    }

    @TestTemplate
    @DisplayName("Metadata dump matches expected layout queue behaviour scenario")
    public void testMetaData6() {
        assumeFalse(named, "Metadata dump test requires unnamed tailer mode");
        try (final ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build();
             final ExcerptAppender appender = chronicle.createAppender()) {

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Quartilla");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                assertFalse(dc.isMetaData(), "Document context should be data for Helen entry");
                dc.wire().write("FirstName").text("Helen");
            }
            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("FirstName").text("Steve");
            }

            final ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

            StringBuilder event = new StringBuilder();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData(), "Document context should be metadata for Quartilla entry");
                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Quartilla", org.junit.jupiter.api.Assertions::assertEquals);
                    break;
                }
            }

            try (DocumentContext dc = tailer.readingDocument(true)) {
                assertTrue(dc.isData(), "Document context should be data for Helen read");
                assertTrue(dc.isPresent(), "Document context should be present for Helen read");
                dc.wire().read("FirstName").text("Helen", org.junit.jupiter.api.Assertions::assertEquals);
            }

            while (true) {
                try (DocumentContext dc = tailer.readingDocument(true)) {
                    assertTrue(dc.isMetaData(), "Document context should be metadata for Steve entry");
                    ValueIn in = dc.wire().read(event);
                    if (!StringUtils.isEqual(event, "FirstName"))
                        continue;

                    in.text("Steve", org.junit.jupiter.api.Assertions::assertEquals);
                    break;
                }
            }
            assertEquals(expectedMetaDataTest2(), tidyDump(chronicle),
                    "Queue dump should match expected metadata");
        }
    }
    // CPD-ON

    @NotNull
    protected String expectedMetaDataTest2() {
        if (wireType == WireType.BINARY || wireType == WireType.BINARY_LIGHT)
            return "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T2', epoch: 0 },\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 4\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexMSynced: -1\n" +
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    552,\n" +
                    "    2370821947392\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  552,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Quartilla\n" +
                    "--- !!data #binary\n" +
                    "FirstName: Helen\n" +
                    "--- !!meta-data #binary\n" +
                    "FirstName: Steve\n" +
                    "...\n";

        throw new IllegalStateException("unknown type " + wireType);
    }

    @TestTemplate
    @DisplayName("toEnd before write reads new data")
    public void testToEndBeforeWrite() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST2_DAILY)
                .build();
             ExcerptAppender appender = chronicle.createAppender();
             ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null)) {

            int entries = chronicle.rollCycle().defaultIndexSpacing() * 2 + 2;

            final StringBuilder actual = new StringBuilder();
            for (int i = 0; i < entries; i++) {
                tailer.toEnd();
                int finalI = i;
                appender.writeDocument(w -> w.writeEventName("hello").text("world" + finalI));
                actual.setLength(0);
                final boolean read = tailer.readDocument(w -> w.read().text(actual));
                assertTrue(read, "toEnd before write: read i=" + finalI);
                assertEquals("world" + finalI, actual.toString(), "toEnd before write: value i=" + finalI);
            }
        }
    }

    @TestTemplate
    @DisplayName("Forward then backward tailer reads entries")
    public void testForwardFollowedBackBackwardTailer() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build();
             ExcerptAppender appender = chronicle.createAppender()) {

            int entries = chronicle.rollCycle().defaultIndexSpacing() + 2;

            for (int i = 0; i < entries; i++) {
                int finalI = i;
                appender.writeDocument(w -> w.writeEventName("hello").text("world" + finalI));
            }
            for (int i = 0; i < 3; i++) {
                assertEquals(entries, readForward(chronicle, entries), "forward read pass=" + i);
                assertEquals(entries, readBackward(chronicle, entries), "backward read pass=" + i);
            }
        }
    }

    @TestTemplate
    @DisplayName("Should read backward from end of queue when direction is set after move to end")
    public void shouldReadBackwardFromEndOfQueueWhenDirectionIsSetAfterMoveToEnd() {
        try (final ChronicleQueue queue = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST2_DAILY)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            appender.writeDocument(w -> w.writeEventName("hello").text("world"));

            final ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            tailer.toEnd();
            tailer.direction(TailerDirection.BACKWARD);

            assertTrue(tailer.readingDocument().isPresent(),
                    "Tailer should read last document when moving backward from end");
        }
    }

    private int readForward(@NotNull ChronicleQueue chronicle, int entries) {
        try (ExcerptTailer forwardTailer = chronicle.createTailer(named ? "named" : null)
                .direction(TailerDirection.FORWARD)
                .toStart()) {

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < entries; i++) {
                try (DocumentContext documentContext = forwardTailer.readingDocument()) {
                    assertTrue(documentContext.isPresent(), "Forward tailer should read document " + i);
                    assertEquals(i, DEFAULT.toSequenceNumber(documentContext.index()),
                            "Sequence number should match forward index " + i);
                    sb.setLength(0);
                    ValueIn valueIn = documentContext.wire().readEventName(sb);
                    assertTrue("hello".contentEquals(sb), "Forward event name should be hello at index " + i);
                    String actual = valueIn.text();
                    assertEquals("world" + i, actual, "document value should match expected world sequence at index " + i);
                }
            }
            try (DocumentContext documentContext = forwardTailer.readingDocument()) {
                assertFalse(documentContext.isPresent(), "Forward tailer should have no more documents");
            }
            return entries;
        }
    }

    private int readBackward(@NotNull ChronicleQueue chronicle, int entries) {
        ExcerptTailer backwardTailer = chronicle.createTailer(named ? "named" : null)
                .direction(TailerDirection.BACKWARD)
                .toEnd();

        StringBuilder sb = new StringBuilder();
        for (int i = entries - 1; i >= 0; i--) {
            try (DocumentContext documentContext = backwardTailer.readingDocument()) {
                assertTrue(documentContext.isPresent(), "Backward tailer should read document " + i);
                final long index = documentContext.index();
                assertEquals(i, (int) index, "document index should match reverse iteration at index " + i);
                assertEquals(i, DEFAULT.toSequenceNumber(index), "Sequence number should match reverse index " + i);
                assertTrue(documentContext.isPresent(), "Document context should remain present during read at index " + i);
                sb.setLength(0);
                ValueIn valueIn = documentContext.wire().readEventName(sb);
                assertTrue("hello".contentEquals(sb), "Backward event name should be hello at index " + i);
                String actual = valueIn.text();
                assertEquals("world" + i, actual, "document value should match expected world sequence in reverse at index " + i);
            }
        }
        try (DocumentContext documentContext = backwardTailer.readingDocument()) {
            assertFalse(documentContext.isPresent(), "Backward tailer should have no more documents");
        }
        return entries;
    }

    @TestTemplate
    @DisplayName("Overread forward from future cycle then read backward tailer")
    public void testOverreadForwardFromFutureCycleThenReadBackwardTailer() {
        // when "forwardToFuture" flag is set, go one cycle to the future
        AtomicBoolean forwardToFuture = new AtomicBoolean(false);
        TimeProvider timeProvider = () -> forwardToFuture.get()
                ? System.currentTimeMillis() + TimeUnit.MILLISECONDS.toDays(1)
                : System.currentTimeMillis();

        try (ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .rollCycle(TestRollCycles.TEST2_DAILY)
                .timeProvider(timeProvider)
                .build();
             ExcerptAppender appender = chronicle.createAppender()) {

            appender.writeDocument(w -> w.writeEventName("hello").text("world"));

            // go to the cycle next to the one the write was made on
            forwardToFuture.set(true);

            ExcerptTailer forwardTailer = chronicle.createTailer(named ? "named" : null)
                    .direction(TailerDirection.FORWARD)
                    .toStart();

            try (DocumentContext context = forwardTailer.readingDocument()) {
                assertTrue(context.isPresent(), "Forward tailer should read document in future cycle");
            }
            try (DocumentContext context = forwardTailer.readingDocument()) {
                assertFalse(context.isPresent(), "Forward tailer should not read second document in future cycle");
            }

            ExcerptTailer backwardTailer = chronicle.createTailer(named ? "named" : null)
                    .direction(TailerDirection.BACKWARD)
                    .toEnd();

            try (DocumentContext context = backwardTailer.readingDocument()) {
                assertTrue(context.isPresent(), "Backward tailer should read document after forward overread");
            }
        }
    }

    @TestTemplate
    @DisplayName("Reads simple messages sequence queue behaviour scenario")
    public void testSomeMessages() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST2_DAILY)
                .build();
             ExcerptAppender appender = chronicle.createAppender();
             ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null)) {

            int entries = chronicle.rollCycle().defaultIndexSpacing() * 2 + 2;

            for (long i = 0; i < entries; i++) {
                long finalI = i;
                String readMessage = "read int64 value should match expected value at index " + i;
                appender.writeDocument(w -> w.writeEventName("hello").int64(finalI));
                long seq = chronicle.rollCycle().toSequenceNumber(appender.lastIndexAppended());
                assertEquals(i, seq, "sequence number should match iteration index after appending message at index " + i);
                tailer.readDocument(w -> w.read().int64(finalI,
                        (a, b) -> assertEquals((long) a, b, readMessage)));
            }
        }
    }

    @TestTemplate
    @DisplayName("Zero length message reads cleanly queue behaviour scenario")
    public void testZeroLengthMessage() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), wireType)
                .rollCycle(TEST_DAILY)
                .build();
             ExcerptAppender appender = chronicle.createAppender()) {

            appender.writeDocument(w -> {
            });
            ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.wire().hasMore(), "Zero-length document should have no data");
            }
        }
    }

    @TestTemplate
    @DisplayName("MoveToIndex works after appends queue behaviour scenario")
    public void testMoveToWithAppender() {
        try (ChronicleQueue syncQ = builder(getTmpDir(), this.wireType)
                .build();
             InternalAppender sync = (InternalAppender) syncQ.createAppender()) {

            File name2 = getTmpDir();
            try (ChronicleQueue chronicle = builder(name2, this.wireType)
                    .build();
                 ExcerptAppender appender = chronicle.createAppender()) {

                appender.writeDocument(w -> w.writeEventName("hello").text("world0"));
                appender.writeDocument(w -> w.writeEventName("hello").text("world1"));
                appender.writeDocument(w -> w.writeEventName("hello").text("world2"));

                ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    sync.writeBytes(documentContext.index(), documentContext.wire().bytes());
                }
                try (DocumentContext documentContext = tailer.readingDocument()) {
                    String text = documentContext.wire().read().text();
                    assertEquals("world1", text, "second document text should match first written value");
                }
            }
        }
    }

    @TestTemplate
    @DisplayName("Map wrapper object round trip queue behaviour scenario")
    public void testMapWrapper() {
        try (ChronicleQueue syncQ = builder(getTmpDir(), this.wireType)
                .build()) {

            File name2 = getTmpDir();
            try (ChronicleQueue chronicle = builder(name2, this.wireType)
                    .build();
                 ExcerptAppender appender = chronicle.createAppender()) {

                MapWrapper myMap = new MapWrapper();
                myMap.map.put("hello", 1.2);

                appender.writeDocument(w -> w.write().object(myMap));

                ExcerptTailer tailer = chronicle.createTailer(named ? "named" : null);

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    MapWrapper object = documentContext.wire().read().object(MapWrapper.class);
                    assertEquals(1.2, object.map.get("hello"), 0.0, "Map should contain hello=1.2");
                }
            }
        }
    }

    @TestTemplate
    @DisplayName("Last index appended increments after write")
    public void testLastIndexAppended() {
        try (ChronicleQueue chronicle = builder(getTmpDir(), this.wireType)
                .build();
             ExcerptAppender appender = chronicle.createAppender()) {

            appender.writeDocument(w -> w.writeEventName("hello").text("world0"));
            final long nextIndexToWrite = appender.lastIndexAppended() + 1;
            appender.writeDocument(w -> w.getValueOut().bytes(new byte[0]));
            assertEquals(nextIndexToWrite,
                    appender.lastIndexAppended(), "Last index appended should advance after empty bytes write");
        }
    }

    @TestTemplate
    @DisplayName("Appended skip to end multi threaded")
    public void testAppendedSkipToEndMultiThreaded() throws InterruptedException {
        // some text to simulate load.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) sb.append(UUID.randomUUID());
        String text = sb.toString();

        try (ChronicleQueue q = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST_SECONDLY)
                .build()) {

            System.err.println(q.file());

            int size = 50_000;
            int threadCount = 8;
            int sizePerThread = size / threadCount;
            CountDownLatch latch = new CountDownLatch(threadCount);

            for (int j = 0; j < threadCount; j++) {
                new Thread(() -> {
                    try (final ExcerptAppender appender = q.createAppender()) {
                        for (int i = 0; i < sizePerThread; i++)
                            writeTestDocument(appender, text);
                    }
                    latch.countDown();
                }).start();
            }

            latch.await();

            try (ExcerptTailer tailer = q.createTailer(named ? "named" : null)) {
                for (int i = 0; i < size; i++) {
                    try (DocumentContext dc = tailer.readingDocument(false)) {
                        if (!dc.isPresent()) {
                            i--;
                            Thread.yield();
                            continue;
                        }
                        long index = dc.index();
                        long actual = dc.wire().read("key").int64();

                        assertEquals(toTextIndex(q, index), toTextIndex(q, actual),
                                "Index should round-trip through toTextIndex at iteration " + i);
                    }
                }
            }
        }
    }

    @NotNull
    private String toTextIndex(ChronicleQueue q, long index) {
        return Long.toHexString(q.rollCycle().toCycle(index)) + "_" + Long.toHexString(q.rollCycle().toSequenceNumber(index));
    }

    /**
     * if one appender if much further ahead than the other, then the new append should jump straight to the end rather than attempting to write a
     * positions that are already occupied
     */
    @TestTemplate
    @DisplayName("Appender skips to end when behind")
    public void testAppendedSkipToEnd() {

        try (ChronicleQueue q = builder(getTmpDir(), this.wireType)
                .build();
             ExcerptAppender appender = q.createAppender();
             ExcerptAppender appender2 = q.createAppender()) {

            int indexCount = 100;

            for (int i = 0; i < indexCount; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("key").text("some more " + 1);
                }
                assertEquals(i, q.rollCycle().toSequenceNumber(appender.lastIndexAppended()),
                        "Sequence number should match write count " + i);
            }

            try (DocumentContext dc = appender2.writingDocument()) {
                dc.wire().write("key").text("some data " + indexCount);
            }
            assertEquals(indexCount, q.rollCycle().toSequenceNumber(appender2.lastIndexAppended()),
                    "Second appender should continue after " + indexCount + " writes");
        }
    }

    @TestTemplate
    @DisplayName("toEnd handles previous cycle EOF queue behaviour scenario")
    public void testToEndPrevCycleEOF() {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        File dir = getTmpDir();
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build();
             final ExcerptAppender appender = q.createAppender()) {
            appender.writeText("first");
        }
        AbstractCloseable.assertCloseablesClosed();
        clock.addAndGet(1100);

        // this will write an EOF
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer tailer = q.createTailer(named ? "named" : null);

            assertEquals("first", tailer.readText(), "Tailer should read first text before EOF");
            assertNull(tailer.readText(), "Tailer should return null after EOF");

        }
        AbstractCloseable.assertCloseablesClosed();

        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build()) {

            ExcerptTailer tailer = q.createTailer(named ? "named" : null).toEnd();

            try (DocumentContext documentContext = tailer.readingDocument()) {
                assertFalse(documentContext.isPresent(), "Tailer should be at end before append");
            }

            try (DocumentContext documentContext = tailer.readingDocument()) {
                assertFalse(documentContext.isPresent(), "Tailer should still be at end before append");
            }
        }
        AbstractCloseable.assertCloseablesClosed();

        clock.addAndGet(50L);

        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build();
             final ExcerptAppender appender = q.createAppender()) {

            ExcerptTailer excerptTailerBeforeAppend = q.createTailer(named ? "named" : null).toEnd();
            appender.writeText("more text");
            ExcerptTailer excerptTailerAfterAppend = q.createTailer(named ? "named" : null).toEnd();
            appender.writeText("even more text");

            assertEquals("more text", excerptTailerBeforeAppend.readText(),
                    "Tailer created before append should read first new text");
            assertEquals("even more text", excerptTailerAfterAppend.readText(),
                    "Tailer created after append should read latest text");
            assertEquals("even more text", excerptTailerBeforeAppend.readText(),
                    "Existing tailer should read latest text after new append");
        }
        AbstractCloseable.assertCloseablesClosed();

    }

    @Disabled("Long running test disabled for standard runs")
    @TestTemplate
    @DisplayName("Random concurrent read and write queue behaviour scenario")
    public void testRandomConcurrentReadWrite() throws
            InterruptedException {

        // some text to simulate load.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) sb.append(UUID.randomUUID());
        String text = sb.toString();

        for (int i = 0; i < 20; i++) {
            ExecutorService executor = Executors.newWorkStealingPool(8);
            try (ChronicleQueue q = builder(getTmpDir(), this.wireType)
                    .rollCycle(MINUTELY)
                    .build()) {

                int size = 20_000_000;

                for (int j = 0; j < size; j++)
                    executor.execute(() -> {
                        try (final ExcerptAppender appender = q.createAppender();
                             final ExcerptTailer tailer = q.createTailer()) {
                            doSomething(appender, tailer, text);
                        }
                    });

                executor.shutdown();
                boolean terminated = executor.awaitTermination(10_000, TimeUnit.SECONDS);
                if (!terminated) {
                    executor.shutdownNow();
                    terminated = executor.awaitTermination(30, TimeUnit.SECONDS);
                }
                assertTrue(terminated, "random concurrent read/write: executor terminated at iteration " + i);

                Jvm.pause(1000);
            }
        }
    }

    @TestTemplate
    @DisplayName("Tailer when cycles where skipped on write")
    public void testTailerWhenCyclesWhereSkippedOnWrite() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        try (final ChronicleQueue queue = binary(getTmpDir())
                .rollCycle(TEST_SECONDLY).timeProvider(timeProvider)
                .syncMode(SyncMode.SYNC)
                .build();
             final ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
            tailer.sync();
            try (final ExcerptAppender appender = queue.createAppender()) {
                appender.sync();

                final List<String> stringsToPut = Arrays.asList("one", "two", "three");

                // writes two strings immediately and one string with 2 seconds delay
                {
                    try (DocumentContext writingContext = appender.writingDocument()) {
                        writingContext.wire()
                                .write().bytes(stringsToPut.get(0).getBytes());
                    }
                    appender.sync();
                    try (DocumentContext writingContext = appender.writingDocument()) {
                        writingContext.wire()
                                .write().bytes(stringsToPut.get(1).getBytes());
                    }
                    appender.sync();
                    timeProvider.advanceMillis(2100);
                    try (DocumentContext writingContext = appender.writingDocument()) {
                        writingContext.wire().write().bytes(stringsToPut.get(2).getBytes());
                    }
                    appender.sync();
                }

                for (String expected : stringsToPut) {
                    try (DocumentContext readingContext = tailer.readingDocument()) {
                        if (!readingContext.isPresent())
                            fail("Reading context should be present for expected value " + expected);
                        String text = readingContext.wire().read().text();
                        assertEquals(expected, text, "read text should match expected string value after queue roll");

                    }
                    tailer.sync();
                }
            }
        }
    }

    private void doSomething(@NotNull ExcerptAppender appender, @NotNull ExcerptTailer tailer, String text) {
        if (Math.random() > 0.5)
            writeTestDocument(appender, text);
        else
            readDocument(tailer, text);
    }

    private void readDocument(@NotNull ExcerptTailer tailer, String text) {
        try (DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent())
                return;
            assertEquals(dc.index(), dc.wire().read("key").int64(), "Document key should match index");
            assertEquals(text, dc.wire().read("text").text(), "Document text should match expected value");
        }
    }

    private void writeTestDocument(@NotNull ExcerptAppender appender, String text) {
        try (DocumentContext dc = appender.writingDocument()) {
            long index = dc.index();
            dc.wire().write("key").int64(index);
            dc.wire().write("text").text(text);
        }
    }

    @TestTemplate
    @DisplayName("Multiple appenders interleave writes correctly queue behaviour scenario")
    public void testMultipleAppenders() {
        try (ChronicleQueue syncQ = builder(getTmpDir(), this.wireType)
                .rollCycle(TEST_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build();
             ExcerptAppender syncA = syncQ.createAppender();
             ExcerptAppender syncB = syncQ.createAppender();
             ExcerptAppender syncC = syncQ.createAppender()) {
            int count = 0;
            for (int i = 0; i < 3; i++) {
                syncA.writeText("hello A" + i);
                assertEquals(count++, (int) syncA.lastIndexAppended(), "syncA should append at expected sequence at iteration " + i);
                syncB.writeText("hello B" + i);
                assertEquals(count++, (int) syncB.lastIndexAppended(), "syncB should append at expected sequence at iteration " + i);
                try (DocumentContext dc = syncC.writingDocument(true)) {
                    dc.wire().getValueOut().text("some meta " + i);
                }
            }
            String expected = expectedMultipleAppenders();
            assertEquals(expected, tidyDump(syncQ), "Queue dump should match expected output for multiple appenders");
        }
    }

    @NotNull
    private static String tidyDump(ChronicleQueue queue) {
        return queue.dump()
                .replaceAll("(?m)^#.+$\\n", "")
                .replaceAll("(\\n0000\\d+ ).*", "$1Binary");
    }

    @NotNull
    protected String expectedMultipleAppenders() {
        if (wireType == WireType.BINARY || wireType == WireType.BINARY_LIGHT)
            return "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 5\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexMSynced: -1\n" +
                    "--- !!data #binary\n" +
                    "normalisedEOFsTo: 18554\n" +
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    512,\n" +
                    "    2199023255557\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 6\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 6\n" +
                    "  400,\n" +
                    "  416,\n" +
                    "  448,\n" +
                    "  464,\n" +
                    "  496,\n" +
                    "  512,\n" +
                    "  0, 0\n" +
                    "]\n" +
                    "--- !!data #binary\n" +
                    "hello A0\n" +
                    "--- !!data #binary\n" +
                    "hello B0\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 0\n" +
                    "--- !!data #binary\n" +
                    "hello A1\n" +
                    "--- !!data #binary\n" +
                    "hello B1\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 1\n" +
                    "--- !!data #binary\n" +
                    "hello A2\n" +
                    "--- !!data #binary\n" +
                    "hello B2\n" +
                    "--- !!meta-data #binary\n" +
                    "some meta 2\n" +
                    "...\n";

        throw new IllegalStateException("unknown wiretype=" + wireType);
    }

    @TestTemplate
    @DisplayName("Should not generate garbage reading document after end of file")
    public void shouldNotGenerateGarbageReadingDocumentAfterEndOfFile() {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        File dir = getTmpDir();
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build();
             final ExcerptAppender appender = q.createAppender()) {
            appender.writeText("first");
        }

        clock.addAndGet(1100);

        // this will write an EOF
        try (ChronicleQueue q = builder(dir, wireType)
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .build();

             ExcerptTailer tailer = q.createTailer(named ? "named" : null)) {

            assertEquals("first", tailer.readText(), "Tailer should read first message before EOF");
            GcControls.waitForGcCycle();
            final long startCollectionCount = GcControls.getGcCount();

            // allow a few GCs due to possible side-effect or re-used JVM
            final long maxAllowedGcCycles = 6;
            final long endCollectionCount = GcControls.getGcCount();
            final long actualGcCycles = endCollectionCount - startCollectionCount;

            assertTrue(actualGcCycles <= maxAllowedGcCycles, String.format("Too many GC cycles. Expected <= %d, but was %d",
                    maxAllowedGcCycles, actualGcCycles));
        }
    }

    @TestTemplate
    @DisplayName("Reading writing when next cycle is in sequence")
    public void testReadingWritingWhenNextCycleIsInSequence() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        final File dir = getTmpDir();
        final RollCycle rollCycle = TEST_SECONDLY;

        writeTwoMessages(dir, rollCycle, timeProvider, 1100);
        verifyTwoMessagesRead(dir, rollCycle, timeProvider, false);
    }

    @TestTemplate
    @DisplayName("Reading writing when cycle is skipped")
    public void testReadingWritingWhenCycleIsSkipped() {

        SetTimeProvider timeProvider = new SetTimeProvider();

        final File dir = getTmpDir();
        final RollCycle rollCycle = TEST_SECONDLY;

        writeTwoMessages(dir, rollCycle, timeProvider, 2100);
        verifyTwoMessagesRead(dir, rollCycle, timeProvider, false);
    }

    @TestTemplate
    @DisplayName("Reading writing when cycle is skipped backwards")
    public void testReadingWritingWhenCycleIsSkippedBackwards() {
        final SetTimeProvider timeProvider = new SetTimeProvider();
        long time = System.currentTimeMillis();
        timeProvider.currentTimeMillis(time);

        final File dir = getTmpDir();
        final RollCycle rollCycle = TEST_SECONDLY;

        writeTwoMessages(dir, rollCycle, timeProvider, 2100);
        verifyTwoMessagesRead(dir, rollCycle, timeProvider, true);
    }

    @TestTemplate
    @DisplayName("Read and write with time provider queue behaviour scenario")
    public void testReadWritingWithTimeProvider() {
        final File dir = getTmpDir();

        long time = System.currentTimeMillis();

        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(time);
        try (ChronicleQueue q1 = binary(dir)
                .timeProvider(timeProvider)
                .build()) {

            try (ChronicleQueue q2 = binary(dir)
                    .timeProvider(timeProvider)
                    .build();

                 final ExcerptAppender appender2 = q2.createAppender();
                 final ExcerptTailer tailer1 = q1.createTailer(named ? "named" : null);
                 final ExcerptTailer tailer2 = q2.createTailer(named ? "named" : null)) {

                try (final DocumentContext dc = appender2.writingDocument()) {
                    dc.wire().write().text("some data");
                }

                try (DocumentContext dc = tailer2.readingDocument()) {
                    assertTrue(dc.isPresent(), "Tailer should see document written by second queue");
                }

                assertEquals(q1.file(), q2.file(), "Queues should point at same directory");
                // this is required for queue to re-request last/first cycle
                timeProvider.advanceMillis(1);

                for (int i = 0; i < 10; i++) {
                    try (DocumentContext dc = tailer1.readingDocument()) {
                        if (dc.isPresent())
                            return;
                    }
                    Jvm.pause(1);
                }
                fail("Tailer should observe document within retry loop");
            }
        }
    }

    @TestTemplate
    @DisplayName("Count excerpts across cycle boundaries queue behaviour scenario")
    public void testCountExceptsBetweenCycles() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        try (final RollingChronicleQueue queue = binary(getTmpDir())
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            long[] indexs = new long[10];
            for (int i = 0; i < indexs.length; i++) {
                try (DocumentContext writingContext = appender.writingDocument()) {
                    writingContext.wire().write().text("some-text-" + i);
                    indexs[i] = writingContext.index();
                }

                // we add the pause times to vary the test, to ensure it can handle when cycles are
                // skipped
                if ((i + 1) % 5 == 0)
                    timeProvider.advanceMillis(2000);
                else if ((i + 1) % 3 == 0)
                    timeProvider.advanceMillis(1000);
            }

            for (int lower = 0; lower < indexs.length; lower++) {
                for (int upper = lower; upper < indexs.length; upper++) {
                    assertEquals(upper - lower, queue.countExcerpts(indexs[lower], indexs[upper]),
                            "Count between indexes should match range " + lower + " to " + upper);
                }
            }

            // check the base line of the test below
            assertEquals(6, queue.countExcerpts(indexs[0], indexs[6]),
                    "Baseline count should be 6 between index 0 and 6");

            /// check for the case when the last index has a sequence number of -1
            assertEquals(0, queue.rollCycle().toSequenceNumber(indexs[6]),
                    "Sequence number should be 0 for index 6 rollover case");
            assertEquals(5, queue.countExcerpts(indexs[0],
                    indexs[6] - 1), "Count should ignore last index when seq is -1");

            /// check for the case when the first index has a sequence number of -1
            assertEquals(7, queue.countExcerpts(indexs[0] - 1,
                    indexs[6]), "Count should include index 6 when lower bound shifts");
        }
    }

    @TestTemplate
    @DisplayName("Long living tailer appender re acquired each second")
    public void testLongLivingTailerAppenderReAcquiredEachSecond() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        final File dir = getTmpDir();
        final RollCycle rollCycle = TEST4_SECONDLY;

        try (ChronicleQueue queuet = binary(dir)
                .rollCycle(rollCycle)
                .testBlockSize()
                .timeProvider(timeProvider)
                .build();
             final ExcerptTailer tailer = queuet.createTailer(named ? "named" : null)) {

            // The look up of the first and last cycle is cached at this point and won't be checked again for 1 millisecond to reduce overhead.
            Jvm.pause(1);

            // write first message
            try (ChronicleQueue queue =
                         binary(dir)
                                 .rollCycle(rollCycle)
                                 .testBlockSize()
                                 .timeProvider(timeProvider)
                                 .build();
                 final ExcerptAppender appender = queue.createAppender()) {

                for (int i = 0; i < 5; i++) {
                    Jvm.pause(1);

                    timeProvider.advanceMillis(1100);
                    try (final DocumentContext dc = appender.writingDocument()) {
                        dc.wire().write("some").int32(i);
                    }

                    try (final DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent())
                            System.out.println(queue.dump());

                        assertTrue(dc.isPresent(), "Tailer should read document at iteration " + i);
                        assertEquals(i, dc.wire().read("some").int32(),
                                "Tailer should read expected value at iteration " + i);
                    }
                }
            }
        }
    }

    @TestTemplate
    @DisplayName("Count excerpts rejects invalid indexes queue behaviour scenario")
    public void testCountExceptsWithRubbishData() {

        try (final RollingChronicleQueue queue = binary(getTmpDir())
                .rollCycle(TEST_SECONDLY)
                .build()) {

            // rubbish data
            assertThrows(IllegalStateException.class,
                    () -> queue.countExcerpts(0x578F542D00000000L, 0x528F542D00000000L),
                    "countExcerpts should reject invalid index range");
        }
    }

    @TestTemplate
    @DisplayName("Read from size prefixed blobs queue behaviour scenario")
    public void testFromSizePrefixedBlobs() {

        try (final ChronicleQueue queue = binary(getTmpDir())
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");
            }
            String s;

            DocumentContext dc0;
            try (DocumentContext dc = queue.createTailer(named ? "named" : null).readingDocument()) {
                s = Wires.fromSizePrefixedBlobs(dc);
                assertTrue(s.contains("some: data"), "Dump should include 'some: data'");
                dc0 = dc;
            }

            String out = Wires.fromSizePrefixedBlobs(dc0);
            assertEquals(s, out, "size prefixed blob output should match original after document context closed");

        }
    }

    @TestTemplate
    @DisplayName("Tailer rollback reads expected entries queue behaviour scenario")
    public void tailerRollBackTest() {
        final File source = getTmpDir();
        try (final ChronicleQueue q = binary(source).build();
             final ExcerptAppender appender = q.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello").text("hello-world");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("hello2").text("hello-world-2");
            }
        }

        try (final ChronicleQueue q = binary(source).build();
             final ExcerptTailer tailer = q.createTailer(named ? "named" : null).toStart()) {

            final StringBuilder eventName = new StringBuilder();
            final StringBuilder value = new StringBuilder();

            final boolean firstRead = tailer.readDocument(wire -> {
                eventName.setLength(0);
                value.setLength(0);
                wire.readEventName(eventName).text(value);
            });
            assertTrue(firstRead, "tailer roll back: read first");
            assertEquals("hello", eventName.toString(), "tailer roll back: first key");
            assertEquals("hello-world", value.toString(), "tailer roll back: first value");

            final boolean secondRead = tailer.readDocument(wire -> {
                eventName.setLength(0);
                value.setLength(0);
                wire.readEventName(eventName).text(value);
            });
            assertTrue(secondRead, "tailer roll back: read second");
            assertEquals("hello2", eventName.toString(), "tailer roll back: second key");
            assertEquals("hello-world-2", value.toString(), "tailer roll back: second value");

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent(), "tailer roll back: end");
            }
        }
    }

    @TestTemplate
    @DisplayName("Copy queue data between directories queue behaviour scenario")
    public void testCopyQueue() {
        final File source = getTmpDir();
        final File target = getTmpDir();
        {

            try (final ChronicleQueue q =
                         binary(source)
                                 .build();
                 ExcerptAppender excerptAppender = q.createAppender()) {

                excerptAppender.writeMessage("one", 1);
                excerptAppender.writeMessage("two", 2);
                excerptAppender.writeMessage("three", 3);
                excerptAppender.writeMessage("four", 4);
            }
        }
        {
            try (final ChronicleQueue s = binary(source).build();
                 final ChronicleQueue t = binary(target).build();
                 ExcerptTailer sourceTailer = s.createTailer(named ? "named" : null);
                 ExcerptAppender appender = t.createAppender()) {

                for (; ; ) {
                    try (DocumentContext rdc = sourceTailer.readingDocument()) {
                        if (!rdc.isPresent())
                            break;

                        try (DocumentContext wdc = appender.writingDocument()) {
                            final Bytes<?> bytes = rdc.wire().bytes();
                            wdc.wire().bytes().write(bytes);
                        }
                    }
                }
            }
        }
        {
            try (final ChronicleQueue t = binary(target).build();
                 final ExcerptTailer tailer = t.createTailer(named ? "named" : null).toStart()) {

                final String[] expectedEventNames = {"one", "two", "three", "four"};
                final long[] expectedValues = {1L, 2L, 3L, 4L};
                final StringBuilder eventName = new StringBuilder();
                final long[] value = new long[1];

                for (int i = 0; i < expectedEventNames.length; i++) {
                    final String expectedEventName = expectedEventNames[i];
                    final long expectedValue = expectedValues[i];
                    final boolean read = tailer.readDocument(wire -> {
                        eventName.setLength(0);
                        final ValueIn valueIn = wire.readEventName(eventName);
                        value[0] = valueIn.int64();
                    });

                    assertTrue(read, "Tailer should read event " + expectedEventName + " at index " + i);
                    assertEquals(expectedEventName, eventName.toString(), "Event name should match expected value at index " + i);
                    assertEquals(expectedValue, value[0], "copy queue should read value for event " + expectedEventName + " at index " + i);
                }

                try (DocumentContext dc = tailer.readingDocument()) {
                    assertFalse(dc.isPresent(), "Copied queue should have no more documents");
                }
            }
        }
    }

    /**
     * see https://github.com/OpenHFT/Chronicle-Queue/issues/299
     */
    @TestTemplate
    @DisplayName("Incorrect excerpt tailer reads after switching tailer direction")
    public void testIncorrectExcerptTailerReadsAfterSwitchingTailerDirection() {

        try (final ChronicleQueue queue = binary(getTmpDir())
                .rollCycle(DAILY).build();
             final ExcerptAppender appender = queue.createAppender()) {

            int value = 0;
            long cycle = 0;

            long startIndex = 0;
            for (int i = 0; i < 56; i++) {
                try (final DocumentContext dc = appender.writingDocument()) {

                    if (cycle == 0)
                        cycle = queue.rollCycle().toCycle(dc.index());
                    final long index = dc.index();
                    final long seq = queue.rollCycle().toSequenceNumber(index);

                    if (seq == 52)
                        startIndex = dc.index();

                    if (seq >= 52) {
                        final int v = value++;
                        dc.wire().write("value").int64(v);
                    } else {
                        dc.wire().write("value").int64(0);
                    }
                }
            }

            try (ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {

                assertTrue(tailer.moveToIndex(startIndex), "Tailer should move to starting index for seq 52");

                tailer.direction(TailerDirection.FORWARD);
                assertEquals(0, action(tailer, queue.rollCycle()),
                        "Tailer should read expected value after forward move");
                assertEquals(1, action(tailer, queue.rollCycle()),
                        "Tailer should read next value after forward move");

                tailer.direction(TailerDirection.BACKWARD);
                assertEquals(2, action(tailer, queue.rollCycle()),
                        "Tailer should read expected value after backward move");
                assertEquals(1, action(tailer, queue.rollCycle()),
                        "Tailer should read previous value after backward move");

                tailer.direction(TailerDirection.FORWARD);
                assertEquals(0, action(tailer, queue.rollCycle()),
                        "Tailer should read expected value after forward reset");
                assertEquals(1, action(tailer, queue.rollCycle()),
                        "Tailer should read next value after forward reset");
            }
        }
    }

    @TestTemplate
    @DisplayName("Existing roll cycle remains unchanged queue behaviour scenario")
    public void testExistingRollCycleIsMaintained() {
        expectException("Overriding roll cycle from ");
        expectException("Overriding roll length from ");

        List<RollCycle> values = StreamSupport.stream(RollCycles.all().spliterator(), false)
                .collect(Collectors.toList());
        for (int i = 0; i < values.size() - 1; i++) {
            final File tmpDir = getTmpDir();

            try (final ChronicleQueue queue = binary(tmpDir)
                    .rollCycle(values.get(i)).build();
                 final ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("hello world");
            }

            try (final ChronicleQueue queue = binary(tmpDir)
                    .rollCycle(values.get(i + 1)).build()) {
                assertEquals(values.get(i), queue.rollCycle(), "Queue roll cycle should remain on existing value at index " + i);
            }
        }
    }

    private long action(@NotNull final ExcerptTailer tailer1, @NotNull final RollCycle rollCycle) {
        try (final DocumentContext dc = tailer1.readingDocument()) {
            return dc.wire().read("value").int64();
        } finally {
            rollCycle.toSequenceNumber(tailer1.index());
        }
    }

    @TestTemplate
    @DisplayName("Check reference counting and check file deletion")
    public void checkReferenceCountingAndCheckFileDeletion() {

        MappedFile mappedFile;

        try (ChronicleQueue queue =
                     binary(getTmpDir())
                             .rollCycle(TEST_SECONDLY)
                             .build();
             ExcerptAppender appender = queue.createAppender()) {

            try (DocumentContext documentContext1 = appender.writingDocument()) {
                documentContext1.wire().write().text("some text");
            }

            try (DocumentContext documentContext = queue.createTailer(named ? "named" : null).readingDocument()) {
                mappedFile = toMappedFile(documentContext);
                assertEquals("some text", documentContext.wire().read().text(),
                        "Tailer should read first written text");
            }
        }

        waitFor(mappedFile::isClosed, "mappedFile is not closed");

        if (OS.isWindows()) {
            System.err.println("#460 Cannot test delete after close on windows");
            return;
        }
        // this used to fail on windows
        assertTrue(mappedFile.file().delete(), "Mapped file should delete after close");

    }

    @TestTemplate
    @DisplayName("Check reference counting when rolling and check file deletion")
    public void checkReferenceCountingWhenRollingAndCheckFileDeletion() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        @SuppressWarnings("unused")
        MappedFile mappedFile1, mappedFile2;

        try (ChronicleQueue queue =
                     binary(getTmpDir())
                             .rollCycle(TEST_SECONDLY)
                             .timeProvider(timeProvider)
                             .build();
             ExcerptAppender appender = queue.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write().text("some text");
                mappedFile1 = toMappedFile(dc);
            }
            timeProvider.advanceMillis(1100);
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write().text("some more text");
                mappedFile2 = toMappedFile(dc);
            }

            try (ExcerptTailer tailer = queue.createTailer(named ? "named" : null)) {
                try (DocumentContext documentContext = tailer.readingDocument()) {
                    assertEquals("some text", documentContext.wire().read().text(),
                            "Tailer should read text from first cycle");

                }

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    assertEquals("some more text", documentContext.wire().read().text(),
                            "Tailer should read text from second cycle");

                }
            }
        }

        waitFor(mappedFile1::isClosed, "mappedFile1 is not closed");
        waitFor(mappedFile2::isClosed, "mappedFile2 is not closed");

        if (OS.isWindows()) {
            System.err.println("#460 Cannot test delete after close on windows");
            return;
        }
        assertTrue(mappedFile1.file().delete(), "First mapped file should delete after close");
        assertTrue(mappedFile2.file().delete(), "Second mapped file should delete after close");
    }

    @TestTemplate
    @DisplayName("Writing document remains atomic queue behaviour scenario")
    @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
    public void testWritingDocumentIsAtomic() {

        final int threadCount = 8;
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount,
                new NamedThreadFactory("test"));
        // remove change of cycle roll in test, cross-cycle atomicity is covered elsewhere
        final AtomicLong fixedClock = new AtomicLong(System.currentTimeMillis());
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(TEST_SECONDLY)
                .timeoutMS(3_000)
                .timeProvider(fixedClock::get)
                .testBlockSize()
                .build()) {
            final int iterationsPerThread = Short.MAX_VALUE / 8;
            final int totalIterations = iterationsPerThread * threadCount;
            final int[] nonAtomicCounter = {0};
            for (int i = 0; i < threadCount; i++) {
                executorService.submit(() -> {
                    try (ExcerptAppender excerptAppender = queue.createAppender()) {
                        for (int j = 0; j < iterationsPerThread; j++) {
                            try (DocumentContext dc = excerptAppender.writingDocument()) {
                                int value = nonAtomicCounter[0]++;
                                dc.wire().write("some key").int64(value);
                            }
                        }
                    }
                });
            }

            long timeout = 20_000 + System.currentTimeMillis();
            ExcerptTailer tailer = queue.createTailer(named ? "named" : null);
            for (int expected = 0; expected < totalIterations; expected++) {
                for (; ; ) {
                    if (System.currentTimeMillis() > timeout)
                        fail("Timed out, having read " + expected + " documents of " + totalIterations);
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent()) {
                            Thread.yield();
                            continue;
                        }

                        long justRead = dc.wire().read("some key").int64();
                        assertEquals(expected, justRead, "Tailer should read sequential value " + expected);
                        break;
                    }
                }
            }
        } finally {
            executorService.shutdownNow();

            try {
                executorService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
    }

    @TestTemplate
    @DisplayName("Should be able to load queue from read only files")
    public void shouldBeAbleToLoadQueueFromReadOnlyFiles() throws IOException {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test read only mode on windows");
            return;
        }
        assumeFalse(named, "Read-only file test requires unnamed tailer mode");

        final File queueDir = getTmpDir();
        try (final ChronicleQueue queue = builder(queueDir, wireType).
                testBlockSize().build();
             final ExcerptAppender appender = queue.createAppender()) {
            appender.writeDocument("foo", ValueOut::text);
        }

        try (Stream<Path> list = Files.list(queueDir.toPath())) {
            list.forEach(p -> assertTrue(p.toFile().setReadOnly(), "File should be read-only: " + p));
        }

        try (final ChronicleQueue queue = builder(queueDir, wireType).
                readOnly(true).
                testBlockSize().build()) {
            assertTrue(queue.createTailer(named ? "named" : null).readingDocument().isPresent(),
                    "Read-only queue should still allow reads");
        }
    }

    @TestTemplate
    @DisplayName("Should create queue in current directory")
    public void shouldCreateQueueInCurrentDirectory() {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test delete after close on windows");
            return;
        }

        expectException("Using the current directory for the queue");
        try (final ChronicleQueue ignored =
                     builder(new File(""), wireType).
                             testBlockSize().build()) {

            assertNotNull(ignored, "queue instance should be non-null even when using current directory");
        }

        assertTrue(new File(QUEUE_METADATA_FILE).delete(), "Metadata file should delete after close");
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(TEST4_DAILY).testBlockSize();
    }

    @TestTemplate
    @DisplayName("Tailer snapping roll with new appender")
    public void testTailerSnappingRollWithNewAppender() throws InterruptedException, ExecutionException, TimeoutException {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(System.currentTimeMillis() - 2_000);
        final File dir = getTmpDir();
        final RollCycle rollCycle = TEST_SECONDLY;

        // write first message
        try (ChronicleQueue queue =
                     binary(dir)
                             .rollCycle(rollCycle)
                             .timeProvider(timeProvider)
                             .build();
             ExcerptAppender excerptAppender = queue.createAppender()) {
            excerptAppender.writeText("someText");

            ExecutorService executorService = Executors.newFixedThreadPool(2,
                    new NamedThreadFactory("test"));

            Future<?> f1 = executorService.submit(() -> {

                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).timeProvider(timeProvider).build();
                     final ExcerptAppender appender = queue2.createAppender()) {
                    appender.writeText("someText more");
                }
                timeProvider.advanceMillis(1100);
                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).timeProvider(timeProvider).build();
                     final ExcerptAppender appender = queue2.createAppender()) {
                    appender.writeText("someText more");
                }
            });

            Future<?> f2 = executorService.submit(() -> {

                // write second message
                try (ChronicleQueue queue2 = binary(dir)
                        .rollCycle(rollCycle).timeProvider(timeProvider).build();
                     final ExcerptAppender appender = queue2.createAppender()) {

                    for (int i = 0; i < 5; i++) {
                        appender.writeText("someText more");
                        timeProvider.advanceMillis(400);
                    }
                }
            });

            f1.get(10, TimeUnit.SECONDS);
            f2.get(10, TimeUnit.SECONDS);

            executorService.shutdownNow();
        }

        int dataDocs = 0;
        try (ChronicleQueue queue = binary(dir)
                .rollCycle(rollCycle)
                .readOnly(true)
                .build();
             ExcerptTailer tailer = queue.createTailer().toStart()) {

            while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        break;
                    }
                    if (dc.isData()) {
                        dataDocs++;
                    }
                }
            }
        }
        assertEquals(8, dataDocs, "tailer snapping roll: data documents");
    }

    @NotNull
    private SingleChronicleQueueBuilder builderWithAppendListener(@NotNull File file, @NotNull WireType wireType) {
        appenderListenerDump.clear();
        return SingleChronicleQueueBuilder.builder(file, wireType)
                .rollCycle(TEST4_DAILY)
                .timeProvider(new SetTimeProvider("2021/11/17T12:34:56").advanceMillis(1000))
                .appenderListener((w, idx) -> {
                    appenderListenerDump.append("idx: ").append(Long.toHexString(idx)).append("\n");
                    w.bytes().readSkip(-4);
                    appenderListenerDump.append(Wires.fromSizePrefixedBlobs(w)).append("\n");
                })
                .testBlockSize();
    }

    @NotNull
    protected SingleChronicleQueueBuilder binary(@NotNull File file) {
        return builder(file, WireType.BINARY_LIGHT);
    }

    private MappedFile toMappedFile(@NotNull DocumentContext documentContext) {
        MappedFile mappedFile;
        MappedBytes bytes = (MappedBytes) documentContext.wire().bytes();
        mappedFile = bytes.mappedFile();
        return mappedFile;
    }

    @TestTemplate
    @DisplayName("Write bytes and index five times with overwrite test")
    public void writeBytesAndIndexFiveTimesWithOverwriteTest() {
        try (final ChronicleQueue sourceQueue =
                     builder(getTmpDir(), wireType).
                             testBlockSize().build();
             final ExcerptAppender excerptAppender = sourceQueue.createAppender()) {

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("hello").text("world" + i);
                }
            }

            try (ExcerptTailer tailer = sourceQueue.createTailer(named ? "named" : null);
                 ChronicleQueue queue =
                         builder(getTmpDir(), wireType).testBlockSize().build();
                 ExcerptAppender appender0 = queue.createAppender()) {

                assumeTrue(appender0 instanceof InternalAppender,
                        "InternalAppender is required for overwrite test");
                InternalAppender appender = (InternalAppender) appender0;
                assumeTrue(appender instanceof StoreAppender,
                        "StoreAppender is required for overwrite test");

                List<BytesWithIndex> bytesWithIndies = new ArrayList<>();
                try {
                    for (int i = 0; i < 5; i++) {
                        bytesWithIndies.add(bytes(tailer));
                    }

                    // ... and try and overwrite starting at beginning
                    // NOTE: stepping here appears to overwrite
                    // and DOES NOT output debug log "Trying to overwrite index..."
                    for (int i = 0; i < 4; i++) {
                        BytesWithIndex b = bytesWithIndies.get(i);
                        appender.writeBytes(b.index, b.bytes);
                    }

                    // this will output debug log "Trying to overwrite index..." as expected
                    for (int i = 0; i < 4; i++) {
                        BytesWithIndex b = bytesWithIndies.get(i);
                        appender.writeBytes(b.index, b.bytes);
                    }

                    BytesWithIndex b = bytesWithIndies.get(4);
                    appender.writeBytes(b.index, b.bytes);

                    ((StoreAppender) appender).checkWritePositionHeaderNumber();
                    appender0.writeText("goodbye");
                } finally {
                    closeQuietly(bytesWithIndies);
                }

                String dump = tidyDump(queue);
                assertTrue(dump.contains(
                        "--- !!data #binary\n" +
                                "hello: world0\n" +
                                "--- !!data #binary\n" +
                                "hello: world1\n" +
                                "--- !!data #binary\n" +
                                "hello: world2\n" +
                                "--- !!data #binary\n" +
                                "hello: world3\n" +
                                "--- !!data #binary\n" +
                                "hello: world4\n" +
                                "--- !!data #binary\n" +
                                "goodbye\n"),
                        dump);

            }
        }
    }

    @TestTemplate
    @DisplayName("Write bytes and index five times test")
    public void writeBytesAndIndexFiveTimesTest() {
        try (final ChronicleQueue sourceQueue =
                     builder(getTmpDir(), wireType).
                             testBlockSize().build();
             final ExcerptAppender excerptAppender = sourceQueue.createAppender()) {

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("hello").text("world" + i);
                }
            }

            String before = tidyDump(sourceQueue);
            try (ExcerptTailer tailer = sourceQueue.createTailer(named ? "named" : null);
                 ChronicleQueue queue =
                         builder(getTmpDir(), wireType).testBlockSize().build();
                 ExcerptAppender appender = queue.createAppender()) {

                if (!(appender instanceof StoreAppender))
                    return;

                for (int i = 0; i < 5; i++) {
                    try (final BytesWithIndex b = bytes(tailer)) {
                        ((InternalAppender) appender).writeBytes(b.index, b.bytes);
                    }
                }

                String dump = tidyDump(queue);
                assertEquals(before, dump, "queue dump should match original after writing bytes with index");
                assertTrue(dump.contains(
                        "--- !!data #binary\n" +
                                "hello: world0\n" +
                                "--- !!data #binary\n" +
                                "hello: world1\n" +
                                "--- !!data #binary\n" +
                                "hello: world2\n" +
                                "--- !!data #binary\n" +
                                "hello: world3\n" +
                                "--- !!data #binary\n" +
                                "hello: world4"),
                        dump);
            }
        }
    }

    @TestTemplate
    @DisplayName("Rollback preserves expected messages queue behaviour scenario")
    public void rollbackTest() {

        File file = getTmpDir();
        try (final ChronicleQueue sourceQueue =
                     builder(file, wireType).
                             testBlockSize().build();
             ExcerptAppender excerptAppender = sourceQueue.createAppender()) {
            try (DocumentContext dc = excerptAppender.writingDocument()) {
                dc.wire().write("hello").text("world1");
            }
            try (DocumentContext dc = excerptAppender.writingDocument()) {
                dc.wire().write("hello2").text("world2");
            }
            try (DocumentContext dc = excerptAppender.writingDocument()) {
                dc.wire().write("hello3").text("world3");
            }
        }
        try (final ChronicleQueue queue =
                     builder(file, wireType).testBlockSize().build();
             ExcerptTailer tailer1 = queue.createTailer(named ? "named" : null)) {

            StringBuilder sb = new StringBuilder();
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                assertEquals("hello", sb.toString(), "event name should be 'hello' from first document before rollback");
                documentContext.rollbackOnClose();

            }

            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                assertEquals("hello", sb.toString(), "event name should be 'hello' from first document after rollback");
            }

            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                documentContext.rollbackOnClose();
                assertEquals("hello2", sb.toString(), "event name should be 'hello2' from second document before rollback");

            }

            try (DocumentContext documentContext = tailer1.readingDocument()) {
                Bytes<?> bytes = documentContext.wire().bytes();
                long rp = bytes.readPosition();
                long wp = bytes.writePosition();
                long wl = bytes.writeLimit();

                try {
                    documentContext.wire().readEventName(sb);
                    assertEquals("hello2", sb.toString(), "Event name should remain hello2 before rollback");
                    documentContext.rollbackOnClose();
                } finally {
                    bytes.readPosition(rp).writePosition(wp).writeLimit(wl);
                }
            }

            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                assertEquals("hello2", sb.toString(), "Event name should be hello2 after rollback");
            }
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                documentContext.wire().readEventName(sb);
                assertEquals("hello3", sb.toString(), "Event name should be hello3 before rollback");
                documentContext.rollbackOnClose();
            }
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                assertTrue(documentContext.isPresent(), "Document should be present for final hello3 read");
                documentContext.wire().readEventName(sb);
                assertEquals("hello3", sb.toString(), "Event name should be hello3 after rollback");
            }
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                assertFalse(documentContext.isPresent(), "Document should not be present after final read");
                documentContext.rollbackOnClose();
            }
            try (DocumentContext documentContext = tailer1.readingDocument()) {
                assertFalse(documentContext.isPresent(), "No additional documents should be present");
            }
        }
    }

    private BytesWithIndex bytes(final ExcerptTailer tailer) {
        try (DocumentContext dc = tailer.readingDocument()) {

            if (!dc.isPresent())
                return null;

            Bytes<?> bytes = dc.wire().bytes();
            long index = dc.index();
            return new BytesWithIndex(bytes, index);
        }
    }

    @TestTemplate
    @DisplayName("Mapped segments should be unmapped as cycle rolls")
    public void mappedSegmentsShouldBeUnmappedAsCycleRolls() throws IOException, InterruptedException {

        assumeTrue(wireType == WireType.BINARY, "this test is slow and does not depend on wire type");

        long now = System.currentTimeMillis();
        long oneHourInMillis = 60 * 60 * 1000;
        long oneDayInMillis = oneHourInMillis * 24;
        long midnight = now - (now % oneDayInMillis);
        AtomicLong clock = new AtomicLong(now);

        StringBuilder builder = new StringBuilder();
        boolean passed = doMappedSegmentUnmappedRollTest(clock, builder);
        passed = passed && doMappedSegmentUnmappedRollTest(setTime(clock, midnight), builder);
        for (int i = 1; i < 3; i += 1)
            passed = passed && doMappedSegmentUnmappedRollTest(setTime(clock, midnight + (i * oneHourInMillis)), builder);

        if (!passed) {
            fail("Mapped segment roll test failed: " + builder);
        }
    }

    private AtomicLong setTime(AtomicLong clock, long newValue) {
        clock.set(newValue);
        return clock;
    }

    private boolean doMappedSegmentUnmappedRollTest(AtomicLong clock, StringBuilder builder) throws IOException {
        String time = Instant.ofEpochMilli(clock.get()).toString();

        final Random random = new Random(0xDEADBEEFL);
        final File queueFolder = getTmpDir();
        try (final ChronicleQueue queue = ChronicleQueue.singleBuilder(queueFolder).
                timeProvider(clock::get).
                testBlockSize().rollCycle(HOURLY).
                build();
             ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < 20_000; i++) {
                final int batchSize = random.nextInt(10);
                appender.writeDocument(batchSize, ValueOut::int64);
                final byte payload = (byte) random.nextInt();
                for (int j = 0; j < batchSize; j++) {
                    appender.writeDocument(payload, ValueOut::int8);
                }
                if (random.nextDouble() > 0.995) {
                    clock.addAndGet(TimeUnit.MINUTES.toMillis(37L));
                    // this give the reference processor a chance to run
                    Jvm.pause(30);
                }
            }

            boolean passed = true;
            if (OS.isLinux()) {
                List<String> openFiles = getMappedQueueFiles();
                int filesOpen = openFiles.size();
                if (filesOpen >= 50) {
                    passed = false;
                    builder.append(String.format("Test for time %s failed: Too many mapped files: %d%n", time, filesOpen))
                            .append("Open files:\n");
                    openFiles.stream().map(s -> s + "\n").forEach(builder::append);
                }
            }

            try (Stream<Path> list = Files.list(queueFolder.toPath())) {
                long fileCount = list.filter(p -> p.toString().endsWith(SUFFIX)).count();
                if (fileCount <= 10L) {
                    passed = false;
                    builder.append(String.format("Test for time %s failed: Too many mapped files: %d%n", time, fileCount));
                }

                if (passed) {
                    builder.append(String.format("Test for time %s passed!%n", time));
                }

                return passed;
            }
        }
    }

    /**
     * relates to https://github.com/OpenHFT/Chronicle-Queue/issues/699
     */
    @TestTemplate
    @DisplayName("Read values using read-only queue queue behaviour scenario")
    public void testReadUsingReadOnly() {
        assumeFalse(OS.isWindows(), "Read-only mode is not supported on Windows");
        assumeFalse(named, "Read-only queue test requires unnamed tailer mode");

        File tmpDir = getTmpDir();
        String expected = "hello world";
        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(tmpDir)
                .build();
             final ExcerptAppender appender = out.createAppender()) {
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().getValueOut().text(expected);
            }
        }

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(tmpDir)
                .readOnly(true)
                .build()) {
            StringBuilder sb = new StringBuilder();
            try (DocumentContext dc = out.createTailer().readingDocument()) {
                dc.wire().getValueIn().text(sb);
            }

            assertEquals(expected, sb.toString(), "Read-only queue should return written value");
        }
    }

    @TestTemplate
    @DisplayName("Last index should return last index for populated queue")
    public void lastIndexShouldReturnLastIndexForPopulatedQueue() {
        File tmpDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).wireType(wireType).build()) {
            assertEquals(-1, queue.lastIndex(), "Empty queue should report -1 last index");

            long actualLastIndex;
            try (ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("Hello!");
                actualLastIndex = appender.lastIndexAppended();
            }
            assertEquals(actualLastIndex, queue.lastIndex(), "Queue last index should match last append");
        }
    }

    @TestTemplate
    @DisplayName("Last index should return negative one for empty queue")
    public void lastIndexShouldReturnNegativeOneForEmptyQueue() {
        File tmpDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).wireType(wireType).build()) {
            assertEquals(-1, queue.lastIndex(), "New queue should report -1 last index");
        }
    }

    @TestTemplate
    @DisplayName("Last index should return negative one for metadata only queue")
    public void lastIndexShouldReturnNegativeOneForMetadataOnlyQueue() {
        File tmpDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir).wireType(wireType).build()) {
            try (ExcerptAppender appender = queue.createAppender()) {
                try (DocumentContext documentContext = appender.writingDocument(true)) {
                    documentContext.wire().write().text("Hello!");
                }
            }
            assertEquals(-1, queue.lastIndex(), "Metadata-only queue should report -1 last index");
        }
    }

    @TestTemplate
    @DisplayName("Should wait for condition when creating appender")
    public void shouldWaitForConditionWhenCreatingAppender() throws TimeoutException {
        File tmpDir = getTmpDir();
        AtomicBoolean gotAppender = new AtomicBoolean(false);
        ReentrantLock createAppenderLock = new ReentrantLock();
        final Condition createAppenderCondition = createAppenderLock.newCondition();
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(tmpDir)
                .wireType(wireType)
                .createAppenderConditionCreator(q -> createAppenderCondition)
                .build()) {
            new Thread(() -> {
                createAppenderLock.lock();
                try (final ExcerptAppender appender = queue.createAppender()) {
                    gotAppender.set(true);
                }
            }).start();

            // Assert createAppender is blocked
            Jvm.pause(100L);
            assertFalse(gotAppender.get(), "Appender should not be acquired before condition is signalled");

            // Release
            createAppenderLock.lock();
            createAppenderCondition.signal();
            createAppenderLock.unlock();

            // Assert appender is acquired
            YieldingPauser pauser = new YieldingPauser(0);
            while (!gotAppender.get()) {
                pauser.pause(1, TimeUnit.SECONDS);
            }
        }
    }

    private static class MapWrapper extends SelfDescribingMarshallable {
        final Map<CharSequence, Double> map = new HashMap<>();
    }

    static class MyMarshable extends SelfDescribingMarshallable implements Demarshallable {
        @UsedViaReflection
        String name;

        @UsedViaReflection
        public MyMarshable(@NotNull WireIn wire) {
            readMarshallable(wire);
        }

        MyMarshable() {
        }
    }

    private static class BytesWithIndex implements Closeable {
        private final BytesStore<?, ?> bytes;
        private final long index;

        BytesWithIndex(Bytes<?> bytes, long index) {
            this.bytes = Bytes.allocateElasticDirect(bytes.readRemaining()).write(bytes);
            this.index = index;
        }

        @Override
        public void close() {
            bytes.releaseLast();
        }
    }
}
