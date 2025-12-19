/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.testframework.ExecutorServiceUtil;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;

import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@SuppressWarnings({"this-escape", "deprecation", "removal"})
public class StoreTailerTest extends QueueTestCommon {
    private final Path dataDirectory = getTmpDir().toPath();

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void testEntryCount() {
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dataDirectory).build();
             final ExcerptAppender appender = queue.createAppender()) {
            assertEquals(0, queue.entryCount(), "Queue should have 0 entries when first created");

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("test").text("value");
            }
            appender.sync();

            assertEquals(1, queue.entryCount(), "Queue should have 1 entry after writing a single document");
        }
    }

    @Test
    public void shouldHandleCycleRollWhenInReadOnlyMode() {
        assumeFalse(OS.isWindows(), "Read-only mode is not supported on Windows");

        final MutableTimeProvider timeProvider = new MutableTimeProvider();
        try (ChronicleQueue queue = build(createQueue(dataDirectory, MINUTELY, 0, "cycleRoll", false).
                timeProvider(timeProvider));
             final ExcerptAppender appender = queue.createAppender()) {

            final OnEvents events = appender.methodWriterBuilder(OnEvents.class).build();
            timeProvider.setTime(System.currentTimeMillis());
            events.onEvent("firstEvent");
            timeProvider.addTime(2, TimeUnit.MINUTES);
            events.onEvent("secondEvent");
            appender.sync();

            try (final ChronicleQueue readerQueue = build(createQueue(dataDirectory, MINUTELY, 0, "cycleRoll", true).
                    timeProvider(timeProvider))) {

                final ExcerptTailer tailer = readerQueue.createTailer();
                tailer.sync();
                tailer.toStart();
                tailer.sync();
                try (final DocumentContext context = tailer.readingDocument()) {
                    assertTrue(context.isPresent(), "Tailer in read-only mode should successfully read the first written document");
                }
                tailer.sync();
                tailer.toEnd();
                tailer.sync();
                try (final DocumentContext context = tailer.readingDocument()) {
                    assertFalse(context.isPresent(), "Tailer positioned at end should return no document when attempting to read beyond the last entry");
                }
                tailer.sync();
            }
        }
    }

    @Test
    public void shouldHandleCycleRoll() {
        File dir = getTmpDir();
        MutableTimeProvider timeProvider = new MutableTimeProvider();
        timeProvider.setTime(System.currentTimeMillis());
        try (ChronicleQueue chronicle = minutely(dir, timeProvider).build();
             ChronicleQueue chronicle2 = minutely(dir, timeProvider).build();
             final ExcerptAppender append = chronicle2.createAppender()) {

            //ExcerptAppender append = chronicle2.acquireAppender();
            //append.writeDocument(w -> w.write("test").text("before text"));

            ExcerptTailer tailer = chronicle.createTailer();
            //tailer.toEnd();

            timeProvider.addTime(10, TimeUnit.MINUTES);

            append.writeDocument(w -> w.write("test").text("text"));

            if (!tailer.readDocument(w -> w.read("test").text("text", org.junit.jupiter.api.Assertions::assertEquals))) {
                // System.out.println("dump chronicle:\n" + chronicle.dump());
                // System.out.println("dump chronicle2:\n" + chronicle2.dump());
                fail("Tailer should successfully read document written after cycle roll (time advanced by 10 minutes)");
            }
        }
    }

    @Test
    public void shouldHaltAtPartiallyInitialisedRollCycle() throws ExecutionException, InterruptedException {
        // Windows doesn't support renaming a file that is open.
        assumeFalse(OS.isWindows());
        expectException("Renamed un-acquirable segment file to");
        File dir = getTmpDir();
        SetTimeProvider tp = new SetTimeProvider();
        try (final SingleChronicleQueue producerQueue = createQueue(dir, tp, 500);
             final SingleChronicleQueue consumerQueue = createQueue(dir, tp, 1000)) {
            try (final ExcerptAppender appender = producerQueue.createAppender()) {
                appender.writeText("one");
                appender.writeText("two");
                // trigger a roll
                tp.advanceMillis(TimeUnit.DAYS.toMillis(1));
                appender.writeText("three");
                appender.writeText("four");
            }

            // simulate second cycle being partially initialised
            final SingleChronicleQueueStore secondCycle = producerQueue.storeForCycle(1, 0, false, null);
            final MappedBytes bytes = secondCycle.bytes();
            bytes.writeInt(0, Wires.NOT_COMPLETE);
            bytes.releaseLast();
            producerQueue.closeStore(secondCycle);

            ExecutorService ex = Executors.newFixedThreadPool(3);
            // Read the queue with the partially initialised roll cycle
            try (final ExcerptTailer tailer = consumerQueue.createTailer()) {
                assertEquals("one", tailer.readText(), "Tailer should read first message 'one' from initial cycle before partially initialised cycle");
                assertEquals("two", tailer.readText(), "Tailer should read second message 'two' from initial cycle before partially initialised cycle");

                // Start the appender, it will over-write the second cycle then create a third
                final Future<?> submit = ex.submit(() -> appendTwoMoreCycles(tp, producerQueue));

                // These reads should proceed after the appends have completed
                String firstRead;
                while ((firstRead = tailer.readText()) == null) {
                    Jvm.pause(1);
                }
                assertEquals("other-three", firstRead, "After repair, tailer should read 'other-three' from overwritten second cycle");
                assertEquals("other-four", tailer.readText(), "After repair, tailer should read 'other-four' from overwritten second cycle");
                assertEquals("five", tailer.readText(), "After repair, tailer should read 'five' from third cycle");
                assertEquals("six", tailer.readText(), "After repair, tailer should read 'six' from third cycle");
                submit.get();
            } finally {
                ExecutorServiceUtil.shutdownAndWaitForTermination(ex);
            }
        }
    }

    private void appendTwoMoreCycles(SetTimeProvider timeProvider, SingleChronicleQueue queue) {
        try (final ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("other-three");
            appender.writeText("other-four");
            // trigger a roll
            timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(1));
            appender.writeText("five");
            appender.writeText("six");
        }
    }

    private SingleChronicleQueue createQueue(File dir, TimeProvider timeProvider, long timeouMS) {
        return SingleChronicleQueueBuilder.binary(dir)
                .timeProvider(timeProvider)
                .rollCycle(RollCycles.FAST_DAILY)
                .timeoutMS(timeouMS)
                .build();
    }

    private SingleChronicleQueueBuilder minutely(@NotNull File file, TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.builder(file, WireType.BINARY).rollCycle(MINUTELY).testBlockSize().timeProvider(timeProvider);
    }

    @NotNull
    private ChronicleQueue createQueue(final Path dataDirectory, final RollCycle rollCycle,
                                       final int sourceId, final String subdirectory) {
        return build(createQueue(dataDirectory, rollCycle, sourceId,
                subdirectory, false));
    }

    @NotNull
    private SingleChronicleQueueBuilder createQueue(final Path dataDirectory, final RollCycle rollCycle,
                                                    final int sourceId, final String subdirectory, final boolean readOnly) {
        return SingleChronicleQueueBuilder
                .binary(dataDirectory.resolve(Paths.get(subdirectory)))
                .sourceId(sourceId)
                .testBlockSize()
                .rollCycle(rollCycle)
                .readOnly(readOnly);
    }

    private ChronicleQueue build(final SingleChronicleQueueBuilder builder) {
        return builder.build();
    }

    @Test
    public void disableThreadSafety() throws InterruptedException {
        AtomicBoolean illegalStateThrown = new AtomicBoolean();
        new ThreadSafetyTestingTemplate() {

            @Override
            void doOnFirstThread(SingleChronicleQueue singleChronicleQueue, ExcerptTailer tailer) {
                tailer.readText();
            }

            @Override
            void doOnSecondThread(ExcerptTailer tailer) {
                try {
                    tailer.readText();
                    fail("Tailer should throw IllegalStateException when accessed from a different thread without disabling thread safety check");
                } catch (IllegalStateException expected) {
                    illegalStateThrown.set(true);
                    // expected.printStackTrace();
                }
                tailer.singleThreadedCheckDisabled(true);
                tailer.readText();
            }
        }.run();
        assertTrue(illegalStateThrown.get(), "IllegalStateException should have been thrown when tailer was accessed from second thread");
    }

    @Test
    public void disableThreadSafetyWithMethodReader() throws InterruptedException {
        AtomicBoolean illegalStateThrown = new AtomicBoolean();
        new ThreadSafetyTestingTemplate() {

            @Override
            void doOnFirstThread(SingleChronicleQueue queue, ExcerptTailer tailer) {
                writeMethodCall(queue, "Testing1");
                writeMethodCall(queue, "Testing2");
                assertEquals("Testing1", readMethodCall(tailer), "MethodReader on first thread should read 'Testing1' message");
            }

            @Override
            void doOnSecondThread(ExcerptTailer tailer) {
                try {
                    readMethodCall(tailer);
                    fail("MethodReader should throw IllegalStateException when tailer is accessed from a different thread without disabling thread safety check");
                } catch (IllegalStateException expected) {
                    illegalStateThrown.set(true);
                    // expected.printStackTrace();
                }
                tailer.singleThreadedCheckDisabled(true);
                assertEquals("Testing2", readMethodCall(tailer), "MethodReader should read 'Testing2' after disabling thread safety check");
            }
        }.run();
        assertTrue(illegalStateThrown.get(), "IllegalStateException should have been thrown when MethodReader accessed tailer from second thread");
    }

    @Test
    public void clearUsedByThread() throws InterruptedException {
        AtomicBoolean illegalStateThrown = new AtomicBoolean();
        new ThreadSafetyTestingTemplate() {

            @Override
            void doOnFirstThread(SingleChronicleQueue singleChronicleQueue, ExcerptTailer tailer) {
                tailer.readText();
            }

            @Override
            void doOnSecondThread(ExcerptTailer tailer) {
                try {
                    tailer.readText();
                    fail("Tailer should throw IllegalStateException when accessed from a different thread without resetting thread ownership");
                } catch (IllegalStateException expected) {
                    illegalStateThrown.set(true);
                    // expected.printStackTrace();
                }
                tailer.singleThreadedCheckReset();
                tailer.readText();
            }
        }.run();
        assertTrue(illegalStateThrown.get(), "IllegalStateException should have been thrown when tailer was accessed from second thread before reset");
    }

    @Test
    public void clearUsedByThreadWithMethodReader() throws InterruptedException {
        AtomicBoolean illegalStateThrown = new AtomicBoolean();
        new ThreadSafetyTestingTemplate() {

            @Override
            void doOnFirstThread(SingleChronicleQueue queue, ExcerptTailer tailer) {
                writeMethodCall(queue, "Testing1");
                writeMethodCall(queue, "Testing2");
                writeMethodCall(queue, "Testing3");
                assertEquals("Testing1", readMethodCall(tailer), "MethodReader on first thread should read 'Testing1' message");
            }

            @Override
            void doOnSecondThread(ExcerptTailer tailer) {
                try {
                    readMethodCall(tailer);
                    fail("MethodReader should throw IllegalStateException when tailer is accessed from a different thread without resetting thread ownership");
                } catch (IllegalStateException expected) {
                    illegalStateThrown.set(true);
                    // expected.printStackTrace();
                }
                tailer.singleThreadedCheckReset();
                assertEquals("Testing2", readMethodCall(tailer), "MethodReader should read 'Testing2' after resetting thread ownership");
            }
        }.run();
        assertTrue(illegalStateThrown.get(), "IllegalStateException should have been thrown when MethodReader accessed tailer from second thread before reset");
    }

    private void writeMethodCall(SingleChronicleQueue queue, String message) {
        final Foobar foobar = queue.methodWriter(Foobar.class);
        foobar.say(message);
    }

    private String readMethodCall(ExcerptTailer tailer) {
        AtomicReference<String> messageHolder = new AtomicReference<>();
        final MethodReader methodReader = tailer.methodReader((Foobar) messageHolder::set);
        methodReader.readOne();
        return messageHolder.get();
    }

    @Test
    public void readMetaData() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build();
             ExcerptTailer tailer = queue.createTailer();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("Hello World");
            try (DocumentContext dc = tailer.readingDocument(true)) {
                assertTrue(dc.isPresent(), "DocumentContext should be present when reading metadata with readingDocument(true)");
                assertTrue(dc.isMetaData(), "DocumentContext should indicate it contains metadata");
                assertEquals("header", dc.wire().readEvent(String.class), "Metadata event should be named 'header'");
            }
        }
    }

    @Test
    public void toEndWorksWhenLastCycleIsEmpty() {
        File dir = getTmpDir();
        SetTimeProvider stp = new SetTimeProvider();

        Supplier<SingleChronicleQueue> createQueue = () -> SingleChronicleQueueBuilder.binary(dir)
                .timeProvider(stp)
                .rollCycle(TEST_SECONDLY)
                .build();
        try (SingleChronicleQueue queue = createQueue.get()) {
            try (ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("At index 0");
                appender.writeText("At index 1");
                appender.writeText("At index 2");
            }
            stp.advanceMillis(100_000);
            try (ExcerptAppender appender = queue.createAppender();
                 DocumentContext documentContext = appender.writingDocument()) {
                // This will create an empty roll cycle
                documentContext.rollbackOnClose();
            }
        }

        try (SingleChronicleQueue queue = createQueue.get();
             ExcerptTailer appender = queue.createTailer()) {
            assertEquals(2, appender.direction(TailerDirection.BACKWARD).toEnd().index(), "Backward tailer's toEnd() should return index 2 (last valid entry) when last cycle is empty");
            assertEquals(3, appender.direction(TailerDirection.FORWARD).toEnd().index(), "Forward tailer's toEnd() should return index 3 (position after last entry) when last cycle is empty");
        }
    }

    interface Foobar {
        void say(String message);
    }

    private static final class CapturingStringEvents implements OnEvents, HelloWorld {
        private final OnEvents delegate;

        CapturingStringEvents(final OnEvents delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onEvent(final String event) {
            delegate.onEvent(event);
        }

        @Override
        public void hello(String s) {
            delegate.onEvent(s);
        }
    }

    private static final class MutableTimeProvider implements TimeProvider {
        private long currentTimeMillis;

        @Override
        public long currentTimeMillis() {
            return currentTimeMillis;
        }

        void setTime(final long millis) {
            this.currentTimeMillis = millis;
        }

        void addTime(final long duration, final TimeUnit unit) {
            this.currentTimeMillis += unit.toMillis(duration);
        }
    }

    abstract class ThreadSafetyTestingTemplate {

        abstract void doOnFirstThread(SingleChronicleQueue queue, ExcerptTailer tailer);

        abstract void doOnSecondThread(ExcerptTailer tailer);

        void run() throws InterruptedException {
            try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dataDirectory).build()) {
                BlockingQueue<ExcerptTailer> tq = new LinkedBlockingQueue<>();
                Thread t = new Thread(() -> {
                    ExcerptTailer tailer = queue.createTailer();
                    doOnFirstThread(queue, tailer);
                    tq.offer(tailer);
                    Jvm.pause(1000);
                });
                t.start();
                doOnSecondThread(tq.take());
                t.interrupt();
                t.join(1000);
            }
        }
    }

    public void cantMoveToStartDuringDocumentReading() {
        assertThrows(IllegalStateException.class,
                () -> assertCannotMoveDuringDocumentReading(ExcerptTailer::toStart),
                "Tailer should throw IllegalStateException when attempting toStart() while reading a document");
    }

    public void cantMoveToEndDuringDocumentReading() {
        assertThrows(IllegalStateException.class,
                () -> assertCannotMoveDuringDocumentReading(ExcerptTailer::toEnd),
                "Tailer should throw IllegalStateException when attempting toEnd() while reading a document");
    }

    private void assertCannotMoveDuringDocumentReading(Consumer<ExcerptTailer> move) {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptTailer tailer = queue.createTailer();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("Hello World");
            try (DocumentContext dc = tailer.readingDocument(true)) {
                assertTrue(dc.isPresent(), "DocumentContext should be present when reading document");
                assertTrue(dc.isMetaData(), "DocumentContext should indicate it contains metadata");
                assertEquals("header", dc.wire().readEvent(String.class), "Metadata event should be named 'header'");
                assertTrue(tailer.toString().contains("StoreTailer{"), "Tailer toString() should contain 'StoreTailer{' type indicator");
                move.accept(tailer); // forbidden
            }
        }
    }

    @Test
    public void testStriding() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptTailer tailer = queue.createTailer()) {
            tailer.striding(true);
            assertTrue(tailer.striding(), "Tailer should return true for striding() after enabling striding mode");
        }
    }

    @Test
    public void testStridingReadForward() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {

            for (int i = 1; i <= 10; i++) {
                appender.writeText("Message " + i);
            }

            tailer.striding(true);

            int stride = 256;
            System.out.println("stride: " + stride);
            for (int i = 1; i <= 10; i += stride) {
                String expectedMessage = "Message " + i;
                assertEquals(expectedMessage, tailer.readText(), "Striding tailer in forward direction should read '" + expectedMessage + "' at position " + i);
            }
        }
    }

    @Test
    public void testStridingReadBackward() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {

            for (int i = 1; i <= 10; i++) {
                appender.writeText("Message " + i);
            }

            tailer.striding(true);
            tailer.direction(TailerDirection.BACKWARD).toEnd();

            int stride = 256;
            for (int i = 10; i >= 1; i -= stride) {
                String expectedMessage = "Message " + i;
                assertEquals(expectedMessage, tailer.readText(), "Striding tailer in backward direction should read '" + expectedMessage + "' at position " + i);
            }
        }
    }

    @Test
    public void testMoveToIndex() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptTailer tailer = queue.createTailer();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("Hello World");
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent(), "DocumentContext should be present after writing and reading a document");
                tailer.moveToIndex(tailer.index() + 1);
            }
        }
    }

    @Test
    public void testExcerptsInCycle() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {

            int cycle = queue.cycle();

            assertEquals(-1, tailer.excerptsInCycle(cycle), "Tailer should return -1 for excerpt count when no messages have been written to the cycle");

            appender.writeText("Message 1");
            appender.writeText("Message 2");

            assertEquals(2, tailer.excerptsInCycle(cycle), "Tailer should return 2 excerpts after writing two messages to the cycle");

            int nonExistentCycle = cycle + 1;
            assertEquals(-1, tailer.excerptsInCycle(nonExistentCycle), "Tailer should return -1 for excerpt count in a non-existent cycle");
        }
    }

    @Test
    public void testMoveToInvalidIndex() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptTailer tailer = queue.createTailer()) {
            // Negative index
            assertFalse(tailer.moveToIndex(-1), "Tailer moveToIndex() should return false when given a negative index");

            // Index beyond the last index
            assertFalse(tailer.moveToIndex(100), "Tailer moveToIndex() should return false when given an index beyond the last written entry");

            // Index in a non-existent cycle
            int nonExistentCycle = queue.rollCycle().toCycle(tailer.index()) + 10;
            long nonExistentIndex = queue.rollCycle().toIndex(nonExistentCycle, 0);
            assertFalse(tailer.moveToIndex(nonExistentIndex), "Tailer moveToIndex() should return false when given an index in a non-existent cycle");
        }
    }

    @Test
    public void testDirectionChange() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("Message 1");
            appender.writeText("Message 2");
            appender.writeText("Message 3");

            tailer.toEnd();

            tailer.direction(TailerDirection.BACKWARD);
            assertEquals("Message 3", tailer.readText(), "Backward tailer should read 'Message 3' first (last written message)");
            assertEquals("Message 2", tailer.readText(), "Backward tailer should read 'Message 2' second");
            assertEquals("Message 1", tailer.readText(), "Backward tailer should read 'Message 1' third (first written message)");
            assertNull(tailer.readText(), "Backward tailer should return null when reading before the start");

            tailer.direction(TailerDirection.FORWARD);
            assertNull(tailer.readText(), "Forward tailer should return null immediately after direction change from backward at start");
            assertEquals("Message 1", tailer.readText(), "Forward tailer should read 'Message 1' first after skipping initial null");
            assertEquals("Message 2", tailer.readText(), "Forward tailer should read 'Message 2' second");
            assertEquals("Message 3", tailer.readText(), "Forward tailer should read 'Message 3' third");
        }
    }

    @Test
    public void testBehaviorOnEmptyQueue() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize().build();
             ExcerptTailer tailer = queue.createTailer()) {

            assertNull(tailer.readText(), "Tailer should return null when attempting to read from an empty queue");

            tailer.toStart();
            tailer.toEnd();

            assertNull(tailer.readText(), "Tailer should still return null after moving to end of an empty queue");
        }
    }

    @Test
    public void shouldHandleCycleRollBackward() {
        File dir = getTmpDir();
        MutableTimeProvider timeProvider = new MutableTimeProvider();
        timeProvider.setTime(System.currentTimeMillis());

        try (ChronicleQueue chronicle = minutely(dir, timeProvider).build();
             ChronicleQueue chronicle2 = minutely(dir, timeProvider).build();
             final ExcerptAppender append = chronicle2.createAppender()) {

            append.writeDocument(w -> w.write("test").text("firstCycle"));

            timeProvider.addTime(10, TimeUnit.MINUTES);

            append.writeDocument(w -> w.write("test").text("secondCycle"));

            // create tailer from the first queue, move it to end, then set direction to BACKWARD.
            ExcerptTailer tailer = chronicle.createTailer();
            tailer.toEnd();
            tailer.direction(TailerDirection.BACKWARD);

            // first backward read should retrieve message from second cycle
            boolean firstRead = tailer.readDocument(w ->
                    assertEquals("secondCycle", w.read("test").text(), "Backward tailer should read 'secondCycle' text from the second cycle")
            );
            assertTrue(firstRead, "Backward tailer should successfully read document from second cycle");

            // second backward read should cross the cycle boundary - reading the message from the first cycle.
            boolean secondRead = tailer.readDocument(w ->
                    assertEquals("firstCycle", w.read("test").text(), "Backward tailer should read 'firstCycle' text after crossing cycle boundary")
            );
            assertTrue(secondRead, "Backward tailer should successfully read document from first cycle after crossing cycle boundary");
        }
    }

    @Test
    public void testOriginalToEndBeforeInitialised() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir).testBlockSize().build();
             ExcerptTailer tailer = queue.createTailer()) {
            tailer.direction(TailerDirection.BACKWARD);
            tailer.toEnd();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent(), "Backward tailer should return no document when reading from an empty queue after toEnd()");
            }
        }
    }

    @Test
    public void currentFileShouldReturnFileIfInitialised() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("Test123");
            tailer.readText();

            assertNotNull(tailer.currentFile(), "Tailer currentFile() should return a non-null File after reading a message");
        }
    }

    @Test
    public void currentFileShouldReturnNullWhenNotInitialised() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir).testBlockSize().build();
             ExcerptTailer tailer = queue.createTailer()) {

            assertNull(tailer.currentFile(), "Tailer currentFile() should return null when tailer has not been initialized by reading");
        }
    }

    @Test
    public void syncShouldReturnNullIfNotInitialised() {
        File dir = getTmpDir();
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dir).testBlockSize().build();
             ExcerptTailer tailer = queue.createTailer()) {
            tailer.sync();

            assertNull(tailer.currentFile(), "Tailer currentFile() should return null after sync() when tailer has not been initialized by reading");
        }
    }
}
