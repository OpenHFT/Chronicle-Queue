/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class StoreTailerTest extends QueueTestCommon {
    private final Path dataDirectory = getTmpDir().toPath();

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void testEntryCount() {
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dataDirectory).build();
             final ExcerptAppender appender = queue.createAppender()) {
            assertEquals(0, queue.entryCount());

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("test").text("value");
            }
            appender.sync();

            assertEquals(1, queue.entryCount());
        }
    }

    @Test
    public void shouldHandleCycleRollWhenInReadOnlyMode() {
        assumeFalse("Read-only mode is not supported on Windows", OS.isWindows());

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
                    assertTrue(context.isPresent());
                }
                tailer.sync();
                tailer.toEnd();
                tailer.sync();
                try (final DocumentContext context = tailer.readingDocument()) {
                    assertFalse(context.isPresent());
                }
                tailer.sync();
            }
        }
    }

    @Test
    public void shouldConsiderSourceIdWhenDeterminingLastWrittenIndex() {
        try (ChronicleQueue firstInputQueue =
                     createQueue(dataDirectory, TEST_DAILY, 1, "firstInputQueue");
             // different RollCycle means that indicies are not identical to firstInputQueue
             ChronicleQueue secondInputQueue =
                     createQueue(dataDirectory, TEST_SECONDLY, 2, "secondInputQueue");
             ChronicleQueue outputQueue =
                     createQueue(dataDirectory, TEST_DAILY, 0, "outputQueue")) {

            final OnEvents firstWriter = firstInputQueue
                    .methodWriterBuilder(OnEvents.class)
                    .get();
            final HelloWorld secondWriter = secondInputQueue
                    .methodWriterBuilder(HelloWorld.class)
                    .get();

            // generate some data in the input queues
            firstWriter.onEvent("one");
            firstWriter.onEvent("two");

            secondWriter.hello("thirteen");
            secondWriter.hello("thirtyOne");

            final OnEvents eventSink = outputQueue
                    .methodWriterBuilder(OnEvents.class).get();

            final CapturingStringEvents outputWriter = new CapturingStringEvents(eventSink);
            final MethodReader firstMethodReader = firstInputQueue.createTailer().methodReader(outputWriter);
            final MethodReader secondMethodReader = secondInputQueue.createTailer().methodReader(outputWriter);

            // replay events from the inputs into the output queue
            assertTrue(firstMethodReader.readOne());
            assertTrue(firstMethodReader.readOne());
            assertFalse(firstMethodReader.readOne());
            assertTrue(secondMethodReader.readOne());
            assertTrue(secondMethodReader.readOne());
            assertFalse(secondMethodReader.readOne());

            // ensures that tailer is not moved to index from the incorrect source
            secondInputQueue.createTailer().afterLastWritten(outputQueue);
        }
    }

    @Test
    public void checkAfterWrittenMessageAtIndexMovesToTheCorrectIndex() {

        // Create three ChronicleQueues, one for input and two for output
        try (ChronicleQueue firstInputQueue =
                     createQueue(dataDirectory, TEST_DAILY, 1, "firstInputQueue");
             // different RollCycle means that indices are not identical to firstInputQueue
             ChronicleQueue secondInputQueue =
                     createQueue(dataDirectory, TEST_DAILY, 2, "secondInputQueue");
             ChronicleQueue outputQueue =
                     createQueue(dataDirectory, TEST_DAILY, 3, "outputQueue")) {

            // Create two MethodWriters for writing data to the input queues
            final OnEvents firstWriter = firstInputQueue
                    .methodWriterBuilder(OnEvents.class)
                    .get();
            final HelloWorld secondWriter = secondInputQueue
                    .methodWriterBuilder(HelloWorld.class)
                    .get();

            // Generate some data in the input queues
            firstWriter.onEvent("one");
            firstWriter.onEvent("two");

            secondWriter.hello("thirteen");
            secondWriter.hello("thirtyOne");

            // Create a MethodWriter for writing data to the output queue
            final OnEvents eventSink = outputQueue
                    .methodWriterBuilder(OnEvents.class).get();

            // Reset the MessageHistory
            MessageHistory.get().reset();

            final CapturingStringEvents outputWriter = new CapturingStringEvents(eventSink);

            // Create two ExcerptTailers for reading data from the input queues
            ExcerptTailer tailer1 = firstInputQueue.createTailer();
            long index1 = tailer1.index();
            final MethodReader firstMethodReader = tailer1.methodReader(outputWriter);

            ExcerptTailer tailer2 = secondInputQueue.createTailer();
            long index2 = tailer2.index();
            final MethodReader secondMethodReader = tailer2.methodReader(outputWriter);

            // Replay events from the inputs into the output queue
            assertTrue(firstMethodReader.readOne());
            assertTrue(secondMethodReader.readOne());

            // Add source and timing information to the MessageHistory for the first and second inputs
            VanillaMessageHistory mh = (VanillaMessageHistory) MessageHistory.get();
            mh.addSource(1, index1);
            mh.addTiming(System.nanoTime());

            mh.addSource(2, index2);
            mh.addTiming(System.nanoTime());

            // Write an event to the output queue
            outputWriter.onEvent("out1");

            // Get the index of the last message appended to the output queue
            // NOTE: Requires thread local appender that is used within the methodWriter
            ExcerptAppender appender = ThreadLocalAppender.acquireThreadLocalAppender(outputQueue);
            long index = appender.lastIndexAppended();
            // Get the current indices of the tailers for the input queues
            index1 = tailer1.index();
            index2 = tailer2.index();

            // Read the next events from the input queues
            assertTrue(firstMethodReader.readOne());
            assertTrue(secondMethodReader.readOne());

            // Reset the MessageHistory and add source and timing information for the next event
            mh.reset();
            mh.addSource(1, index1);
            mh.addTiming(System.nanoTime());

            mh.addSource(2, index2);
            mh.addTiming(System.nanoTime());

            // Write another event to the output queue
            outputWriter.onEvent("out2");

            // Reset the MessageHistory
            mh.reset();

            // ensures that tailer is not moved to index from the incorrect source
            tailer1.afterWrittenMessageAtIndex(outputQueue, index);
            Assert.assertEquals(index1, tailer1.index());

            // ensures that tailer is not moved to index from the incorrect source
            tailer2.afterWrittenMessageAtIndex(outputQueue, index);
            Assert.assertEquals(index2, tailer2.index());

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

            if (!tailer.readDocument(w -> w.read("test").text("text", Assert::assertEquals))) {
                // System.out.println("dump chronicle:\n" + chronicle.dump());
                // System.out.println("dump chronicle2:\n" + chronicle2.dump());
                fail("readDocument false");
            }

        }
    }

    @Test
    public void shouldHaltAtPartiallyInitialisedRollCycle() throws ExecutionException, InterruptedException {
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
                assertEquals("one", tailer.readText());
                assertEquals("two", tailer.readText());

                // Start the appender, it will over-write the second cycle then create a third
                final Future<?> submit = ex.submit(() -> appendTwoMoreCycles(tp, producerQueue));

                // These reads should proceed after the appends have completed
                String firstRead;
                while ((firstRead = tailer.readText()) == null) {
                    Jvm.pause(1);
                }
                assertEquals("other-three", firstRead);
                assertEquals("other-four", tailer.readText());
                assertEquals("five", tailer.readText());
                assertEquals("six", tailer.readText());
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
        new ThreadSafetyTestingTemplate() {

            @Override
            void doOnFirstThread(SingleChronicleQueue singleChronicleQueue, ExcerptTailer tailer) {
                tailer.readText();
            }

            @Override
            void doOnSecondThread(ExcerptTailer tailer) {
                try {
                    tailer.readText();
                    fail();
                } catch (IllegalStateException expected) {
//                    expected.printStackTrace();
                }
                tailer.singleThreadedCheckDisabled(true);
                tailer.readText();
            }
        }.run();
    }

    @Test
    public void disableThreadSafetyWithMethodReader() throws InterruptedException {
        new ThreadSafetyTestingTemplate() {

            @Override
            void doOnFirstThread(SingleChronicleQueue queue, ExcerptTailer tailer) {
                writeMethodCall(queue, "Testing1");
                writeMethodCall(queue, "Testing2");
                assertEquals("Testing1", readMethodCall(tailer));
            }

            @Override
            void doOnSecondThread(ExcerptTailer tailer) {
                try {
                    readMethodCall(tailer);
                    fail();
                } catch (IllegalStateException expected) {
//                    expected.printStackTrace();
                }
                tailer.singleThreadedCheckDisabled(true);
                assertEquals("Testing2", readMethodCall(tailer));
            }
        }.run();
    }

    @Test
    public void clearUsedByThread() throws InterruptedException {
        new ThreadSafetyTestingTemplate() {

            @Override
            void doOnFirstThread(SingleChronicleQueue singleChronicleQueue, ExcerptTailer tailer) {
                tailer.readText();
            }

            @Override
            void doOnSecondThread(ExcerptTailer tailer) {
                try {
                    tailer.readText();
                    fail();
                } catch (IllegalStateException expected) {
//                    expected.printStackTrace();
                }
                ((AbstractCloseable) tailer).singleThreadedCheckReset();
                tailer.readText();
            }
        }.run();
    }

    @Test
    public void clearUsedByThreadWithMethodReader() throws InterruptedException {
        new ThreadSafetyTestingTemplate() {

            @Override
            void doOnFirstThread(SingleChronicleQueue queue, ExcerptTailer tailer) {
                writeMethodCall(queue, "Testing1");
                writeMethodCall(queue, "Testing2");
                writeMethodCall(queue, "Testing3");
                assertEquals("Testing1", readMethodCall(tailer));
            }

            @Override
            void doOnSecondThread(ExcerptTailer tailer) {
                try {
                    readMethodCall(tailer);
                    fail();
                } catch (IllegalStateException expected) {
//                    expected.printStackTrace();
                }
                ((AbstractCloseable) tailer).singleThreadedCheckReset();
                assertEquals("Testing2", readMethodCall(tailer));
            }
        }.run();
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
                assertTrue(dc.isPresent());
                assertTrue(dc.isMetaData());
                assertEquals("header", dc.wire().readEvent(String.class));
            }
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

        public void run() throws InterruptedException {
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
}
