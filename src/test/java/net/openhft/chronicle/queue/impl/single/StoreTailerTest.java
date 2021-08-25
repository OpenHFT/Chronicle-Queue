package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class StoreTailerTest extends ChronicleQueueTestBase {
    private final Path dataDirectory = getTmpDir().toPath();

    @Test
    public void testEntryCount() {
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dataDirectory).build()) {
            assertEquals(0, queue.entryCount());

            try (DocumentContext dc = queue.acquireAppender().writingDocument()) {
                dc.wire().write("test").text("value");
            }

            assertEquals(1, queue.entryCount());
        }
    }

    @Test
    public void shouldHandleCycleRollWhenInReadOnlyMode() {
        assumeFalse("Read-only mode is not supported on Windows", OS.isWindows());

        final MutableTimeProvider timeProvider = new MutableTimeProvider();
        try (ChronicleQueue queue = build(createQueue(dataDirectory, RollCycles.MINUTELY, 0, "cycleRoll", false).
                timeProvider(timeProvider))) {

            final OnEvents events = queue.acquireAppender().methodWriterBuilder(OnEvents.class).build();
            timeProvider.setTime(System.currentTimeMillis());
            events.onEvent("firstEvent");
            timeProvider.addTime(2, TimeUnit.MINUTES);
            events.onEvent("secondEvent");

            try (final ChronicleQueue readerQueue = build(createQueue(dataDirectory, RollCycles.MINUTELY, 0, "cycleRoll", true).
                    timeProvider(timeProvider))) {

                final ExcerptTailer tailer = readerQueue.createTailer();
                tailer.toStart();
                try (final DocumentContext context = tailer.readingDocument()) {
                    assertTrue(context.isPresent());
                }
                tailer.toEnd();
                try (final DocumentContext context = tailer.readingDocument()) {
                    assertFalse(context.isPresent());
                }
            }
        }
    }

    @Test
    public void shouldConsiderSourceIdWhenDeterminingLastWrittenIndex() {
        try (ChronicleQueue firstInputQueue =
                     createQueue(dataDirectory, RollCycles.TEST_DAILY, 1, "firstInputQueue");
             // different RollCycle means that indicies are not identical to firstInputQueue
             ChronicleQueue secondInputQueue =
                     createQueue(dataDirectory, RollCycles.TEST_SECONDLY, 2, "secondInputQueue");
             ChronicleQueue outputQueue =
                     createQueue(dataDirectory, RollCycles.TEST_DAILY, 0, "outputQueue")) {

            final OnEvents firstWriter = firstInputQueue.acquireAppender()
                    .methodWriterBuilder(OnEvents.class)
                    .get();
            final HelloWorld secondWriter = secondInputQueue.acquireAppender()
                    .methodWriterBuilder(HelloWorld.class)
                    .get();

            // generate some data in the input queues
            firstWriter.onEvent("one");
            firstWriter.onEvent("two");

            secondWriter.hello("thirteen");
            secondWriter.hello("thirtyOne");

            final OnEvents eventSink = outputQueue.acquireAppender().
                    methodWriterBuilder(OnEvents.class).get();

            final CapturingStringEvents outputWriter = new CapturingStringEvents(eventSink);
            final MethodReader firstMethodReader = firstInputQueue.createTailer().methodReader(outputWriter);
            final MethodReader secondMethodReader = secondInputQueue.createTailer().methodReader(outputWriter);

            // replay events from the inputs into the output queue
            assertTrue(firstMethodReader.readOne());
            assertTrue(firstMethodReader.readOne());
            assertTrue(secondMethodReader.readOne());
            assertTrue(secondMethodReader.readOne());

            // ensures that tailer is not moved to index from the incorrect source
            secondInputQueue.createTailer().afterLastWritten(outputQueue);
        }
    }

    @Test
    public void shouldHandleCycleRoll() {
        File dir = getTmpDir();
        MutableTimeProvider timeProvider = new MutableTimeProvider();
        timeProvider.setTime(System.currentTimeMillis());
        try (ChronicleQueue chronicle = minutely(dir, timeProvider).build();
             ChronicleQueue chronicle2 = minutely(dir, timeProvider).build()) {

            //ExcerptAppender append = chronicle2.acquireAppender();
            //append.writeDocument(w -> w.write("test").text("before text"));

            ExcerptTailer tailer = chronicle.createTailer();
            //tailer.toEnd();

            timeProvider.addTime(10, TimeUnit.MINUTES);

            ExcerptAppender append = chronicle2.acquireAppender();
            append.writeDocument(w -> w.write("test").text("text"));

            if (!tailer.readDocument(w -> w.read("test").text("text", Assert::assertEquals))) {
                // System.out.println("dump chronicle:\n" + chronicle.dump());
                // System.out.println("dump chronicle2:\n" + chronicle2.dump());
                fail("readDocument false");
            }
        }
    }

    private SingleChronicleQueueBuilder minutely(@NotNull File file, TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.builder(file, WireType.BINARY).rollCycle(RollCycles.MINUTELY).testBlockSize().timeProvider(timeProvider);
    }

    @NotNull
    private ChronicleQueue createQueue(final Path dataDirectory, final RollCycles rollCycle,
                                       final int sourceId, final String subdirectory) {
        return build(createQueue(dataDirectory, rollCycle, sourceId,
                subdirectory, false));
    }

    @NotNull
    private SingleChronicleQueueBuilder createQueue(final Path dataDirectory, final RollCycles rollCycle,
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

    private static final class CapturingStringEvents implements OnEvents {
        private final OnEvents delegate;

        CapturingStringEvents(final OnEvents delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onEvent(final String event) {
            delegate.onEvent(event);
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
                tailer.disableThreadSafetyCheck(true).readText();
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
                tailer.disableThreadSafetyCheck(true);
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
                ((AbstractCloseable) tailer).clearUsedByThread();
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
                ((AbstractCloseable) tailer).clearUsedByThread();
                assertEquals("Testing2", readMethodCall(tailer));
            }
        }.run();
    }

    private void writeMethodCall(SingleChronicleQueue queue, String message) {
        try (final ExcerptAppender appender = queue.acquireAppender()) {
            final Foobar foobar = appender.methodWriter(Foobar.class);
            foobar.say(message);
        }
    }

    private String readMethodCall(ExcerptTailer tailer) {
        AtomicReference<String> messageHolder = new AtomicReference<>();
        final MethodReader methodReader = tailer.methodReader((Foobar) messageHolder::set);
        methodReader.readOne();
        return messageHolder.get();
    }

    interface Foobar {
        void say(String message);
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