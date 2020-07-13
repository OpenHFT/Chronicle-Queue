package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.service.HelloWorld;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class StoreTailerTest extends ChronicleQueueTestBase {
    private final Collection<ChronicleQueue> createdQueues = new ArrayList<>();
    private final Path dataDirectory = DirectoryUtils.tempDir(StoreTailerTest.class.getSimpleName()).toPath();

    private static void closeQueues(final ChronicleQueue... queues) {
        for (ChronicleQueue queue : queues) {
            if (queue != null) {
                queue.close();
            }
        }
    }

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
            //append.writeDocument(w -> w.write(() -> "test").text("before text"));

            ExcerptTailer tailer = chronicle.createTailer();
            //tailer.toEnd();

            timeProvider.addTime(10, TimeUnit.MINUTES);

            ExcerptAppender append = chronicle2.acquireAppender();
            append.writeDocument(w -> w.write(() -> "test").text("text"));

            assertTrue(tailer.readDocument(w -> w.read(() -> "test").text("text", Assert::assertEquals)));
        }
    }

    private SingleChronicleQueueBuilder minutely(@NotNull File file, TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.builder(file, WireType.BINARY).rollCycle(RollCycles.MINUTELY).testBlockSize().timeProvider(timeProvider);
    }

    @After
    public void after() {
        closeQueues(createdQueues.toArray(new ChronicleQueue[0]));
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
        final ChronicleQueue queue = builder.build();
        createdQueues.add(queue);
        return queue;
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
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(dataDirectory).build()) {
            BlockingQueue<ExcerptTailer> tq = new LinkedBlockingQueue<>();
            Thread t = new Thread(() -> {
                ExcerptTailer tailer = queue.createTailer();
                tailer.readText();
                tq.offer(tailer);
                Jvm.pause(1000);
            });
            t.start();
            ExcerptTailer tailer = tq.take();
            try {
                tailer.readText();
                fail();
            } catch (IllegalStateException expected) {
                //
            }
            tailer.disableThreadSafetyCheck(true).readText();

            tailer.close();
            t.interrupt();
            t.join(1000);
        }
    }
}