package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

class StreamsTest {

    static final String Q_NAME = StreamsTest.class.getSimpleName();

    @BeforeEach
    void beforeEach() {
        cleanup();
    }

    @AfterEach
    void afterEach() {
        cleanup();
    }

    @Test
    void streamTextMessage() {
        try (SingleChronicleQueue q = createAppending(a -> a.writeText("Hello"))) {
            ExcerptTailer tailer = q.createTailer();

            Stream<String> stream = Streams.of(tailer, (wire, index) -> wire.getValueIn().text());
            assertFalse(stream.isParallel());
            List<String> actualContent = stream.collect(toList());
            assertEquals(Collections.singletonList("Hello"), actualContent);
        }
    }

    @Test
    void iteratorEmpty() {
        try (SingleChronicleQueue q = createQueue()) {
            ExcerptTailer tailer = q.createTailer();
            Iterator<String> iterator = Streams.iterator(tailer, (wire, index) -> "A");
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void iteratorTextMessage() {
        try (SingleChronicleQueue q = createAppending(a -> a.writeText("Hello"))) {
            ExcerptTailer tailer = q.createTailer();
            Iterator<String> iterator = Streams.iterator(tailer, (wire, index) -> wire.getValueIn().text());
            assertTrue(iterator.hasNext());
            // Make sure another call does not change the state
            assertTrue(iterator.hasNext());
            assertEquals("Hello", iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void iteratorTailerClosed() {
        try (SingleChronicleQueue q = createAppending(a -> a.writeText("Hello"))) {
            ExcerptTailer tailer = q.createTailer();
            Iterator<String> iterator = Streams.iterator(tailer, (wire, index) -> wire.getValueIn().text());
            assertTrue(iterator.hasNext());
            tailer.close();
            // Make sure another call does not change the state
            assertTrue(iterator.hasNext());
            assertEquals("Hello", iterator.next());
            assertFalse(iterator.hasNext());
        }
    }


    private SingleChronicleQueue createAppending(@NotNull final Consumer<ExcerptAppender> mutator) {
        final Consumer<SingleChronicleQueue> queueMutator = q -> {
            ExcerptAppender excerptAppender = q.acquireAppender();
            mutator.accept(excerptAppender);
        };
        return createQueue(queueMutator);
    }

    private SingleChronicleQueue createQueue(@NotNull final Consumer<SingleChronicleQueue> queueMutator) {
        final SingleChronicleQueue q = createQueue();
        queueMutator.accept(q);
        return q;
    }

    @NotNull
    private static SingleChronicleQueue createQueue() {
        final SetTimeProvider tp = new SetTimeProvider(TimeUnit.HOURS.toNanos(365 * 24)).autoIncrement(1, TimeUnit.SECONDS);
        return SingleChronicleQueueBuilder
                .single(Q_NAME)
                .timeProvider(tp)
                .build();
    }

    void cleanup() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

}