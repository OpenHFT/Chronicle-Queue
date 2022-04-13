package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.incubator.streaming.Accumulation.Builder.Accumulator;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AppenderListenerTest {

    @Test
    public void appenderListenerTest() {
        String path = OS.getTarget() + "/appenderListenerTest";
        StringBuilder results = new StringBuilder();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .appenderListener((wire, index) -> {
                    long offset = ((index >>> 32) << 40) | wire.bytes().readPosition();
                    String event = wire.readEvent(String.class);
                    String text = wire.getValueIn().text();
                    results.append(event)
                            .append(" ").append(text)
                            .append(", addr:").append(Long.toHexString(offset))
                            .append(", index: ").append(Long.toHexString(index)).append("\n");
                })
                .timeProvider(new SetTimeProvider("2021/11/29T13:53:59").advanceMillis(1000))
                .build();
             ExcerptAppender appender = q.acquireAppender()) {
            final HelloWorld writer = appender.methodWriter(HelloWorld.class);
            writer.hello("G'Day");
            writer.hello("Bye-now");
        }
        IOTools.deleteDirWithFiles(path);
        assertEquals("" +
                "hello G'Day, addr:4a100000010114, index: 4a1000000000\n" +
                "hello Bye-now, addr:4a100000010128, index: 4a1000000001\n", results.toString());
    }

    public interface HelloWorld {
        void hello(String s);
    }

    private static final class MyAtomicLongView {
        private final AtomicLong delegate;

        public MyAtomicLongView(AtomicLong delegate) {
            this.delegate = delegate;
        }

        long value() {
            return delegate.get();
        }

    }

    @Test
    public void aggregate() {
        String path = OS.getTarget() + "/appenderListenerAggregateTest";

        // This accumulator shows the last used index
        final Accumulation<MyAtomicLongView> appenderListener =
                Accumulation.builder(AtomicLong::new)
                        .withAccumulator(((a, wire, index) -> a.set(index)))
                        .addViewer(MyAtomicLongView::new)
                        .build();

        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .appenderListener(appenderListener::onExcerpt)
                .timeProvider(new SetTimeProvider("2021/11/29T13:53:59").advanceMillis(1000))
                .build();
             ExcerptAppender appender = q.acquireAppender()) {
            final HelloWorld writer = appender.methodWriter(HelloWorld.class);
            writer.hello("G'Day");
            writer.hello("Bye-now");
        }
        IOTools.deleteDirWithFiles(path);

        assertEquals(0x4A1000000001L, appenderListener.accumulation().value());
    }

    @Test
    public void aggregateMap() {
        String path = OS.getTarget() + "/appenderListenerAggregateMapTest";

        final Accumulation<Map<String, String>> appenderListener =
                // Here we use a special static method providing Map key and value types directly
                Accumulation.builder(ConcurrentHashMap::new, String.class, String.class)
                        .withAccumulator((accumulation, wire, index) -> {
                                    accumulation.merge(wire.readEvent(String.class),
                                            wire.getValueIn().text(),
                                            Accumulator.replacingMerger());
                                }
                        )
                        .addViewer(Collections::unmodifiableMap)
                        .build();

        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .appenderListener(appenderListener::onExcerpt)
                .timeProvider(new SetTimeProvider("2021/11/29T13:53:59").advanceMillis(1000))
                .build();
             ExcerptAppender appender = q.acquireAppender()) {
            final HelloWorld writer = appender.methodWriter(HelloWorld.class);
            writer.hello("G'Day");
            writer.hello("Bye-now");
        }
        IOTools.deleteDirWithFiles(path);

        final Map<String, String> expected = new HashMap<>();
        expected.put("hello", "Bye-now");

        assertEquals(expected, appenderListener.accumulation());

        final Map<String, String> accumulation = appenderListener.accumulation();
        try {
            accumulation.clear();
            fail("Was not unmodifiable");
        } catch (UnsupportedOperationException u) {
            // ignore
        }
    }

    @Test
    public void aggregateListCustom() {
        String path = OS.getTarget() + "/appenderListenerAggregateCollectionTest";

        final Accumulation<List<String>> appenderListener =
                Accumulation.builder(() -> Collections.synchronizedList(new ArrayList<>()), String.class)
                        .withAccumulator(Accumulator.reducing(ExcerptExtractor.builder(String.class).withMethod(HelloWorld.class, HelloWorld::hello).build(), List::add))
                        .addViewer(Collections::unmodifiableList)
                        .build();

        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .appenderListener(appenderListener)
                .timeProvider(new SetTimeProvider("2021/11/29T13:53:59").advanceMillis(1000))
                .build();
             ExcerptAppender appender = q.acquireAppender()) {
            final HelloWorld writer = appender.methodWriter(HelloWorld.class);
            writer.hello("G'Day");
            writer.hello("Bye-now");
        }
        IOTools.deleteDirWithFiles(path);

        assertEquals(Arrays.asList("G'Day", "Bye-now"), appenderListener.accumulation());

        final List<String> accumulation = appenderListener.accumulation();
        try {
            accumulation.clear();
            fail("Was not unmodifiable");
        } catch (UnsupportedOperationException u) {
            // ignore
        }
    }


    private static final class MyAtomicLongView2 {
        MyAtomicLongView delegate;

        public MyAtomicLongView2(MyAtomicLongView delegate) {
            this.delegate = delegate;
        }

        long valuePlusTen() {
            return delegate.value() + 10L;
        }

    }

    @Test
    public void aggregateDualView() {
        String path = OS.getTarget() + "/appenderListenerAggregateTest";


        final Accumulation<MyAtomicLongView2> appenderListener =
                Accumulation.builder(AtomicLong::new)
                        .withAccumulator(((a, wire, index) -> a.set(index)))
                        // Apply a series of views
                        .addViewer(MyAtomicLongView::new)
                        .addViewer(MyAtomicLongView2::new)
                        .build();

        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .appenderListener(appenderListener::onExcerpt)
                .timeProvider(new SetTimeProvider("2021/11/29T13:53:59").advanceMillis(1000))
                .build();
             ExcerptAppender appender = q.acquireAppender()) {
            final HelloWorld writer = appender.methodWriter(HelloWorld.class);
            writer.hello("G'Day");
            writer.hello("Bye-now");
        }
        IOTools.deleteDirWithFiles(path);

        assertEquals(0x4A1000000001L + 10, appenderListener.accumulation().valuePlusTen());
    }


    @Test
    public void aggregateMapping() {
        String path = OS.getTarget() + "/appenderListenerMappingTest";

        final Accumulation<Accumulation.MapperTo<Long>> appenderListener =
                Accumulation.builder(AtomicLong::new)
                        .withAccumulator(((a, wire, index) -> a.set(index)))
                        // Only exposes the value of the underlying accumulation
                        .withMapper(AtomicLong::get)
                        .build();

        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .appenderListener(appenderListener::onExcerpt)
                .timeProvider(new SetTimeProvider("2021/11/29T13:53:59").advanceMillis(1000))
                .build();
             ExcerptAppender appender = q.acquireAppender()) {
            final HelloWorld writer = appender.methodWriter(HelloWorld.class);
            writer.hello("G'Day");
            writer.hello("Bye-now");
        }
        IOTools.deleteDirWithFiles(path);

        assertEquals(0x4A1000000001L, (long) appenderListener.accumulation().map());
    }

}