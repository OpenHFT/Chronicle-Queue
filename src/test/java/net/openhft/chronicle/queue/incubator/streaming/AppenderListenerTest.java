package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.*;
import static net.openhft.chronicle.queue.incubator.streaming.ConcurrentCollectors.replacingMerger;
import static net.openhft.chronicle.queue.incubator.streaming.ConcurrentCollectors.toConcurrentList;
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
    public void aggregateMap() {
        String path = OS.getTarget() + "/appenderListenerAggregateMapTest";


        final Reduction<Map<String, String>> appenderListener = new Reduction<Map<String, String>>() {

            final ConcurrentMap<String, String> map = new ConcurrentHashMap<>();

            @Override
            public void onExcerpt(@NotNull Wire wire, long index) {
                map.merge(wire.readEvent(String.class), wire.getValueIn().text(), replacingMerger());
            }

            @Override
            public @NotNull Map<String, String> reduction() {
                return Collections.unmodifiableMap(map);
            }
        };

/*
        final Accumulation<Map<String, String>> appenderListener =
                // Here we use a special static method providing Map key and value types directly
                Accumulation.mapBuilder(ConcurrentHashMap::new, String.class, String.class)
                        .withAccumulator((accumulation, wire, index) -> {
                                    accumulation.merge(wire.readEvent(String.class),
                                            wire.getValueIn().text(),
                                            Accumulator.replacingMerger());
                                }
                        )
                        .addViewer(Collections::unmodifiableMap)
                        .build();*/

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

        final Map<String, String> expected = new HashMap<>();
        expected.put("hello", "Bye-now");

        assertEquals(expected, appenderListener.reduction());

        final Map<String, String> accumulation = appenderListener.reduction();
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

        Reduction<List<String>> appenderListener = Reductions.of(
                DocumentExtractor.builder(String.class).withMethod(HelloWorld.class, HelloWorld::hello).build(),
                collectingAndThen(toConcurrentList(), Collections::unmodifiableList)
        );

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

        assertEquals(Arrays.asList("G'Day", "Bye-now"), appenderListener.reduction());

        final List<String> accumulation = appenderListener.reduction();
        try {
            accumulation.clear();
            fail("Was not unmodifiable");
        } catch (UnsupportedOperationException u) {
            // ignore
        }
    }


}