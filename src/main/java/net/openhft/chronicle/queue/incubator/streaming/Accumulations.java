package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.incubator.streaming.Accumulation.Builder.Accumulator;
import net.openhft.chronicle.queue.internal.streaming.AccumulationUtil;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.LongBinaryOperator;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;
import static net.openhft.chronicle.queue.incubator.streaming.Accumulation.Builder.Accumulator.longViewer;

public final class Accumulations {

    // Suppresses default constructor, ensuring non-instantiability.
    private Accumulations() {
    }

    public static Accumulation<LongSupplier> counting() {
        return Accumulation.builder(AtomicLong::new)
                .withAccumulator((accumulation, wire, index) -> accumulation.getAndIncrement())
                .addViewer(longViewer(AtomicLong::get))
                .build();
    }

    public static Accumulation<LongSupplier> reducingLong(@NotNull final ToLongExcerptExtractor longExtractor,
                                                          final long identity,
                                                          @NotNull final LongBinaryOperator accumulator) {
        requireNonNull(longExtractor);
        requireNonNull(accumulator);

        return Accumulation.builder(() -> new LongAccumulator(accumulator, identity))
                .withAccumulator((accumulation, wire, index) -> {
                    final long value = longExtractor.extractAsLong(wire, index);
                    if (value != Long.MIN_VALUE) {
                        accumulation.accumulate(longExtractor.extractAsLong(wire, index));
                    }
                })
                .addViewer(longViewer(LongAccumulator::get))
                .build();
    }

    public static <K, V> Accumulation<Map<K, V>> toMap(@NotNull final Accumulator<? super Map<K, V>> accumulator) {
        requireNonNull(accumulator);
        return Accumulation.<Map<K, V>>builder(ConcurrentHashMap::new)
                .withAccumulator(accumulator)
                .addViewer(Collections::unmodifiableMap)
                .build();
    }

    public static <E> Accumulation<Set<E>> toSet(@NotNull ExcerptExtractor<? extends E> extractor) {
        return Accumulation.<Set<E>>builder(() -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                .withAccumulator((accumulation, wire, index) -> {
                    final E value = extractor.extract(wire, index);
                    if (value != null) {
                        accumulation.add(value);
                    }
                })
                .addViewer(Collections::unmodifiableSet)
                .build();
    }

    public static <E> Accumulation<List<E>> toList(@NotNull ExcerptExtractor<? extends E> extractor) {
        return Accumulation.<List<E>>builder(() -> Collections.synchronizedList(new ArrayList<>()))
                .withAccumulator((accumulation, wire, index) -> {
                    final E value = extractor.extract(wire, index);
                    if (value != null) {
                        accumulation.add(value);
                    }
                })
                .addViewer(Collections::unmodifiableList)
                .build();
    }

    public static <E, A, R> Accumulation<R> of(@NotNull final ExcerptExtractor<E> extractor,
                                               @NotNull final Collector<E, A, ? extends R> collector) {
        requireNonNull(extractor);
        requireNonNull(collector);
        return new CollectorAccumulation<>(extractor, collector);
    }

    private static final class CollectorAccumulation<E, A, R> implements Accumulation<R> {
        private final ExcerptExtractor<E> extractor;
        private final Collector<E, A, ? extends R> collector;

        private final A accumulation;

        public CollectorAccumulation(@NotNull final ExcerptExtractor<E> extractor,
                                     @NotNull final Collector<E, A, ? extends R> collector) {
            this.extractor = extractor;
            this.collector = collector;

            if (!collector.characteristics().contains(Collector.Characteristics.CONCURRENT)) {
                Jvm.warn().on(CollectorAccumulation.class, "The collector " + collector + " should generally have the characteristics CONCURRENT");
            }
            this.accumulation = collector.supplier().get();
        }

        @Override
        public void onExcerpt(@NotNull Wire wire, long index) {
            final E element = extractor.extract(wire, index);
            if (element != null) {
                collector.accumulator()
                        .accept(accumulation, element);
            }
        }

        @SuppressWarnings("unchecked")
        @NotNull
        @Override
        public R accumulation() {
            if (collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
                return (R) accumulation;
            }
            return collector.finisher().apply(accumulation);
        }

        @Override
        public long accept(@NotNull final ExcerptTailer tailer) {
            Objects.requireNonNull(tailer);
            return AccumulationUtil.accept(this, tailer);
        }
    }

    public static final class LongViewer<T> implements LongSupplier {

        private final T delegate;
        private final ToLongFunction<T> extractor;

        public LongViewer(@NotNull final T delegate,
                          @NotNull final ToLongFunction<T> extractor) {
            this.delegate = delegate;
            this.extractor = extractor;
        }

        @Override
        public long getAsLong() {
            return extractor.applyAsLong(delegate);
        }
    }

}