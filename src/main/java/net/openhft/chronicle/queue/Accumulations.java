package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.AppenderListener.Accumulation;
import net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.Accumulator;
import net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.Extractor;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.LongBinaryOperator;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;
import static net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.Accumulator.longViewer;

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

    public static Accumulation<LongSupplier> reducingLong(@NotNull final ExcerptToLong longExtractor,
                                                          final long identity,
                                                          @NotNull final LongBinaryOperator accumulator) {
        requireNonNull(longExtractor);
        requireNonNull(accumulator);

        return Accumulation.builder(() -> new LongAccumulator(accumulator, identity))
                .withAccumulator((accumulation, wire, index) -> accumulation.accumulate(longExtractor.apply(wire, index)))
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

    public static <E> Accumulation<Set<E>> toSet(@NotNull Extractor<? extends E> extractor) {
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

    public static <E> Accumulation<List<E>> toList(@NotNull Extractor<? extends E> extractor) {
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

/*
    private static <K, V> Builder<Map<K, V>, Map<K, V>> mapBuilder(@NotNull final Accumulator<? super Map<K, V>> accumulator) {
        return Accumulation.<Map<K, V>>builder(ConcurrentHashMap::new)
                .withAccumulator(accumulator)
                .addViewer(Collections::unmodifiableMap);
    }*/


    public static ExcerptToLong extractingIndex() {
        return (wire, index) -> index;
    }

    @FunctionalInterface
    public interface ExcerptToLong {

        long apply(@NotNull Wire wire, @NonNegative long index);
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