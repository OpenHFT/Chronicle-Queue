package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.queue.internal.streaming.AccumulationUtil;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.*;
import java.util.stream.Collector;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;
import static net.openhft.chronicle.queue.incubator.streaming.CollectorUtil.throwingMerger;

public final class Accumulations {

    // Suppresses default constructor, ensuring non-instantiability.
    private Accumulations() {
    }

    public static <E, A, R>
    Accumulation<R> of(@NotNull final ExcerptExtractor<E> extractor,
                       @NotNull final Collector<E, A, ? extends R> collector) {
        requireNonNull(extractor);
        requireNonNull(collector);
        return new AccumulationUtil.CollectorAccumulation<>(extractor, collector);
    }

    public static <A>
    Accumulation<LongSupplier> ofLong(@NotNull final ToLongExcerptExtractor extractor,
                                      @NotNull final Supplier<A> supplier,
                                      @NotNull final ObjLongConsumer<A> accumulator,
                                      @NotNull final ToLongFunction<A> finisher) {
        requireNonNull(extractor);
        requireNonNull(supplier);
        requireNonNull(accumulator);
        requireNonNull(finisher);
        return new AccumulationUtil.LongSupplierAccumulation<>(extractor, supplier, accumulator, finisher);
    }

    // Specialized Object Accumulations

    public static <E> Accumulation<E> reducing(@NotNull final ExcerptExtractor<E> extractor,
                                               final E identity,
                                               @NotNull final BinaryOperator<E> accumulator) {
        requireNonNull(extractor);
        requireNonNull(accumulator);

        final Collector<E, AtomicReference<E>, E> collector = Collector.of(
                () -> new AtomicReference<>(identity),
                (AtomicReference<E> ar, E e) -> ar.accumulateAndGet(e, accumulator),
                throwingMerger(),
                AtomicReference::get,
                Collector.Characteristics.CONCURRENT
        );

        return Accumulations.of(extractor, collector);
    }

    public static <E> Accumulation<Optional<E>> reducing(@NotNull final ExcerptExtractor<E> extractor,
                                                         @NotNull final BinaryOperator<E> accumulator) {
        requireNonNull(extractor);
        requireNonNull(accumulator);

        final BinaryOperator<E> internalAccumulator = (a, b) -> {
            if (a == null) {
                return b;
            }
            return accumulator.apply(a, b);
        };

        final Collector<E, AtomicReference<E>, Optional<E>> collector = Collector.of(
                AtomicReference::new,
                (AtomicReference<E> ar, E e) -> ar.accumulateAndGet(e, internalAccumulator),
                throwingMerger(),
                (AtomicReference<E> a) -> Optional.of(a.get()),
                Collector.Characteristics.CONCURRENT
        );

        return Accumulations.of(extractor, collector);
    }


    // Specialized Long Accumulations


    public static Accumulation<LongSupplier> reducingLong(@NotNull final ToLongExcerptExtractor longExtractor,
                                                          final long identity,
                                                          @NotNull final LongBinaryOperator accumulator) {
        requireNonNull(longExtractor);
        requireNonNull(accumulator);

        return Accumulations.ofLong(
                longExtractor,
                () -> new LongAccumulator(accumulator, identity),
                LongAccumulator::accumulate,
                LongAccumulator::get);
    }

    public static Accumulation<LongSupplier> counting() {
        return Accumulations.ofLong(
                (wire, index) -> 1L,
                LongAdder::new,
                LongAdder::add,
                LongAdder::sum
        );
    }



/*

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
*/


}