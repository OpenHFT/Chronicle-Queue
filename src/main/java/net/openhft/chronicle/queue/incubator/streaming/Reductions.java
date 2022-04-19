package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.queue.internal.streaming.ReductionUtil;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.*;
import java.util.stream.Collector;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class Reductions {

    // Suppresses default constructor, ensuring non-instantiability.
    private Reductions() {
    }

    public static <E, A, R>
    Reduction<R> of(@NotNull final ExcerptExtractor<E> extractor,
                    @NotNull final Collector<E, A, ? extends R> collector) {
        requireNonNull(extractor);
        requireNonNull(collector);
        return new ReductionUtil.CollectorAccumulation<>(extractor, collector);
    }

    public static <A>
    Reduction<LongSupplier> ofLong(@NotNull final ToLongExcerptExtractor extractor,
                                   @NotNull final Supplier<A> supplier,
                                   @NotNull final ObjLongConsumer<A> accumulator,
                                   @NotNull final ToLongFunction<A> finisher) {
        requireNonNull(extractor);
        requireNonNull(supplier);
        requireNonNull(accumulator);
        requireNonNull(finisher);
        return new ReductionUtil.LongSupplierAccumulation<>(extractor, supplier, accumulator, finisher);
    }

    // Specialized Long Accumulations

    public static Reduction<LongSupplier> reducingLong(@NotNull final ToLongExcerptExtractor longExtractor,
                                                       final long identity,
                                                       @NotNull final LongBinaryOperator accumulator) {
        requireNonNull(longExtractor);
        requireNonNull(accumulator);

        return Reductions.ofLong(
                longExtractor,
                () -> new LongAccumulator(accumulator, identity),
                LongAccumulator::accumulate,
                LongAccumulator::get);
    }

    public static Reduction<LongSupplier> counting() {
        return Reductions.ofLong(
                (wire, index) -> 1L,
                LongAdder::new,
                LongAdder::add,
                LongAdder::sum
        );
    }


    /**
     * A Reduction class that counts the number of excerpts that have been processed.
     */
    public static final class Counting extends SelfDescribingMarshallable implements Reduction<LongSupplier> {

        private static final AtomicLongFieldUpdater<Counting> UPDATER =
                AtomicLongFieldUpdater.newUpdater(Counting.class, "counter");

        private volatile long counter;

        @Override
        public void onExcerpt(@NotNull Wire wire, long index) {
            UPDATER.getAndIncrement(this);
        }

        @NotNull
        @Override
        public LongSupplier reduction() {
            return () -> counter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Counting that = (Counting) o;
            return this.counter == that.counter;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(counter);
        }
    }

}