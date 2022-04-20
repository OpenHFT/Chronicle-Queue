package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor;
import net.openhft.chronicle.queue.incubator.streaming.ToDoubleExcerptExtractor;
import net.openhft.chronicle.queue.incubator.streaming.ToLongExcerptExtractor;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class StreamsUtil {

    static {
        IncubatorWarning.warnOnce();
    }

    private static final int BATCH_UNIT_INCREASE = 1 << 10;
    private static final int MAX_BATCH_SIZE = 1 << 24;

    // Suppresses default constructor, ensuring non-instantiability.
    private StreamsUtil() {
    }

    public static final class VanillaSpliterator<T> implements Spliterator<T> {

        private final Iterator<T> iterator;
        private int batchSize = 2 * BATCH_UNIT_INCREASE;

        public VanillaSpliterator(@NotNull final Iterator<T> iterator) {
            requireNonNull(iterator);
            this.iterator = iterator;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            synchronized (iterator) {
                if (iterator.hasNext()) {
                    action.accept(iterator.next());
                    return true;
                }
                return false;
            }
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            // A bit problematic
            synchronized (iterator) {
                iterator.forEachRemaining(action);
            }
        }

        @Override
        public Spliterator<T> trySplit() {
            synchronized (iterator) {
                if (iterator.hasNext()) {
                    final int n = Math.min(MAX_BATCH_SIZE, batchSize);
                    @SuppressWarnings("unchecked") final T[] a = (T[]) new Object[n];
                    int j = 0;
                    do {
                        a[j] = iterator.next();
                    } while (++j < n && iterator.hasNext());
                    batchSize += BATCH_UNIT_INCREASE;
                    return Spliterators.spliterator(a, 0, j, characteristics());
                }
                return null;
            }
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED + NONNULL;
        }
    }

    public static final class VanillaSpliteratorOfLong
            extends AbstractPrimitiveSpliterator<Long, LongConsumer, Spliterator.OfLong, PrimitiveIterator.OfLong>
            implements Spliterator.OfLong {

        public VanillaSpliteratorOfLong(@NotNull final PrimitiveIterator.OfLong iterator) {
            super(iterator, (a, i) -> a.accept(i.nextLong()), PrimitiveIterator.OfLong::forEachRemaining);
        }

        @NotNull
        protected OfLong split(int n) {
            final long[] a = new long[n];
            int j = 0;
            do {
                a[j] = iterator.nextLong();
            } while (++j < n && iterator.hasNext());
            return Spliterators.spliterator(a, 0, j, characteristics());
        }
    }

    public static final class VanillaSpliteratorOfDouble
            extends AbstractPrimitiveSpliterator<Double, DoubleConsumer, Spliterator.OfDouble, PrimitiveIterator.OfDouble>
            implements Spliterator.OfDouble {

        public VanillaSpliteratorOfDouble(@NotNull final PrimitiveIterator.OfDouble iterator) {
            super(iterator, (a, i) -> a.accept(i.nextDouble()), PrimitiveIterator.OfDouble::forEachRemaining);
        }

        @NotNull
        protected OfDouble split(int n) {
            final double[] a = new double[n];
            int j = 0;
            do {
                a[j] = iterator.nextDouble();
            } while (++j < n && iterator.hasNext());
            return Spliterators.spliterator(a, 0, j, characteristics());
        }
    }

    static abstract class AbstractPrimitiveSpliterator<
            T,
            C,
            S extends Spliterator.OfPrimitive<T, C, S>,
            I extends PrimitiveIterator<T, C>
            >
            implements Spliterator.OfPrimitive<T, C, S> {

        protected final I iterator;
        protected final BiConsumer<C, I> advancer;
        protected final BiConsumer<I, C> forEachRemainer;
        private int batchSize = 2 * BATCH_UNIT_INCREASE;

        protected AbstractPrimitiveSpliterator(@NotNull final I iterator,
                                               @NotNull final BiConsumer<C, I> advancer,
                                               BiConsumer<I, C> forEachRemainer) {

            this.iterator = requireNonNull(iterator);
            this.advancer = requireNonNull(advancer);
            this.forEachRemainer = requireNonNull(forEachRemainer);
        }

        @Override
        public S trySplit() {
            synchronized (iterator) {
                if (iterator.hasNext()) {
                    final int n = Math.min(MAX_BATCH_SIZE, batchSize);
                    batchSize += BATCH_UNIT_INCREASE;
                    return split(n);
                }
                return null;
            }
        }

        @NotNull
        abstract S split(int n);

        @Override
        public boolean tryAdvance(C action) {
            synchronized (iterator) {
                if (iterator.hasNext()) {
                    advancer.accept(action, iterator);
                    return true;
                }
                return false;
            }
        }

        @Override
        public void forEachRemaining(C action) {
            // A bit problematic
            synchronized (iterator) {
                forEachRemainer.accept(iterator, action);
            }
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED;
        }
    }

    public static final class ExcerptTailerIterator<T> implements Iterator<T> {

        private final ExcerptTailer tailer;
        private final ExcerptExtractor<T> extractor;

        private T next;

        public ExcerptTailerIterator(@NotNull final ExcerptTailer tailer,
                                     @NotNull final ExcerptExtractor<T> extractor) {
            this.tailer = tailer;
            this.extractor = extractor;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            long lastIndex = -1;
            for (; ; ) {
                try (final DocumentContext dc = tailer.readingDocument()) {
                    final Wire wire = dc.wire();
                    if (dc.isPresent() && wire != null) {
                        lastIndex = dc.index();
                        next = extractor.extract(wire, lastIndex);
                        if (next != null) {
                            return true;
                        }
                        // Retry reading yet another message
                    } else {
                        // We made no progress so we are at the end
                        break;
                    }
                }
            }
            return false;
        }

        @Override
        public T next() {
            if (next == null && !hasNext()) {
                throw new NoSuchElementException();
            }
            final T val = next;
            next = null;
            return val;
        }

    }

    public static final class ExcerptTailerIteratorOfLong implements PrimitiveIterator.OfLong {

        private final ExcerptTailer tailer;
        private final ToLongExcerptExtractor extractor;

        private long next = Long.MIN_VALUE;

        public ExcerptTailerIteratorOfLong(@NotNull final ExcerptTailer tailer,
                                           @NotNull final ToLongExcerptExtractor extractor) {
            this.tailer = tailer;
            this.extractor = extractor;
        }

        @Override
        public boolean hasNext() {
            if (next != Long.MIN_VALUE) {
                return true;
            }
            long lastIndex = -1;
            for (; ; ) {
                try (final DocumentContext dc = tailer.readingDocument()) {
                    final Wire wire = dc.wire();
                    if (dc.isPresent() && wire != null) {
                        lastIndex = dc.index();
                        next = extractor.extractAsLong(wire, lastIndex);
                        if (next != Long.MIN_VALUE) {
                            return true;
                        }
                        // Retry reading yet another message
                    } else {
                        // We made no progress so we are at the end
                        break;
                    }
                }
            }
            return false;
        }

        @Override
        public long nextLong() {
            if (next == Long.MIN_VALUE && !hasNext()) {
                throw new NoSuchElementException();
            }
            final long val = next;
            next = Long.MIN_VALUE;
            return val;
        }

    }

    public static final class ExcerptTailerIteratorOfDouble implements PrimitiveIterator.OfDouble {

        private final ExcerptTailer tailer;
        private final ToDoubleExcerptExtractor extractor;

        private double next = Double.NaN;

        public ExcerptTailerIteratorOfDouble(@NotNull final ExcerptTailer tailer,
                                             @NotNull final ToDoubleExcerptExtractor extractor) {
            this.tailer = tailer;
            this.extractor = extractor;
        }

        @Override
        public boolean hasNext() {
            if (Double.isNaN(next)) {
                return true;
            }
            long lastIndex = -1;
            for (; ; ) {
                try (final DocumentContext dc = tailer.readingDocument()) {
                    final Wire wire = dc.wire();
                    if (dc.isPresent() && wire != null) {
                        lastIndex = dc.index();
                        next = extractor.extractAsDouble(wire, lastIndex);
                        if (!Double.isNaN(next)) {
                            return true;
                        }
                        // Retry reading yet another message
                    } else {
                        // We made no progress so we are at the end
                        break;
                    }
                }
            }
            return false;
        }

        @Override
        public double nextDouble() {
            if (Double.isNaN(next) && !hasNext()) {
                throw new NoSuchElementException();
            }
            final double val = next;
            next = Double.NaN;
            return val;
        }

    }

}