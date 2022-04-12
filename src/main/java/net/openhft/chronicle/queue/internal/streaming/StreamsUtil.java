package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.incubator.streaming.ExcerptExtractor;
import net.openhft.chronicle.queue.incubator.streaming.ToLongExcerptExtractor;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.Consumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class StreamsUtil {

    private static final int MAX_SPLIT = 1;  // For some strange reason, different threads are used on the same Spliterator
    private static final int MAX_BATCH_SIZE = 2048;

    // Suppresses default constructor, ensuring non-instantiability.
    private StreamsUtil() {
    }

    public static final class VanillaSpliterator<T> implements Spliterator<T> {

        private final Iterator<T> iterator;
        private int splits;

        public VanillaSpliterator(@NotNull final Iterator<T> iterator) {
            requireNonNull(iterator);
            this.iterator = iterator;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (iterator.hasNext()) {
                action.accept(iterator.next());
                return true;
            }
            return false;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            iterator.forEachRemaining(action);
        }

        @Override
        public Spliterator<T> trySplit() {
            if (splits++ < MAX_SPLIT) {
                // For some reason, this spliterator gets invoked by different threads.
                if (iterator.hasNext()) {
                    final int n = MAX_BATCH_SIZE;
                    @SuppressWarnings("unchecked") final T[] a = (T[]) new Object[n];
                    int j = 0;
                    do {
                        a[j] = iterator.next();
                    } while (++j < n && iterator.hasNext());
                    return new ArraySpliterator<>(a, 0, j, characteristics());
                }
            }
            return null;
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

    static final class ArraySpliterator<T> implements Spliterator<T> {
        private final T[] array;
        private int index;
        private final int fence;  // last pos + 1
        private final int characteristics;

        public ArraySpliterator(@NotNull final T[] array,
                                @NonNegative final int origin,
                                @NonNegative final int fence,
                                @NonNegative final int additionalCharacteristics) {
            this.array = array;
            this.index = origin;
            this.fence = fence;
            this.characteristics = additionalCharacteristics | Spliterator.SIZED | Spliterator.SUBSIZED;
        }

        @Override
        public Spliterator<T> trySplit() {
            final int lo = index;
            final int mid = (lo + fence) >>> 1;
            return (lo >= mid)
                    ? null
                    : new ArraySpliterator<>(array, lo, index = mid, characteristics);
        }

        @Override
        public void forEachRemaining(@NotNull Consumer<? super T> action) {
            requireNonNull(action);
            for (int i = index; i < fence; i++) {
                action.accept(array[i]);
            }
        }

        @Override
        public boolean tryAdvance(@NotNull Consumer<? super T> action) {
            requireNonNull(action);
            if (index >= 0 && index < fence) {
                final T t = array[index++];
                action.accept(t);
                return true;
            }
            return false;
        }

        @Override
        public long estimateSize() {
            return (fence - index);
        }

        @Override
        public int characteristics() {
            return characteristics;
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

}