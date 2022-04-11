package net.openhft.chronicle.queue.stream;

import net.openhft.chronicle.queue.AppenderListener.Accumulation.Builder.ExcerptExtractor;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class Streams {

    // Suppresses default constructor, ensuring non-instantiability.
    public Streams() {
    }

    @NotNull
    public static <T> Stream<T> stream(@NotNull final ExcerptTailer tailer,
                                       @NotNull final ExcerptExtractor<T> extractor) {
        requireNonNull(tailer);
        requireNonNull(extractor);
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator(tailer, extractor),
                        Spliterator.ORDERED | Spliterator.NONNULL),
                false);
    }

    @NotNull
    public static <T> Iterator<T> iterator(@NotNull final ExcerptTailer tailer,
                                           @NotNull final ExcerptExtractor<T> extractor) {
        requireNonNull(tailer);
        requireNonNull(extractor);
        return new ExcerptTailerIterator<>(tailer, extractor);
    }

    private static final class ExcerptTailerIterator<T> implements Iterator<T> {

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
                            break;
                        }
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
            return next;
        }

    }
}