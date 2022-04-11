package net.openhft.chronicle.queue.internal.streaming;

import net.openhft.chronicle.queue.incubator.streaming.Accumulation;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class StreamUtil {

    // Suppresses default constructor, ensuring non-instantiability.
    private StreamUtil() {
    }

    public static final class ExcerptTailerIterator<T> implements Iterator<T> {

        private final ExcerptTailer tailer;
        private final Accumulation.Builder.ExcerptExtractor<T> extractor;

        private T next;

        public ExcerptTailerIterator(@NotNull final ExcerptTailer tailer,
                                     @NotNull final Accumulation.Builder.ExcerptExtractor<T> extractor) {
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

}