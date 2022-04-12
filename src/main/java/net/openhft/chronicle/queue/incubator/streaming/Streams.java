package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.internal.streaming.StreamsUtil;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

/**
 * Factories to create standard {@link java.util.stream.Stream} and related objects from Chronicle Queues.
 * <p>
 * If the provided {@code extractor } is reusing objects, then care must be taken in the
 * returned objects (e.g. Streams, Spliterators and Iterators) to account for this.
 * <p>
 * Generally, these objects will create underlying objects and should not be used in JVMs running
 * deterministic low-latency code. Instead, they are suitable for convenient off-line analysis of queue content.
 */
public final class Streams {

    // Suppresses default constructor, ensuring non-instantiability.
    public Streams() {
    }

    /**
     * Creates and returns a new sequential ordered {@link Stream} whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code tailer}.
     * <p>
     * The Stream does not contain any {@code null} elements.
     *
     * @param <T>       the type of stream elements
     * @param tailer    from which excerpts are obtained
     * @param extractor used to extract elements of type T from excerpts
     * @return the new stream
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static <T> Stream<T> of(@NotNull final ExcerptTailer tailer,
                                   @NotNull final ExcerptExtractor<T> extractor) {
        requireNonNull(tailer);
        requireNonNull(extractor);
        return StreamSupport.stream(spliterator(tailer, extractor), false);
    }

    /**
     * Creates and returns a new sequential ordered {@link LongStream} whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code tailer}.
     * <p>
     * The Stream does not contain any {@link Long#MIN_VALUE} elements.
     *
     * @param tailer    from which excerpts are obtained
     * @param extractor used to extract elements of type T from excerpts
     * @return the new stream
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static LongStream ofLong(@NotNull final ExcerptTailer tailer,
                                    @NotNull final ToLongExcerptExtractor extractor) {
        requireNonNull(tailer);
        requireNonNull(extractor);
        return StreamSupport.longStream(spliteratorOfLong(tailer, extractor), false);
    }

    /**
     * Creates and returns a new ordered {@link Spliterator } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code tailer}, with no initial size estimate.
     * <p>
     * The Spliterator does not contain any {@code null} elements.
     * <p>
     * The Spliterator implements {@code trySplit} to permit limited parallelism.
     *
     * @param <T>       the type of stream elements
     * @param tailer    from which excerpts are obtained
     * @param extractor used to extract elements of type T from excerpts
     * @return the new Spliterator
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static <T> Spliterator<T> spliterator(@NotNull final ExcerptTailer tailer,
                                                 @NotNull final ExcerptExtractor<T> extractor) {
        requireNonNull(tailer);
        requireNonNull(extractor);
        return new StreamsUtil.VanillaSpliterator<>(iterator(tailer, extractor));
    }

    /**
     * Creates and returns a new ordered {@link Spliterator.OfLong } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code tailer}, with no initial size estimate.
     * <p>
     * The Spliterator does not contain any {@link Long#MIN_VALUE} elements.
     * <p>
     * The Spliterator implements {@code trySplit} to permit limited parallelism.
     *
     * @param tailer    from which excerpts are obtained
     * @param extractor used to extract elements of type T from excerpts
     * @return the new Spliterator.OfLong
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static Spliterator.OfLong spliteratorOfLong(@NotNull final ExcerptTailer tailer,
                                                       @NotNull final ToLongExcerptExtractor extractor) {
        requireNonNull(tailer);
        requireNonNull(extractor);
        return Spliterators.spliteratorUnknownSize(iteratorOfLong(tailer, extractor), Spliterator.ORDERED);
    }

    /**
     * Creates and returns a new {@link Iterator } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code tailer}.
     * <p>
     * The Iterator does not contain any {@code null} elements.
     *
     * @param <T>       the type of stream elements
     * @param tailer    from which excerpts are obtained
     * @param extractor used to extract elements of type T from excerpts
     * @return the new Iterator
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static <T> Iterator<T> iterator(@NotNull final ExcerptTailer tailer,
                                           @NotNull final ExcerptExtractor<T> extractor) {
        requireNonNull(tailer);
        requireNonNull(extractor);
        return new StreamsUtil.ExcerptTailerIterator<>(tailer, extractor);
    }

    /**
     * Creates and returns a new {@link PrimitiveIterator.OfLong } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code tailer}.
     * <p>
     * The Iterator does not contain any {@link Long#MIN_VALUE} elements.
     *
     * @param tailer    from which excerpts are obtained
     * @param extractor used to extract elements of type T from excerpts
     * @return the new Iterator
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static PrimitiveIterator.OfLong iteratorOfLong(@NotNull final ExcerptTailer tailer,
                                                          @NotNull final ToLongExcerptExtractor extractor) {
        requireNonNull(tailer);
        requireNonNull(extractor);
        return new StreamsUtil.ExcerptTailerIteratorOfLong(tailer, extractor);
    }

}