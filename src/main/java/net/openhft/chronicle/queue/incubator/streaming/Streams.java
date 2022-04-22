package net.openhft.chronicle.queue.incubator.streaming;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.internal.streaming.StreamsUtil;
import net.openhft.chronicle.wire.MarshallableIn;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.stream.DoubleStream;
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
 * deterministic low-latency code. Instead, they are suitable for convenient <em>off-line analysis of queue content</em>.
 */
public final class Streams {

    // Suppresses default constructor, ensuring non-instantiability.
    private Streams() {
    }

    /**
     * Creates and returns a new sequential ordered {@link Stream} whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}.
     * <p>
     * The Stream does not contain any {@code null} elements.
     * <p>
     * Tailers are handled in a thread-safe way and if a parallel stream is to be used, thread
     * safety checks must be turned off using the {@link ExcerptTailer#disableThreadSafetyCheck(boolean)} method or
     * else an Exception might be thrown during Stream evaluation.
     *
     * @param <T>              the type of stream elements
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type T from excerpts
     * @return the new stream
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static <T> Stream<T> of(@NotNull final MarshallableIn documentProvider,
                                   @NotNull final DocumentExtractor<T> extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);
        return StreamSupport.stream(spliterator(documentProvider, extractor), false);
    }

    /**
     * Creates and returns a new sequential ordered {@link LongStream} whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}.
     * <p>
     * The Stream does not contain any {@link Long#MIN_VALUE} elements.
     * <p>
     * Tailers are handled in a thread-safe way and if a parallel stream is to be used, thread
     * safety checks must be turned off using the {@link ExcerptTailer#disableThreadSafetyCheck(boolean)} method or
     * else an Exception might be thrown during Stream evaluation.
     *
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type long from excerpts
     * @return the new stream
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static LongStream ofLong(@NotNull final MarshallableIn documentProvider,
                                    @NotNull final ToLongDocumentExtractor extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);
        return StreamSupport.longStream(spliteratorOfLong(documentProvider, extractor), false);
    }

    /**
     * Creates and returns a new sequential ordered {@link DoubleStream} whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}.
     * <p>
     * The Stream does not contain any {@link Double#NaN} elements.
     * <p>
     * Tailers are handled in a thread-safe way and if a parallel stream is to be used, thread
     * safety checks must be turned off using the {@link ExcerptTailer#disableThreadSafetyCheck(boolean)} method or
     * else an Exception might be thrown during Stream evaluation.
     *
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type double from excerpts
     * @return the new stream
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static DoubleStream ofDouble(@NotNull final MarshallableIn documentProvider,
                                        @NotNull final ToDoubleDocumentExtractor extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);
        return StreamSupport.doubleStream(spliteratorOfDouble(documentProvider, extractor), false);
    }

    /**
     * Creates and returns a new ordered {@link Spliterator } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}, with no initial size estimate.
     * <p>
     * The Spliterator does not contain any {@code null} elements.
     * <p>
     * The Spliterator implements {@code trySplit} to permit limited parallelism.
     * <p>
     * Tailers are handled in a thread-safe way and if a Spliterator splits are anticipated, thread
     * safety checks must be turned off using the {@link ExcerptTailer#disableThreadSafetyCheck(boolean)} method or
     * else an Exception might be thrown during splits.
     *
     * @param <T>              the type of stream elements
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type T from excerpts
     * @return the new Spliterator
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static <T> Spliterator<T> spliterator(@NotNull final MarshallableIn documentProvider,
                                                 @NotNull final DocumentExtractor<T> extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);
        return new StreamsUtil.VanillaSpliterator<>(iterator(documentProvider, extractor));
    }

    /**
     * Creates and returns a new ordered {@link Spliterator.OfLong } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}, with no initial size estimate.
     * <p>
     * The Spliterator does not contain any {@link Long#MIN_VALUE} elements.
     * <p>
     * The Spliterator implements {@code trySplit} to permit limited parallelism.
     * <p>
     * Tailers are handled in a thread-safe way and if a Spliterator splits are anticipated, thread
     * safety checks must be turned off using the {@link ExcerptTailer#disableThreadSafetyCheck(boolean)} method or
     * else an Exception might be thrown during splits.
     *
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type long from excerpts
     * @return the new Spliterator.OfLong
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static Spliterator.OfLong spliteratorOfLong(@NotNull final MarshallableIn documentProvider,
                                                       @NotNull final ToLongDocumentExtractor extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);
        return new StreamsUtil.VanillaSpliteratorOfLong(iteratorOfLong(documentProvider, extractor));
    }

    /**
     * Creates and returns a new ordered {@link Spliterator.OfDouble } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}, with no initial size estimate.
     * <p>
     * The Spliterator does not contain any {@link Double#NaN} elements.
     * <p>
     * The Spliterator implements {@code trySplit} to permit limited parallelism.
     * <p>
     * Tailers are handled in a thread-safe way and if a Spliterator splits are anticipated, thread
     * safety checks must be turned off using the {@link ExcerptTailer#disableThreadSafetyCheck(boolean)} method or
     * else an Exception might be thrown during splits.
     *
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type double from excerpts
     * @return the new Spliterator.OfDouble
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static Spliterator.OfDouble spliteratorOfDouble(@NotNull final MarshallableIn documentProvider,
                                                           @NotNull final ToDoubleDocumentExtractor extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);
        return new StreamsUtil.VanillaSpliteratorOfDouble(iteratorOfDouble(documentProvider, extractor));
    }

    /**
     * Creates and returns a new {@link Iterator } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}.
     * <p>
     * The Iterator does not contain any {@code null} elements.
     * <p>
     * The returned iterator is confined to a single thread.
     *
     * @param <T>              the type of stream elements
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type T from excerpts
     * @return the new Iterator
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static <T> Iterator<T> iterator(@NotNull final MarshallableIn documentProvider,
                                           @NotNull final DocumentExtractor<T> extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);
        return new StreamsUtil.ExcerptIterator<>(documentProvider, extractor);
    }

    /**
     * Creates and returns a new {@link PrimitiveIterator.OfLong } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}.
     * <p>
     * The Iterator does not contain any {@link Long#MIN_VALUE} elements.
     * <p>
     * The returned iterator is confined to a single thread.
     *
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type long from excerpts
     * @return the new iterator
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static PrimitiveIterator.OfLong iteratorOfLong(@NotNull final MarshallableIn documentProvider,
                                                          @NotNull final ToLongDocumentExtractor extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);
        return new StreamsUtil.ExcerptIteratorOfLong(documentProvider, extractor);
    }

    /**
     * Creates and returns a new {@link PrimitiveIterator.OfDouble } whose elements are obtained
     * by successively applying the provided {@code extractor} on excerpts from the
     * provided {@code documentProvider}.
     * <p>
     * The Iterator does not contain any {@link Double#NaN} elements.
     * <p>
     * The returned iterator is confined to a single thread.
     *
     * @param documentProvider from which excerpts are obtained
     * @param extractor        used to extract elements of type double from excerpts
     * @return the new iterator
     * @throws NullPointerException if any of the provided parameters are {@code null}
     */
    @NotNull
    public static PrimitiveIterator.OfDouble iteratorOfDouble(@NotNull final MarshallableIn documentProvider,
                                                              @NotNull final ToDoubleDocumentExtractor extractor) {
        requireNonNull(documentProvider);
        requireNonNull(extractor);

        return new StreamsUtil.ExcerptIteratorOfDouble(documentProvider, extractor);
    }

}